package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** FIX-133 shared assertions for H2/JUnit and isolated MySQL verification.
 * Calls real CandleStore methods inside real transactions; no mocked publisher.
 * Fixture RESET IS DESTRUCTIVE: the standalone runner accepts local test schemas only. */
public final class Fix133Scenarios {
    static final List<String> CASES=List.of("restThenClose", "closeThenRestThenDuplicate",
        "formingRestClose", "staleAfterRest", "legacyContamination", "legacyEqualFence",
        "noReopening", "oldLaneHistorical", "rollback", "feedOff", "legacyConsumerState");
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final OwnershipGate gate=new OwnershipGate(true);
    private final CandleStore store;

    Fix133Scenarios(DataSource ds) throws Exception {
        jdbc=new JdbcTemplate(ds); tx=new TransactionTemplate(new DataSourceTransactionManager(ds));
        for(String table:List.of("market_data_stream_ws_version","market_data_stream_event",
                "market_data_stream_lane","market_data_stream_version","market_data_stream_cursor",
                "market_data_stream_owner","market_data_candle_event","candle")) jdbc.execute("DROP TABLE IF EXISTS "+table);
        jdbc.execute("""
            CREATE TABLE candle(id BIGINT AUTO_INCREMENT PRIMARY KEY,symbol VARCHAR(30),interval_code VARCHAR(10),
            open_time TIMESTAMP(6),close_time TIMESTAMP(6),open_price DECIMAL(30,12),high_price DECIMAL(30,12),
            low_price DECIMAL(30,12),close_price DECIMAL(30,12),volume DECIMAL(38,12),quote_asset_volume DECIMAL(38,12),
            number_of_trades BIGINT,taker_buy_base_volume DECIMAL(38,12),taker_buy_quote_volume DECIMAL(38,12),
            closed BOOLEAN,created_at TIMESTAMP(6),updated_at TIMESTAMP(6),UNIQUE(symbol,interval_code,open_time))
            """);
        jdbc.execute("""
            CREATE TABLE market_data_candle_event(symbol VARCHAR(30),interval_code VARCHAR(10),candle_open_time TIMESTAMP(6),
            candle_close_time TIMESTAMP(6),source VARCHAR(30),observed_at TIMESTAMP(6),created_at TIMESTAMP(6),
            claimed_by VARCHAR(160),processed_at TIMESTAMP(6),UNIQUE(symbol,interval_code,candle_open_time))
            """);
        for(String file:List.of("FIX-132-source-tables.sql","FIX-133-websocket-version.sql")) {
            String ddl=Files.readString(Path.of("md/sql",file)).replaceAll("(?m)^--.*$", "");
            for(String statement:ddl.split(";"))if(!statement.isBlank())jdbc.execute(statement);
        }
        var env=new StandardEnvironment();
        env.getPropertySources().addFirst(new MapPropertySource("test",Map.of("market-data.stream.enabled",true)));
        var publisher=new StreamPublisher(jdbc,env); set(publisher,"gate",gate);
        gate.acquired("test-owner");
        jdbc.update("INSERT INTO market_data_stream_owner VALUES(1,'test-owner',TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)))");
        store=new CandleStore(jdbc);set(store,"streamPublisher",publisher);
        publisher.validateSchema();
    }
    private static void set(Object target,String field,Object value)throws Exception {
        var f=target.getClass().getDeclaredField(field);f.setAccessible(true);f.set(target,value);
    }
    private void ws(long event,boolean closed,String price) throws Exception { ws(event,closed,price,1000); }
    private void ws(long event,boolean closed,String price,long open) throws Exception {
        var json=new ObjectMapper().readTree("{\"E\":"+event+",\"k\":{\"s\":\"BTCUSDT\",\"i\":\"1m\",\"t\":"+open+",\"T\":"+(open+59999)+",\"x\":"+closed+",\"c\":\""+price+"\"}}");
        try(var permit=gate.admit()){tx.executeWithoutResult(s->store.persistWebsocket(json));}
    }
    private void rest() {
        var p=BigDecimal.TEN;
        var k=new BinanceKline(Instant.ofEpochMilli(1000),p,p,p,p,p,Instant.ofEpochMilli(60999),p,1,p,p);
        try(var permit=gate.admit()){tx.executeWithoutResult(s->store.persistRest("BTCUSDT","1m",k,"STARTUP_RECOVERY",Instant.ofEpochMilli(62000)));}
    }
    private void classes(String... expected) {
        equal(List.of(expected),jdbc.queryForList("SELECT classification FROM market_data_stream_event ORDER BY symbol_sequence",String.class));
    }
    private static void equal(Object expected,Object actual) {
        if(!expected.equals(actual))throw new AssertionError("expected="+expected+", actual="+actual);
    }
    private long wsTime() {return jdbc.queryForObject("SELECT observed_at FROM market_data_stream_ws_version",Timestamp.class).toInstant().toEpochMilli();}
    void check(String scenario) throws Exception {
        switch(scenario) {
            case "restThenClose" -> {rest();ws(61000,true,"2");classes("HISTORICAL_ONLY","LIVE");equal(61000L,wsTime());}
            case "closeThenRestThenDuplicate" -> {
                ws(61000,true,"2");rest();ws(61000,true,"2");classes("LIVE","HISTORICAL_ONLY","DUPLICATE");equal(61000L,wsTime());
            }
            case "formingRestClose" -> {
                ws(3000,false,"1");rest();ws(61000,true,"2");classes("LIVE","HISTORICAL_ONLY","LIVE");equal(61000L,wsTime());
            }
            case "staleAfterRest" -> {
                ws(61000,true,"2");rest();ws(60000,true,"1");classes("LIVE","HISTORICAL_ONLY","OUT_OF_ORDER");equal(61000L,wsTime());
            }
            case "legacyContamination" -> {
                ws(3000,false,"1");rest();jdbc.update("DELETE FROM market_data_stream_ws_version");
                ws(61000,true,"2");classes("LIVE","HISTORICAL_ONLY","LIVE");equal(61000L,wsTime());
            }
            case "legacyEqualFence" -> {
                ws(61000,true,"2");rest();jdbc.update("DELETE FROM market_data_stream_ws_version");
                ws(61000,true,"2");classes("LIVE","HISTORICAL_ONLY","OUT_OF_ORDER");
                equal(0,jdbc.queryForObject("SELECT COUNT(*) FROM market_data_stream_ws_version",Integer.class));
            }
            case "noReopening" -> {
                rest();ws(63000,false,"1");classes("HISTORICAL_ONLY","OUT_OF_ORDER");equal(true,jdbc.queryForObject("SELECT closed FROM candle",Boolean.class));
            }
            case "oldLaneHistorical" -> {
                ws(100000,false,"3",61000);rest();ws(61000,true,"2");classes("LIVE","HISTORICAL_ONLY","HISTORICAL_ONLY");
                equal(100000L,jdbc.queryForObject("SELECT observed_at FROM market_data_stream_lane",Timestamp.class).toInstant().toEpochMilli());
            }
            case "rollback" -> {
                jdbc.execute("DROP TABLE market_data_stream_event");boolean failed=false;
                try{ws(61000,true,"2");}catch(org.springframework.dao.DataAccessException expected){failed=true;}
                equal(true,failed);
                for(String table:List.of("candle","market_data_stream_ws_version","market_data_stream_lane","market_data_stream_version","market_data_stream_cursor"))
                    equal(0,jdbc.queryForObject("SELECT COUNT(*) FROM "+table,Integer.class));
            }
            case "feedOff" -> {
                jdbc.execute("DROP TABLE market_data_stream_ws_version");
                var env=new StandardEnvironment();env.getPropertySources().addFirst(new MapPropertySource("off",Map.of("market-data.stream.enabled",false)));
                var off=new StreamPublisher(jdbc,env);off.validateSchema();set(store,"streamPublisher",off);
                rest();ws(61000,true,"2");equal(1,jdbc.queryForObject("SELECT COUNT(*) FROM candle",Integer.class));classes();
            }
            case "legacyConsumerState" -> {
                rest();jdbc.update("UPDATE market_data_candle_event SET claimed_by='other-consumer',processed_at=CURRENT_TIMESTAMP(6)");
                ws(61000,true,"2");classes("HISTORICAL_ONLY","LIVE");
                equal("other-consumer",jdbc.queryForObject("SELECT claimed_by FROM market_data_candle_event",String.class));
                equal(1,jdbc.queryForObject("SELECT COUNT(*) FROM market_data_candle_event WHERE processed_at IS NOT NULL",Integer.class));
            }
            default -> throw new IllegalArgumentException(scenario);
        }
    }
    public static void main(String[] args)throws Exception {
        String url=System.getenv("FIX133_TEST_MYSQL_URL");
        if(url==null || !url.matches("jdbc:mysql://(127\\.0\\.0\\.1|localhost):[0-9]+/fix133_test[a-zA-Z0-9_]*(\\?.*)?"))
            throw new IllegalArgumentException("Only a local fix133_test* schema is permitted; fixture drops its tables");
        var ds=new DriverManagerDataSource(url,System.getenv().getOrDefault("FIX133_TEST_MYSQL_USER","root"),System.getenv().getOrDefault("FIX133_TEST_MYSQL_PASSWORD",""));
        for(String scenario:CASES){new Fix133Scenarios(ds).check(scenario);System.out.println("PASS "+scenario);}
        System.out.println("FIX-133 MySQL scenarios passed: "+CASES.size());
    }
}
