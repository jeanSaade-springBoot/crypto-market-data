package com.crypto.marketdata;

import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.UUID;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class StreamPublisherTest {
    JdbcTemplate jdbc; TransactionTemplate tx; CandleStore store; StreamPublisher publisher;
    @BeforeEach void setup() throws Exception {
        var ds=new DriverManagerDataSource("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1","sa","");
        jdbc=new JdbcTemplate(ds);tx=new TransactionTemplate(new DataSourceTransactionManager(ds));
        jdbc.execute("""
            CREATE TABLE candle(id BIGINT AUTO_INCREMENT PRIMARY KEY,symbol VARCHAR(30),interval_code VARCHAR(10),open_time TIMESTAMP,
            close_time TIMESTAMP,open_price DECIMAL(30,12),high_price DECIMAL(30,12),low_price DECIMAL(30,12),close_price DECIMAL(30,12),
            volume DECIMAL(38,12),quote_asset_volume DECIMAL(38,12),number_of_trades BIGINT,taker_buy_base_volume DECIMAL(38,12),
            taker_buy_quote_volume DECIMAL(38,12),closed BOOLEAN,created_at TIMESTAMP,updated_at TIMESTAMP,UNIQUE(symbol,interval_code,open_time))
            """);
        jdbc.execute("CREATE TABLE market_data_candle_event(symbol VARCHAR(30),interval_code VARCHAR(10),candle_open_time TIMESTAMP,candle_close_time TIMESTAMP,source VARCHAR(30),observed_at TIMESTAMP,created_at TIMESTAMP,UNIQUE(symbol,interval_code,candle_open_time))");
        String ddl=Files.readString(Path.of("md/sql/FIX-132-source-tables.sql")).replaceAll("(?m)^--.*$","");
        for(String statement:ddl.split(";"))if(!statement.isBlank())jdbc.execute(statement);
        publisher=new StreamPublisher(jdbc,new MockEnvironment().withProperty("market-data.stream.enabled","true"));
        store=new CandleStore(jdbc);ReflectionTestUtils.setField(store,"streamPublisher",publisher);
    }
    void write(long observed,boolean closed,String price) throws Exception {
        var json=new ObjectMapper().readTree("{\"E\":"+observed+",\"k\":{\"s\":\"BTCUSDT\",\"i\":\"1m\",\"t\":1000,\"T\":60999,\"x\":"+closed+",\"c\":\""+price+"\"}}");
        tx.executeWithoutResult(status->store.persistWebsocket(json));
    }
    @Test void offRequiresNoFeedTables() throws Exception {
        for(String table:new String[]{"market_data_stream_event","market_data_stream_version","market_data_stream_cursor","market_data_stream_owner","market_data_stream_lane"})jdbc.execute("DROP TABLE "+table);
        ReflectionTestUtils.setField(store,"streamPublisher",new StreamPublisher(jdbc,new MockEnvironment()));
        write(2000,false,"1");assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM candle",Integer.class));
    }
    @Test void candleAndEventRollbackTogetherWhenPublicationFails() {
        jdbc.execute("DROP TABLE market_data_stream_event");
        assertThrows(Exception.class,()->write(2000,false,"1"));
        assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM candle",Integer.class));
        assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM market_data_stream_cursor",Integer.class));
    }
    @Test void olderFormingEventCannotReopenClosedCandle() throws Exception {
        write(61000,true,"2");write(3000,false,"1");
        assertTrue(jdbc.queryForObject("SELECT closed FROM candle",Boolean.class));
        assertEquals(0,jdbc.queryForObject("SELECT close_price FROM candle",java.math.BigDecimal.class).compareTo(new java.math.BigDecimal("2")));
        assertEquals("OUT_OF_ORDER",jdbc.queryForObject("SELECT classification FROM market_data_stream_event WHERE symbol_sequence=2",String.class));
        assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM market_data_candle_event",Integer.class));
    }
    @Test void duplicateIsEvidenceButNewTimestampAtSamePriceIsLive() throws Exception {
        write(2000,false,"1");write(2000,false,"1");write(3000,false,"1");
        assertEquals(java.util.List.of("LIVE","DUPLICATE","LIVE"),jdbc.queryForList("SELECT classification FROM market_data_stream_event ORDER BY symbol_sequence",String.class));
    }
    @Test void secondPublisherCannotWriteWhileOwnerLeaseIsValid() throws Exception {
        write(2000,false,"1");
        ReflectionTestUtils.setField(store,"streamPublisher",new StreamPublisher(jdbc,new MockEnvironment().withProperty("market-data.stream.enabled","true")));
        assertThrows(IllegalStateException.class,()->write(3000,false,"2"));
        assertEquals(1L,jdbc.queryForObject("SELECT last_sequence FROM market_data_stream_cursor",Long.class));
    }
    @Test void repairIsHistoricalEvenWithHigherSequence() throws Exception {
        write(2000,false,"1");
        tx.executeWithoutResult(status->{var e=publisher.prepare("BTCUSDT","1m",Instant.ofEpochMilli(1000),Instant.ofEpochMilli(60999),true,
            Instant.now(),java.math.BigDecimal.TEN,"REST_REPAIR","repair");publisher.publish(e);});
        assertEquals("HISTORICAL_ONLY",jdbc.queryForObject("SELECT classification FROM market_data_stream_event WHERE symbol_sequence=2",String.class));
    }

    @Test void delayedDifferentCandleIsStoredAsHistoryWithoutReorderingLiveLane() throws Exception {
        write(100000,false,"1");
        var older=new ObjectMapper().readTree("{\"E\":61000,\"k\":{\"s\":\"BTCUSDT\",\"i\":\"1m\",\"t\":0,\"T\":59999,\"x\":true,\"c\":\"2\"}}");
        tx.executeWithoutResult(status->store.persistWebsocket(older));
        assertEquals(2,jdbc.queryForObject("SELECT COUNT(*) FROM candle",Integer.class));
        assertEquals("HISTORICAL_ONLY",jdbc.queryForObject("SELECT classification FROM market_data_stream_event WHERE symbol_sequence=2",String.class));
        assertEquals(100000,jdbc.queryForObject("SELECT observed_at FROM market_data_stream_lane",java.sql.Timestamp.class).toInstant().toEpochMilli());
    }
}
