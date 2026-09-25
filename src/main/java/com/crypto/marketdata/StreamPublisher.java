package com.crypto.marketdata;

import org.springframework.stereotype.Component;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Instant;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.util.*;

/** FIX-132 additive outbox. Called INSIDE CandleStore's existing transaction;
 * publication errors roll back the candle too when enabled. Off touches no table.
 * All publishers use owner -> symbol -> candle-version -> candle lock order. */
@Component
public class StreamPublisher {
    private final JdbcTemplate jdbc;
    private final boolean enabled;
    private final String owner=UUID.randomUUID().toString();
    public StreamPublisher(JdbcTemplate jdbc, Environment env) {
        this.jdbc=jdbc; enabled=env.getProperty("market-data.stream.enabled",Boolean.class,false);
    }
    @jakarta.annotation.PostConstruct
    public void validateSchema() {
        if(enabled)for(String table:List.of("market_data_stream_owner","market_data_stream_cursor","market_data_stream_version","market_data_stream_event","market_data_stream_lane"))
            jdbc.queryForObject("SELECT COUNT(*) FROM "+table+" WHERE 1=0",Long.class);
        org.slf4j.LoggerFactory.getLogger(StreamPublisher.class).info("[FIX-132][SOURCE_CONFIGURATION] publicationEnabled={}, publisher={}",enabled,owner);
    }
    public record Observation(String symbol,String interval,Instant open,Instant close,boolean closed,
        Instant observed,Instant received,BigDecimal price,String source,String hash,String classification) {
        boolean accepted() { return "LIVE".equals(classification) || "HISTORICAL_ONLY".equals(classification); }
    }
    public Observation prepare(String symbol,String interval,Instant open,Instant close,boolean closed,
            Instant observed,BigDecimal price,String source,String payload) {
        if(!enabled) return null;
        Instant received=Instant.now();
        if(!TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("FIX-132 publishing requires the candle transaction");
        jdbc.update("INSERT IGNORE INTO market_data_stream_owner VALUES(1,?,TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)))",owner);
        var lease=jdbc.queryForMap("SELECT owner_token, expires_at<CURRENT_TIMESTAMP(6) expired FROM market_data_stream_owner WHERE id=1 FOR UPDATE");
        boolean expired=Boolean.TRUE.equals(lease.get("expired")) || "1".equals(String.valueOf(lease.get("expired")));
        if(!owner.equals(lease.get("owner_token")) && !expired) throw new IllegalStateException("FIX-132 another collector owns publication");
        jdbc.update("UPDATE market_data_stream_owner SET owner_token=?,expires_at=TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)) WHERE id=1",owner);
        jdbc.update("INSERT IGNORE INTO market_data_stream_cursor(symbol) VALUES(?)",symbol);
        jdbc.queryForObject("SELECT last_sequence FROM market_data_stream_cursor WHERE symbol=? FOR UPDATE",Long.class,symbol);
        String hash=hash(payload);
        var versions=jdbc.queryForList("SELECT observed_at,closed,payload_hash FROM market_data_stream_version WHERE symbol=? AND interval_code=? AND open_time=? FOR UPDATE",symbol,interval,Timestamp.from(open));
        String classification="LIVE_WEBSOCKET".equals(source)?"LIVE":"HISTORICAL_ONLY";
        // First feed-enabled update may encounter pre-existing closed history.
        // Never let a forming WS update reopen it merely because the sidecar is new.
        if(versions.isEmpty() && !closed) {
            Integer existing=jdbc.queryForObject("SELECT COUNT(*) FROM candle WHERE symbol=? AND interval_code=? AND open_time=? AND closed=1",Integer.class,symbol,interval,Timestamp.from(open));
            if(existing!=null && existing>0) classification="OUT_OF_ORDER";
        }
        if(!versions.isEmpty() && !closed) {
            Object value=versions.getFirst().get("closed");
            if(Boolean.TRUE.equals(value)||"1".equals(String.valueOf(value)))classification="OUT_OF_ORDER";
        }
        if("LIVE".equals(classification)) {
            if(observed==null) classification="UNKNOWN_TIME";
            else if(!versions.isEmpty()) {
                var old=versions.getFirst();
                Instant oldTime=old.get("observed_at")==null?null:((Timestamp)old.get("observed_at")).toInstant();
                boolean oldClosed=Boolean.TRUE.equals(old.get("closed")) || "1".equals(String.valueOf(old.get("closed")));
                if(hash.equals(old.get("payload_hash")) && observed.equals(oldTime)) classification="DUPLICATE";
                else if((oldClosed&&!closed) || (oldTime!=null && (observed.isBefore(oldTime) || (observed.equals(oldTime)&&!(closed&&!oldClosed))))) classification="OUT_OF_ORDER";
            }
        }
        if("LIVE".equals(classification)) {
            // Persistence order is not Binance order. An older observation from a
            // different candle must not become a new live close simply because its
            // own per-candle version sidecar is absent. Preserve its candle as history.
            var lane=jdbc.queryForList("SELECT observed_at,open_time FROM market_data_stream_lane WHERE symbol=? AND interval_code=? FOR UPDATE",symbol,interval);
            if(!lane.isEmpty() && (observed.isBefore(((Timestamp)lane.getFirst().get("observed_at")).toInstant())
                    || open.isBefore(((Timestamp)lane.getFirst().get("open_time")).toInstant()))) classification="HISTORICAL_ONLY";
            else jdbc.update("INSERT INTO market_data_stream_lane(symbol,interval_code,observed_at,open_time) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE observed_at=VALUES(observed_at),open_time=VALUES(open_time)",symbol,interval,Timestamp.from(observed),Timestamp.from(open));
        }
        Observation result=new Observation(symbol,interval,open,close,closed,observed,received,price,source,hash,classification);
        if(result.accepted()) jdbc.update("""
            INSERT INTO market_data_stream_version(symbol,interval_code,open_time,observed_at,closed,payload_hash) VALUES(?,?,?,?,?,?)
            ON DUPLICATE KEY UPDATE observed_at=VALUES(observed_at),closed=VALUES(closed),payload_hash=VALUES(payload_hash)
            """,symbol,interval,Timestamp.from(open),observed==null?null:Timestamp.from(observed),closed,hash);
        return result;
    }
    public void publish(Observation e) {
        if(e==null || (!"1m".equals(e.interval())&&!e.closed())) return;
        long sequence=jdbc.queryForObject("SELECT last_sequence FROM market_data_stream_cursor WHERE symbol=?",Long.class,e.symbol())+1;
        jdbc.update("UPDATE market_data_stream_cursor SET last_sequence=? WHERE symbol=?",sequence,e.symbol());
        jdbc.update("""
            INSERT INTO market_data_stream_event(symbol,symbol_sequence,interval_code,candle_open_time,candle_close_time,
            closed,observed_at,received_at,price,source,classification,payload_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,e.symbol(),sequence,e.interval(),Timestamp.from(e.open()),Timestamp.from(e.close()),e.closed(),
            e.observed()==null?null:Timestamp.from(e.observed()),Timestamp.from(e.received()),e.price(),e.source(),e.classification(),e.hash());
    }
    static String hash(String value) {
        try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
        catch(java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
