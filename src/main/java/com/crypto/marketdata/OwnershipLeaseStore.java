package com.crypto.marketdata;

import com.zaxxer.hikari.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import jakarta.annotation.PreDestroy;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/** FIX-132: privately owned one-connection pool, NOT a Spring DataSource bean.
 * Only this class changes leases. Candle transactions retain the original datasource.
 * Server lock timeout rolls back the ENTIRE attempt; ambiguous connections are evicted. */
@Component
public class OwnershipLeaseStore {
    public enum Result { ACQUIRED, BUSY, RENEWED, LOST, RELEASED }
    private final HikariDataSource pool;
    private final MeterRegistry meters;
    @org.springframework.beans.factory.annotation.Autowired
    public OwnershipLeaseStore(Environment env,ObjectProvider<MeterRegistry> meters) {
        this.meters=meters.getIfAvailable();
        if(!env.getProperty("market-data.stream.enabled",Boolean.class,false)){pool=null;return;}
        HikariConfig c=new HikariConfig();c.setPoolName("fix132-ownership");c.setMaximumPoolSize(1);c.setMinimumIdle(0);
        c.setConnectionTimeout(2000);c.setValidationTimeout(1000);c.setInitializationFailTimeout(-1);
        c.setJdbcUrl(env.getRequiredProperty("spring.datasource.url"));
        c.setUsername(env.getProperty("spring.datasource.username"));c.setPassword(env.getProperty("spring.datasource.password"));
        c.addDataSourceProperty("connectTimeout","2000");c.addDataSourceProperty("socketTimeout","3000");
        c.addDataSourceProperty("connectionTimeZone","UTC");c.addDataSourceProperty("forceConnectionTimeZoneToSession","true");
        c.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=1, time_zone='+00:00'");
        pool=new HikariDataSource(c);if(this.meters!=null)pool.setMetricRegistry(this.meters);
    }
    // Test seam uses the real pool/driver; never publishes this datasource into Spring.
    OwnershipLeaseStore(HikariDataSource pool,MeterRegistry meters){this.pool=pool;this.meters=meters;}
    public Result acquire(String token) throws SQLException{return execute("acquire",token);}
    public Result renew(String token) throws SQLException{return execute("renew",token);}
    public Result release(String token) throws SQLException{return execute("release",token);}
    private Result execute(String operation,String token) throws SQLException {
        if(pool==null)throw new IllegalStateException("Ownership pool disabled");
        long start=System.nanoTime(),borrow=start,sql=0;Connection c=null;boolean evict=false;String outcome="failed";
        try {
            try{c=pool.getConnection();}finally{record("pool_acquire",operation,System.nanoTime()-borrow);}
            c.setAutoCommit(false);sql=System.nanoTime();
            Result result;
            if(operation.equals("acquire")) {
                try(var s=c.prepareStatement("INSERT IGNORE INTO market_data_stream_owner(id,owner_token,expires_at) VALUES(1,?,TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)))")){s.setString(1,token);s.executeUpdate();}
            }
            String existing=null;boolean valid=false;
            // Lock first, THEN evaluate database time in a fresh statement: MySQL NOW()
            // is statement-start time, which must not precede a potentially long lock wait.
            try(var s=c.prepareStatement("SELECT owner_token FROM market_data_stream_owner WHERE id=1 FOR UPDATE");var rs=s.executeQuery()){if(rs.next())existing=rs.getString(1);}
            if(existing!=null)try(var s=c.prepareStatement("SELECT expires_at>CURRENT_TIMESTAMP(6) FROM market_data_stream_owner WHERE id=1");var rs=s.executeQuery()){if(rs.next())valid=rs.getBoolean(1);}
            if(operation.equals("release")) {
                try(var s=c.prepareStatement("DELETE FROM market_data_stream_owner WHERE id=1 AND owner_token=?")){s.setString(1,token);s.executeUpdate();}
                result=Result.RELEASED; // Different/missing token also proves THIS token no longer owns.
            } else if(operation.equals("renew") && (!token.equals(existing)||!valid)) result=Result.LOST;
            else if(operation.equals("acquire") && valid&&!token.equals(existing)) result=Result.BUSY;
            else {
                try(var s=c.prepareStatement("UPDATE market_data_stream_owner SET owner_token=?,expires_at=TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)) WHERE id=1")){s.setString(1,token);if(s.executeUpdate()!=1)throw new SQLException("Ownership row disappeared");}
                result=operation.equals("acquire")?Result.ACQUIRED:Result.RENEWED;
            }
            record("sql",operation,System.nanoTime()-sql);sql=0;c.commit();outcome=result.name();return result;
        } catch(SQLException|RuntimeException failure) {
            if(sql!=0)record("sql",operation,System.nanoTime()-sql);
            long cleanup=System.nanoTime();
            // Only known server lock timeout/deadlock is reusable after successful full rollback/reset.
            evict=!(failure instanceof SQLException s&&(s.getErrorCode()==1205||s.getErrorCode()==1213));
            if(c!=null)try{c.rollback();}catch(SQLException rollback){evict=true;failure.addSuppressed(rollback);}
            record("rollback",operation,System.nanoTime()-cleanup);throw failure;
        } finally {
            long cleanup=System.nanoTime();SQLException cleanupFailure=null;
            if(c!=null) {
                try{if(!evict)c.setAutoCommit(true);}catch(SQLException reset){evict=true;cleanupFailure=reset;}
                // A proxy close is NOT eviction. Explicitly mark/discard uncertain physical connections.
                if(evict)pool.evictConnection(c);
                else try{c.close();}catch(SQLException close){pool.evictConnection(c);cleanupFailure=close;}
                // evictConnection already removes/closes an in-use physical entry.
                // Do NOT then close/reset its proxy: it may reference the removed entry.
            }
            record("cleanup",operation,System.nanoTime()-cleanup);
            record("attempt",operation,System.nanoTime()-start);
            org.slf4j.LoggerFactory.getLogger(getClass()).debug("[FIX-132][OWNERSHIP_ATTEMPT] operation={}, outcome={}, elapsedMs={}, evicted={}",operation,outcome,(System.nanoTime()-start)/1_000_000,evict);
            if(cleanupFailure!=null)throw cleanupFailure;
        }
    }
    private void record(String phase,String operation,long nanos){if(meters!=null)meters.timer("fix132.ownership."+phase,"operation",operation).record(nanos,TimeUnit.NANOSECONDS);}
    @PreDestroy public void close(){if(pool!=null)pool.close();}
}
