package com.crypto.marketdata;

import com.zaxxer.hikari.*;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** FIX-132 explicit integration profile. Refuses non-loopback/non-test schemas.
 * Real InnoDB tests; rollback-failure case injects a JDBC failure over a real pool.
 * No Binance connection or production schema is used by this suite. */
class OwnershipMySqlIT {
    String url;HikariDataSource pool;OwnershipLeaseStore store;
    @BeforeEach void setup()throws Exception {
        url=System.getenv("FIX132_TEST_MYSQL_URL");
        assertNotNull(url,"Set FIX132_TEST_MYSQL_URL to an isolated local MySQL test schema");
        assertTrue(url.matches("jdbc:mysql://(127\\.0\\.0\\.1|localhost):[0-9]+/fix132_test[a-zA-Z0-9_]*(\\?.*)?"),"Refusing non-local/non-test database");
        try(var c=connection();var s=c.createStatement()){
            s.execute("CREATE TABLE IF NOT EXISTS market_data_stream_owner(id INT PRIMARY KEY, owner_token VARCHAR(36),expires_at TIMESTAMP(6)) ENGINE=InnoDB");s.execute("DELETE FROM market_data_stream_owner");
            s.execute("INSERT INTO market_data_stream_owner VALUES(1,'old',TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(6)))");
        }
        pool=new HikariDataSource(config());store=new OwnershipLeaseStore(pool,null);
    }
    HikariConfig config(){var c=new HikariConfig();c.setJdbcUrl(url);c.setUsername(System.getenv().getOrDefault("FIX132_TEST_MYSQL_USER","root"));c.setPassword(System.getenv().getOrDefault("FIX132_TEST_MYSQL_PASSWORD",""));c.setMaximumPoolSize(1);c.setMinimumIdle(0);c.setConnectionTimeout(2000);c.setValidationTimeout(1000);c.addDataSourceProperty("connectTimeout","2000");c.addDataSourceProperty("socketTimeout","3000");c.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=1, time_zone='+00:00'");return c;}
    Connection connection()throws SQLException{return DriverManager.getConnection(url,System.getenv().getOrDefault("FIX132_TEST_MYSQL_USER","root"),System.getenv().getOrDefault("FIX132_TEST_MYSQL_PASSWORD",""));}
    @AfterEach void close(){if(pool!=null)pool.close();}
    long id()throws SQLException{try(var c=pool.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT CONNECTION_ID()")){r.next();assertTrue(c.getAutoCommit());return r.getLong(1);}}
    void expire()throws SQLException{try(var c=connection();var s=c.createStatement()){s.execute("UPDATE market_data_stream_owner SET expires_at=TIMESTAMP '2000-01-01 00:00:00'");}}
    @Test void lockTimeoutRollsBackAndReusesConnectionEvenAfterExpiry()throws Exception{
        expire();long before=id();
        try(var predecessor=connection();var s=predecessor.createStatement()){
            predecessor.setAutoCommit(false);s.executeQuery("SELECT * FROM market_data_stream_owner WHERE id=1 FOR UPDATE").close();
            long begin=System.nanoTime();var failure=assertThrows(SQLException.class,()->store.acquire("new"));long elapsed=(System.nanoTime()-begin)/1_000_000;
            assertEquals(1205,failure.getErrorCode());assertTrue(elapsed>=500&&elapsed<5000,"Observed lock wait ms="+elapsed);
            assertEquals(before,id());predecessor.rollback();
        }
        assertEquals(OwnershipLeaseStore.Result.ACQUIRED,store.acquire("new"));assertEquals(before,id());
    }
    @Test void crashTakeoverRequiresExpiryAndStaleRenewalCannotReclaim()throws Exception{
        assertEquals(OwnershipLeaseStore.Result.BUSY,store.acquire("new"));expire();
        assertEquals(OwnershipLeaseStore.Result.ACQUIRED,store.acquire("new"));
        assertEquals(OwnershipLeaseStore.Result.LOST,store.renew("old"));
        store.release("old");assertEquals(OwnershipLeaseStore.Result.RENEWED,store.renew("new"));
    }
    @Test void repeatedCyclesReuseOneConnectionWithoutLeaking()throws Exception{
        expire();long original=id();for(int i=0;i<12;i++){
            assertEquals(OwnershipLeaseStore.Result.ACQUIRED,store.acquire("owner"));assertEquals(OwnershipLeaseStore.Result.RENEWED,store.renew("owner"));store.release("owner");assertEquals(original,id());
        }
        assertEquals(0,pool.getHikariPoolMXBean().getActiveConnections());assertEquals(1,pool.getHikariPoolMXBean().getTotalConnections());
    }
    @Test void actualDisconnectedConnectionIsReplaced()throws Exception{
        long old=id();try(var c=connection();var s=c.createStatement()){s.execute("KILL CONNECTION "+old);}
        // Hikari may detect death during borrow; otherwise the attempt fails and evicts it.
        try{store.acquire("new");}catch(SQLException expected){}
        assertNotEquals(old,id());
    }
    @Test void failedRollbackPhysicallyEvictsInsteadOfReturningHandle()throws Exception{
        pool.close();var faultPool=new FaultPool(config());pool=faultPool;store=new OwnershipLeaseStore(pool,null);expire();long old=id();
        try(var predecessor=connection();var s=predecessor.createStatement()){
            predecessor.setAutoCommit(false);s.executeQuery("SELECT * FROM market_data_stream_owner WHERE id=1 FOR UPDATE").close();faultPool.failRollback=true;
            var failure=assertThrows(SQLException.class,()->store.acquire("new"));assertEquals(1205,failure.getErrorCode());assertTrue(failure.getSuppressed().length>0);
            assertTrue(faultPool.evictions>0);faultPool.failRollback=false;predecessor.rollback();
        }
        assertNotEquals(old,id());assertEquals(OwnershipLeaseStore.Result.ACQUIRED,store.acquire("new"));
    }
    @Test void coordinatorsWaitForGracefulDrainThenTransferWithoutOverlap()throws Exception{
        expire();var firstGate=new OwnershipGate(true);var first=new OwnershipCoordinator(firstGate,store,()->0L);
        try(var secondPool=new HikariDataSource(config())){
            var secondGate=new OwnershipGate(true);var second=new OwnershipCoordinator(secondGate,new OwnershipLeaseStore(secondPool,null),()->0L);
            first.tick();assertEquals(OwnershipGate.State.OWNED,firstGate.state());
            second.tick();assertEquals(OwnershipGate.State.WAITING_FOR_OWNERSHIP,secondGate.state());
            try(var work=firstGate.admit()){
                first.beginStop();assertFalse(first.releaseIfDrained());second.tick();assertNull(secondGate.admit());
            }
            assertTrue(first.releaseIfDrained());second.tick();assertEquals(OwnershipGate.State.OWNED,secondGate.state());
            assertNull(firstGate.admit());second.beginStop();assertTrue(second.releaseIfDrained());
        }
    }
    @Test void failedResetEvictsAndReportsUncertainty()throws Exception{
        pool.close();var faultPool=new FaultPool(config());pool=faultPool;store=new OwnershipLeaseStore(pool,null);expire();long old=id();
        faultPool.failReset=true;assertThrows(SQLException.class,()->store.acquire("new"));
        assertTrue(faultPool.evictions>0);faultPool.failReset=false;assertNotEquals(old,id());
    }
    @Test void realRenewalLockFailureClosesAdmissionAndRecoveryWaitsForDrain()throws Exception{
        expire();var time=new java.util.concurrent.atomic.AtomicLong();var gate=new OwnershipGate(true);
        var coordinator=new OwnershipCoordinator(gate,store,time::get);coordinator.tick();
        assertEquals(OwnershipGate.State.OWNED,gate.state());long epoch=gate.epoch();
        try(var permit=gate.admit();var blocker=connection();var sql=blocker.createStatement()){
            blocker.setAutoCommit(false);sql.executeQuery("SELECT * FROM market_data_stream_owner WHERE id=1 FOR UPDATE").close();
            time.set(java.util.concurrent.TimeUnit.SECONDS.toNanos(6));coordinator.tick();
            assertEquals(OwnershipGate.State.OWNERSHIP_UNCERTAIN,gate.state());assertNull(gate.admit());
            coordinator.tick();assertEquals(OwnershipGate.State.OWNERSHIP_UNCERTAIN,gate.state());blocker.rollback();
        }
        coordinator.tick();assertEquals(OwnershipGate.State.OWNED,gate.state());assertTrue(gate.epoch()>epoch);
        coordinator.beginStop();assertTrue(coordinator.releaseIfDrained());
    }
    @Test void enabledPrivatePoolPreservesBootDatasourceAndSessionLimits()throws Exception{
        try(var c=connection();var statement=c.createStatement()){
            String ddl=java.nio.file.Files.readString(java.nio.file.Path.of("md/sql/FIX-132-source-tables.sql")).replaceAll("(?m)^--.*$","");
            for(String sql:ddl.split(";"))if(!sql.isBlank())statement.execute(sql);
        }
        new org.springframework.boot.test.context.runner.ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class,
                org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration.class,
                org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration.class))
            .withUserConfiguration(OwnershipWiringTest.Config.class)
            .withPropertyValues("spring.datasource.url="+url,"spring.datasource.username="+System.getenv().getOrDefault("FIX132_TEST_MYSQL_USER","root"),
                "spring.datasource.password="+System.getenv().getOrDefault("FIX132_TEST_MYSQL_PASSWORD",""),"market-data.stream.enabled=true")
            .run(context->{
                org.assertj.core.api.Assertions.assertThat(context).hasNotFailed().hasSingleBean(javax.sql.DataSource.class);
                var main=context.getBean(javax.sql.DataSource.class);
                assertSame(main,context.getBean(org.springframework.jdbc.datasource.DataSourceTransactionManager.class).getDataSource());
                var privatePool=(HikariDataSource)org.springframework.test.util.ReflectionTestUtils.getField(context.getBean(OwnershipLeaseStore.class),"pool");
                assertNotSame(main,privatePool);assertEquals(1,privatePool.getMaximumPoolSize());assertEquals(2000,privatePool.getConnectionTimeout());
                try(var c=privatePool.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT @@session.innodb_lock_wait_timeout,@@session.time_zone")){
                    assertTrue(r.next());assertEquals(1,r.getInt(1));assertEquals("+00:00",r.getString(2));
                }
            });
    }
    static class FaultPool extends HikariDataSource {
        boolean failRollback,failReset;int evictions;final Map<Connection,Connection> handles=new IdentityHashMap<>();
        FaultPool(HikariConfig c){super(c);}
        @Override public Connection getConnection()throws SQLException{
            Connection real=super.getConnection();if(!failRollback&&!failReset)return real;
            Connection wrapper=(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{Connection.class},(p,m,a)->{
                if(failReset&&m.getName().equals("setAutoCommit")&&Boolean.TRUE.equals(a[0]))throw new SQLException("injected reset ambiguity","08006");
                if(failRollback&&m.getName().equals("rollback"))throw new SQLException("injected rollback network ambiguity","08006");
                try{return m.invoke(real,a);}catch(InvocationTargetException e){throw e.getCause();}
            });handles.put(wrapper,real);return wrapper;
        }
        @Override public void evictConnection(Connection c){evictions++;super.evictConnection(handles.getOrDefault(c,c));}
    }
}
