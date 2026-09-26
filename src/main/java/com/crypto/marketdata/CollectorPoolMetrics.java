package com.crypto.marketdata;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.ObjectProvider;
import io.micrometer.core.instrument.MeterRegistry;
/** FIX-132 gauges use pool counters only; scraping metrics never borrows a connection. */
@Component
public class CollectorPoolMetrics {
    public CollectorPoolMetrics(DataSource source,ObjectProvider<MeterRegistry> provider){
        var m=provider.getIfAvailable();if(m==null||!(source instanceof HikariDataSource pool))return;
        m.gauge("fix132.candle_pool.active",pool,p->p.getHikariPoolMXBean()==null?0:p.getHikariPoolMXBean().getActiveConnections());
        m.gauge("fix132.candle_pool.pending",pool,p->p.getHikariPoolMXBean()==null?0:p.getHikariPoolMXBean().getThreadsAwaitingConnection());
    }
}
