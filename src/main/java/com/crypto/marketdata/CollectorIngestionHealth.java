package com.crypto.marketdata;
import org.springframework.stereotype.Component;
import org.springframework.boot.health.contributor.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
/** FIX-132 local-state-only readiness: health requests never acquire JDBC connections.
 * Ownership outages do not alter liveness or cause database-driven restart loops. */
@Component("collectorIngestion")
public class CollectorIngestionHealth implements HealthIndicator {
    private final OwnershipGate gate;private final BinanceWebSocketManager manager;
    public CollectorIngestionHealth(OwnershipGate gate,BinanceWebSocketManager manager,ObjectProvider<MeterRegistry> registry){
        this.gate=gate;this.manager=manager;
        var meters=registry.getIfAvailable();if(meters!=null)meters.gauge("fix132.ingestion.inflight",gate,OwnershipGate::inFlight);
    }
    @Override public Health health(){
        // OFF contributes UP without overriding existing DB/readiness checks.
        boolean ready=gate.state()==OwnershipGate.State.FEED_DISABLED||manager.ingestionReady();
        return (ready?Health.up():Health.outOfService()).withDetail("state",gate.state().name())
            .withDetail("reason",gate.reason()).withDetail("inFlight",gate.inFlight()).build();
    }
}
