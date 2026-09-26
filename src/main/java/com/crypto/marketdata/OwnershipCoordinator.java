package com.crypto.marketdata;

import org.springframework.stereotype.Component;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import jakarta.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.*;
import org.slf4j.LoggerFactory;

/** FIX-132: only one bounded JDBC operation at a time, on a private fixed-delay clock.
 * Drain checks never hold a connection. Renewal failure atomically closes the gate.
 * Socket/recovery work run elsewhere and cannot delay the ownership clock. */
@Component
public class OwnershipCoordinator {
    private final OwnershipGate gate;private final OwnershipLeaseStore leases;
    private final ScheduledExecutorService clock=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"fix132-ownership"));
    private Supplier<Boolean> transportDrained=()->true;
    private final LongSupplier nanos;
    private final String token=UUID.randomUUID().toString();private long renewedAt;private long lastWaitLog;private volatile boolean stopping;
    @org.springframework.beans.factory.annotation.Autowired
    public OwnershipCoordinator(OwnershipGate gate,OwnershipLeaseStore leases){this(gate,leases,System::nanoTime);}
    OwnershipCoordinator(OwnershipGate gate,OwnershipLeaseStore leases,LongSupplier nanos){this.gate=gate;this.leases=leases;this.nanos=nanos;}
    public void transportDrained(Supplier<Boolean> check){transportDrained=check;}
    @EventListener(ApplicationReadyEvent.class) public void start(){
        LoggerFactory.getLogger(getClass()).info("[FIX-132][OWNERSHIP_CONFIGURATION] enabled={}, state={}, leaseSeconds=30, renewalSeconds=5, retryDelaySeconds=1, ownershipPool=1",gate.enabled(),gate.state());
        if(gate.enabled())clock.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);
    }
    synchronized void tick(){
        if(stopping||!gate.enabled())return;
        try {
            var state=gate.state();
            if(state==OwnershipGate.State.OWNED) {
                if(nanos.getAsLong()-renewedAt<TimeUnit.SECONDS.toNanos(5))return;
                if(leases.renew(token)==OwnershipLeaseStore.Result.RENEWED)renewedAt=nanos.getAsLong();
                else gate.lost("database lease no longer valid");
                return;
            }
            if(state==OwnershipGate.State.STOPPING||state==OwnershipGate.State.STOPPED)return;
            if(gate.inFlight()!=0||!transportDrained.get())return;
            if(state!=OwnershipGate.State.WAITING_FOR_OWNERSHIP)gate.waiting();
            String candidate=token;
            if(leases.acquire(candidate)==OwnershipLeaseStore.Result.ACQUIRED){
                renewedAt=nanos.getAsLong();
                // Shutdown can close admission while JDBC is running; never reopen it.
                synchronized(gate){if(!stopping&&gate.state()==OwnershipGate.State.WAITING_FOR_OWNERSHIP)gate.acquired(token);}
            } else {
                gate.waitingReason("lease held by another writer");
                if(nanos.getAsLong()-lastWaitLog>=TimeUnit.SECONDS.toNanos(10)){
                    lastWaitLog=nanos.getAsLong();LoggerFactory.getLogger(getClass()).warn("[FIX-132][WAITING_FOR_OWNERSHIP] ingestion paused; next attempt after one second");
                }
            }
        }catch(Exception failure){
            gate.uncertain("ownership SQL failed; admission suspended");
            gate.waitingReason("ownership database attempt failed");
            LoggerFactory.getLogger(getClass()).warn("[FIX-132][OWNERSHIP_ATTEMPT_FAILED] state={}, exception={}",gate.state(),failure.getClass().getSimpleName());
        }
    }
    /** Manager calls this before closing transport, so new REST work also stops immediately. */
    public void beginStop(){stopping=true;gate.stopping();clock.shutdown();}
    synchronized boolean releaseIfDrained(){
        if(gate.inFlight()!=0||!transportDrained.get())return false;
        try {
            if(gate.enabled()&&token!=null)leases.release(token);
            gate.stopped();return true;
        }catch(Exception e){LoggerFactory.getLogger(getClass()).warn("[FIX-132][LEASE_RELEASE_UNCONFIRMED] expiry remains authoritative");return false;}
    }
    @PreDestroy public void stop(){
        beginStop();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(System.nanoTime()<deadline){
            if(releaseIfDrained())return;
            try{Thread.sleep(100);}catch(InterruptedException e){Thread.currentThread().interrupt();break;}
        }
        LoggerFactory.getLogger(getClass()).warn("[FIX-132][DRAIN_UNCONFIRMED] inFlight={}; no early lease release",gate.inFlight());
    }
}
