package com.crypto.marketdata;

import org.springframework.stereotype.Component;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** FIX-132: one pass shared by startup, reconnect and scheduled recovery. No queued
 * duplicate passes. A generation permit includes REST network work in the drain;
 * each subsequent write checks admission again before entering its transaction. */
@Component
public class RecoveryCoordinator {
    private final AtomicBoolean running=new AtomicBoolean();
    private final OwnershipGate gate;private final GapRepairService repair;private final MarketDataProperties properties;
    public RecoveryCoordinator(OwnershipGate gate,GapRepairService repair,MarketDataProperties properties){this.gate=gate;this.repair=repair;this.properties=properties;}
    public void reconcile(List<String> symbols){
        if(!running.compareAndSet(false,true))return;
        try(var permit=gate.admit()){
            if(permit==null)return;
            for(String symbol:symbols)for(String interval:properties.getIntervals()){
                if(!permit.stillCurrent())return;
                try{repair.reconcile(symbol,interval);}catch(IngestionPaused paused){return;}
                catch(RuntimeException failure){org.slf4j.LoggerFactory.getLogger(getClass()).error("[FIX-132][RECOVERY_FAILED] symbol={}, interval={}",symbol,interval,failure);}
            }
        }finally{running.set(false);}
    }
}
