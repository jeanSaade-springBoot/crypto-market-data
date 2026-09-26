package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class BinanceWebSocketManager {
    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketManager.class);

    private final MarketDataProperties properties;
    private final CoinConfigurationReader coinReader;
    private final BinanceStreamUrlBuilder urlBuilder;
    private final GapRepairService gapRepairService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final CollectorConnectionOwner owner;
    private final CollectorWebSocketTransport transport = new CollectorWebSocketTransport();
    private final OwnershipGate gate;
    private final OwnershipCoordinator ownership;
    private final RecoveryCoordinator recovery;
    private long connectedEpoch=-1;
    private long refreshedAt;
    private volatile boolean stopped;
    private volatile List<String> connectedSymbols = List.of();

    public BinanceWebSocketManager(MarketDataProperties properties, CoinConfigurationReader coinReader,
                                   BinanceStreamUrlBuilder urlBuilder, ObjectMapper objectMapper,
                                   CandleStore candleStore, GapRepairService gapRepairService, OwnershipGate gate, OwnershipCoordinator ownership, RecoveryCoordinator recovery) {
        this.gate=gate;this.ownership=ownership;this.recovery=recovery;
        this.properties = properties;
        this.coinReader = coinReader;
        this.urlBuilder = urlBuilder;
        this.gapRepairService = gapRepairService;
        this.owner = new CollectorConnectionOwner(objectMapper, candleStore,
                transport, Duration.ofSeconds(15));
        owner.ownershipGate(gate);ownership.transportDrained(owner::quiescent);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if(gate.enabled()) {
            // Separate from renewal: REST/handshake latency cannot monopolize the lease clock.
            scheduler.scheduleWithFixedDelay(this::ownershipTick,0,1,TimeUnit.SECONDS);
        } else {
            List<String> symbols=coinReader.enabledSymbols();reconcileAll(symbols);connect(symbols);
            if(stopped)return;
            scheduler.scheduleWithFixedDelay(this::healthCheck,properties.getReconnectHealthSeconds(),properties.getReconnectHealthSeconds(),TimeUnit.SECONDS);
            scheduler.scheduleWithFixedDelay(this::refreshConfiguration,properties.getConfigRefreshSeconds(),properties.getConfigRefreshSeconds(),TimeUnit.SECONDS);
        }
    }
    private void reconcileAll(List<String> symbols){recovery.reconcile(symbols);}
    private void ownershipTick(){
        if(stopped)return;
        try {
            if(!gate.admitting()){owner.pause();return;}
            long epoch=gate.epoch();
            if(connectedEpoch!=epoch){
                owner.pause();if(!owner.quiescent())return;
                var symbols=coinReader.enabledSymbols();reconcileAll(symbols);
                if(!gate.admitting()||gate.epoch()!=epoch)return;
                connect(symbols);if(owner.isConnected())connectedEpoch=epoch;
            }else if(!owner.isConnected())healthCheck();
            if(System.nanoTime()-refreshedAt>TimeUnit.SECONDS.toNanos(properties.getConfigRefreshSeconds())){
                refreshedAt=System.nanoTime();refreshConfiguration();
            }
        }catch(RuntimeException e){log.error("[FIX-132][INGESTION_LIFECYCLE_FAILED]",e);}
    }
    public boolean ingestionReady(){return gate.admitting()&&owner.isConnected()&&(!gate.enabled()||connectedEpoch==gate.epoch());}

    private void connect(List<String> symbols) {
        if (stopped || !gate.admitting()) return;
        if (owner.connect(URI.create(urlBuilder.build(symbols))) == CollectorConnectionOwner.Outcome.CONNECTED) {
            connectedSymbols = List.copyOf(symbols);
            log.info("[FIX-132][SOURCE_CONFIGURATION] collector connected: symbols={}, intervals={}",
                    connectedSymbols, properties.getIntervals());
        }
    }

    private void healthCheck() {
        if (stopped) return;
        try {
            if (!owner.isConnected()) {
                List<String> symbols = coinReader.enabledSymbols();
                reconcileAll(symbols);
                connect(symbols);
            }
        } catch (RuntimeException exception) {
            // A failed symbol query must not cancel all future fixed-delay checks.
            log.error("[FIX-132][HEALTH_CHECK_FAILED] collector will retry on next check", exception);
        }
    }

    private void refreshConfiguration() {
        if (stopped) return;
        try {
            List<String> symbols = coinReader.enabledSymbols();
            if (!symbols.equals(connectedSymbols)) {
                log.warn("FIX-139 enabled symbol set changed; reconciling and reloading streams: old={}, new={}", connectedSymbols, symbols);
                reconcileAll(symbols);
                connect(symbols);
            }
        } catch (RuntimeException exception) {
            log.error("FIX-139 symbol configuration refresh failed", exception);
        }
    }

    @PreDestroy
    public void stop() {
        stopped = true;
        ownership.beginStop();
        owner.stop();
        scheduler.shutdownNow();
        transport.close();
    }
}
