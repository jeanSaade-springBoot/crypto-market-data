package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

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
    private final ObjectMapper objectMapper;
    private final CandleStore candleStore;
    private final GapRepairService gapRepairService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile BinanceWebSocketHandler handler;
    private volatile List<String> connectedSymbols = List.of();

    public BinanceWebSocketManager(MarketDataProperties properties, CoinConfigurationReader coinReader,
                                   BinanceStreamUrlBuilder urlBuilder, ObjectMapper objectMapper,
                                   CandleStore candleStore, GapRepairService gapRepairService) {
        this.properties = properties;
        this.coinReader = coinReader;
        this.urlBuilder = urlBuilder;
        this.objectMapper = objectMapper;
        this.candleStore = candleStore;
        this.gapRepairService = gapRepairService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        List<String> symbols = coinReader.enabledSymbols();
        reconcileAll(symbols);
        connect(symbols);
        scheduler.scheduleWithFixedDelay(this::healthCheck,
                properties.getReconnectHealthSeconds(), properties.getReconnectHealthSeconds(), TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::refreshConfiguration,
                properties.getConfigRefreshSeconds(), properties.getConfigRefreshSeconds(), TimeUnit.SECONDS);
    }

    private void reconcileAll(List<String> symbols) {
        for (String symbol : symbols) {
            for (String interval : properties.getIntervals()) {
                try { gapRepairService.reconcile(symbol, interval); }
                catch (RuntimeException exception) {
                    log.error("FIX-139 startup reconciliation failed: symbol={}, interval={}", symbol, interval, exception);
                }
            }
        }
    }

    private synchronized void connect(List<String> symbols) {
        try {
            closeCurrent();
            String url = urlBuilder.build(symbols);
            BinanceWebSocketHandler next = new BinanceWebSocketHandler(objectMapper, candleStore, gapRepairService);
            new StandardWebSocketClient().execute(next, null, URI.create(url))
                    .get(Duration.ofSeconds(15).toMillis(), TimeUnit.MILLISECONDS);
            handler = next;
            connectedSymbols = List.copyOf(symbols);
            log.info("FIX-139 Binance candle collector connected: symbols={}, intervals={}", connectedSymbols, properties.getIntervals());
        } catch (Exception exception) {
            handler = null;
            log.error("FIX-139 unable to connect Binance candle collector", exception);
        }
    }

    private void healthCheck() {
        BinanceWebSocketHandler current = handler;
        if (current == null || !current.isConnected()) {
            List<String> symbols = coinReader.enabledSymbols();
            reconcileAll(symbols);
            connect(symbols);
        }
    }

    private void refreshConfiguration() {
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

    private synchronized void closeCurrent() {
        BinanceWebSocketHandler current = handler;
        handler = null;
        if (current != null) current.close();
    }

    @PreDestroy
    public void stop() {
        closeCurrent();
        scheduler.shutdownNow();
    }
}
