package com.crypto.marketdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** FIX-139 self-healing reconciliation for outages that do not force a WebSocket reconnect. */
@Component
public class PeriodicReconciliationService {
    private static final Logger log = LoggerFactory.getLogger(PeriodicReconciliationService.class);

    private final CoinConfigurationReader coinReader;
    private final MarketDataProperties properties;
    private final GapRepairService gapRepairService;

    public PeriodicReconciliationService(CoinConfigurationReader coinReader,
                                         MarketDataProperties properties,
                                         GapRepairService gapRepairService) {
        this.coinReader = coinReader;
        this.properties = properties;
        this.gapRepairService = gapRepairService;
    }

    @Scheduled(fixedDelayString = "${market-data.binance.reconciliation-delay-ms:60000}")
    public void reconcile() {
        for (String symbol : coinReader.enabledSymbols()) {
            for (String interval : properties.getIntervals()) {
                try {
                    gapRepairService.reconcile(symbol, interval);
                } catch (RuntimeException exception) {
                    log.error("FIX-139 periodic reconciliation failed: symbol={}, interval={}", symbol, interval, exception);
                }
            }
        }
    }
}
