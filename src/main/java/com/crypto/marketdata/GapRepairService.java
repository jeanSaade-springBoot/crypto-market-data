package com.crypto.marketdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class GapRepairService {
    private static final Logger log = LoggerFactory.getLogger(GapRepairService.class);

    private final CandleStore store;
    private final BinanceRestClient client;
    private final MarketDataProperties properties;

    public GapRepairService(CandleStore store, BinanceRestClient client, MarketDataProperties properties) {
        this.store = store;
        this.client = client;
        this.properties = properties;
    }

    public void reconcile(String symbol, String interval) {
        Instant observedAt = Instant.now();
        Duration step = IntervalSupport.duration(interval);
        var latest = store.latestClosedOpenTime(symbol, interval);

        if (latest.isEmpty()) {
            List<BinanceKline> bootstrap = client.latest(symbol, interval, properties.getBootstrapLimit());
            persistClosed(symbol, interval, bootstrap, "STARTUP_BOOTSTRAP", observedAt);
            log.info("FIX-139 initial bootstrap: symbol={}, interval={}, received={}", symbol, interval, bootstrap.size());
            return;
        }

        Instant next = latest.get().plus(step);
        int repaired = 0;
        while (next.isBefore(observedAt)) {
            List<BinanceKline> rows = client.from(symbol, interval, next, 1000);
            if (rows.isEmpty()) {
                break;
            }
            int closed = persistClosed(symbol, interval, rows, "STARTUP_RECOVERY", observedAt);
            repaired += closed;
            BinanceKline last = rows.get(rows.size() - 1);
            Instant candidate = last.openTime().plus(step);
            if (!candidate.isAfter(next)) {
                break;
            }
            next = candidate;
            if (closed == 0 && last.closeTime().isAfter(observedAt)) {
                break;
            }
        }
        if (repaired > 0) {
            log.warn("FIX-139 startup/gap reconciliation repaired candles: symbol={}, interval={}, count={}", symbol, interval, repaired);
        }
    }

    public void repairBefore(String symbol, String interval, Instant incomingOpenTime) {
        Duration step = IntervalSupport.duration(interval);
        var latest = store.latestClosedOpenTime(symbol, interval);
        if (latest.isEmpty()) {
            return;
        }
        Instant expected = latest.get().plus(step);
        if (!incomingOpenTime.isAfter(expected)) {
            return;
        }

        Instant observedAt = Instant.now();
        Instant cursor = expected;
        int repaired = 0;
        while (cursor.isBefore(incomingOpenTime)) {
            List<BinanceKline> rows = client.from(symbol, interval, cursor, 1000);
            if (rows.isEmpty()) {
                break;
            }
            for (BinanceKline row : rows) {
                if (!row.openTime().isBefore(incomingOpenTime)) {
                    returnAfterRepair(symbol, interval, repaired);
                    return;
                }
                if (!row.closeTime().isAfter(observedAt)) {
                    store.persistRest(symbol, interval, row, "REST_GAP_REPAIR", observedAt);
                    repaired++;
                }
            }
            BinanceKline last = rows.get(rows.size() - 1);
            Instant candidate = last.openTime().plus(step);
            if (!candidate.isAfter(cursor)) {
                break;
            }
            cursor = candidate;
        }
        returnAfterRepair(symbol, interval, repaired);
    }

    private int persistClosed(String symbol, String interval, List<BinanceKline> rows, String source, Instant observedAt) {
        int count = 0;
        for (BinanceKline row : rows) {
            if (row.closeTime().isAfter(observedAt)) {
                continue;
            }
            store.persistRest(symbol, interval, row, source, observedAt);
            count++;
        }
        return count;
    }

    private void returnAfterRepair(String symbol, String interval, int repaired) {
        if (repaired > 0) {
            log.warn("FIX-139 websocket gap repaired before live candle: symbol={}, interval={}, repaired={}", symbol, interval, repaired);
        }
    }
}
