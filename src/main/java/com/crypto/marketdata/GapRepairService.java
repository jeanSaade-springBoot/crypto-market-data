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
        repairInternalGaps(symbol, interval, observedAt);
    }

    /**
     * FIX-228: repair holes behind the latest candle. This is deliberately bounded and only calls
     * Binance when SQL has proved a gap exists; normal reconciliation remains inexpensive.
     */
    void repairInternalGaps(String symbol, String interval, Instant observedAt) {
        Duration step = IntervalSupport.duration(interval);
        int lookbackHours = Math.max(1, Math.min(168, properties.getGapLookbackHours()));
        int maxGaps = Math.max(1, Math.min(250, properties.getMaxInternalGapsPerPass()));
        List<CandleStore.CandleGap> gaps = store.findInternalGaps(
                symbol, interval, observedAt.minus(Duration.ofHours(lookbackHours)), step.toSeconds(), maxGaps);
        for (CandleStore.CandleGap gap : gaps) {
            int repaired = repairRange(symbol, interval, gap, observedAt);
            if (repaired > 0) {
                log.warn("FIX-228 repaired internal candle gap: symbol={}, interval={}, start={}, endExclusive={}, repaired={}",
                        symbol, interval, gap.startInclusive(), gap.endExclusive(), repaired);
            }
        }
    }

    private int repairRange(String symbol, String interval, CandleStore.CandleGap gap, Instant observedAt) {
        Duration step = IntervalSupport.duration(interval);
        Instant cursor = gap.startInclusive();
        int repaired = 0;
        while (cursor.isBefore(gap.endExclusive())) {
            List<BinanceKline> rows = client.from(symbol, interval, cursor, 1000);
            if (rows.isEmpty()) break;
            Instant lastAccepted = null;
            for (BinanceKline row : rows) {
                if (row.openTime().isBefore(cursor)) continue;
                if (!row.openTime().isBefore(gap.endExclusive())) return repaired;
                if (row.closeTime().isAfter(observedAt)) return repaired;
                store.persistRest(symbol, interval, row, "REST_INTERNAL_GAP_REPAIR", observedAt);
                repaired++;
                lastAccepted = row.openTime();
            }
            if (lastAccepted == null) break;
            Instant candidate = lastAccepted.plus(step);
            if (!candidate.isAfter(cursor)) break;
            cursor = candidate;
        }
        return repaired;
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

}
