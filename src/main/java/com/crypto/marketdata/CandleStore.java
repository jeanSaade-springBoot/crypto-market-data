package com.crypto.marketdata;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CandleStore {
    private static final Logger log = LoggerFactory.getLogger(CandleStore.class);
    private static final long MISSING_EVENT_TABLE_WARNING_INTERVAL_MS = 60_000L;

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong lastMissingEventTableWarningMillis = new AtomicLong(0L);

    public CandleStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean persistWebsocket(JsonNode root) {
        JsonNode data = root.has("data") ? root.path("data") : root;
        JsonNode k = data.path("k");
        if (k.isMissingNode() || k.isNull()) {
            throw new IllegalArgumentException("Binance message does not contain kline data");
        }
        String symbol = k.path("s").asText().trim().toUpperCase(Locale.ROOT);
        String interval = k.path("i").asText().trim();
        Instant open = Instant.ofEpochMilli(k.path("t").asLong());
        Instant close = Instant.ofEpochMilli(k.path("T").asLong());
        boolean closed = k.path("x").asBoolean(false);
        upsert(symbol, interval, open, close,
                decimal(k, "o"), decimal(k, "h"), decimal(k, "l"), decimal(k, "c"),
                decimal(k, "v"), decimal(k, "q"), k.path("n").asLong(), decimal(k, "V"), decimal(k, "Q"), closed);
        if (closed) {
            Instant observed = data.path("E").asLong(0L) > 0 ? Instant.ofEpochMilli(data.path("E").asLong()) : Instant.now();
            insertEvent(symbol, interval, open, close, "LIVE_WEBSOCKET", observed);
        }
        return closed;
    }

    @Transactional
    public void persistRest(String symbol, String interval, BinanceKline kline, String source, Instant observedAt) {
        boolean closed = !kline.closeTime().isAfter(observedAt);
        upsert(symbol, interval, kline.openTime(), kline.closeTime(),
                kline.openPrice(), kline.highPrice(), kline.lowPrice(), kline.closePrice(),
                kline.volume(), kline.quoteAssetVolume(), kline.numberOfTrades(),
                kline.takerBuyBaseVolume(), kline.takerBuyQuoteVolume(), closed);
        if (closed) {
            insertEvent(symbol, interval, kline.openTime(), kline.closeTime(), source, observedAt);
        }
    }

    public Optional<Instant> latestClosedOpenTime(String symbol, String interval) {
        return jdbcTemplate.query("""
                SELECT open_time FROM candle
                WHERE symbol = ? AND interval_code = ? AND closed = 1
                ORDER BY open_time DESC LIMIT 1
                """, rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty(), symbol, interval);
    }

    private void upsert(String symbol, String interval, Instant openTime, Instant closeTime,
                        BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close,
                        BigDecimal volume, BigDecimal quoteVolume, long trades,
                        BigDecimal takerBase, BigDecimal takerQuote, boolean closed) {
        jdbcTemplate.update("""
                INSERT INTO candle (
                    symbol, interval_code, open_time, close_time, open_price, high_price, low_price,
                    close_price, volume, quote_asset_volume, number_of_trades,
                    taker_buy_base_volume, taker_buy_quote_volume, closed, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE
                    close_time=VALUES(close_time), open_price=VALUES(open_price), high_price=VALUES(high_price),
                    low_price=VALUES(low_price), close_price=VALUES(close_price), volume=VALUES(volume),
                    quote_asset_volume=VALUES(quote_asset_volume), number_of_trades=VALUES(number_of_trades),
                    taker_buy_base_volume=VALUES(taker_buy_base_volume), taker_buy_quote_volume=VALUES(taker_buy_quote_volume),
                    closed=VALUES(closed), updated_at=CURRENT_TIMESTAMP(6)
                """,
                symbol, interval, Timestamp.from(openTime), Timestamp.from(closeTime), open, high, low, close,
                volume, quoteVolume, trades, takerBase, takerQuote, closed);
    }

    private void insertEvent(String symbol, String interval, Instant openTime, Instant closeTime, String source, Instant observedAt) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO market_data_candle_event (
                        symbol, interval_code, candle_open_time, candle_close_time, source, observed_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6))
                    ON DUPLICATE KEY UPDATE
                        candle_close_time = VALUES(candle_close_time),
                        observed_at = COALESCE(observed_at, VALUES(observed_at))
                    """, symbol, interval, Timestamp.from(openTime), Timestamp.from(closeTime), source,
                    observedAt == null ? null : Timestamp.from(observedAt));
        } catch (DataAccessException ex) {
            if (!isMissingDurableEventTable(ex)) {
                throw ex;
            }

            warnMissingDurableEventTable(symbol, interval, openTime);
        }
    }

    private boolean isMissingDurableEventTable(DataAccessException ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof java.sql.SQLException sqlException
                    && (sqlException.getErrorCode() == 1146 || "42S02".equals(sqlException.getSQLState()))) {
                String message = sqlException.getMessage();
                return message != null && message.contains("market_data_candle_event");
            }
            current = current.getCause();
        }
        return false;
    }

    private void warnMissingDurableEventTable(String symbol, String interval, Instant openTime) {
        long now = System.currentTimeMillis();
        long previous = lastMissingEventTableWarningMillis.get();
        if (now - previous >= MISSING_EVENT_TABLE_WARNING_INTERVAL_MS
                && lastMissingEventTableWarningMillis.compareAndSet(previous, now)) {
            log.warn("FIX-139 durable event table is missing; candle persistence continues and event creation will retry "
                            + "on the next closed candle. table=market_data_candle_event, symbol={}, interval={}, openTime={}",
                    symbol, interval, openTime);
        }
    }

    private BigDecimal decimal(JsonNode node, String field) {
        String value = node.path(field).asText();
        return value == null || value.isBlank() ? BigDecimal.ZERO : new BigDecimal(value);
    }
}
