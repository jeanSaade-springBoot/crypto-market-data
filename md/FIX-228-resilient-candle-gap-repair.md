# FIX-228 — Resilient Candle Gap Repair

## Production evidence

One-minute candles were missing at shared timestamps across many symbols. The previous collector
performed synchronous REST repair before persisting a WebSocket close. A REST exception therefore
skipped the live persistence call, while latest-only reconciliation could not discover an internal
hole after newer candles had arrived.

## Correction

- WebSocket callbacks persist received candles without making REST calls.
- Periodic/startup reconciliation searches a bounded recent window for internal gaps.
- Binance REST is called only for proven gaps and only the missing range is persisted.
- Repaired candle writes and durable-event writes remain transactional and idempotent.
- Existing stale-event protections in `crypto-ai-next` remain authoritative; repair never causes
  retroactive Production trading.

## Configuration

- `BINANCE_GAP_LOOKBACK_HOURS` defaults to 24 and is capped in code at 168.
- `BINANCE_MAX_INTERNAL_GAPS_PER_PASS` defaults to 25 and is capped in code at 250.

This fix changes infrastructure integrity only. It does not change signal, entry, exit, blocker, or
position-sizing behavior.
