# FIX-231 — Enable native Binance 15m candles

Add `15m` to the collector defaults. The existing Binance WebSocket subscription, 500-row REST bootstrap, idempotent candle persistence and durable close events apply without a new table or interval implementation. If `BINANCE_INTERVALS` is explicitly configured in production, add `15m` there too (for example `1m,5m,15m,1h,4h`). Deploy this collector alongside the crypto-ai-next FIX-231 overlay. Bootstrap only writes closed historical context; stale close events cannot authorize retroactive live execution.
