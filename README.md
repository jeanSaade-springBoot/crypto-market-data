# crypto-market-data — FIX-139

Independent Binance candle ingestion service for `crypto-ai-next`.

## Ownership

This service owns:
- Binance kline WebSocket collection
- candle upserts
- startup reconciliation
- WebSocket gap detection + REST repair
- durable `market_data_candle_event` creation

It deliberately does **not** own Flyway or trading decisions. `crypto-ai-next` remains the schema owner and the only trading authority.

## Deployment order

1. Deploy the updated `crypto-ai-next` once so Flyway V92 creates `market_data_candle_event`.
2. Start `crypto-market-data` and verify `FIX-139 Binance candle collector connected` in its log.
3. Set `CANDLE_INGESTION_MODE=EXTERNAL_DURABLE` and restart `crypto-ai-next` only after the collector is confirmed healthy. The supplied default remains `LOCAL` so deployment itself cannot create a gap.
4. Keep the existing crypto-ai-next Binance WebSocket enabled: in FIX-139 it remains the canonical live 1m feed for mechanical position protection, but it no longer owns candle persistence or closed-candle analysis events.

Emergency rollback: stop the collector and set `CANDLE_INGESTION_MODE=LOCAL` before restarting `crypto-ai-next`.
