-- Apply to crypto_ai_v2 before deploying FIX-133 with publication enabled.
-- Additive: no history rewriting, no consumer-state changes, no backfill from
-- market_data_stream_version.observed_at (its timestamp provenance is mixed).
CREATE TABLE IF NOT EXISTS market_data_stream_ws_version (
 symbol VARCHAR(30) NOT NULL,
 interval_code VARCHAR(10) NOT NULL,
 open_time TIMESTAMP(6) NOT NULL,
 observed_at TIMESTAMP(6) NOT NULL,
 closed BOOLEAN NOT NULL,
 payload_hash VARCHAR(64) NOT NULL,
 PRIMARY KEY(symbol,interval_code,open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
