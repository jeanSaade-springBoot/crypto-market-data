-- Apply explicitly in crypto_ai_v2 using the schema owner's migration process.
-- Additive only. Never alter market_data_candle_event's existing consumer fields.
CREATE TABLE IF NOT EXISTS market_data_stream_owner (
 id INT PRIMARY KEY, owner_token VARCHAR(36) NOT NULL, expires_at TIMESTAMP(6) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS market_data_stream_cursor (
 symbol VARCHAR(30) PRIMARY KEY, last_sequence BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS market_data_stream_version (
 symbol VARCHAR(30) NOT NULL, interval_code VARCHAR(10) NOT NULL, open_time TIMESTAMP(6) NOT NULL,
 observed_at TIMESTAMP(6) NULL, closed BOOLEAN NOT NULL, payload_hash VARCHAR(64) NOT NULL,
 PRIMARY KEY(symbol,interval_code,open_time)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS market_data_stream_event (
 id BIGINT PRIMARY KEY AUTO_INCREMENT, symbol VARCHAR(30) NOT NULL, symbol_sequence BIGINT NOT NULL,
 interval_code VARCHAR(10) NOT NULL, candle_open_time TIMESTAMP(6) NOT NULL,
 candle_close_time TIMESTAMP(6) NOT NULL, closed BOOLEAN NOT NULL,
 observed_at TIMESTAMP(6) NULL, received_at TIMESTAMP(6) NOT NULL,
 price DECIMAL(30,12) NOT NULL, source VARCHAR(30) NOT NULL,
 classification VARCHAR(40) NOT NULL, payload_hash VARCHAR(64) NOT NULL, payload_version INT NOT NULL DEFAULT 1,
 created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY uk_stream_symbol_sequence(symbol,symbol_sequence)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS market_data_stream_lane (
 symbol VARCHAR(30) NOT NULL, interval_code VARCHAR(10) NOT NULL,
 observed_at TIMESTAMP(6) NOT NULL, open_time TIMESTAMP(6) NOT NULL,
 PRIMARY KEY(symbol,interval_code)
) ENGINE=InnoDB;
