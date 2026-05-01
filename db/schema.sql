-- Connect to the market_data database
-- \c market_data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the OHLCV table
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    time        TIMESTAMPTZ       NOT NULL,
    symbol      TEXT              NOT NULL,
    open        DOUBLE PRECISION  NULL,
    high        DOUBLE PRECISION  NULL,
    low         DOUBLE PRECISION  NULL,
    close       DOUBLE PRECISION  NULL,
    volume      BIGINT            NULL,
    PRIMARY KEY (time, symbol)
);

-- Convert the standard Postgres table into a TimescaleDB hypertable partitioned by time
-- Chunk time interval is set to 1 month (ideal for daily data)
SELECT create_hypertable('daily_ohlcv', 'time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

-- Create an index on the symbol for fast filtering
CREATE INDEX IF NOT EXISTS ix_symbol_time ON daily_ohlcv (symbol, time DESC);

-- Example continuous aggregate for Weekly data (optional but highly optimized)
-- CREATE MATERIALIZED VIEW weekly_ohlcv
-- WITH (timescaledb.continuous) AS
-- SELECT time_bucket('1 week', time) AS bucket,
--        symbol,
--        first(open, time) AS open,
--        max(high) AS high,
--        min(low) AS low,
--        last(close, time) AS close,
--        sum(volume) AS volume
-- FROM daily_ohlcv
-- GROUP BY bucket, symbol;
