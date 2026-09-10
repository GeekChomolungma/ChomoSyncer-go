-- Raw append-only kline fact table (design doc section 3.1).
--
-- This build ingests ONLY 1-minute klines from Binance. Every coarser interval
-- (5m / 15m / 1h / 4h / 1d ...) is a ClickHouse-side rollup of this table —
-- see 002_kline_rollups.sql. Do not create per-interval raw tables here.
--
-- ReplacingMergeTree(created_at) dedupes late re-sends of the same
-- (symbol, start_time) on background merge; queries use FINAL for read-time
-- dedup.

CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.fapi_kline_1m
(
    symbol                  LowCardinality(String),
    start_time              DateTime64(3, 'UTC'),
    end_time                DateTime64(3, 'UTC'),
    open                    Float64,
    high                    Float64,
    low                     Float64,
    close                   Float64,
    volume                  Float64,
    quote_volume            Float64,
    taker_buy_volume        Float64,
    taker_buy_quote_volume  Float64,
    trades_count            UInt32,
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;
