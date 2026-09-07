-- Raw append-only kline fact tables (design doc section 3.1).
-- ReplacingMergeTree(created_at) dedupes late re-sends of the same (symbol, start_time).

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

CREATE TABLE IF NOT EXISTS market.fapi_kline_1h
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
