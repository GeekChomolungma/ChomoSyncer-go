-- ============================================================================
-- Coarser-interval kline rollups, derived entirely from market.fapi_kline_1m.
--
-- WHY THIS SHAPE
--   The service ingests only 1m klines. Every coarser bar here is recomputed
--   from scratch with sum()/argMin()/argMax()/min()/max() over
--   `fapi_kline_1m FINAL`. There is NO incremental accumulator anywhere, so a
--   late re-send of a 1m row (reconnect replay, backfill overlap) can never
--   double-count into a rollup: FINAL dedupes the source, and the rollup row is
--   fully re-derived. This is the idempotency guarantee. Do NOT "optimise" this
--   into a SummingMergeTree / incremental AggregatingMergeTree MV — those
--   accumulate and WILL drift on 1m re-sends.
--
--   Target tables are ReplacingMergeTree(rollup_version). Each (re)computation
--   stamps rollup_version = now64(3); the newest write for a (symbol,start_time)
--   wins on merge, and queries use FINAL for read-time dedup — identical to how
--   fapi_kline_1m is consumed. Re-running any recompute is therefore safe.
--
-- BUCKET ALIGNMENT
--   toStartOfInterval(t, INTERVAL N ...) is anchored at the unix epoch
--   (1970-01-01 00:00:00 UTC). For 5m / 15m / 1h / 4h / 1d that lands exactly on
--   Binance's boundaries. It does NOT for 1w (epoch is a Thursday) or calendar
--   months — those are intentionally not offered here.
--
-- NOT auto-run by docker-compose (only 001 is). Apply it by hand — see below.
--
-- APPLY ORDER (two-phase cold start — see docs/OPERATIONS.md §A.1 / §B.1)
--   1. Phase one: run the collector with -backfill-offline-only so
--      fapi_kline_1m holds the full history.
--   2. Apply THIS file. The refreshable MVs then only pick up NEW live 1m rows.
--   3. Run 003_rollup_backfill.sql once to fold the already-present 1m history
--      into the rollup tables.
--   4. Phase two: start the live service.
--
--   On a FRESH empty DB (no deep-history phase one) just run this right after
--   001 — there is nothing to fold, and the MVs start tracking live 1m data.
--
--   Applying this WHILE a big backfill is still running is not *wrong* (the
--   refreshable MV and 003 both recompute from FINAL, never accumulate — so no
--   double-counting), just wasteful. That is why the recommended order builds
--   the rollup layer only after the 1m history has landed.
--
-- REQUIREMENTS
--   Refreshable materialized views need ClickHouse >= 24.8 (the SET below is
--   required on 24.x; on newer builds it is a harmless no-op, but if your server
--   rejects it as an unknown setting, just delete that one line). If refreshable
--   MVs are unavailable at all, delete every "CREATE MATERIALIZED VIEW ..."
--   block below and instead schedule 003_rollup_backfill.sql (rolling-window
--   variant) from cron / a systemd timer every 1-2 minutes.
-- ============================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- Drop the legacy physical 1h table from older 001_fapi_kline.sql, if present,
-- so the rollup table below can take its place. (No-op on fresh installs.)
DROP VIEW  IF EXISTS market.fapi_kline_1h_rmv;
DROP TABLE IF EXISTS market.fapi_kline_1h;

-- ---------------------------------------------------------------------------
-- 5m
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_kline_5m
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
    rollup_version          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_kline_5m_rmv
REFRESH EVERY 60 SECOND APPEND TO market.fapi_kline_5m AS
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 5 MINUTE)  AS bucket_start,
    max(end_time)                                     AS end_time,
    argMin(open,  start_time)                         AS open,
    max(high)                                         AS high,
    min(low)                                          AS low,
    argMax(close, start_time)                         AS close,
    sum(volume)                                       AS volume,
    sum(quote_volume)                                 AS quote_volume,
    sum(taker_buy_volume)                             AS taker_buy_volume,
    sum(taker_buy_quote_volume)                       AS taker_buy_quote_volume,
    toUInt32(sum(trades_count))                       AS trades_count,
    now64(3)                                          AS rollup_version
FROM market.fapi_kline_1m FINAL
WHERE start_time >= now() - INTERVAL 3 DAY
GROUP BY symbol, bucket_start;

-- ---------------------------------------------------------------------------
-- 15m
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_kline_15m
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
    rollup_version          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_kline_15m_rmv
REFRESH EVERY 60 SECOND APPEND TO market.fapi_kline_15m AS
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 15 MINUTE) AS bucket_start,
    max(end_time)                                     AS end_time,
    argMin(open,  start_time)                         AS open,
    max(high)                                         AS high,
    min(low)                                          AS low,
    argMax(close, start_time)                         AS close,
    sum(volume)                                       AS volume,
    sum(quote_volume)                                 AS quote_volume,
    sum(taker_buy_volume)                             AS taker_buy_volume,
    sum(taker_buy_quote_volume)                       AS taker_buy_quote_volume,
    toUInt32(sum(trades_count))                       AS trades_count,
    now64(3)                                          AS rollup_version
FROM market.fapi_kline_1m FINAL
WHERE start_time >= now() - INTERVAL 3 DAY
GROUP BY symbol, bucket_start;

-- ---------------------------------------------------------------------------
-- 1h
-- ---------------------------------------------------------------------------
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
    rollup_version          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_kline_1h_rmv
REFRESH EVERY 60 SECOND APPEND TO market.fapi_kline_1h AS
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 HOUR)    AS bucket_start,
    max(end_time)                                     AS end_time,
    argMin(open,  start_time)                         AS open,
    max(high)                                         AS high,
    min(low)                                          AS low,
    argMax(close, start_time)                         AS close,
    sum(volume)                                       AS volume,
    sum(quote_volume)                                 AS quote_volume,
    sum(taker_buy_volume)                             AS taker_buy_volume,
    sum(taker_buy_quote_volume)                       AS taker_buy_quote_volume,
    toUInt32(sum(trades_count))                       AS trades_count,
    now64(3)                                          AS rollup_version
FROM market.fapi_kline_1m FINAL
WHERE start_time >= now() - INTERVAL 3 DAY
GROUP BY symbol, bucket_start;

-- ---------------------------------------------------------------------------
-- 4h
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_kline_4h
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
    rollup_version          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_kline_4h_rmv
REFRESH EVERY 120 SECOND APPEND TO market.fapi_kline_4h AS
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 4 HOUR)    AS bucket_start,
    max(end_time)                                     AS end_time,
    argMin(open,  start_time)                         AS open,
    max(high)                                         AS high,
    min(low)                                          AS low,
    argMax(close, start_time)                         AS close,
    sum(volume)                                       AS volume,
    sum(quote_volume)                                 AS quote_volume,
    sum(taker_buy_volume)                             AS taker_buy_volume,
    sum(taker_buy_quote_volume)                       AS taker_buy_quote_volume,
    toUInt32(sum(trades_count))                       AS trades_count,
    now64(3)                                          AS rollup_version
FROM market.fapi_kline_1m FINAL
WHERE start_time >= now() - INTERVAL 7 DAY
GROUP BY symbol, bucket_start;

-- ---------------------------------------------------------------------------
-- 1d
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_kline_1d
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
    rollup_version          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_kline_1d_rmv
REFRESH EVERY 300 SECOND APPEND TO market.fapi_kline_1d AS
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 DAY)     AS bucket_start,
    max(end_time)                                     AS end_time,
    argMin(open,  start_time)                         AS open,
    max(high)                                         AS high,
    min(low)                                          AS low,
    argMax(close, start_time)                         AS close,
    sum(volume)                                       AS volume,
    sum(quote_volume)                                 AS quote_volume,
    sum(taker_buy_volume)                             AS taker_buy_volume,
    sum(taker_buy_quote_volume)                       AS taker_buy_quote_volume,
    toUInt32(sum(trades_count))                       AS trades_count,
    now64(3)                                          AS rollup_version
FROM market.fapi_kline_1m FINAL
WHERE start_time >= now() - INTERVAL 10 DAY
GROUP BY symbol, bucket_start;

-- ============================================================================
-- Adding another serve interval? Copy one block above and change ONLY:
--   * the table name          fapi_kline_<IV>
--   * the MV name             fapi_kline_<IV>_rmv
--   * both INTERVAL <N UNIT>  expressions (they must match)
--   * the REFRESH EVERY / WHERE lookback (>= a few target buckets wide)
-- Then add "<IV>" to collector.serve_intervals (so the derived kline_ready
-- fires) and to 003_rollup_backfill.sql (for the historical fold).
-- Only Xs/Xm/Xh multiples of 1m, or 1d, are supported — see config.Validate.
-- ============================================================================
