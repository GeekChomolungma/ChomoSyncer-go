-- ============================================================================
-- Coarser-interval rollups of the 5m open-interest table (004): 15m / 1h / 4h / 1d / 1mo.
--
-- REBUILD, NOT MIGRATE (same contract as 002_kline_rollups.sql)
--   Applying this file DROPS the five refreshable MVs and the five rollup tables and
--   recreates them EMPTY; market.fapi_oi_5m is never touched. Run
--   006_oi_rollup_backfill.sql afterwards to refill the history. A wrong rollup layer
--   is cheaper to recompute from the 5m table than to patch. (Do NOT confuse the
--   rollups with fapi_oi_5m itself: that table holds live snapshots that cannot be
--   replayed and must never be dropped.)
--
-- WHY THIS SHAPE
--   Every coarse row is recomputed from scratch over `fapi_oi_5m FINAL`: no
--   accumulator, no 15m -> 1h -> 4h cascade (each level reads the 5m table directly).
--   A later hist/archive overwrite of a 5m row therefore re-derives every parent on
--   the next refresh and can never leave a stale or double-counted one. Targets are
--   ReplacingMergeTree(rollup_version); readers use FINAL. Do NOT turn these into
--   SummingMergeTree / incremental MVs.
--
-- LOOKBACK MUST BE BUCKET-ALIGNED AND UTC-ANCHORED
--   Each refresh recomputes only rows newer than a lookback bound. A bound in the
--   middle of a bucket makes the oldest bucket be aggregated from a partial slice,
--   and — stamped with the newest rollup_version — REPLACE the correct row. So every
--   WHERE snaps the bound down to a bucket start, computed in UTC:
--       start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
--   now() carries the SERVER timezone; without toTimeZone(.., 'UTC') the 1d bound would
--   snap to the server's local midnight (Asia/Shanghai = 16:00 UTC) and cut a UTC day
--   in half. See deploy/clickhouse-fixes/README.md.
--
-- COLUMN SEMANTICS  (a 5m row labelled t holds the OI at t + 5m, the kline's close)
--   samples                   number of 5m rows in the bucket. A complete bucket has
--                             3 / 12 / 48 / 288 (15m/1h/4h/1d) or days_in_month*288
--                             (1mo). Filter on it before use.
--   sum_open_interest_close   OI at the bucket's close = the row with the largest
--                             start_time in the bucket (= bucket_end - 5m label).
--   sum_open_interest_high/low  max / min of the closing snapshots inside the bucket.
--   There is no `open`: a bucket's opening OI is the previous bucket's close (use lag()).
--
-- MONTH BUCKETS
--   toStartOfMonth is calendar-aware (toStartOfInterval is anchored at the epoch and
--   is not for months/weeks), so 1mo is offered here.
--
-- APPLY ORDER
--   004 -> load history into fapi_oi_5m -> this file -> 006 (fold the history).
--   Same rationale as 002/003: build the rollup layer after the bulk load.
--
-- REQUIREMENTS
--   ClickHouse >= 24.10 (refreshable MV + APPEND). Not auto-run by docker-compose.
--   No refreshable-MV support: drop the CREATE MATERIALIZED VIEW blocks and run 006
--   from cron with a rolling window (idempotent).
-- ============================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- >>> REBUILD-DROPS (start)
-- Views first (they write into the tables). fapi_oi_5m is NOT in this block.
DROP VIEW  IF EXISTS market.fapi_oi_15m_rmv;
DROP VIEW  IF EXISTS market.fapi_oi_1h_rmv;
DROP VIEW  IF EXISTS market.fapi_oi_4h_rmv;
DROP VIEW  IF EXISTS market.fapi_oi_1d_rmv;
DROP VIEW  IF EXISTS market.fapi_oi_1mo_rmv;
DROP TABLE IF EXISTS market.fapi_oi_15m;
DROP TABLE IF EXISTS market.fapi_oi_1h;
DROP TABLE IF EXISTS market.fapi_oi_4h;
DROP TABLE IF EXISTS market.fapi_oi_1d;
DROP TABLE IF EXISTS market.fapi_oi_1mo;
-- <<< REBUILD-DROPS (end)


-- ---------------------------------------------------------------------------
-- 15m
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_oi_15m
(
    symbol                   LowCardinality(String),
    start_time               DateTime64(3, 'UTC'),
    samples                  UInt16,
    sum_open_interest_close  Float64,
    sum_open_interest_high   Float64,
    sum_open_interest_low    Float64,
    rollup_version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_oi_15m_rmv
REFRESH EVERY 60 SECOND APPEND TO market.fapi_oi_15m AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 15 MINUTE)                                   AS bucket_start,
        toUInt16(count())                          AS samples,
        argMax(sum_open_interest, start_time)      AS sum_open_interest_close,
        max(sum_open_interest)                     AS sum_open_interest_high,
        min(sum_open_interest)                     AS sum_open_interest_low,
        now64(3)                                   AS rollup_version
    FROM market.fapi_oi_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY,  INTERVAL 15 MINUTE)
    GROUP BY symbol, bucket_start
);

-- ---------------------------------------------------------------------------
-- 1h
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_oi_1h
(
    symbol                   LowCardinality(String),
    start_time               DateTime64(3, 'UTC'),
    samples                  UInt16,
    sum_open_interest_close  Float64,
    sum_open_interest_high   Float64,
    sum_open_interest_low    Float64,
    rollup_version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_oi_1h_rmv
REFRESH EVERY 120 SECOND APPEND TO market.fapi_oi_1h AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 1 HOUR)                                   AS bucket_start,
        toUInt16(count())                          AS samples,
        argMax(sum_open_interest, start_time)      AS sum_open_interest_close,
        max(sum_open_interest)                     AS sum_open_interest_high,
        min(sum_open_interest)                     AS sum_open_interest_low,
        now64(3)                                   AS rollup_version
    FROM market.fapi_oi_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY,  INTERVAL 1 HOUR)
    GROUP BY symbol, bucket_start
);

-- ---------------------------------------------------------------------------
-- 4h
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_oi_4h
(
    symbol                   LowCardinality(String),
    start_time               DateTime64(3, 'UTC'),
    samples                  UInt16,
    sum_open_interest_close  Float64,
    sum_open_interest_high   Float64,
    sum_open_interest_low    Float64,
    rollup_version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_oi_4h_rmv
REFRESH EVERY 300 SECOND APPEND TO market.fapi_oi_4h AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 4 HOUR)                                   AS bucket_start,
        toUInt16(count())                          AS samples,
        argMax(sum_open_interest, start_time)      AS sum_open_interest_close,
        max(sum_open_interest)                     AS sum_open_interest_high,
        min(sum_open_interest)                     AS sum_open_interest_low,
        now64(3)                                   AS rollup_version
    FROM market.fapi_oi_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 7 DAY,  INTERVAL 4 HOUR)
    GROUP BY symbol, bucket_start
);

-- ---------------------------------------------------------------------------
-- 1d
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_oi_1d
(
    symbol                   LowCardinality(String),
    start_time               DateTime64(3, 'UTC'),
    samples                  UInt16,
    sum_open_interest_close  Float64,
    sum_open_interest_high   Float64,
    sum_open_interest_low    Float64,
    rollup_version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_oi_1d_rmv
REFRESH EVERY 600 SECOND APPEND TO market.fapi_oi_1d AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 1 DAY)                                   AS bucket_start,
        toUInt16(count())                          AS samples,
        argMax(sum_open_interest, start_time)      AS sum_open_interest_close,
        max(sum_open_interest)                     AS sum_open_interest_high,
        min(sum_open_interest)                     AS sum_open_interest_low,
        now64(3)                                   AS rollup_version
    FROM market.fapi_oi_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 10 DAY, INTERVAL 1 DAY)
    GROUP BY symbol, bucket_start
);

-- ---------------------------------------------------------------------------
-- 1mo
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_oi_1mo
(
    symbol                   LowCardinality(String),
    start_time               DateTime64(3, 'UTC'),
    samples                  UInt16,
    sum_open_interest_close  Float64,
    sum_open_interest_high   Float64,
    sum_open_interest_low    Float64,
    rollup_version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_oi_1mo_rmv
REFRESH EVERY 900 SECOND APPEND TO market.fapi_oi_1mo AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toDateTime64(toStartOfMonth(start_time), 3, 'UTC')                                   AS bucket_start,
        toUInt16(count())                          AS samples,
        argMax(sum_open_interest, start_time)      AS sum_open_interest_close,
        max(sum_open_interest)                     AS sum_open_interest_high,
        min(sum_open_interest)                     AS sum_open_interest_low,
        now64(3)                                   AS rollup_version
    FROM market.fapi_oi_5m FINAL
    WHERE start_time >= toDateTime64(toStartOfMonth(toTimeZone(now(), 'UTC') - INTERVAL 40 DAY), 3, 'UTC')
    GROUP BY symbol, bucket_start
);

-- ============================================================================
-- Adding another interval? Copy one block above and change ONLY the table name, the MV
-- name, the bucket expression (it appears once, in the inner SELECT), and the lookback
-- (>= a few target buckets wide, snapped to a bucket start in UTC as shown above); then
-- add the same block to 006_oi_rollup_backfill.sql.
-- ============================================================================
