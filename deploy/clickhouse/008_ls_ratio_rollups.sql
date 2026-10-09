-- ============================================================================
-- Coarser-interval rollups of the 5m long/short ratio table (007): 15m / 1h / 4h / 1d.
--
-- REBUILD, NOT MIGRATE (same contract as 005_oi_rollups.sql)
--   Applying this file DROPS the four refreshable MVs and the four rollup tables and
--   recreates them EMPTY; market.fapi_ls_ratio_5m is never touched. Run
--   009_ls_ratio_rollup_backfill.sql (rollup_ls_ratio_per_month.sh) afterwards to
--   refill the history.
--
-- WHY THIS SHAPE (see 005 for the long version)
--   Every coarse row is recomputed from scratch over `fapi_ls_ratio_5m FINAL`, each
--   level directly from the 5m table, into ReplacingMergeTree(rollup_version); readers
--   use FINAL. A later archive overwrite of a 5m row re-derives its parents on the next
--   refresh. Lookbacks are snapped down to a bucket start IN UTC (toTimeZone(now(),
--   'UTC')): the server runs in Asia/Shanghai and a local-midnight bound would cut a UTC
--   day in half (deploy/clickhouse-fixes/README.md).
--
-- COLUMN SEMANTICS  (a 5m row labelled t holds the ratios at t + 5m, the kline's close)
--   samples        number of 5m rows in the bucket: 3 / 12 / 48 / 288 when complete.
--   <ratio>_close  the ratio at the bucket's close = the newest 5m row in the bucket
--                  that has a value for it (argMax skips NULLs).
--   <ratio>_high / _low  max / min of the closing snapshots inside the bucket.
--   A ratio column is NULL only when no 5m row of the bucket had a value for it.
--   There is no `open`: a bucket's opening ratio is the previous bucket's close.
--
-- APPLY ORDER
--   007 -> import the archive (cmd/test-tools/import_ls_ratio_archive.py) -> this file
--   -> rollup_ls_ratio_per_month.sh (009) -> enable open_interest.ls_ratio (its start-up
--   pass fills the days after the newest archive file).
--
-- REQUIREMENTS
--   ClickHouse >= 24.10 (refreshable MV + APPEND). Not auto-run by docker-compose.
-- ============================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- >>> REBUILD-DROPS (start)
-- Views first (they write into the tables). fapi_ls_ratio_5m is NOT in this block.
DROP VIEW  IF EXISTS market.fapi_ls_ratio_15m_rmv;
DROP VIEW  IF EXISTS market.fapi_ls_ratio_1h_rmv;
DROP VIEW  IF EXISTS market.fapi_ls_ratio_4h_rmv;
DROP VIEW  IF EXISTS market.fapi_ls_ratio_1d_rmv;
DROP TABLE IF EXISTS market.fapi_ls_ratio_15m;
DROP TABLE IF EXISTS market.fapi_ls_ratio_1h;
DROP TABLE IF EXISTS market.fapi_ls_ratio_4h;
DROP TABLE IF EXISTS market.fapi_ls_ratio_1d;
-- <<< REBUILD-DROPS (end)


-- ---------------------------------------------------------------------------
-- 15m
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_ls_ratio_15m
(
    symbol                                   LowCardinality(String),
    start_time                               DateTime64(3, 'UTC'),
    samples                                  UInt16,
    count_toptrader_long_short_ratio_close   Nullable(Float64),
    count_toptrader_long_short_ratio_high    Nullable(Float64),
    count_toptrader_long_short_ratio_low     Nullable(Float64),
    sum_toptrader_long_short_ratio_close     Nullable(Float64),
    sum_toptrader_long_short_ratio_high      Nullable(Float64),
    sum_toptrader_long_short_ratio_low       Nullable(Float64),
    count_long_short_ratio_close             Nullable(Float64),
    count_long_short_ratio_high              Nullable(Float64),
    count_long_short_ratio_low               Nullable(Float64),
    rollup_version                           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_ls_ratio_15m_rmv
REFRESH EVERY 60 SECOND APPEND TO market.fapi_ls_ratio_15m AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 15 MINUTE) AS bucket_start,
        toUInt16(count()) AS samples,
        argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
        max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
        min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
        argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
        max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
        min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
        argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
        max(count_long_short_ratio) AS count_long_short_ratio_high,
        min(count_long_short_ratio) AS count_long_short_ratio_low,
        now64(3) AS rollup_version
    FROM market.fapi_ls_ratio_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 15 MINUTE)
    GROUP BY symbol, bucket_start
);


-- ---------------------------------------------------------------------------
-- 1h
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_ls_ratio_1h
(
    symbol                                   LowCardinality(String),
    start_time                               DateTime64(3, 'UTC'),
    samples                                  UInt16,
    count_toptrader_long_short_ratio_close   Nullable(Float64),
    count_toptrader_long_short_ratio_high    Nullable(Float64),
    count_toptrader_long_short_ratio_low     Nullable(Float64),
    sum_toptrader_long_short_ratio_close     Nullable(Float64),
    sum_toptrader_long_short_ratio_high      Nullable(Float64),
    sum_toptrader_long_short_ratio_low       Nullable(Float64),
    count_long_short_ratio_close             Nullable(Float64),
    count_long_short_ratio_high              Nullable(Float64),
    count_long_short_ratio_low               Nullable(Float64),
    rollup_version                           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_ls_ratio_1h_rmv
REFRESH EVERY 120 SECOND APPEND TO market.fapi_ls_ratio_1h AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 1 HOUR) AS bucket_start,
        toUInt16(count()) AS samples,
        argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
        max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
        min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
        argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
        max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
        min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
        argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
        max(count_long_short_ratio) AS count_long_short_ratio_high,
        min(count_long_short_ratio) AS count_long_short_ratio_low,
        now64(3) AS rollup_version
    FROM market.fapi_ls_ratio_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
    GROUP BY symbol, bucket_start
);


-- ---------------------------------------------------------------------------
-- 4h
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_ls_ratio_4h
(
    symbol                                   LowCardinality(String),
    start_time                               DateTime64(3, 'UTC'),
    samples                                  UInt16,
    count_toptrader_long_short_ratio_close   Nullable(Float64),
    count_toptrader_long_short_ratio_high    Nullable(Float64),
    count_toptrader_long_short_ratio_low     Nullable(Float64),
    sum_toptrader_long_short_ratio_close     Nullable(Float64),
    sum_toptrader_long_short_ratio_high      Nullable(Float64),
    sum_toptrader_long_short_ratio_low       Nullable(Float64),
    count_long_short_ratio_close             Nullable(Float64),
    count_long_short_ratio_high              Nullable(Float64),
    count_long_short_ratio_low               Nullable(Float64),
    rollup_version                           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_ls_ratio_4h_rmv
REFRESH EVERY 300 SECOND APPEND TO market.fapi_ls_ratio_4h AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 4 HOUR) AS bucket_start,
        toUInt16(count()) AS samples,
        argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
        max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
        min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
        argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
        max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
        min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
        argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
        max(count_long_short_ratio) AS count_long_short_ratio_high,
        min(count_long_short_ratio) AS count_long_short_ratio_low,
        now64(3) AS rollup_version
    FROM market.fapi_ls_ratio_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 7 DAY, INTERVAL 4 HOUR)
    GROUP BY symbol, bucket_start
);


-- ---------------------------------------------------------------------------
-- 1d
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.fapi_ls_ratio_1d
(
    symbol                                   LowCardinality(String),
    start_time                               DateTime64(3, 'UTC'),
    samples                                  UInt16,
    count_toptrader_long_short_ratio_close   Nullable(Float64),
    count_toptrader_long_short_ratio_high    Nullable(Float64),
    count_toptrader_long_short_ratio_low     Nullable(Float64),
    sum_toptrader_long_short_ratio_close     Nullable(Float64),
    sum_toptrader_long_short_ratio_high      Nullable(Float64),
    sum_toptrader_long_short_ratio_low       Nullable(Float64),
    count_long_short_ratio_close             Nullable(Float64),
    count_long_short_ratio_high              Nullable(Float64),
    count_long_short_ratio_low               Nullable(Float64),
    rollup_version                           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rollup_version)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.fapi_ls_ratio_1d_rmv
REFRESH EVERY 600 SECOND APPEND TO market.fapi_ls_ratio_1d AS
SELECT
    symbol,
    bucket_start AS start_time,
    samples,
    count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
    rollup_version
FROM
(
    SELECT
        symbol,
        toStartOfInterval(start_time, INTERVAL 1 DAY) AS bucket_start,
        toUInt16(count()) AS samples,
        argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
        max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
        min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
        argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
        max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
        min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
        argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
        max(count_long_short_ratio) AS count_long_short_ratio_high,
        min(count_long_short_ratio) AS count_long_short_ratio_low,
        now64(3) AS rollup_version
    FROM market.fapi_ls_ratio_5m FINAL
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 10 DAY, INTERVAL 1 DAY)
    GROUP BY symbol, bucket_start
);

-- ============================================================================
-- Adding another interval? Copy one block above and change ONLY the table name, the MV
-- name, the bucket expression and the lookback (>= a few buckets wide, snapped to a
-- bucket start in UTC); then add the same block to 009_ls_ratio_rollup_backfill.sql.
-- ============================================================================
