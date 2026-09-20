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
-- MV SHAPE — why the inner subquery
--   26.10+ requires a `... TO <table>` MV's SELECT output column names to match
--   the target table, so column 2 must be named `start_time`. But we also filter
--   and argMin/argMax on the RAW 1m `start_time`. If the bucket expression were
--   aliased `AS start_time` directly it would shadow the raw column and either
--   break analysis or silently make argMin(open, start_time) pick by bucket.
--   So: the inner query aggregates with the bucket aliased `bucket_start` (raw
--   `start_time` stays unambiguous); the outer query only renames it to
--   `start_time` for the target-table match.
--
-- LOOKBACK MUST BE BUCKET-ALIGNED AND UTC-ANCHORED
--   Each refresh recomputes only 1m rows newer than a lookback bound. If that bound
--   falls in the MIDDLE of a bucket, the oldest bucket is aggregated from a partial
--   slice and — stamped with the newest rollup_version — REPLACES the correct row;
--   as the bound slides forward, every bucket it passes ends up holding only its
--   last few minutes. So every WHERE below snaps the bound down to a bucket start:
--       start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
--   The toTimeZone(now(), 'UTC') anchor matters: now() carries the SERVER timezone,
--   and toStartOfInterval(.., INTERVAL 1 DAY) would snap to the server's local
--   midnight (Asia/Shanghai = 16:00 UTC), cutting a UTC day bucket in half.
--   Older versions of this file used a raw `now() - INTERVAL 3 DAY` and corrupted
--   rollup rows outside the lookback window; repair with
--   deploy/clickhouse-fixes/001_fix_kline_rollup_lookback.sh.
--
-- REBUILD, NOT MIGRATE: this file is a full rebuild of the rollup layer. Re-running
-- it drops and recreates all derived tables empty (fapi_kline_1m is untouched), so
-- it must always be followed by 003. That is deliberate: a rollup layer that is
-- wrong is cheaper to recompute from 1m than to patch.
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
--   The refreshable-MV + APPEND syntax used below needs ClickHouse >= 24.10
--   (24.8 parses REFRESH but not APPEND). The SET line is required on 24.x and a
--   harmless no-op on newer builds; if your server rejects it as an unknown
--   setting, just delete that line. If refreshable MVs are unavailable at all,
--   delete every "CREATE MATERIALIZED VIEW ..." block below and instead schedule
--   003_rollup_backfill.sql (rolling-window variant) from cron / a systemd timer
--   every 1-2 minutes.
-- ============================================================================

SET allow_experimental_refreshable_materialized_view = 1;

-- >>> REBUILD-DROPS (start)
-- REBUILD SEMANTICS: applying this file DROPS every derived rollup view and table
-- (5m / 15m / 1h / 4h / 1d) and recreates them EMPTY. fapi_kline_1m is never
-- touched. Run 003_rollup_backfill.sql afterwards to refill the history.
-- Views go first (they write into the tables); this also covers the legacy
-- physical 1h table of older 001_fapi_kline.sql. On a fresh install every
-- statement is a no-op.
-- (deploy/clickhouse-fixes/001_fix_kline_rollup_lookback.sh strips exactly this
-- block, between the two REBUILD-DROPS markers, for its partial-range mode —
-- keep the markers.)
DROP VIEW  IF EXISTS market.fapi_kline_5m_rmv;
DROP VIEW  IF EXISTS market.fapi_kline_15m_rmv;
DROP VIEW  IF EXISTS market.fapi_kline_1h_rmv;
DROP VIEW  IF EXISTS market.fapi_kline_4h_rmv;
DROP VIEW  IF EXISTS market.fapi_kline_1d_rmv;
DROP TABLE IF EXISTS market.fapi_kline_5m;
DROP TABLE IF EXISTS market.fapi_kline_15m;
DROP TABLE IF EXISTS market.fapi_kline_1h;
DROP TABLE IF EXISTS market.fapi_kline_4h;
DROP TABLE IF EXISTS market.fapi_kline_1d;
-- <<< REBUILD-DROPS (end)

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
    bucket_start AS start_time,
    end_time, open, high, low, close,
    volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
    trades_count, rollup_version
FROM
(
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
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 5 MINUTE)
    GROUP BY symbol, bucket_start
);

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
    bucket_start AS start_time,
    end_time, open, high, low, close,
    volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
    trades_count, rollup_version
FROM
(
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
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 15 MINUTE)
    GROUP BY symbol, bucket_start
);

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
    bucket_start AS start_time,
    end_time, open, high, low, close,
    volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
    trades_count, rollup_version
FROM
(
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
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
    GROUP BY symbol, bucket_start
);

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
    bucket_start AS start_time,
    end_time, open, high, low, close,
    volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
    trades_count, rollup_version
FROM
(
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
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 7 DAY, INTERVAL 4 HOUR)
    GROUP BY symbol, bucket_start
);

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
    bucket_start AS start_time,
    end_time, open, high, low, close,
    volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
    trades_count, rollup_version
FROM
(
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
    WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 10 DAY, INTERVAL 1 DAY)
    GROUP BY symbol, bucket_start
);

-- ============================================================================
-- Adding another serve interval? Copy one block above and change ONLY:
--   * the table name          fapi_kline_<IV>
--   * the MV name             fapi_kline_<IV>_rmv
--   * both INTERVAL <N UNIT>  expressions in the inner query (they must match)
--   * the REFRESH EVERY / WHERE lookback (>= a few target buckets wide, and keep the
--     bound snapped to a bucket start in UTC as shown above)
-- Then add "<IV>" to collector.serve_intervals (so the derived kline_ready
-- fires) and to 003_rollup_backfill.sql (for the historical fold).
-- Only Xs/Xm/Xh multiples of 1m, or 1d, are supported — see config.Validate.
-- ============================================================================
