-- ============================================================================
-- One-time historical fold: populate fapi_ls_ratio_{15m,1h,4h,1d} from the 5m history
-- in market.fapi_ls_ratio_5m (after 008, or after re-importing archive files).
-- Idempotent for the same reason as 006: everything is recomputed from `FINAL`,
-- stamped rollup_version = now64(3), and the newest row per (symbol, start_time) wins.
--
-- Drive it per calendar month (month boundaries are bucket-aligned for every interval
-- here; a range that cuts a bucket writes a partial one):
--     ./rollup_ls_ratio_per_month.sh --password '<pw>'
-- or by hand:
--     clickhouse-client --param_m_start='2024-01-01 00:00:00' \
--                       --param_m_end='2024-02-01 00:00:00' \
--                       --queries-file deploy/clickhouse/009_ls_ratio_rollup_backfill.sql
-- ============================================================================

-- 15m
INSERT INTO market.fapi_ls_ratio_15m
    (symbol, start_time, samples,
     count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 15 MINUTE) AS start_time_bucket,
    toUInt16(count()),
    argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
    max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
    min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
    argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
    max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
    min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
    argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
    max(count_long_short_ratio) AS count_long_short_ratio_high,
    min(count_long_short_ratio) AS count_long_short_ratio_low,
    now64(3)
FROM market.fapi_ls_ratio_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 1h
INSERT INTO market.fapi_ls_ratio_1h
    (symbol, start_time, samples,
     count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 HOUR) AS start_time_bucket,
    toUInt16(count()),
    argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
    max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
    min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
    argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
    max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
    min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
    argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
    max(count_long_short_ratio) AS count_long_short_ratio_high,
    min(count_long_short_ratio) AS count_long_short_ratio_low,
    now64(3)
FROM market.fapi_ls_ratio_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 4h
INSERT INTO market.fapi_ls_ratio_4h
    (symbol, start_time, samples,
     count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 4 HOUR) AS start_time_bucket,
    toUInt16(count()),
    argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
    max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
    min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
    argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
    max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
    min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
    argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
    max(count_long_short_ratio) AS count_long_short_ratio_high,
    min(count_long_short_ratio) AS count_long_short_ratio_low,
    now64(3)
FROM market.fapi_ls_ratio_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 1d
INSERT INTO market.fapi_ls_ratio_1d
    (symbol, start_time, samples,
     count_toptrader_long_short_ratio_close, count_toptrader_long_short_ratio_high, count_toptrader_long_short_ratio_low, sum_toptrader_long_short_ratio_close, sum_toptrader_long_short_ratio_high, sum_toptrader_long_short_ratio_low, count_long_short_ratio_close, count_long_short_ratio_high, count_long_short_ratio_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 DAY) AS start_time_bucket,
    toUInt16(count()),
    argMax(count_toptrader_long_short_ratio, start_time) AS count_toptrader_long_short_ratio_close,
    max(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_high,
    min(count_toptrader_long_short_ratio) AS count_toptrader_long_short_ratio_low,
    argMax(sum_toptrader_long_short_ratio, start_time) AS sum_toptrader_long_short_ratio_close,
    max(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_high,
    min(sum_toptrader_long_short_ratio) AS sum_toptrader_long_short_ratio_low,
    argMax(count_long_short_ratio, start_time) AS count_long_short_ratio_close,
    max(count_long_short_ratio) AS count_long_short_ratio_high,
    min(count_long_short_ratio) AS count_long_short_ratio_low,
    now64(3)
FROM market.fapi_ls_ratio_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;
