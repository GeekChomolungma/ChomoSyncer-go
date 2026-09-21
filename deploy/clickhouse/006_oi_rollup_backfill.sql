-- ============================================================================
-- One-time historical fold: populate fapi_oi_{15m,1h,4h,1d} from the 5m history
-- already in market.fapi_oi_5m BEFORE 005 was applied (or after re-importing archive
-- files). Idempotent for the same reason as 003: everything is recomputed from
-- `FINAL`, stamped rollup_version = now64(3), and the newest row per
-- (symbol, start_time) wins.
--
-- CHUNKING
--   Drive it per calendar month (month-aligned ranges are also bucket-aligned for
--   every interval here):
--     clickhouse-client --param_m_start='2024-01-01 00:00:00' \
--                       --param_m_end='2024-02-01 00:00:00' \
--                       --queries-file deploy/clickhouse/006_oi_rollup_backfill.sql
--   or wide open for a small table:
--     --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00'
--   IMPORTANT: m_start / m_end must be month boundaries (any UTC-midnight boundary is
--   bucket-aligned for these four intervals, but stick to months). A range that cuts a
--   bucket in half writes a partial bucket (see the LOOKBACK note in 005).
-- ============================================================================


-- 15m
INSERT INTO market.fapi_oi_15m
    (symbol, start_time, samples,
     sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 15 MINUTE) AS start_time_bucket,
    toUInt16(count()),
    argMax(sum_open_interest, start_time),
    max(sum_open_interest),
    min(sum_open_interest),
    now64(3)
FROM market.fapi_oi_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 1h
INSERT INTO market.fapi_oi_1h
    (symbol, start_time, samples,
     sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 HOUR) AS start_time_bucket,
    toUInt16(count()),
    argMax(sum_open_interest, start_time),
    max(sum_open_interest),
    min(sum_open_interest),
    now64(3)
FROM market.fapi_oi_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 4h
INSERT INTO market.fapi_oi_4h
    (symbol, start_time, samples,
     sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 4 HOUR) AS start_time_bucket,
    toUInt16(count()),
    argMax(sum_open_interest, start_time),
    max(sum_open_interest),
    min(sum_open_interest),
    now64(3)
FROM market.fapi_oi_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;

-- 1d
INSERT INTO market.fapi_oi_1d
    (symbol, start_time, samples,
     sum_open_interest_close, sum_open_interest_high, sum_open_interest_low,
     rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 DAY) AS start_time_bucket,
    toUInt16(count()),
    argMax(sum_open_interest, start_time),
    max(sum_open_interest),
    min(sum_open_interest),
    now64(3)
FROM market.fapi_oi_5m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, start_time_bucket;
