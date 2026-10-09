-- Raw 5-minute long/short ratio fact table (design doc: new_requirements/oi.md §2, §7).
--
-- One row = one symbol's three long/short ratios AT THE CLOSE of one 5m kline:
--   start_time = the kline's OPEN time, exactly like market.fapi_kline_5m.start_time
--   and market.fapi_oi_5m.start_time, so all three join on (symbol, start_time). The
--   values are the snapshot taken at start_time + 5m, parallel to that kline's `close`.
--
-- Columns <- sources (/futures/data REST field `longShortRatio` | archive metrics CSV column):
--   count_toptrader_long_short_ratio  topLongShortAccountRatio    | count_toptrader_long_short_ratio
--   sum_toptrader_long_short_ratio    topLongShortPositionRatio   | sum_toptrader_long_short_ratio
--   count_long_short_ratio            globalLongShortAccountRatio | count_long_short_ratio
-- A ratio is long/short, so the long share is r / (1 + r); the shares are not stored.
--
-- Two writers, one key (there is no live endpoint for these ratios). src_rank decides
-- who wins on merge (highest rank kept; equal rank -> last inserted wins):
--   2 = hist     the three REST endpoints, merged per label (label L is written to L - 5m)
--   3 = archive  data.binance.vision daily metrics files, see the ALIGNMENT note
-- ReplacingMergeTree replaces WHOLE rows, so every writer must write all three columns
-- of a row at once. The hist reconciler therefore fetches the three endpoints for a
-- symbol before writing anything; a column is NULL only where its endpoint (or the
-- archive file) genuinely has no value for that bar.
--
-- ALIGNMENT (measured 2026-10-09, see new_requirements/oi.md §3.5)
--   REST label L is the state at L (published ~40s after L)       -> start_time = L - 5m.
--   Archive create_time T changed meaning on 2024-03-04:
--     files dated <= 2024-03-03: snapshot at T                    -> start_time = T - 5m
--     files dated >= 2024-03-04: snapshot at T + 5m (= REST T+5m) -> start_time = T
--   snap_time is the snapshot instant in both cases (= start_time + 5m).
--
-- Unlike fapi_oi_5m this table holds nothing that cannot be re-fetched (hist keeps ~30
-- days, the archive keeps everything), but treat it like 004 anyway: never drop it.
--
-- Rollups: 008_ls_ratio_rollups.sql (15m / 1h / 4h / 1d).

CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.fapi_ls_ratio_5m
(
    symbol                            LowCardinality(String),
    start_time                        DateTime64(3, 'UTC'),
    count_toptrader_long_short_ratio  Nullable(Float64),
    sum_toptrader_long_short_ratio    Nullable(Float64),
    count_long_short_ratio            Nullable(Float64),
    snap_time                         DateTime64(3, 'UTC'),
    src_rank                          UInt8,               -- 2 = hist, 3 = archive
    created_at                        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(src_rank)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;
