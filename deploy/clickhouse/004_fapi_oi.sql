-- Raw 5-minute open-interest fact table (design doc: new_requirements/oi.md).
--
-- One row = one symbol's open interest AT THE CLOSE of one 5m kline:
--   start_time = the kline's OPEN time, exactly like market.fapi_kline_5m.start_time,
--   so the two tables join on (symbol, start_time). The value is the snapshot taken
--   at start_time + 5m, parallel to that kline's `close`, and both are known at the
--   same instant.
--
-- Three writers, one key. Rows are keyed (symbol, start_time); src_rank decides who
-- wins on merge (highest rank kept; equal rank -> last inserted wins):
--   1 = live     GET /fapi/v1/openInterest, snapshotted ~20-30s before the kline closes
--   2 = hist     GET /futures/data/openInterestHist (label T is written to T - 5m)
--   3 = archive  data.binance.vision metrics files (create_time T is written to T - 5m)
-- Later sources overwrite earlier ones for the same key. They do NOT agree exactly
-- (hist vs archive median 0.02%, max 0.4%; live is snapshotted up to ~30s earlier).
--
-- snap_time is the instant the snapshot was actually taken: live = the response's
-- `time`; hist / archive = the label T (= start_time + 5m).
--
-- NEVER DROP THIS TABLE. Live snapshots cannot be replayed (the live endpoint has
-- no history), unlike the rollup tables in 005 which are fully derived from this one.
--
-- Rollups: 005_oi_rollups.sql (15m / 1h / 4h / 1d / 1mo).
-- sum_open_interest_value is intentionally not stored: it equals
-- sum_open_interest * markPrice, and sum_open_interest * fapi_kline_5m.close is a
-- ~1 bp approximation of it (see the design doc).

CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.fapi_oi_5m
(
    symbol             LowCardinality(String),
    start_time         DateTime64(3, 'UTC'),
    sum_open_interest  Float64,
    snap_time          DateTime64(3, 'UTC'),
    src_rank           UInt8,               -- 1 = live, 2 = hist, 3 = archive
    created_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(src_rank)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;
