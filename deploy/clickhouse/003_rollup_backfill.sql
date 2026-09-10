-- ============================================================================
-- One-time historical fold: populate the rollup tables from the 1m history that
-- was already in market.fapi_kline_1m BEFORE 002_kline_rollups.sql was applied.
--
-- The refreshable MVs in 002 only see 1m rows inserted AFTER they were created,
-- so this file covers everything from before. Run it ONCE, after phase-one
-- offline backfill has finished and 002 has been applied (see 002 header).
--
-- ----------------------------------------------------------------------------
-- IDEMPOTENCY — why re-running this can never double-count volume
-- ----------------------------------------------------------------------------
--   * The aggregates below (sum / argMin / argMax / min / max) are recomputed
--     from scratch over `market.fapi_kline_1m FINAL` every time. Nothing is
--     added to a previous value — there is no accumulator.
--   * FINAL on the source collapses any duplicate (symbol, start_time) 1m rows
--     before aggregation, so even a 1m table with re-sends yields the correct
--     bucket sum.
--   * Each run stamps rollup_version = now64(3). The target tables are
--     ReplacingMergeTree(rollup_version): a later run's rows REPLACE the earlier
--     ones for the same (symbol, start_time) on merge, and every reader uses
--     FINAL. So running this file twice = running it once.
--
--   Belt-and-braces (only if you want zero stale parts before the next merge,
--   e.g. to reclaim disk immediately): drop the affected month partition first,
--       ALTER TABLE market.fapi_kline_1h DROP PARTITION '202601';
--   then re-insert, then optionally
--       OPTIMIZE TABLE market.fapi_kline_1h FINAL;
--
--   NEVER change the rollup engine to SummingMergeTree, and never turn the MVs
--   into plain (non-refreshable) incremental MVs — both accumulate per insert
--   block and WILL double-count on 1m re-sends. The recompute-from-FINAL design
--   here is the whole point.
--
-- ----------------------------------------------------------------------------
-- MEMORY / CHUNKING
-- ----------------------------------------------------------------------------
--   A multi-year 1m table has ~400 symbols * 525k rows/year. Grouping all of it
--   in one statement can exceed max_memory_usage. Prefer to loop per calendar
--   month. Each block below is written with a :m_start / :m_end parameter pair;
--   drive it from a shell loop, e.g.:
--
--     for m in $(python - <<'PY'
--     import datetime as d
--     s=d.date(2024,1,1); e=d.date.today().replace(day=1)
--     while s<e: print(s.isoformat()); s=(s.replace(day=28)+d.timedelta(days=7)).replace(day=1)
--     PY
--     ); do
--       nxt=$(date -u -d "$m +1 month" +%F)
--       clickhouse-client --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00" \
--         --queries-file deploy/clickhouse/003_rollup_backfill.sql
--     done
--
--   To fold the WHOLE history in one shot instead (small tables / lots of RAM),
--   pass a wide-open range once:
--     clickhouse-client --param_m_start='2000-01-01 00:00:00' \
--                       --param_m_end='2099-01-01 00:00:00' \
--                       --queries-file deploy/clickhouse/003_rollup_backfill.sql
--
--   The refreshable MVs keep the last few days fresh, so this file only has to
--   reach up to "a few days ago"; a 1-2 day overlap with the MV window is fine
--   (idempotent). Nothing here needs to run on a schedule.
--
--   CRON FALLBACK (no refreshable-MV support): run this same file every 1-2 min
--   with a rolling window, e.g. --param_m_start="$(date -u -d '-3 days' '+%F %T')"
--   --param_m_end="$(date -u -d '+1 day' '+%F %T')". Idempotent, as above.
-- ============================================================================

-- ---------------------------------------------------------------------------
INSERT INTO market.fapi_kline_5m
    (symbol, start_time, end_time, open, high, low, close, volume, quote_volume,
     taker_buy_volume, taker_buy_quote_volume, trades_count, rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 5 MINUTE),
    max(end_time),
    argMin(open, start_time), max(high), min(low), argMax(close, start_time),
    sum(volume), sum(quote_volume), sum(taker_buy_volume), sum(taker_buy_quote_volume),
    toUInt32(sum(trades_count)),
    now64(3)
FROM market.fapi_kline_1m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, toStartOfInterval(start_time, INTERVAL 5 MINUTE);

INSERT INTO market.fapi_kline_15m
    (symbol, start_time, end_time, open, high, low, close, volume, quote_volume,
     taker_buy_volume, taker_buy_quote_volume, trades_count, rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 15 MINUTE),
    max(end_time),
    argMin(open, start_time), max(high), min(low), argMax(close, start_time),
    sum(volume), sum(quote_volume), sum(taker_buy_volume), sum(taker_buy_quote_volume),
    toUInt32(sum(trades_count)),
    now64(3)
FROM market.fapi_kline_1m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, toStartOfInterval(start_time, INTERVAL 15 MINUTE);

INSERT INTO market.fapi_kline_1h
    (symbol, start_time, end_time, open, high, low, close, volume, quote_volume,
     taker_buy_volume, taker_buy_quote_volume, trades_count, rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 HOUR),
    max(end_time),
    argMin(open, start_time), max(high), min(low), argMax(close, start_time),
    sum(volume), sum(quote_volume), sum(taker_buy_volume), sum(taker_buy_quote_volume),
    toUInt32(sum(trades_count)),
    now64(3)
FROM market.fapi_kline_1m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, toStartOfInterval(start_time, INTERVAL 1 HOUR);

INSERT INTO market.fapi_kline_4h
    (symbol, start_time, end_time, open, high, low, close, volume, quote_volume,
     taker_buy_volume, taker_buy_quote_volume, trades_count, rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 4 HOUR),
    max(end_time),
    argMin(open, start_time), max(high), min(low), argMax(close, start_time),
    sum(volume), sum(quote_volume), sum(taker_buy_volume), sum(taker_buy_quote_volume),
    toUInt32(sum(trades_count)),
    now64(3)
FROM market.fapi_kline_1m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, toStartOfInterval(start_time, INTERVAL 4 HOUR);

INSERT INTO market.fapi_kline_1d
    (symbol, start_time, end_time, open, high, low, close, volume, quote_volume,
     taker_buy_volume, taker_buy_quote_volume, trades_count, rollup_version)
SELECT
    symbol,
    toStartOfInterval(start_time, INTERVAL 1 DAY),
    max(end_time),
    argMin(open, start_time), max(high), min(low), argMax(close, start_time),
    sum(volume), sum(quote_volume), sum(taker_buy_volume), sum(taker_buy_quote_volume),
    toUInt32(sum(trades_count)),
    now64(3)
FROM market.fapi_kline_1m FINAL
WHERE start_time >= {m_start:DateTime64(3,'UTC')} AND start_time < {m_end:DateTime64(3,'UTC')}
GROUP BY symbol, toStartOfInterval(start_time, INTERVAL 1 DAY);

-- Optional after a large fold, per rollup table:
--   OPTIMIZE TABLE market.fapi_kline_1h FINAL;
