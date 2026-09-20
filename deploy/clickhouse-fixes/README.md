> **Language:** English | [简体中文](README.zh-CN.md)

# ClickHouse repair scripts

Scripts that repair an **already deployed** ClickHouse. `../clickhouse/` answers "how to create it on a fresh install"; this directory answers "it was created wrong — how to put it right". Add new numbered fixes here and register them in the index below.

## Index

| Script | Repairs | Applies when |
|---|---|---|
| [`001_fix_kline_rollup_lookback.sh`](001_fix_kline_rollup_lookback.sh) | `fapi_kline_{5m,15m,1h,4h,1d}` rows older than the MV lookback window that hold only a slice of their bucket (e.g. an hour's volume equal to the last 1m bar's volume) | An old 002 was ever applied (`WHERE start_time >= now() - INTERVAL N DAY`, bound not aligned to a bucket start). **`apply` with no arguments = drop every 5m+ derived table and MV, then rebuild the whole history from 1m** |

Shared connection settings and helpers live in [`lib.sh`](lib.sh).

## Usage

```bash
export CH_HOST=127.0.0.1 CH_PORT=9000 CH_USER=default CH_PASSWORD=...   # CH_DB defaults to market

# 1. Read-only: are the MVs still the old version? do rollups match a fresh re-aggregation of 1m? (default: last 10 days)
./001_fix_kline_rollup_lookback.sh check

# 2. Rehearse on a scratch database first (recommended): the `market.` prefix in the SQL is rewritten to $CH_DB
CH_DB=market_rehearsal ./001_fix_kline_rollup_lookback.sh apply --yes

# 3. Real run: no date range = FULL REBUILD (asks you to type the database name)
./001_fix_kline_rollup_lookback.sh apply

# 4. Partial re-fold / resume: recreate the MVs only, keep existing tables, recompute the given months
./001_fix_kline_rollup_lookback.sh apply --from 2026-08 [--to 2026-10]
```

**Full rebuild** (`apply` without `--from/--to`) does:

1. Applies [`../clickhouse/002_kline_rollups.sql`](../clickhouse/002_kline_rollups.sql), whose leading `REBUILD-DROPS` block drops the 5 refreshable MVs and the 5 derived tables (5m/15m/1h/4h/1d) and recreates them empty. **`fapi_kline_1m` is never touched.**
2. Re-runs 003 month by month, refilling every derived table from the whole 1m history.
3. Verifies that rollups match a fresh re-aggregation of 1m.

While it runs, the 5m+ tables are empty or partial, so readers of them see incomplete data. Stopping chomosyncer-go is **not** required (it only writes 1m) but is harmless. If interrupted, resume with `apply --from <month of the last chunk logged>` (that mode keeps the tables).

## ClickHouse materialized-view facts you must know first

The rollup layer uses **refreshable materialized views**: `REFRESH EVERY N SECOND APPEND TO <target> AS SELECT ...`.

1. **An MV is a stored SELECT; the data lives in the target table.** For an MV created with `TO` / `APPEND TO`, `DROP VIEW` removes only the query — **the target table and its data stay** (confirmed in the 001 rehearsal: a marker row in the target survived dropping and recreating the MV). An MV without `TO` uses an implicit inner table that is dropped with it; this repo never uses that form.
2. **`CREATE MATERIALIZED VIEW IF NOT EXISTS` is a no-op when the MV exists.** Editing the query in the `.sql` file and re-running it leaves the live MV on the old query. **To change an MV, `DROP VIEW` then `CREATE`.** `ALTER TABLE ... MODIFY QUERY` depends on MV type and server version (the compose image is 24.8, and APPEND on refreshable MVs needs 24.10+), so drop-and-recreate is the portable rule.
3. **An MV only works forward; it never repairs history.** A classic (incremental) MV processes newly inserted blocks only; a refreshable MV recomputes only what its `WHERE` covers. After replacing an MV, **historical rows are still the old (wrong) ones** — re-run the backfill (003) to recompute and overwrite them.
4. **Targets are `ReplacingMergeTree(rollup_version)`; "repair" means writing newer-version rows.** For one `(symbol, start_time)` the highest version wins and readers use `FINAL`, so backfills are safely repeatable. Superseded rows are only physically removed by background merges; run `OPTIMIZE TABLE ... FINAL` when you need the space back immediately.
5. **Order: drop the old MV → create the new MV → backfill → verify.** Backfilling first lets the still-running old MV re-corrupt just-repaired buckets as its lookback bound keeps sliding.
6. **There is a short gap between `DROP` and `CREATE`** (new 1m bars are not rolled up). The new MV's first refresh covers the whole lookback window (3–10 days), so that gap heals itself.
7. **Time zones.** `now()` carries the server's timezone. When comparing with UTC-typed columns or aligning with `toStartOfInterval(..., INTERVAL 1 DAY)`, write `toTimeZone(now(), 'UTC')`, otherwise day buckets are cut at the server's local midnight.

## Conventions for a new fix script

1. Name it `NNN_fix_<topic>.sh` (or `.sql`), increment the number, and add it to the index above.
2. The header states four things: **symptom, cause, what the script does (in order), usage**.
3. Provide at least a read-only `check` (non-zero exit on failure) and a mutating `apply` that calls `check` at the end.
4. Change MVs only by DROP then CREATE, in the order drop → create → backfill.
5. Confirm before destructive steps (type the database name, or `--yes`); run all SQL through `lib.sh`'s `ch` / `ch_file` so `CH_DB` rehearsals work.
6. Keep it idempotent; never hard-code passwords.
7. Rehearse on a scratch database before touching production.
