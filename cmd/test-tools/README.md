> **Language:** English | [简体中文](README.zh-CN.md)

# ChomoSyncer Test & Validation Toolkit

This directory provides a complete suite of Python scripts dedicated to end-to-end data integrity, continuity, freshness, and multi-tier storage consistency validation for the **ChomoSyncer-go** system — before deployment, during operation, or after recovering from a disconnect.

Every tool prints a human-readable table and ends with a clear verdict; all except the pure monitor (`monitor_redis_kline_ready.py`) exit `0` on PASS and non-zero on FAIL, so they drop straight into cron/CI without extra glue.

---

## Recommended Order to Run These

The sections below are ordered to match **when each check first becomes meaningful in a real deployment**, not alphabetically. Two phases:

| Phase | When | Tools (in order) |
| :--- | :--- | :--- |
| **A — before the live service starts** (right after offline backfill + `002`/`003` rollup) | ClickHouse-only; no Redis, no live daemon needed | 1) `check_clickhouse_integrity.py` → 2) `check_vs_binance.py` |
| **B — after the live service is running** | Needs the daemon streaming into Redis | 3) `check_redis_livebars.py` → 4) `check_redis_closed_windows.py` → 5) `monitor_redis_kline_ready.py` → 6) `e2e_reconciliation.py` |
| **C — anytime, wraps A2 + all of B** | One command, go/no-go verdict | 7) `run_all_checks.py` |
| **D — after the `open_interest` module is enabled** | ClickHouse-only (plus Binance with `--vs-binance`) | 8) `check_oi_consistency.py` (also runnable from 7 with `--oi`) |

This mirrors `docs/OPERATIONS.md` §3 (B.1/B.2): you validate ClickHouse's own history and cross-check it against Binance's ground truth *before* flipping on the online pipeline (see the two-phase cold-start rationale there), then validate the live Redis-facing paths once the daemon is actually running.

---

## Directory Layout

| File | Description |
| :--- | :--- |
| `check_clickhouse_integrity.py` | **[Phase A·1] ClickHouse kline integrity checker**: detects timestamp continuity (gap/discontinuity) issues and non-null/validity problems across the 1m raw table and every rollup table |
| `backfill_missing_1m.py` | **[Repair] Backfill missing 1m klines**: reads the CSV from `check_clickhouse_integrity.py --gaps-csv`, fetches exactly those minutes from Binance REST and inserts them into `market.fapi_kline_1m`. Dry run unless `--apply` |
| `check_vs_binance.py` | **[Phase A·2] External ground-truth reconciliation**: reconciles ClickHouse (raw 1m table / rollup tables) bar-by-bar against Binance `/fapi/v1/klines`, catching systematic deviations in field mapping, units, rollup bucket alignment, and aggregation functions |
| `check_redis_livebars.py` | **[Phase B·1] Redis unclosed live-bar checker**: verifies the existence, TTL freshness, latency, and field completeness of each symbol's `livebar:{SYM}:1m` hash |
| `check_redis_closed_windows.py` | **[Phase B·2] Redis closed-window checker**: verifies the existence of the 200-bar `kline:{SYM}:1m` rolling window, its strictly decreasing continuity, whether any bar is missing internally, and whether the head is up to date |
| `monitor_redis_kline_ready.py` | **[Phase B·3] Redis cross-section notification stream monitor**: watches `stream:market:kline_ready` (including derived coarser-interval signals) and evaluates readiness latency, symbol coverage, and the aggregation trigger reason |
| `e2e_reconciliation.py` | **[Phase B·4] Cache-vs-storage reconciliation tool**: compares the Redis `kline:{SYM}:1m` window against ClickHouse `fapi_kline_1m`, bar-by-bar, for timestamp and OHLCV consistency |
| `run_all_checks.py` | **[Phase C] One-shot pre-flight orchestrator**: runs every check with a single command and outputs a visual red/green health scorecard |
| `check_oi_consistency.py` | **[Phase D] Open-interest consistency checker**: read-only checks of `market.fapi_oi_5m` — gaps, 5-minute grid, `snap_time` semantics, freshness, calibration by hist, coverage against the kline table, per-bar cross-section, and (optionally) value-for-value against Binance |
| `backfill_missing_oi.py` | **[Repair] Backfill missing 5m open-interest bars**: reads the CSV from `check_oi_consistency.py --gaps-csv`, fetches those bars from Binance `openInterestHist` (only the latest ~30 days exist) and inserts them into `market.fapi_oi_5m` as `src_rank = 2`. Dry run unless `--apply` |
| `common.py` | Shared utility library: config loading, ClickHouse/Redis clients, symbol discovery, time utilities, table formatting, etc. Not a check by itself. |
| `test_toolkit.py` | The toolkit's built-in unit tests and mock validation suite (`python -m pytest cmd/test-tools/test_toolkit.py`, or run directly) |
| `requirements.txt` | Python dependency manifest |

---

## Environment Setup & Dependency Installation

Run the following in any environment with Python 3.9+ installed:

```bash
pip install -r cmd/test-tools/requirements.txt
```

> **Tips**:
> - Scripts preferentially connect using the addresses configured in `config.yaml` (falling back to `config.example.yaml`); pass `--config /path/to/config.yaml` to point elsewhere, or use the per-tool `--ch-*` / `--redis-*` flags to override individual fields without touching the file.
> - ClickHouse access supports two protocols: `clickhouse-connect` is preferred; if its native C extension is not installed, it automatically and seamlessly falls back to ClickHouse's native HTTP interface (port 8123), with no cross-platform compilation dependencies to worry about.
> - Every tool that talks to Binance REST (`check_vs_binance.py`, and `check_oi_consistency.py --vs-binance`) shares the exchange's per-IP rate budget with the live daemon's own traffic — see the rate-limit notes in their sections below before running them against the whole universe.

---

## Detailed Tool Usage

### 1. ClickHouse Historical Kline Integrity Check (`check_clickhouse_integrity.py`)

**Phase A·1 — run right after the offline backfill + `002`/`003` rollup, before starting the live service.** Pure ClickHouse read; no Redis, no running daemon required.

Uses ClickHouse's vectorized window functions (`lagInFrame`) to scan hundreds of millions of kline rows across the whole database in seconds, precisely pinpointing every gap interval and any anomalous/dirty data.

#### What it checks:
- **Timestamp continuity**: computes `diff_ms` between adjacent klines to detect gaps `> interval_ms`, and calculates the cumulative number of missing bars and coverage rate.
- **Field non-null and logical validity**:
  - Price sanity: `open > 0`, `high > 0`, `low > 0`, `close > 0`
  - Price bound logic: `high >= low`, `high >= max(open, close)`, `low <= min(open, close)`
  - Volume: `volume >= 0`, `quote_volume >= 0`
  - Trade count: `trades_count >= 0` (warns on suspicious zero-trade bars for illiquid symbols)
  - Timestamp alignment: `end_time > start_time` and strictly matches the interval duration.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1m,5m,15m,1h,4h,1d` | Comma-separated intervals; each maps to `<table-prefix>_<interval>` |
| `--symbol` | *(all)* | One specific symbol instead of the whole database |
| `--limit-symbols` | *(all)* | Cap the number of symbols inspected |
| `--table-prefix` | `market.fapi_kline` | ClickHouse table prefix |
| `--start-date` / `--end-date` | *(all history)* | Restrict the scan window, e.g. `"2024-01-01"` or `"2024-01-01 00:00:00"` |
| `--max-gaps` | `5` | Max gaps printed per symbol before truncating |
| `--show-all-gaps` | off | Print every gap, no truncation |
| `--gaps-csv PATH` | off | Also write **every** gap (no `--max-gaps` truncation) to a CSV for `backfill_missing_1m.py` (columns `symbol,interval,from_ms,to_ms,from_utc,to_utc,missing_count`; the missing bar-open times are `[from_ms, to_ms)`). Use with `--intervals 1m` |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | from config | Per-field ClickHouse connection overrides |

#### Example usage:
```bash
# Check all symbols in the whole database across the default configured intervals (1m + all rollups)
python cmd/test-tools/check_clickhouse_integrity.py

# Check a specific symbol and interval
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --intervals 1m

# Check a specific time range (e.g. since a given date) and list every gap found
python cmd/test-tools/check_clickhouse_integrity.py --start-date "2026-09-01" --show-all-gaps

# Quick sampled check of 10 symbols
python cmd/test-tools/check_clickhouse_integrity.py --limit-symbols 10
```

Exit code `0` = no gaps / no invalid rows found; `1` = integrity issues detected (or connection failure).

---

### 1b. Backfill Missing 1m Klines (`backfill_missing_1m.py`)

**Repair tool — the follow-up to check #1.** The checker names the holes; this script fills them. (The running service also repairs the last 24 hours by itself every 30 minutes — the `backfill.sweep_*` settings — so this script is for older history, for a stopped service, and for a manual re-check.)

```bash
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m --gaps-csv gaps.csv   # 1. list every hole
python cmd/test-tools/backfill_missing_1m.py gaps.csv                                    # 2. dry run: fetch + report, writes nothing
python cmd/test-tools/backfill_missing_1m.py gaps.csv --apply                            # 3. insert what was found
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m                       # 4. verify
```

What it does per symbol: drops the minutes ClickHouse already has (so re-running is safe), groups the rest into as few `/fapi/v1/klines` requests as is cheap, fetches them paced against the IP weight limit (honours `429`/`418`), and inserts only **closed** bars that were asked for.

**Some holes cannot be filled.** Binance returns no kline for a minute in which the symbol had no trades; those are reported as *empty at exchange*, are not inserted, and the integrity checker will keep listing them. Judge them by the symbol's liquidity, not as a fault.

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `csv` (positional) | required | The gaps CSV. Only `1m` lines are used |
| `--apply` | off | Actually `INSERT`. Without it nothing is written |
| `--symbol` | *(all in file)* | Only this symbol |
| `--table-prefix` | `market.fapi_kline` | Writes `<prefix>_1m` |
| `--weight-budget` | `600` | Max request weight this script spends per minute (the IP limit is 2400 and the live service shares it) |
| `--soft-limit` | `1800` | Wait for the next minute when Binance's `X-MBX-USED-WEIGHT-1M` reaches this |
| `--rest-url` | from config | Binance REST base URL |
| `--batch-rows` | `2000` | Rows per `INSERT` |
| `--ch-*` | from config | ClickHouse connection overrides |

**Rollups.** Only the 1m table is written. `5m`/`15m`/`1h` pick the repair up on their next refresh if it is within 3 days (`4h`: 7, `1d`: 10). For older repairs the script prints a hint to refold them with `deploy/clickhouse-fixes/001_fix_kline_rollup_lookback.sh apply --from <YYYY-MM> --to <YYYY-MM>`.

---

### 2. ClickHouse vs. Binance External Ground-Truth Reconciliation (`check_vs_binance.py`)

**Phase A·2 — run right after check #1, still before starting the live service.** `e2e_reconciliation.py` (tool #6) compares "the pipeline against itself" (Redis and ClickHouse are both written by the same pipeline); this tool instead reconciles ClickHouse bar-by-bar against **Binance's official** `/fapi/v1/klines` — the only check capable of catching **systematic deviations** in 1m collection or rollup aggregation (field mapping bugs, unit errors, rollup bucket misalignment, a broken aggregation function).

#### What it checks:
- `start_time` must align exactly (bucket alignment / collection time base).
- OHLC relative tolerance `1e-9` (`argMin/argMax/min/max` → exactly equal).
- `volume / quote_volume / taker_buy_*` relative tolerance `1e-6` (floating-point summation order differs: Binance sums from trades, we sum 1m bars).
- `trades_count` must be exactly equal (integer sum).
- The comparison window is always **the last N closed bars ending now** (`--bars`, not a date range) — so it naturally works as a recent-history spot-check, e.g. `--intervals 1m --bars 720` covers the last 12 hours without you computing any timestamps.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1h,4h,1d` | Comma-separated intervals; add `1m` to validate raw ingestion, or use rollup intervals to validate Phase B aggregation |
| `--symbol` | *(sample)* | One specific symbol instead of sampling the active universe |
| `--limit-symbols` | `8` | Symbols to sample (alphabetically) when `--symbol` is omitted; pass a number ≥ the active symbol count to cover the whole universe |
| `--bars` | `48` | Closed bars per (symbol, interval) to compare, counted back from now — e.g. `720` = last 12h of `1m` |
| `--settle-lag` | `1` | Excludes this many of the newest closed bars from the comparison. ClickHouse write latency (`chwriter` batches on a `flush_interval` of a couple seconds) and Binance's own WS-close-vs-REST-kline settling can make the single newest bar briefly read as missing or mismatched on an otherwise healthy pipeline — comparing hundreds of symbols one by one (`--symbol-delay`) means "now" keeps advancing as the run progresses, so without this margin a full-universe run flags a rotating set of false positives. Set to `0` to compare right up to the edge (noisier; the flagged bars typically resolve within seconds — re-check the same symbol a minute later to confirm). |
| `--price-tol` | `1e-9` | Relative tolerance for OHLC |
| `--volume-tol` | `1e-6` | Relative tolerance for volume fields |
| `--table-prefix` | `market.fapi_kline` | ClickHouse table prefix |
| `--http-timeout` | `15` | Binance REST timeout (seconds) |
| `--symbol-delay` | `0.35` | Seconds slept between symbols. Every `/fapi/v1/klines` call here uses `limit=1500` (weight 10) regardless of `--bars`; `0.35s` ≈ 2.9 req/s ≈ 1740 weight/min, staying under Binance's 2400/min per-IP cap with headroom for the live daemon's own REST use. **Do not set this to 0** for a `--limit-symbols` run covering the whole universe — see `docs/OPERATIONS.md`'s `backfill.rest_rps` notes for the same weight-budget math. |
| `--show-only-failures` | off | Only print rows that aren't PASS |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | from config | Per-field ClickHouse connection overrides |

#### Example usage:
```bash
# 1m raw table vs. Binance 1m — verifies collection field mapping/units, 8-symbol sample
python cmd/test-tools/check_vs_binance.py --intervals 1m --limit-symbols 10

# Rollup tables vs. Binance — verifies Phase B's bucket alignment and aggregation functions
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d --symbol BTCUSDT --bars 96

# Whole-universe spot-check of the last 12h of 1m data (paced to stay under the rate limit; ~4-6 min for ~500 symbols)
python cmd/test-tools/check_vs_binance.py --intervals 1m --bars 720 --limit-symbols 600 --symbol-delay 0.35 --show-only-failures
```

Exit code `0` = every checked bar matched within tolerance; `1` = mismatch or missing bar found.

---

### 3. Redis Unclosed Live-Bar Check (`check_redis_livebars.py`)

**Phase B·1 — first check that's meaningful once the live daemon is running.** A fresh `livebar:*` hash should appear within seconds of the WS collector receiving its first frame per symbol, so this is the fastest "is it actually streaming" signal.

Checks the still-forming, unclosed kline hashes in Redis (`livebar:<SYMBOL>:<interval>`).

#### What it checks:
- **Key existence**: whether every active market-wide symbol has a live bar present in Redis.
- **TTL validity**: whether the TTL is greater than 0 and within the expected window (`<= ttl_multiple * interval`).
- **Real-time freshness**: whether `now_ms - t` falls within the current interval; if it exceeds `2 * interval`, the bar is flagged as STALE (half-dead).
- **Completeness of the 11 fields**: `t, o, h, l, c, v, qv, tbv, tbqv, n, x`.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1m` | Comma-separated intervals; only the base interval has Redis live bars |
| `--symbol` | *(all)* | One specific symbol instead of the whole active universe |
| `--limit-symbols` | *(all)* | Cap the number of symbols inspected |
| `--prefix` | from config / `livebar` | Key prefix override |
| `--show-only-failures` | off | Only display failed/missing/stale bars |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | from config | Per-field Redis connection overrides |

#### Example usage:
```bash
# Check real-time market data across the whole market
python cmd/test-tools/check_redis_livebars.py

# Show only symbols with anomalies/missing data
python cmd/test-tools/check_redis_livebars.py --show-only-failures
```

Exit code `0` = every symbol has a fresh, complete live bar; `1` = missing/stale/incomplete bars found.

---

### 4. Redis Closed Rolling Window Check (`check_redis_closed_windows.py`)

**Phase B·2.** Meaningful as soon as the first bar closes post-startup (a cold-started symbol may take up to `window_size` intervals to fill to full length organically — or be instantly full if `RebuildWindow` already populated it from ClickHouse during cold-start backfill).

Checks the closed rolling window (`kline:<SYMBOL>:<interval>`) that quant strategy engines consume from Redis.

#### What it checks:
- **Window length**: whether it reaches the expected length (200 bars by default in production).
- **Head freshness**: whether the start timestamp of the first list element (index 0) is strictly aligned with the boundary of the interval that just closed, and whether it lags behind.
- **Strictly decreasing order**: verifies timestamps are strictly ordered from newest to oldest (`t[0] > t[1] > ...`).
- **Internal continuity with no gaps**: verifies bar-by-bar that `t[i] - t[i+1] == interval_ms`, ruling out any missing bar inside the window.
- **Compact JSON format validation**: parses the 10-element compact array `[t, o, h, l, c, v, qv, tbv, tbqv, n]` and validates its values.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1m` | Comma-separated intervals; only the base interval has a Redis closed-window — coarser intervals live in ClickHouse rollup tables |
| `--symbol` | *(all)* | One specific symbol instead of the whole active universe |
| `--limit-symbols` | *(all)* | Cap the number of symbols inspected |
| `--window-size` | from config / `200` | Expected window length override |
| `--prefix` | from config / `kline` | Key prefix override |
| `--show-only-failures` | off | Only display failed/short/stale windows |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | from config | Per-field Redis connection overrides |

#### Example usage:
```bash
# Check the default (1m) window
python cmd/test-tools/check_redis_closed_windows.py

# Check with a custom window size (e.g. 100 bars)
python cmd/test-tools/check_redis_closed_windows.py --window-size 100
```

Exit code `0` = every window is full-length, ordered, gap-free and fresh; `1` = a window is short, out of order, has an internal gap, or has a stale head.

---

### 5. Redis Cross-Section Ready Notification Monitor (`monitor_redis_kline_ready.py`)

**Phase B·3.** Diagnostic/observability tool, not a pass/fail gate — it has no PASS/WARN/FAIL verdict or exit-code contract beyond a connection failure (`1`); read its latency/coverage numbers by eye (or in a dashboard) rather than scripting on its exit code.

Listens to (or replays) the `stream:market:kline_ready` cross-section notification stream to understand the aggregator's working state.

#### What it checks:
- **Aggregation readiness latency**: `publish time − kline close time`.
  - `< 1.0s`: instantly complete within the second (`onComplete`)
  - `~ 5.0s`: fallback timeout trigger (`onTimeout` safety-net mechanism, driven by `dispatcher.section_timeout`)
  - `> 10.0s`: network congestion or worker blocking
- **Section symbol coverage rate**: the number of symbols received vs. the market-wide target symbol count (e.g. 248/250, 99.2%).
- **Section continuity**: whether the timestamp difference between two adjacent events is exactly one interval.

> Reminder from `docs/OPERATIONS.md`: while any `1m` key is held by `windowgate` (cold-start backfill, or a shard-reconnect gap-fill), the `kline_ready` publish for that entire interval is suppressed and **not retried** — a quiet stretch here right after a restart or a reconnect is expected, not necessarily a bug. See `todo_improvement/cold-start-gate-batch-latency.md` for the full mechanism.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--recent` | `20` | Number of recent events to replay from the stream |
| `--tail` | off | Continuously tail and listen for live events instead of replaying |
| `--max-events` | *(unbounded)* | Stop after this many events when `--tail` is enabled |
| `--stream-key` | from config / `stream:market:kline_ready` | Stream key override |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | from config | Per-field Redis connection overrides |

#### Example usage:
```bash
# Replay the most recent 20 published cross-section ready events
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# Real-time listening mode (prints alerts and latency stats immediately upon receiving an event)
python cmd/test-tools/monitor_redis_kline_ready.py --tail

# Tail for exactly 50 events then stop (handy for a bounded smoke test)
python cmd/test-tools/monitor_redis_kline_ready.py --tail --max-events 50
```

---

### 6. End-to-End Dual-Write Consistency Reconciliation (`e2e_reconciliation.py`)

**Phase B·4 — run last among the live checks.** Needs both Redis and ClickHouse to already hold fresh, matching recent data, so it's the natural "do the two storage tiers agree with each other" capstone once #3-#5 already look healthy.

Verifies full data consistency between the Redis 1m rolling window cache and ClickHouse `fapi_kline_1m`.

#### What it checks:
- Fetches up to `--window-size` bars from Redis `kline:SYMBOL:1m` and the same count (descending) from ClickHouse `market.fapi_kline_1m`.
- Aligns timestamps `t` bar-by-bar.
- Compares `Open, High, Low, Close, Volume, QuoteVolume` bar-by-bar; floating-point differences must be within tolerance (`< 1e-5`).

> Applies to `1m` only: coarser intervals have no Redis window. Their correctness is verified via `check_vs_binance.py` (tool #2) against Binance.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1m` | Comma-separated intervals; only the base interval has a Redis window |
| `--symbol` | *(sample)* | One specific symbol instead of sampling the active universe |
| `--limit-symbols` | `10` | Symbols to sample when `--symbol` is omitted |
| `--window-size` | from config / `200` | Bars to reconcile per symbol |
| `--prefix` | from config / `kline` | Redis key prefix override |
| `--table-prefix` | `market.fapi_kline` | ClickHouse table prefix |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | from config | Per-field Redis connection overrides |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | from config | Per-field ClickHouse connection overrides |

#### Example usage:
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 10
python cmd/test-tools/e2e_reconciliation.py --symbol BTCUSDT --window-size 200
```

Exit code `0` = Redis and ClickHouse agree on every compared bar; `1` = a mismatch or missing bar found on either side.

---

### 7. One-Shot Pre-Flight Scorecard (`run_all_checks.py`)

**Phase C — the final gate, and the tool most external users will actually run.** Internally runs tools #1, #3, #4, #5, #6 in that order (plus #2 only when `--vs-binance` is passed, since it's the slow, REST-heavy one, and #8 only when `--oi` is passed, since the open-interest module is off by default), and aggregates everything into one red/green scorecard with a go/no-go verdict (`READY FOR PRODUCTION DEPLOYMENT` or `DEPLOYMENT BLOCKED`).

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` |
| `--intervals` | `1m` | Redis-facing intervals (only the base interval has Redis windows / live bars) |
| `--ch-intervals` | `1m,5m,15m,1h,4h,1d` | ClickHouse intervals for the integrity check (base + rollup tables) |
| `--symbol` | *(sample/all, per sub-check)* | Restrict every sub-check to one symbol |
| `--quick` | off | Quick smoke check, sampling only the first 5 symbols |
| `--skip-ch` | off | Skip the ClickHouse integrity check |
| `--skip-redis` | off | Skip the Redis checks (live bars + closed windows) |
| `--skip-e2e` | off | Skip the end-to-end reconciliation |
| `--vs-binance` | off | Also run `check_vs_binance.py` against the ClickHouse rollups (slow; hits the exchange — see tool #2's rate-limit note) |
| `--oi` | off | Also run `check_oi_consistency.py` (last 6h; needs the `open_interest` module to be running). With `--vs-binance` it also compares a sample of OI rows with Binance |
| `--verbose` | off | Print the full stdout of every sub-check, not just the summary |

#### Example usage:
```bash
# Quick smoke pre-flight (samples the first 5 core symbols)
python cmd/test-tools/run_all_checks.py --quick

# Full deep pre-flight (ClickHouse + Redis + e2e, no Binance)
python cmd/test-tools/run_all_checks.py

# Full deep pre-flight including the external Binance ground-truth check
python cmd/test-tools/run_all_checks.py --vs-binance

# Verbose output with subcommand details
python cmd/test-tools/run_all_checks.py --verbose
```

Exit code `0` = every sub-check that ran passed; `1` = at least one sub-check failed.

---

### 8. Open-Interest Table Consistency Check (`check_oi_consistency.py`)

**Phase D — after the `open_interest` module has been enabled** (design: [`new_requirements/oi.md`](../../new_requirements/oi.md), consumer view: [`docs/DATA_CONSUMER_GUIDE.md`](../../docs/DATA_CONSUMER_GUIDE.md)). **Read-only.** It inspects `market.fapi_oi_5m` over the last `--hours` of bars that should already exist, and answers "can a strategy trust this series?". Only bars whose close is at least `--settle-minutes` old are examined, so a bar hist has not published yet is never reported as missing.

| Check | What it looks at | Verdict |
| :--- | :--- | :--- |
| **A. Series integrity** (per symbol) | missing bars between the first and last bar; every `start_time` on the 5-minute grid; finite, non-negative values; **`snap_time` vs `start_time`** (hist/archive rows: exactly `start_time + 5m`; live rows are stored as Binance returned them, so one further than `--live-accept` from it is only counted); the newest bar is at most `--max-lag-bars` behind | gap / off-grid / bad hist snap / stale → **FAIL**; zero value, or a live row whose `snap_time` is far from the close → WARN |
| **B. Freshness & source health** | newest bar overall and newest *calibrated* (`src_rank >= 2`) bar; live rows older than `--max-uncalibrated-hours` that hist never replaced | table stale → **FAIL**; calibration stalled or uncalibrated live rows → WARN (**FAIL** with `--require-calibration`) |
| **C. Consumer view** | for every `fapi_kline_5m` bar in the window, is there an OI row? per-symbol coverage | below `--min-coverage` (default 99.5%) → **FAIL**; a symbol with klines but no OI row at all is named explicitly |
| **D. Cross-section** | per bar, the share of symbols that have an OI row | any bar below `--min-cross-section` (default 99%) → **FAIL** |
| **E. vs Binance** (`--vs-binance`) | a sample of symbols compared value-for-value with Binance's own `openInterestHist`: label `T` must be stored at `start_time = T-5m`; rank ≥ 2 rows must equal Binance's value exactly; live rows within `--live-tol` | mismatch or missing row → **FAIL** |

What the checks mean for the data (why they exist):
- **The `T-5m` rule** (hist/archive `snap_time == start_time + 5m`) proves hist labels are stored one bar earlier; live rows are attributed by the round's boundary, so their `snap_time` only records when Binance took the snapshot; an off-by-one-bar shift would otherwise be invisible because adjacent OI values differ by only ~0.02%.
- **Off-grid rows** are the fingerprint of a timezone or unit error at import time.
- **Uncalibrated live rows** mean the hourly hist calibration is not running; live values would then never be replaced by Binance's own series.

#### Flags:
| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--config` | auto-discover | Path to `config.yaml` (`open_interest.table` is used if set) |
| `--table` | `market.fapi_oi_5m` | OI table to inspect |
| `--kline-table` | `market.fapi_kline_5m` | 5m kline table used for checks C and D |
| `--hours` | `24` | Window length |
| `--settle-minutes` | `10` | Only examine bars whose close is at least this old |
| `--symbol` | whole market | Comma-separated symbols, e.g. `BTCUSDT,ETHUSDT` |
| `--limit-symbols` | all | Inspect only the first N symbols |
| `--max-lag-bars` | `2` | Newest bar may be at most this many bars behind |
| `--live-accept` | `60` | Live rows whose `snap_time` is further than this from the bar's close are counted as a WARN (they are stored as returned, not rejected) |
| `--max-uncalibrated-hours` | `2` | Live rows older than this should have been replaced by hist |
| `--require-calibration` | off | Make uncalibrated live rows a FAIL instead of a WARN |
| `--min-coverage` / `--min-cross-section` | `0.995` / `0.99` | Thresholds for checks C / D |
| `--skip-coverage` | off | Skip the join against the kline table (C and D) |
| `--start-date` | off | Window start (UTC), e.g. `2020-09-01` or `2024-01-01 06:30`; overrides `--hours`. Combine with `--skip-coverage` for a whole-history scan |
| `--gaps-csv PATH` | off | Write every missing-bar range (between two rows of the same symbol, on-grid rows only) to a CSV for `backfill_missing_oi.py`; same columns as the kline gaps CSV, interval `5m` |
| `--gaps-tail` | off | With `--gaps-csv`: also list the bars between a symbol's newest row and the window end. Delisted symbols show up here and can never be filled |
| `--vs-binance` | off | Also run check E (hits the exchange) |
| `--binance-symbols` / `--binance-bars` | `5` / `48` | Symbols sampled / newest hist points fetched per symbol |
| `--live-tol` | `0.005` | Relative tolerance for live rows vs Binance |
| `--symbol-delay` | `0.5` | Seconds between Binance requests |
| `--show-all` / `--max-rows` | off / `30` | List every symbol / cap rows per section |
| `--ch-host/-port/-db/-user/-password` | from config | ClickHouse overrides |

> **Rate limit note:** `--vs-binance` calls `/futures/data/openInterestHist`, which is a **different pool** from the `/fapi` weight budget (1000 requests per 5 minutes per IP, counted per request, shared with the live service's own hist calibration). It makes one request per sampled symbol, paced by `--symbol-delay`; a default run is 5 requests.

#### Example usage:
```bash
# Whole market, last 24h, ClickHouse only
python cmd/test-tools/check_oi_consistency.py

# A few symbols over the last 6h, with the external ground-truth comparison
python cmd/test-tools/check_oi_consistency.py --symbol BTCUSDT,ETHUSDT --hours 6 --vs-binance

# Right after the first `hist_enabled` run, before live is switched on: demand calibration
python cmd/test-tools/check_oi_consistency.py --require-calibration --max-uncalibrated-hours 1

# Against a rehearsal database
python cmd/test-tools/check_oi_consistency.py --table oi_rehearsal.fapi_oi_5m --kline-table market.fapi_kline_5m

# Whole history (e.g. after an archive import): list every missing bar, ClickHouse only
python cmd/test-tools/check_oi_consistency.py --start-date 2020-09-01 --skip-coverage --ch-password "<pw>" --gaps-csv oi_gaps.csv
```

Exit code `0` = no FAIL (WARNs allowed); `1` = at least one FAIL, an empty/missing table (the message says to apply `deploy/clickhouse/004_fapi_oi.sql`), or a connection error.

---

### 8b. Backfill Missing 5m Open-Interest Bars (`backfill_missing_oi.py`)

**Repair tool — the follow-up to check #8.** The checker names the missing bars; this script fills what Binance can still serve.

```bash
python cmd/test-tools/check_oi_consistency.py --start-date 2020-09-01 --skip-coverage --ch-password "<pw>" --gaps-csv oi_gaps.csv   # 1. list every gap
python cmd/test-tools/backfill_missing_oi.py oi_gaps.csv --ch-password "<pw>"           # 2. dry run: fetch + report, writes nothing
python cmd/test-tools/backfill_missing_oi.py oi_gaps.csv --ch-password "<pw>" --apply   # 3. insert what was found
python cmd/test-tools/check_oi_consistency.py --start-date 2020-09-01 --skip-coverage --ch-password "<pw>"   # 4. verify
```

What it does per symbol: drops the part of each gap older than Binance's retention (~30 days, reported as *beyond retention*), drops the bars ClickHouse already has (so re-running is safe and nothing is overwritten), groups the rest into as few `openInterestHist` requests as fit one page (500 bars), fetches them paced against the `/futures/data` pool, and inserts only the bars that were asked for. Rows use the same mapping as the Go module: a label `T` is stored at `start_time = T-5m`, `snap_time = T`, `src_rank = 2`.

**Some gaps cannot be filled here.** Bars older than the retention need the archive; bars Binance returns nothing for are *empty at exchange*; a symbol Binance rejects (delisted) is reported and skipped. The checker keeps listing all of them.

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `csv` (positional) | required | The gaps CSV. Only `5m` lines are used |
| `--apply` | off | Actually `INSERT`. Without it nothing is written |
| `--symbol` | *(all in file)* | Only this symbol |
| `--table` | `market.fapi_oi_5m` | OI table (`open_interest.table` from config if set) |
| `--rps` | `2` | Requests per second (same as `open_interest.data_rps`) |
| `--window-cap` | `900` | Max requests in any 5 minutes (the IP limit is 1000, shared with the service's hist pass) |
| `--retention-days` | `30` | Older bars are reported, not requested |
| `--rest-url` | from config | Binance REST base URL |
| `--batch-rows` | `2000` | Rows per `INSERT` |
| `--ch-host/-port/-db/-user/-password` | from config | ClickHouse connection overrides |

If the repair reaches back beyond the rollups' refresh lookback, the tool prints the exact `./rollup_oi_per_month.sh` command to refold those months. Exit code `1` if any symbol failed. Full runbook: `docs/OPERATIONS.md` §3.4, "After importing the archive".

---

## Pre-Flight Verdict Rules

| Status | Meaning | Recommended Action |
| :--- | :--- | :--- |
| **PASS** (green) | All metrics fully meet production standards (no gaps, data is complete, clocks are aligned) | Safe to deploy directly |
| **WARN** (yellow) | Non-critical minor deviations exist (e.g. zero trade count for an illiquid symbol, a just-cold-started window not yet filled to 200 bars) | May proceed selectively after manual confirmation of the cause |
| **FAIL** (red) | Serious data-quality issues exist (missing-bar gaps in klines, price or timestamp inversions, a lagging Redis head) | **Deployment strictly forbidden** — investigate Backfill or the Collector |
