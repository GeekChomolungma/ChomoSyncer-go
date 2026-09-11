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

This mirrors `docs/OPERATIONS.md` §3 (B.1/B.2): you validate ClickHouse's own history and cross-check it against Binance's ground truth *before* flipping on the online pipeline (see the two-phase cold-start rationale there), then validate the live Redis-facing paths once the daemon is actually running.

---

## Directory Layout

| File | Description |
| :--- | :--- |
| `check_clickhouse_integrity.py` | **[Phase A·1] ClickHouse kline integrity checker**: detects timestamp continuity (gap/discontinuity) issues and non-null/validity problems across the 1m raw table and every rollup table |
| `check_vs_binance.py` | **[Phase A·2] External ground-truth reconciliation**: reconciles ClickHouse (raw 1m table / rollup tables) bar-by-bar against Binance `/fapi/v1/klines`, catching systematic deviations in field mapping, units, rollup bucket alignment, and aggregation functions |
| `check_redis_livebars.py` | **[Phase B·1] Redis unclosed live-bar checker**: verifies the existence, TTL freshness, latency, and field completeness of each symbol's `livebar:{SYM}:1m` hash |
| `check_redis_closed_windows.py` | **[Phase B·2] Redis closed-window checker**: verifies the existence of the 200-bar `kline:{SYM}:1m` rolling window, its strictly decreasing continuity, whether any bar is missing internally, and whether the head is up to date |
| `monitor_redis_kline_ready.py` | **[Phase B·3] Redis cross-section notification stream monitor**: watches `stream:market:kline_ready` (including derived coarser-interval signals) and evaluates readiness latency, symbol coverage, and the aggregation trigger reason |
| `e2e_reconciliation.py` | **[Phase B·4] Cache-vs-storage reconciliation tool**: compares the Redis `kline:{SYM}:1m` window against ClickHouse `fapi_kline_1m`, bar-by-bar, for timestamp and OHLCV consistency |
| `run_all_checks.py` | **[Phase C] One-shot pre-flight orchestrator**: runs every check with a single command and outputs a visual red/green health scorecard |
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
> - Every tool that talks to Binance REST (`check_vs_binance.py`) shares the exchange's per-IP rate budget with the live daemon's own backfill traffic — see the rate-limit note in its section below before running it against the whole universe.

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
- **Compact JSON format validation**: parses the 9-element compact array `[t, o, h, l, c, v, qv, tbv, tbqv]` and validates its values.

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

**Phase C — the final gate, and the tool most external users will actually run.** Internally runs tools #1, #3, #4, #5, #6 in that order (plus #2 only when `--vs-binance` is passed, since it's the slow, REST-heavy one), and aggregates everything into one red/green scorecard with a go/no-go verdict (`READY FOR PRODUCTION DEPLOYMENT` or `DEPLOYMENT BLOCKED`).

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

## Pre-Flight Verdict Rules

| Status | Meaning | Recommended Action |
| :--- | :--- | :--- |
| **PASS** (green) | All metrics fully meet production standards (no gaps, data is complete, clocks are aligned) | Safe to deploy directly |
| **WARN** (yellow) | Non-critical minor deviations exist (e.g. zero trade count for an illiquid symbol, a just-cold-started window not yet filled to 200 bars) | May proceed selectively after manual confirmation of the cause |
| **FAIL** (red) | Serious data-quality issues exist (missing-bar gaps in klines, price or timestamp inversions, a lagging Redis head) | **Deployment strictly forbidden** — investigate Backfill or the Collector |
