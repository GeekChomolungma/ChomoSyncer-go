> **Language:** English | [简体中文](README.zh-CN.md)

# ChomoSyncer Test & Validation Toolkit

This directory provides a complete suite of Python scripts dedicated to end-to-end data integrity, continuity, freshness, and multi-tier storage consistency validation for the **ChomoSyncer-go** system — before deployment, during operation, or after recovering from a disconnect.

---

## Directory Layout

| File | Description |
| :--- | :--- |
| `common.py` | Shared utility library: config loading, ClickHouse/Redis clients, symbol discovery, time utilities, table formatting, etc. |
| `check_clickhouse_integrity.py` | **ClickHouse kline integrity checker**: detects timestamp continuity (gap/discontinuity) issues and non-null/validity problems across the 1m raw table and every rollup table |
| `check_redis_livebars.py` | **Redis unclosed live-bar checker**: verifies the existence, TTL freshness, latency, and field completeness of each symbol's `livebar:{SYM}:1m` hash |
| `check_redis_closed_windows.py` | **Redis closed-window checker**: verifies the existence of the 200-bar `kline:{SYM}:1m` rolling window, its strictly decreasing continuity, whether any bar is missing internally, and whether the head is up to date |
| `monitor_redis_kline_ready.py` | **Redis cross-section notification stream monitor**: watches `stream:market:kline_ready` (including derived coarser-interval signals) and evaluates readiness latency, symbol coverage, and the aggregation trigger reason |
| `e2e_reconciliation.py` | **Cache-vs-storage reconciliation tool**: compares the Redis `kline:{SYM}:1m` window against ClickHouse `fapi_kline_1m`, bar-by-bar, for timestamp and OHLCV consistency |
| `check_vs_binance.py` | **External ground-truth reconciliation**: reconciles ClickHouse (raw 1m table / rollup tables) bar-by-bar against Binance `/fapi/v1/klines`, catching systematic deviations in field mapping, units, rollup bucket alignment, and aggregation functions |
| `run_all_checks.py` | **One-shot pre-flight orchestrator**: runs every check with a single command and outputs a visual red/green health scorecard |
| `test_toolkit.py` | The toolkit's built-in unit tests and mock validation suite |
| `requirements.txt` | Python dependency manifest |

---

## Environment Setup & Dependency Installation

Run the following in any environment with Python 3.9+ installed:

```bash
pip install -r cmd/test-tools/requirements.txt
```

> **Tips**:
> - Scripts preferentially connect using the addresses configured in `config.yaml` or `config.example.yaml`.
> - ClickHouse access supports two protocols: `clickhouse-connect` is preferred; if its native C extension is not installed, it automatically and seamlessly falls back to ClickHouse's native HTTP interface (port 8123), with no cross-platform compilation dependencies to worry about.

---

## Detailed Tool Usage

### 1. ClickHouse Historical Kline Integrity Check (`check_clickhouse_integrity.py`)

Uses ClickHouse's vectorized window functions (`lagInFrame`) to scan hundreds of millions of kline rows across the whole database in seconds, precisely pinpointing every gap interval and any anomalous/dirty data.

#### What it checks:
- **Timestamp continuity**: computes `diff_ms` between adjacent klines to detect gaps `> interval_ms`, and calculates the cumulative number of missing bars and coverage rate.
- **Field non-null and logical validity**:
  - Price sanity: `open > 0`, `high > 0`, `low > 0`, `close > 0`
  - Price bound logic: `high >= low`, `high >= max(open, close)`, `low <= min(open, close)`
  - Volume: `volume >= 0`, `quote_volume >= 0`
  - Trade count: `trades_count >= 0` (warns on suspicious zero-trade bars for illiquid symbols)
  - Timestamp alignment: `end_time > start_time` and strictly matches the interval duration.

#### Example usage:
```bash
# Check all symbols in the whole database across the default configured intervals (1m, 1h)
python cmd/test-tools/check_clickhouse_integrity.py

# Check a specific symbol and interval
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --intervals 1m

# Check a specific time range (e.g. the last week)
python cmd/test-tools/check_clickhouse_integrity.py --start-date "2026-09-01" --max-gaps 10

# Quick sampled check of 10 symbols
python cmd/test-tools/check_clickhouse_integrity.py --limit-symbols 10
```

---

### 2. Redis Unclosed Live-Bar Check (`check_redis_livebars.py`)

Checks the still-forming, unclosed kline hashes in Redis (`livebar:<SYMBOL>:<interval>`).

#### What it checks:
- **Key existence**: whether every active market-wide symbol has a live bar present in Redis.
- **TTL validity**: whether the TTL is greater than 0 and within the expected window (`<= ttl_multiple * interval`).
- **Real-time freshness**: whether `now_ms - t` falls within the current interval; if it exceeds `2 * interval`, the bar is flagged as STALE (half-dead).
- **Completeness of the 11 fields**: `t, o, h, l, c, v, qv, tbv, tbqv, n, x`.

#### Example usage:
```bash
# Check real-time market data across the whole market
python cmd/test-tools/check_redis_livebars.py

# Show only symbols with anomalies/missing data
python cmd/test-tools/check_redis_livebars.py --show-only-failures
```

---

### 3. Redis Closed Rolling Window Check (`check_redis_closed_windows.py`)

Checks the closed rolling window (`kline:<SYMBOL>:<interval>`) that quant strategy engines consume from Redis.

#### What it checks:
- **Window length**: whether it reaches the expected length (200 bars by default in production).
- **Head freshness**: whether the start timestamp of the first list element (index 0) is strictly aligned with the boundary of the interval that just closed, and whether it lags behind.
- **Strictly decreasing order**: verifies timestamps are strictly ordered from newest to oldest (`t[0] > t[1] > ...`).
- **Internal continuity with no gaps**: verifies bar-by-bar that `t[i] - t[i+1] == interval_ms`, ruling out any missing bar inside the window.
- **Compact JSON format validation**: parses the 9-element compact array `[t, o, h, l, c, v, qv, tbv, tbqv]` and validates its values.

#### Example usage:
```bash
# Check the 1m and 1h windows
python cmd/test-tools/check_redis_closed_windows.py

# Check with a custom window size (e.g. 100 bars)
python cmd/test-tools/check_redis_closed_windows.py --window-size 100
```

---

### 4. Redis Cross-Section Ready Notification Monitor (`monitor_redis_kline_ready.py`)

Listens to (or replays) the `stream:market:kline_ready` cross-section notification stream to understand the aggregator's working state.

#### What it checks:
- **Aggregation readiness latency**: `publish time − kline close time`.
  - `< 1.0s`: instantly complete within the second (`onComplete`)
  - `~ 5.0s`: fallback timeout trigger (`onTimeout` safety-net mechanism)
  - `> 10.0s`: network congestion or worker blocking
- **Section symbol coverage rate**: the number of symbols received vs. the market-wide target symbol count (e.g. 248/250, 99.2%).
- **Section continuity**: whether the timestamp difference between two adjacent events is exactly one interval.

#### Example usage:
```bash
# Replay the most recent 20 published cross-section ready events
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# Real-time listening mode (prints alerts and latency stats immediately upon receiving an event)
python cmd/test-tools/monitor_redis_kline_ready.py --tail
```

---

### 5. End-to-End Dual-Write Consistency Reconciliation (`e2e_reconciliation.py`)

Verifies full data consistency between the Redis 1m rolling window cache and ClickHouse `fapi_kline_1m`.

#### What it checks:
- Fetches the 200 bars from Redis `kline:SYMBOL:1m` and the top 200 bars (descending) from ClickHouse `market.fapi_kline_1m`.
- Aligns timestamps `t` bar-by-bar.
- Compares `Open, High, Low, Close, Volume, QuoteVolume` bar-by-bar; floating-point differences must be within tolerance (`< 1e-5`).

> Applies to `1m` only: coarser intervals have no Redis window. Their correctness is verified via `check_vs_binance.py` against Binance.

#### Example usage:
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 10
python cmd/test-tools/e2e_reconciliation.py --symbol BTCUSDT --window-size 200
```

---

### 5b. ClickHouse vs. Binance External Ground-Truth Reconciliation (`check_vs_binance.py`)

`e2e_reconciliation.py` compares "the pipeline against itself" (Redis and ClickHouse are both written by the same pipeline). This tool reconciles ClickHouse bar-by-bar against **Binance's official** `/fapi/v1/klines` — the only check capable of catching **systematic deviations** in 1m collection or rollup aggregation.

#### What it checks:
- `start_time` must align exactly (bucket alignment / collection time base).
- OHLC relative tolerance `1e-9` (`argMin/argMax/min/max` → exactly equal).
- `volume / quote_volume / taker_buy_*` relative tolerance `1e-6` (due to floating-point summation order differences, ~1e-8).
- `trades_count` must be exactly equal (integer sum).

#### Example usage:
```bash
# 1m raw table vs. Binance 1m — verifies collection field mapping/units
python cmd/test-tools/check_vs_binance.py --intervals 1m --limit-symbols 10

# Rollup tables vs. Binance — verifies Phase B's bucket alignment and aggregation functions
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d --symbol BTCUSDT --bars 96
```

---

### 6. One-Shot Pre-Flight Scorecard (`run_all_checks.py`)

Before an official release or go-live, run all of the above checks with a single command; it automatically aggregates the results into an overview table and gives a go/no-go verdict (`READY FOR PRODUCTION DEPLOYMENT` or `DEPLOYMENT BLOCKED`).

#### Example usage:
```bash
# Quick smoke pre-flight (samples the first 5 core symbols)
python cmd/test-tools/run_all_checks.py --quick

# Full deep pre-flight
python cmd/test-tools/run_all_checks.py

# Verbose output with subcommand details
python cmd/test-tools/run_all_checks.py --verbose
```

---

## Pre-Flight Verdict Rules

| Status | Meaning | Recommended Action |
| :--- | :--- | :--- |
| **PASS** (green) | All metrics fully meet production standards (no gaps, data is complete, clocks are aligned) | Safe to deploy directly |
| **WARN** (yellow) | Non-critical minor deviations exist (e.g. zero trade count for an illiquid symbol, a just-cold-started window not yet filled to 200 bars) | May proceed selectively after manual confirmation of the cause |
| **FAIL** (red) | Serious data-quality issues exist (missing-bar gaps in klines, price or timestamp inversions, a lagging Redis head) | **Deployment strictly forbidden** — investigate Backfill or the Collector |
