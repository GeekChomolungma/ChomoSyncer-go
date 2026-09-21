# -*- coding: utf-8 -*-
"""
Open-interest (market.fapi_oi_5m) consistency checker.  READ-ONLY.

Design: new_requirements/oi.md.  A row's start_time is the OPEN time of the 5-minute
kline whose CLOSE the value belongs to, so the table joins fapi_kline_5m on
(symbol, start_time).  src_rank says who wrote it: 1 = live snapshot, 2 = hist
(GET /futures/data/openInterestHist), 3 = archive; on a merge the highest rank wins.

What it verifies, over the last --hours of bars that should already exist:

  A. The series itself, per symbol
       * no missing bars between the first and the last bar of the window
       * every start_time is on the 5-minute grid (catches a timezone / unit shift)
       * values are finite and non-negative (0 is only a WARN)
       * snap_time is consistent with start_time:
           hist/archive rows: snap_time == start_time + 5m exactly (FAIL otherwise)
           live rows:         any snap_time is stored as Binance returned it: an illiquid
                              symbol's older snapshot is that bar's value, and a start-up
                              catch-up round is taken after the close. Rows further than
                              --live-accept from start_time + 5m are only counted (WARN).
         (this proves hist labels are stored at T-5m; live rows are attributed by the
          round's boundary, not by snap_time)
       * the series is fresh: its newest bar is no more than --max-lag-bars behind
  B. Source health
       * live rows older than --max-uncalibrated-hours that hist never replaced
         (WARN; FAIL with --require-calibration): hist calibration is not running
       * newest calibrated (rank >= 2) bar is not stale
  C. Consumer view: for every fapi_kline_5m bar in the window, is there an OI row?
       per-symbol coverage >= --min-coverage
  D. Cross-section: per bar, the share of symbols that have an OI row
       >= --min-cross-section for every bar
  E. (--vs-binance) rows are compared value-for-value with Binance's own
       openInterestHist for a sample of symbols:
           a label T must be stored at start_time T-5m
           rank >= 2 rows must equal Binance's value (they were copied from it)
           rank 1 (live) rows must be within --live-tol of it
       Uses the /futures/data pool (1000 requests / 5 min per IP): one request per
       sampled symbol, paced by --symbol-delay.

Only bars whose close is at least --settle-minutes old are examined, so a bar that
hist has not published yet is not reported as missing.

  F. (--gaps-csv PATH) writes every missing-bar range of section A to a CSV that
       backfill_missing_oi.py repairs from Binance's openInterestHist. Add --gaps-tail to
       also list the bars between a symbol's newest row and the end of the window.

Exit code 0 = no FAIL (WARNs allowed).  1 = at least one FAIL or a connection error.

Examples
  python check_oi_consistency.py                          # whole market, last 24h
  python check_oi_consistency.py --symbol BTCUSDT,ETHUSDT --hours 6
  python check_oi_consistency.py --vs-binance --limit-symbols 5
  python check_oi_consistency.py --start-date 2020-09-01 --skip-coverage --gaps-csv oi_gaps.csv   # whole history
  python check_oi_consistency.py --table oi_scratch.fapi_oi_5m --kline-table market.fapi_kline_5m
"""
import os
import sys
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    Colors,
    get_clickhouse_client,
    load_config,
    format_ms_to_utc,
    print_table,
)
from check_vs_binance import rel_close
from check_clickhouse_integrity import write_gaps_csv

try:
    import requests
except ImportError:
    requests = None

BAR_MS = 300_000  # 5 minutes


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in test_toolkit.py)
# ---------------------------------------------------------------------------

def bar_window(now_ms: int, hours: float, settle_minutes: float) -> Tuple[int, int]:
    """
    (lo_ms, hi_ms): the inclusive range of bar start_times that should exist.

    A bar starting at s closes at s+5m; it is examined only once that close is at
    least `settle_minutes` old, so hist (which publishes a label 1-3 minutes late)
    has had time to run.  The range holds hours*12 bars.
    """
    settled = now_ms - int(settle_minutes * 60_000)
    hi = (settled // BAR_MS) * BAR_MS - BAR_MS
    lo = hi - int(hours * 3_600_000) + BAR_MS
    return lo, hi


def parse_start_date(text: str) -> int:
    """'2024-01-01' or '2024-01-01 06:30[:00]' (UTC) -> epoch ms, rounded UP to the next 5-minute bar."""
    text = text.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"cannot parse {text!r}; use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]' (UTC)")
    ms = int(dt.timestamp() * 1000)
    return -(-ms // BAR_MS) * BAR_MS


def tail_gaps(stats: Dict[str, Dict[str, Any]], hi_ms: int) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    One gap per symbol whose newest row is before `hi_ms`: the missing bar-open times are
    [newest + 5m, hi_ms + 5m).  Rows are (symbol, "5m", {from_ms, to_ms, missing_count}).
    """
    out = []
    for sym in sorted(stats):
        newest = int(stats[sym]["max_ms"])
        if newest < hi_ms:
            out.append((sym, "5m", {"from_ms": newest + BAR_MS, "to_ms": hi_ms + BAR_MS,
                                    "missing_count": (hi_ms - newest) // BAR_MS}))
    return out


def evaluate_symbol(row: Dict[str, Any], lo_ms: int, hi_ms: int,
                    max_lag_bars: int = 2, require_calibration: bool = False) -> Dict[str, Any]:
    """
    Turns one per-symbol aggregate (see fetch_symbol_stats) into a verdict.

    Returns {"status": PASS|WARN|FAIL, "fails": [...], "warns": [...], "missing": int,
             "lag_bars": int, "expected": int}.
    """
    n = int(row["n"])
    off_grid = int(row.get("off_grid", 0))
    min_ms = int(row["min_ms"])
    max_ms = int(row["max_ms"])
    on_grid = n - off_grid
    expected = (max_ms - min_ms) // BAR_MS + 1 if max_ms >= min_ms else n
    missing = max(expected - on_grid, 0)
    lag_bars = max((hi_ms - max_ms) // BAR_MS, 0)

    fails: List[str] = []
    warns: List[str] = []
    if missing > 0:
        fails.append(f"{missing} missing bar(s) inside the series")
    if off_grid > 0:
        fails.append(f"{off_grid} row(s) not on the 5-minute grid")
    if int(row.get("bad_value", 0)) > 0:
        fails.append(f"{row['bad_value']} non-finite or negative value(s)")
    if int(row.get("bad_snap_cal", 0)) > 0:
        fails.append(f"{row['bad_snap_cal']} hist/archive row(s) with snap_time != start_time+5m")
    if int(row.get("bad_snap_live", 0)) > 0:  # by design: live stores what Binance answers, a stale value is a sign of an illiquid symbol
        warns.append(f"{row['bad_snap_live']} live row(s) with snap_time far from the bar's close (stored as returned)")
    if int(row.get("bad_rank", 0)) > 0:
        fails.append(f"{row['bad_rank']} row(s) with an invalid src_rank")
    if lag_bars > max_lag_bars:
        fails.append(f"newest bar is {lag_bars} bar(s) behind the expected newest")
    if int(row.get("zero_value", 0)) > 0:
        warns.append(f"{row['zero_value']} zero-valued row(s)")
    stale = int(row.get("stale_live", 0))
    if stale > 0:
        msg = f"{stale} live row(s) never calibrated by hist"
        (fails if require_calibration else warns).append(msg)
    if min_ms > lo_ms + BAR_MS:  # series starts later than the window
        warns.append(f"series starts {(min_ms - lo_ms) // BAR_MS} bar(s) after the window start")

    status = "FAIL" if fails else ("WARN" if warns else "PASS")
    return {"status": status, "fails": fails, "warns": warns, "missing": missing,
            "lag_bars": lag_bars, "expected": expected}


def compare_with_binance(rows: Dict[int, Dict[str, Any]], points: List[Tuple[int, float]],
                         lo_ms: int, hi_ms: int, live_tol: float = 0.005,
                         exact_tol: float = 1e-9) -> Dict[str, Any]:
    """
    rows:   start_ms -> {"oi": float, "rank": int}   (the table, FINAL)
    points: [(label_ms, value)]                      (Binance openInterestHist)

    A label T belongs to the bar starting at T-5m.  Returns counts and the mismatches.
    """
    res = {"compared": 0, "ok_cal": 0, "ok_live": 0, "missing": [], "mismatch": [], "skipped": 0}
    for label_ms, val in points:
        start = label_ms - BAR_MS
        if start < lo_ms or start > hi_ms:
            res["skipped"] += 1
            continue
        row = rows.get(start)
        if row is None:
            res["missing"].append(start)
            continue
        res["compared"] += 1
        is_live = int(row["rank"]) == 1
        tol = live_tol if is_live else exact_tol
        if rel_close(float(row["oi"]), val, tol):
            res["ok_live" if is_live else "ok_cal"] += 1
        else:
            res["mismatch"].append({"start": start, "table": float(row["oi"]), "binance": val,
                                    "rank": int(row["rank"])})
    return res


def coverage_status(bars: int, with_oi: int, min_coverage: float) -> Tuple[float, str]:
    """Consumer-view coverage of one symbol (or one bar) and its PASS/FAIL."""
    if bars <= 0:
        return 1.0, "PASS"
    cov = with_oi / bars
    return cov, ("PASS" if cov >= min_coverage else "FAIL")


# ---------------------------------------------------------------------------
# ClickHouse queries
# ---------------------------------------------------------------------------

def ts_expr(ms: int) -> str:
    return f"fromUnixTimestamp64Milli({int(ms)}, 'UTC')"


def fetch_symbol_stats(ch, table: str, lo_ms: int, hi_ms: int, live_accept_ms: int,
                       stale_before_ms: int, symbols: Optional[List[str]]) -> Dict[str, Dict[str, Any]]:
    sym_filter = ""
    if symbols:
        quoted = ", ".join("'" + s.replace("'", "") + "'" for s in symbols)
        sym_filter = f" AND symbol IN ({quoted})"
    sql = f"""
    SELECT symbol,
        count() AS n,
        min(toUnixTimestamp64Milli(start_time)) AS min_ms,
        max(toUnixTimestamp64Milli(start_time)) AS max_ms,
        countIf(toUnixTimestamp64Milli(start_time) % {BAR_MS} != 0) AS off_grid,
        countIf(NOT isFinite(sum_open_interest) OR sum_open_interest < 0) AS bad_value,
        countIf(sum_open_interest = 0) AS zero_value,
        countIf(src_rank = 1) AS n_live,
        countIf(src_rank = 2) AS n_hist,
        countIf(src_rank = 3) AS n_archive,
        countIf(src_rank NOT IN (1, 2, 3)) AS bad_rank,
        countIf(src_rank = 1 AND toUnixTimestamp64Milli(start_time) < {int(stale_before_ms)}) AS stale_live,
        countIf(src_rank = 1 AND abs(toUnixTimestamp64Milli(snap_time) - (toUnixTimestamp64Milli(start_time) + {BAR_MS})) > {int(live_accept_ms)}) AS bad_snap_live,
        countIf(src_rank >= 2 AND toUnixTimestamp64Milli(snap_time) != toUnixTimestamp64Milli(start_time) + {BAR_MS}) AS bad_snap_cal
    FROM {table} FINAL
    WHERE start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)} {sym_filter}
    GROUP BY symbol ORDER BY symbol
    """
    out = {}
    for r in ch.query(sql):
        out[r["symbol"]] = {k: (v if k == "symbol" else int(v)) for k, v in r.items()}
    return out


def fetch_gaps(ch, table: str, lo_ms: int, hi_ms: int,
               symbols: Optional[List[str]]) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Missing bars strictly between two rows of the same symbol.  A symbol's first row is
    not compared with anything (its listing date is unknown), so leading gaps are not
    reported.  Only rows ON the 5-minute grid count: an off-grid row (reported by section A)
    is not a bar, so the bar it stands in for is listed as missing.  The missing bar-open
    times of a gap are [from_ms, to_ms).
    """
    sym_filter = ""
    if symbols:
        quoted = ", ".join("'" + s.replace("'", "") + "'" for s in symbols)
        sym_filter = f" AND symbol IN ({quoted})"
    rows = ch.query(f"""
    SELECT symbol,
           toUnixTimestamp64Milli(prev) + {BAR_MS} AS from_ms,
           toUnixTimestamp64Milli(start_time)      AS to_ms
    FROM (
        SELECT symbol, start_time,
               lagInFrame(start_time, 1) OVER (PARTITION BY symbol ORDER BY start_time
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev
        FROM {table} FINAL
        WHERE start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)} {sym_filter}
          AND toUnixTimestamp64Milli(start_time) % {BAR_MS} = 0
    )
    WHERE toUnixTimestamp64Milli(prev) > 0
      AND toUnixTimestamp64Milli(start_time) - toUnixTimestamp64Milli(prev) > {BAR_MS}
    ORDER BY symbol, from_ms""")
    out = []
    for r in rows:
        a, b = int(r["from_ms"]), int(r["to_ms"])
        out.append((r["symbol"], "5m", {"from_ms": a, "to_ms": b, "missing_count": (b - a) // BAR_MS}))
    return out


def fetch_freshness(ch, table: str) -> Dict[str, int]:
    rows = ch.query(f"""
        SELECT toUnixTimestamp64Milli(max(start_time)) AS newest,
               toUnixTimestamp64Milli(maxIf(start_time, src_rank >= 2)) AS newest_cal,
               count() AS total
        FROM {table}""")
    r = rows[0] if rows else {}
    return {"newest": int(r.get("newest", 0)), "newest_cal": int(r.get("newest_cal", 0)), "total": int(r.get("total", 0))}


def fetch_kline_symbols(ch, kline_table: str, lo_ms: int, hi_ms: int) -> List[str]:
    rows = ch.query(f"""SELECT DISTINCT symbol FROM {kline_table} FINAL
        WHERE start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)} ORDER BY symbol""")
    return [r["symbol"] for r in rows]


def _join_sql(kline_table: str, oi_table: str, lo_ms: int, hi_ms: int, group_expr: str, symbols: Optional[List[str]]) -> str:
    sym_filter = ""
    if symbols:
        quoted = ", ".join("'" + s.replace("'", "") + "'" for s in symbols)
        sym_filter = f" AND symbol IN ({quoted})"
    return f"""
    SELECT {group_expr} AS grp, count() AS bars, countIf(o.has = 1) AS with_oi
    FROM (SELECT symbol, start_time FROM {kline_table} FINAL
          WHERE start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)} {sym_filter}) AS k
    LEFT JOIN (SELECT symbol, start_time, 1 AS has FROM {oi_table} FINAL
          WHERE start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)} {sym_filter}) AS o
      ON k.symbol = o.symbol AND k.start_time = o.start_time
    GROUP BY grp ORDER BY grp
    """


def fetch_coverage_by_symbol(ch, kline_table, oi_table, lo_ms, hi_ms, symbols) -> Dict[str, Tuple[int, int]]:
    sql = _join_sql(kline_table, oi_table, lo_ms, hi_ms, "k.symbol", symbols)
    return {r["grp"]: (int(r["bars"]), int(r["with_oi"])) for r in ch.query(sql)}


def fetch_coverage_by_bar(ch, kline_table, oi_table, lo_ms, hi_ms, symbols) -> Dict[int, Tuple[int, int]]:
    sql = _join_sql(kline_table, oi_table, lo_ms, hi_ms, "toUnixTimestamp64Milli(k.start_time)", symbols)
    return {int(r["grp"]): (int(r["bars"]), int(r["with_oi"])) for r in ch.query(sql)}


def fetch_symbol_rows(ch, table: str, symbol: str, lo_ms: int, hi_ms: int) -> Dict[int, Dict[str, Any]]:
    rows = ch.query(f"""
        SELECT toUnixTimestamp64Milli(start_time) AS start_ms, sum_open_interest AS oi, src_rank AS rank
        FROM {table} FINAL
        WHERE symbol = '{symbol.replace("'", "")}' AND start_time >= {ts_expr(lo_ms)} AND start_time <= {ts_expr(hi_ms)}""")
    return {int(r["start_ms"]): {"oi": float(r["oi"]), "rank": int(r["rank"])} for r in rows}


def binance_hist(rest_url: str, symbol: str, limit: int, timeout: int = 15) -> List[Tuple[int, float]]:
    if requests is None:
        raise RuntimeError("the 'requests' package is required for --vs-binance")
    resp = requests.get(f"{rest_url}/futures/data/openInterestHist",
                        params={"symbol": symbol, "period": "5m", "limit": limit}, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Binance {resp.status_code}: {resp.text[:160]}")
    pts = [(int(x["timestamp"]), float(x["sumOpenInterest"])) for x in resp.json()]
    pts.sort()
    return pts


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def paint(status: str) -> str:
    color = {"PASS": Colors.GREEN, "WARN": Colors.YELLOW, "FAIL": Colors.RED}.get(status, "")
    return f"{color}{status}{Colors.RESET}"


def main():
    p = argparse.ArgumentParser(description="Read-only consistency check of the 5-minute open-interest table.")
    p.add_argument("--config", help="Path to config.yaml (auto-discovered if omitted)")
    p.add_argument("--table", default=None, help="OI table (default: open_interest.table from config, else market.fapi_oi_5m)")
    p.add_argument("--kline-table", default="market.fapi_kline_5m", help="5m kline table used for the coverage check")
    p.add_argument("--hours", type=float, default=24.0, help="Window length in hours (default 24)")
    p.add_argument("--start-date", help="Window start (UTC), e.g. '2020-09-01' or '2024-01-01 06:30'; overrides --hours. Use with --skip-coverage to scan the whole history")
    p.add_argument("--gaps-csv", metavar="PATH", help="Write every missing-bar range to this CSV (input of backfill_missing_oi.py)")
    p.add_argument("--gaps-tail", action="store_true", help="With --gaps-csv: also list bars between a symbol's newest row and the window end (delisted symbols show up here and can never be filled)")
    p.add_argument("--settle-minutes", type=float, default=10.0, help="Only examine bars whose close is at least this old (default 10)")
    p.add_argument("--symbol", help="Comma-separated symbols to check; whole market if omitted")
    p.add_argument("--limit-symbols", type=int, default=None, help="Check only the first N symbols (alphabetical)")
    p.add_argument("--max-lag-bars", type=int, default=2, help="Newest bar may be at most this many bars behind (default 2)")
    p.add_argument("--live-accept", type=float, default=60.0, help="Live rows whose snap_time is further than this many seconds from the bar's close are counted as WARN, not rejected (default 60)")
    p.add_argument("--max-uncalibrated-hours", type=float, default=2.0, help="Live rows older than this should have been replaced by hist (default 2)")
    p.add_argument("--require-calibration", action="store_true", help="Make uncalibrated live rows a FAIL instead of a WARN")
    p.add_argument("--min-coverage", type=float, default=0.995, help="Per-symbol share of kline bars that have an OI row (default 0.995)")
    p.add_argument("--min-cross-section", type=float, default=0.99, help="Per-bar share of symbols that have an OI row (default 0.99)")
    p.add_argument("--skip-coverage", action="store_true", help="Skip the join against the kline table (C and D)")
    p.add_argument("--show-all", action="store_true", help="List every symbol, not just the ones with findings")
    p.add_argument("--max-rows", type=int, default=30, help="Max symbols listed per section (default 30)")
    p.add_argument("--vs-binance", action="store_true", help="Also compare rows with Binance openInterestHist (hits the exchange)")
    p.add_argument("--binance-bars", type=int, default=48, help="Newest hist points fetched per sampled symbol (default 48)")
    p.add_argument("--binance-symbols", type=int, default=5, help="How many symbols to sample for --vs-binance (default 5)")
    p.add_argument("--live-tol", type=float, default=0.005, help="Relative tolerance for live (rank 1) rows vs Binance (default 0.005)")
    p.add_argument("--symbol-delay", type=float, default=0.5, help="Seconds between Binance requests (default 0.5)")
    p.add_argument("--rest-url", default=None, help="Binance REST base URL (default: universe.rest_url from config)")
    p.add_argument("--ch-host"); p.add_argument("--ch-port", type=int); p.add_argument("--ch-db")
    p.add_argument("--ch-user"); p.add_argument("--ch-password")
    args = p.parse_args()

    config = load_config(args.config)
    oi_cfg = config.get("open_interest", {}) or {}
    table = args.table or oi_cfg.get("table") or "market.fapi_oi_5m"
    rest_url = args.rest_url or (config.get("universe", {}) or {}).get("rest_url", "https://fapi.binance.com")
    ch = get_clickhouse_client(config, args)

    print(f"{Colors.BOLD}{Colors.CYAN}=== Open-Interest Consistency Check ==={Colors.RESET}")
    print(f"ClickHouse: {ch.host}:{ch.port}   OI table: {table}   kline table: {args.kline_table}")
    ok, msg = ch.test_connection()
    if not ok:
        print(f"{Colors.RED}[ERROR] Cannot connect to ClickHouse: {msg}{Colors.RESET}")
        sys.exit(1)

    for t in ([table] if args.skip_coverage else [table, args.kline_table]):
        res = ch.query(f"EXISTS TABLE {t}")
        if not res or int(list(res[0].values())[0]) == 0:
            hint = "  Apply deploy/clickhouse/004_fapi_oi.sql first." if t == table else ""
            print(f"{Colors.RED}[FAIL] Table {t} does not exist.{hint}{Colors.RESET}")
            sys.exit(1)

    now_ms = int(ch.query("SELECT toUnixTimestamp(now()) AS ts")[0]["ts"]) * 1000
    lo_ms, hi_ms = bar_window(now_ms, args.hours, args.settle_minutes)
    if args.start_date:
        try:
            lo_ms = parse_start_date(args.start_date)
        except ValueError as e:
            print(f"{Colors.RED}[ERROR] --start-date: {e}{Colors.RESET}")
            sys.exit(1)
        if lo_ms > hi_ms:
            print(f"{Colors.RED}[ERROR] --start-date is after the newest settled bar ({format_ms_to_utc(hi_ms)}).{Colors.RESET}")
            sys.exit(1)
    print(f"Window: bars {format_ms_to_utc(lo_ms)} .. {format_ms_to_utc(hi_ms)} UTC "
          f"({int((hi_ms - lo_ms) / BAR_MS) + 1} bars; close >= {args.settle_minutes:g} min old)\n")

    fails: List[str] = []
    warns: List[str] = []

    # symbols in scope
    if args.symbol:
        symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
    else:
        symbols = None
    if args.limit_symbols and args.limit_symbols > 0 and symbols is None:
        universe = fetch_kline_symbols(ch, args.kline_table, lo_ms, hi_ms) if not args.skip_coverage else []
        symbols = universe[:args.limit_symbols] if universe else None

    # ---- freshness ----------------------------------------------------------
    fresh = fetch_freshness(ch, table)
    print(f"{Colors.BOLD}--- B. Freshness & source health ---{Colors.RESET}")
    if fresh["total"] == 0:
        print(f"{Colors.RED}[FAIL] {table} is empty.{Colors.RESET}")
        sys.exit(1)
    lag = max((hi_ms - fresh["newest"]) // BAR_MS, 0)
    lag_cal = max((hi_ms - fresh["newest_cal"]) // BAR_MS, 0) if fresh["newest_cal"] > 0 else None
    print(f"rows in table: {fresh['total']:,}   newest bar: {format_ms_to_utc(fresh['newest'])} ({lag} bar(s) behind expected)")
    if fresh["newest_cal"] > 0:
        print(f"newest calibrated (hist/archive) bar: {format_ms_to_utc(fresh['newest_cal'])} ({lag_cal} bar(s) behind expected)")
    else:
        print("newest calibrated (hist/archive) bar: none")
    if lag > args.max_lag_bars:
        fails.append(f"table is stale: newest bar is {lag} bars behind (is the module running?)")
    if lag_cal is None:
        warns.append("no calibrated (rank >= 2) rows at all: hist has not run")
    elif lag_cal * BAR_MS > args.max_uncalibrated_hours * 3_600_000:
        warns.append(f"newest calibrated bar is {lag_cal} bars behind: hourly hist calibration looks stalled")

    # ---- A. per-symbol series ----------------------------------------------
    print(f"\n{Colors.BOLD}--- A. Series integrity per symbol ---{Colors.RESET}")
    stale_before = hi_ms - int(args.max_uncalibrated_hours * 3_600_000)
    stats = fetch_symbol_stats(ch, table, lo_ms, hi_ms, int(args.live_accept * 1000), stale_before, symbols)
    if not stats:
        print(f"{Colors.RED}[FAIL] no OI rows in the window.{Colors.RESET}")
        fails.append("no OI rows in the window")
    results = {s: evaluate_symbol(r, lo_ms, hi_ms, args.max_lag_bars, args.require_calibration) for s, r in stats.items()}
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for r in results.values():
        counts[r["status"]] += 1
    tot = {k: sum(r.get(k, 0) for r in stats.values()) for k in ("n", "n_live", "n_hist", "n_archive")}
    print(f"symbols with rows: {len(stats)}   {Colors.GREEN}{counts['PASS']} PASS{Colors.RESET}, "
          f"{Colors.YELLOW}{counts['WARN']} WARN{Colors.RESET}, {Colors.RED}{counts['FAIL']} FAIL{Colors.RESET}")
    print(f"rows in window by source: live={tot['n_live']:,}  hist={tot['n_hist']:,}  archive={tot['n_archive']:,}  (total {tot['n']:,})")
    listed = [(s, results[s]) for s in results if args.show_all or results[s]["status"] != "PASS"]
    if listed:
        rows_out = []
        for s, r in listed[:args.max_rows]:
            st = stats[s]
            rows_out.append([s, st["n"], r["missing"], r["lag_bars"], f"{st['n_live']}/{st['n_hist']}/{st['n_archive']}",
                             paint(r["status"]), "; ".join(r["fails"] + r["warns"])])
        print_table(["Symbol", "Bars", "Missing", "Lag", "live/hist/arch", "Status", "Findings"], rows_out)
        if len(listed) > args.max_rows:
            print(f"... and {len(listed) - args.max_rows} more (use --max-rows / --show-all)")
    for s, r in results.items():
        for f in r["fails"]:
            fails.append(f"{s}: {f}")
        for w in r["warns"]:
            warns.append(f"{s}: {w}")
    fails = fails[:200]

    # ---- F. gaps CSV ---------------------------------------------------------
    if args.gaps_csv:
        gap_rows = fetch_gaps(ch, table, lo_ms, hi_ms, symbols)
        if args.gaps_tail:
            gap_rows += tail_gaps(stats, hi_ms)
        gap_rows.sort(key=lambda g: (g[0], g[2]["from_ms"]))
        n = write_gaps_csv(args.gaps_csv, gap_rows)
        bars = sum(g[2]["missing_count"] for g in gap_rows)
        print(f"\nWrote {n} gap line(s) ({bars:,} missing bar(s), {len({g[0] for g in gap_rows})} symbol(s)) to {args.gaps_csv}")
        if n:
            print(f"Repair: python cmd/test-tools/backfill_missing_oi.py {args.gaps_csv}   (dry run; add --apply to write)")

    # ---- C/D. consumer view ---------------------------------------------------
    if not args.skip_coverage:
        print(f"\n{Colors.BOLD}--- C. Consumer view: kline bars that have an OI row ---{Colors.RESET}")
        cov = fetch_coverage_by_symbol(ch, args.kline_table, table, lo_ms, hi_ms, symbols)
        bad, total_bars, total_with = [], 0, 0
        for s, (bars, with_oi) in cov.items():
            total_bars += bars
            total_with += with_oi
            c, st = coverage_status(bars, with_oi, args.min_coverage)
            if st == "FAIL":
                bad.append((s, bars, with_oi, c))
        overall = total_with / total_bars if total_bars else 1.0
        print(f"kline symbols: {len(cov)}   bars: {total_bars:,}   with OI: {total_with:,} ({overall * 100:.3f}%)   "
              f"below {args.min_coverage * 100:.1f}%: {len(bad)}")
        if bad:
            bad.sort(key=lambda x: x[3])
            print_table(["Symbol", "Kline bars", "With OI", "Coverage"],
                        [[s, b, w, f"{Colors.RED}{c * 100:.2f}%{Colors.RESET}"] for s, b, w, c in bad[:args.max_rows]])
            for s, b, w, c in bad:
                if w == 0:
                    fails.append(f"{s}: has {b} kline bars but no OI row at all (never written?)")
                else:
                    fails.append(f"{s}: only {c * 100:.2f}% of its kline bars have an OI row ({w}/{b})")

        print(f"\n{Colors.BOLD}--- D. Cross-section per bar ---{Colors.RESET}")
        bybar = fetch_coverage_by_bar(ch, args.kline_table, table, lo_ms, hi_ms, symbols)
        worst = sorted(((w / b if b else 1.0, t, b, w) for t, (b, w) in bybar.items()))
        below = [x for x in worst if x[0] < args.min_cross_section]
        print(f"bars examined: {len(bybar)}   below {args.min_cross_section * 100:.1f}% of the symbols: {len(below)}")
        if below:
            print_table(["Bar start (UTC)", "Symbols with kline", "With OI", "Share"],
                        [[format_ms_to_utc(t), b, w, f"{Colors.RED}{c * 100:.1f}%{Colors.RESET}"] for c, t, b, w in below[:10]])
            fails.append(f"{len(below)} bar(s) have OI for fewer than {args.min_cross_section * 100:.1f}% of the symbols (worst {below[0][0] * 100:.1f}% at {format_ms_to_utc(below[0][1])})")

    # ---- E. vs Binance -------------------------------------------------------
    if args.vs_binance:
        print(f"\n{Colors.BOLD}--- E. Rows vs Binance openInterestHist ---{Colors.RESET}")
        sample = (symbols or sorted(stats))[:args.binance_symbols]
        rows_out = []
        for i, s in enumerate(sample):
            if i:
                time.sleep(args.symbol_delay)
            try:
                pts = binance_hist(rest_url, s, args.binance_bars)
            except Exception as e:
                rows_out.append([s, "-", "-", "-", "-", paint("FAIL"), str(e)[:60]])
                fails.append(f"{s}: Binance request failed: {e}")
                continue
            table_rows = fetch_symbol_rows(ch, table, s, lo_ms, hi_ms)
            cmpres = compare_with_binance(table_rows, pts, lo_ms, hi_ms, args.live_tol)
            status = "PASS"
            detail = ""
            if cmpres["mismatch"]:
                status = "FAIL"
                m = cmpres["mismatch"][0]
                detail = f"e.g. bar {format_ms_to_utc(m['start'])}: table {m['table']} vs Binance {m['binance']} (rank {m['rank']})"
                fails.append(f"{s}: {len(cmpres['mismatch'])} row(s) disagree with Binance ({detail})")
            if cmpres["missing"]:
                status = "FAIL"
                detail = detail or f"no row for bar {format_ms_to_utc(cmpres['missing'][0])}"
                fails.append(f"{s}: {len(cmpres['missing'])} published hist label(s) have no row at start_time = label - 5m")
            if cmpres["compared"] == 0 and status == "PASS":
                status = "WARN"
                detail = "no label fell inside the window"
                warns.append(f"{s}: nothing to compare with Binance")
            rows_out.append([s, cmpres["compared"], cmpres["ok_cal"], cmpres["ok_live"],
                             len(cmpres["mismatch"]) + len(cmpres["missing"]), paint(status), detail])
        print_table(["Symbol", "Compared", "Exact (hist/archive)", "Within tol (live)", "Bad", "Status", "Detail"], rows_out)

    # ---- verdict --------------------------------------------------------------
    print()
    if warns:
        print(f"{Colors.YELLOW}{len(warns)} warning(s){Colors.RESET}" + (":" if len(warns) <= 5 else " (first 5):"))
        for w in warns[:5]:
            print(f"  - {w}")
    if fails:
        print(f"{Colors.RED}{Colors.BOLD}FAIL: {len(fails)} finding(s){Colors.RESET}" + (":" if len(fails) <= 10 else " (first 10):"))
        for f in fails[:10]:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{Colors.GREEN}{Colors.BOLD}PASS: open-interest table is consistent.{Colors.RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
