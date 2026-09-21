# -*- coding: utf-8 -*-
"""
Backfill missing 5m open-interest bars into ClickHouse from Binance's openInterestHist.

Input is the CSV written by check_oi_consistency.py --gaps-csv:

    python3 check_oi_consistency.py --start-date 2020-09-01 --skip-coverage --gaps-csv oi_gaps.csv
    python3 backfill_missing_oi.py oi_gaps.csv            # dry run: fetch + report, write nothing
    python3 backfill_missing_oi.py oi_gaps.csv --apply    # insert the bars that were found

What it does, per symbol:
  1. drops the parts of each gap that are older than Binance's retention (~30 days; only the
     latest month of /futures/data/openInterestHist exists) and counts them as "beyond
     retention": those need the archive (data.binance.vision), not this script;
  2. drops the bars ClickHouse already has (safe to re-run; existing rows are never overwritten);
  3. groups the remaining bars into as few requests as fit one page (500 bars);
  4. fetches them, paced against the /futures/data pool (1000 requests / 5 min per IP; this
     script keeps to --rps and --window-cap, the same numbers as open_interest.data_rps and
     data_window_cap, and honours 429/418);
  5. inserts only bars that were asked for and are still missing, as src_rank = 2 (hist),
     with the same mapping as the Go module: a label T is stored at start_time = T - 5m,
     snap_time = T.

Binance may return nothing for a bar (no data for that time); those bars are counted as
"empty at exchange" and are not inserted. The checker keeps listing them.

Only market.fapi_oi_5m is written. The 15m..1d rollups pick a repair up on their next refresh
if it is inside their lookback (3 days for 15m/1h, 7 for 4h, 10 for 1d); older
repairs need rollup_oi_per_month.sh, see the hint printed at the end.
"""
import argparse
import csv
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import Colors, ClickHouseClient, get_clickhouse_client, load_config
from backfill_missing_1m import fmt_ts, merge_ranges

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

BAR_MS = 300_000
MAX_LIMIT = 500                # /futures/data/openInterestHist hard cap per request
SRC_RANK_HIST = 2
SYMBOL_RE = re.compile(r"^\w{1,32}$")   # the symbol is put into SQL: no quotes, spaces, semicolons
INSERT_COLUMNS = ["symbol", "start_time", "sum_open_interest", "snap_time", "src_rank"]
# Rollup lookbacks in days (deploy/clickhouse/005_oi_rollups.sql).
ROLLUP_LOOKBACK_DAYS = {"15m": 3, "1h": 3, "4h": 7, "1d": 10}

Range = Tuple[int, int]  # [from_ms, to_ms) of bar-open times


class InvalidSymbol(Exception):
    """Binance rejected the symbol (HTTP 400, code -1121): delisted or never listed."""


# ----------------------------------------------------------------------------- pure helpers

def read_gaps_csv(path: str) -> Tuple[Dict[str, List[Range]], List[str]]:
    """
    Reads the gaps CSV. Returns ({symbol: [(from_ms, to_ms), ...]}, [warnings]).
    Only 5m lines are used; malformed or misaligned lines are skipped with a warning.
    """
    out: Dict[str, List[Range]] = {}
    warns: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        need = {"symbol", "interval", "from_ms", "to_ms"}
        if not rd.fieldnames or not need.issubset(rd.fieldnames):
            raise ValueError(f"{path}: expected columns {sorted(need)}, got {rd.fieldnames}")
        for n, row in enumerate(rd, start=2):
            sym = (row["symbol"] or "").strip().upper()
            if row["interval"].strip() != "5m":
                warns.append(f"line {n}: interval {row['interval']!r} is not 5m; skipped")
                continue
            if not SYMBOL_RE.match(sym):
                warns.append(f"line {n}: bad symbol {sym!r}; skipped")
                continue
            try:
                a, b = int(row["from_ms"]), int(row["to_ms"])
            except ValueError:
                warns.append(f"line {n}: non-integer from_ms/to_ms; skipped")
                continue
            if a % BAR_MS or b % BAR_MS or b <= a:
                warns.append(f"line {n}: range [{a},{b}) is not a whole-bar non-empty range; skipped")
                continue
            out.setdefault(sym, []).append((a, b))
    return {s: merge_ranges(r) for s, r in out.items()}, warns


def subtract_present(ranges: List[Range], present_ms: Iterable[int]) -> List[Range]:
    """Removes the bars in `present_ms` from `ranges`; returns the missing sub-ranges."""
    present = set(present_ms)
    out: List[Range] = []
    for a, b in ranges:
        start: Optional[int] = None
        for t in range(a, b, BAR_MS):
            if t in present:
                if start is not None:
                    out.append((start, t))
                    start = None
            elif start is None:
                start = t
        if start is not None:
            out.append((start, b))
    return out


def clip_to_retention(ranges: List[Range], oldest_ms: int) -> Tuple[List[Range], int]:
    """Drops the part of `ranges` before `oldest_ms`. Returns (kept ranges, bars dropped)."""
    kept: List[Range] = []
    dropped = 0
    for a, b in ranges:
        if b <= oldest_ms:
            dropped += (b - a) // BAR_MS
            continue
        if a < oldest_ms:
            dropped += (oldest_ms - a) // BAR_MS
            a = oldest_ms
        kept.append((a, b))
    return kept, dropped


def plan_spans(ranges: List[Range], max_bars: int = MAX_LIMIT) -> List[Range]:
    """
    Groups sorted, disjoint missing ranges into request spans. The pool is limited by request
    COUNT, not weight, so two neighbours share one request whenever the combined span (bars in
    between are fetched and discarded) fits one page; a range longer than a page is cut into pages.
    """
    pieces: List[Range] = []
    for a, b in ranges:
        while (b - a) // BAR_MS > max_bars:
            pieces.append((a, a + max_bars * BAR_MS))
            a += max_bars * BAR_MS
        pieces.append((a, b))
    spans: List[Range] = []
    for a, b in pieces:
        if spans and (b - spans[-1][0]) // BAR_MS <= max_bars:
            spans[-1] = (spans[-1][0], b)
        else:
            spans.append((a, b))
    return spans


def wanted(start_ms: int, ranges: List[Range]) -> bool:
    return any(a <= start_ms < b for a, b in ranges)


def point_to_row(symbol: str, label_ms: int, oi: float) -> Dict[str, Any]:
    """One openInterestHist point -> one fapi_oi_5m row (same mapping as the Go module: T is stored at T-5m)."""
    return {
        "symbol": symbol,
        "start_time": fmt_ts(label_ms - BAR_MS),
        "sum_open_interest": oi,
        "snap_time": fmt_ts(label_ms),
        "src_rank": SRC_RANK_HIST,
    }


def rollup_hint(min_ms: int, now_ms: int) -> List[str]:
    """Which rollup tables no longer refresh over the repaired bars."""
    age_days = (now_ms - min_ms) / 86_400_000.0
    return [iv for iv, d in ROLLUP_LOOKBACK_DAYS.items() if age_days > d]


def month_start(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-01")


def next_month_start(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    y, m = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
    return f"{y:04d}-{m:02d}-01"


# ----------------------------------------------------------------------------- Binance REST

class DataPacer:
    """
    Admission for the /futures/data pool (1000 requests per 5 minutes per IP, no weight header):
    at most `rps` requests per second and at most `window_cap` in any rolling `window_s`
    seconds. pause_for() blocks everything for a while after a 429/418.
    """

    def __init__(self, rps: float = 2.0, window_cap: int = 900, window_s: float = 300.0,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep):
        self.min_gap = 1.0 / rps if rps > 0 else 0.0
        self.window_cap, self.window_s = window_cap, window_s
        self.clock, self.sleep = clock, sleep
        self.events: List[float] = []
        self.blocked_until = 0.0

    def before(self) -> None:
        while True:
            now = self.clock()
            self.events = [t for t in self.events if now - t < self.window_s]
            wait = max(0.0, self.blocked_until - now)
            if self.events:
                wait = max(wait, self.events[-1] + self.min_gap - now)
            if len(self.events) >= self.window_cap:
                wait = max(wait, self.events[0] + self.window_s - now)
            if wait <= 0:
                self.events.append(now)
                return
            self.sleep(wait)

    def pause_for(self, seconds: float) -> None:
        self.blocked_until = max(self.blocked_until, self.clock() + seconds)


class OIHist:
    def __init__(self, rest_url: str, pacer: DataPacer, session: Any = None, timeout: int = 15,
                 sleep: Callable[[float], None] = time.sleep):
        if session is None and requests is None:
            raise RuntimeError("the 'requests' package is required for backfill_missing_oi.py")
        self.url = rest_url.rstrip("/") + "/futures/data/openInterestHist"
        self.pacer, self.timeout, self.sleep = pacer, timeout, sleep
        self.s = session or requests.Session()

    def fetch(self, symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
        """
        [(label_ms, open_interest)] for the bars with open time in [start_ms, end_ms): their
        labels are start_ms+5m .. end_ms. One request; the span is at most one page.
        """
        limit = max(1, min(MAX_LIMIT, (end_ms - start_ms) // BAR_MS))
        params = {"symbol": symbol, "period": "5m", "startTime": start_ms + BAR_MS,
                  "endTime": end_ms, "limit": limit}
        for attempt in range(8):
            self.pacer.before()
            resp = self.s.get(self.url, params=params, timeout=self.timeout)
            if resp.status_code in (418, 429):
                wait = float(resp.headers.get("Retry-After", "0") or 0) or min(60.0, 5.0 * (attempt + 1))
                print(f"{Colors.YELLOW}  rate limited ({resp.status_code}); waiting {wait:.0f}s{Colors.RESET}")
                self.pacer.pause_for(wait)
                continue
            if resp.status_code >= 500:
                self.sleep(min(10.0, 1.0 * (attempt + 1)))
                continue
            if resp.status_code == 400:
                try:
                    code = resp.json().get("code")
                except Exception:
                    code = None
                if code == -1121:
                    raise InvalidSymbol(symbol)
            resp.raise_for_status()
            pts = []
            for x in resp.json():
                try:
                    pts.append((int(x["timestamp"]), float(x["sumOpenInterest"])))
                except (KeyError, TypeError, ValueError):
                    continue
            pts.sort()
            return pts
        raise RuntimeError(f"{symbol}: openInterestHist request kept failing after retries")


# ----------------------------------------------------------------------------- ClickHouse

def existing_bars(ch: ClickHouseClient, table: str, symbol: str, lo: int, hi: int) -> List[int]:
    """start_times (on the 5m grid) the table already holds in [lo, hi)."""
    sql = f"""
    SELECT toUnixTimestamp64Milli(start_time) AS t
    FROM {table} FINAL
    WHERE symbol = '{symbol}'
      AND start_time >= fromUnixTimestamp64Milli({lo}, 'UTC')
      AND start_time <  fromUnixTimestamp64Milli({hi}, 'UTC')
      AND toUnixTimestamp64Milli(start_time) % {BAR_MS} = 0
    """
    return [int(r["t"]) for r in ch.query(sql)]


# ----------------------------------------------------------------------------- main flow

def run(ch: ClickHouseClient, table: str, gaps: Dict[str, List[Range]], fetcher: OIHist,
        apply: bool, now_ms: int, retention_days: float = 30.0, batch_rows: int = 2000,
        out: Callable[[str], None] = print) -> Dict[str, Any]:
    """Repairs every symbol in `gaps`. Returns a summary dict."""
    # A bar is published once its label (start + 5m) is in the past; later ones cannot exist yet.
    last_start = (now_ms // BAR_MS) * BAR_MS - BAR_MS
    oldest = ((now_ms - int(retention_days * 86_400_000)) // BAR_MS + 1) * BAR_MS
    stats: Dict[str, Any] = {"symbols": len(gaps), "requested_bars": 0, "beyond_retention": 0,
                             "already_present": 0, "fetched": 0, "inserted": 0, "empty_at_exchange": 0,
                             "failed_symbols": [], "invalid_symbols": [], "min_ms": None, "max_ms": None}
    pending: List[Dict[str, Any]] = []

    def flush() -> None:
        if pending and apply:
            stats["inserted"] += ch.insert_json_rows(table, INSERT_COLUMNS, pending)
        pending.clear()

    for i, (sym, ranges) in enumerate(sorted(gaps.items()), start=1):
        ranges = [(a, min(b, last_start + BAR_MS)) for a, b in ranges if a <= last_start]
        if not ranges:
            continue
        total = sum((b - a) // BAR_MS for a, b in ranges)
        stats["requested_bars"] += total
        ranges, dropped = clip_to_retention(ranges, oldest)
        stats["beyond_retention"] += dropped
        if not ranges:
            out(f"  [{i}/{len(gaps)}] {sym}: {dropped} bar(s) beyond Binance's retention, nothing to fetch")
            continue
        try:
            present = existing_bars(ch, table, sym, ranges[0][0], ranges[-1][1])
            todo = subtract_present(ranges, present)
            missing = sum((b - a) // BAR_MS for a, b in todo)
            stats["already_present"] += (total - dropped) - missing
            got = 0
            for span in plan_spans(todo):
                for label_ms, oi in fetcher.fetch(sym, span[0], span[1]):
                    start = label_ms - BAR_MS
                    if start % BAR_MS or not wanted(start, todo) or start > last_start:
                        continue
                    if math.isnan(oi) or math.isinf(oi) or oi < 0:
                        continue
                    pending.append(point_to_row(sym, label_ms, oi))
                    got += 1
                    stats["min_ms"] = start if stats["min_ms"] is None else min(stats["min_ms"], start)
                    stats["max_ms"] = start if stats["max_ms"] is None else max(stats["max_ms"], start)
                if len(pending) >= batch_rows:
                    flush()
            empty = missing - got
            stats["fetched"] += got
            stats["empty_at_exchange"] += max(0, empty)
            out(f"  [{i}/{len(gaps)}] {sym}: missing {missing}, found at exchange {got}"
                + (f", empty at exchange {empty}" if empty else "")
                + (f", beyond retention {dropped}" if dropped else "")
                + (f" ({(total - dropped) - missing} were already present)" if (total - dropped) != missing else ""))
        except InvalidSymbol:
            stats["invalid_symbols"].append(sym)
            out(f"{Colors.YELLOW}  [{i}/{len(gaps)}] {sym}: rejected by Binance (delisted?); skipped{Colors.RESET}")
        except Exception as e:  # one bad symbol must not abort the run
            stats["failed_symbols"].append(sym)
            out(f"{Colors.RED}  [{i}/{len(gaps)}] {sym}: FAILED: {e}{Colors.RESET}")
    flush()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill missing 5m open-interest bars into ClickHouse from Binance openInterestHist, "
                    "from a check_oi_consistency.py --gaps-csv file. Dry run unless --apply is given.")
    ap.add_argument("csv", help="Gaps CSV written by check_oi_consistency.py --gaps-csv")
    ap.add_argument("--apply", action="store_true", help="Actually INSERT the bars (default: dry run, write nothing)")
    ap.add_argument("--config", help="Path to config.yaml (auto-discovered if omitted)")
    ap.add_argument("--table", default=None, help="OI table (default: open_interest.table from config, else market.fapi_oi_5m)")
    ap.add_argument("--symbol", help="Only repair this symbol")
    ap.add_argument("--rest-url", help="Binance REST base URL (default: universe.rest_url from config)")
    ap.add_argument("--rps", type=float, default=2.0, help="Requests per second (default 2, = open_interest.data_rps)")
    ap.add_argument("--window-cap", type=int, default=900,
                    help="Max requests in any 5 minutes (default 900 of the 1000 per IP; = open_interest.data_window_cap). "
                         "If the running service is doing its own hist pass, lower this")
    ap.add_argument("--retention-days", type=float, default=30.0,
                    help="Binance keeps about this much openInterestHist; older bars are reported, not requested (default 30)")
    ap.add_argument("--batch-rows", type=int, default=2000, help="Rows per INSERT (default 2000)")
    ap.add_argument("--ch-host"); ap.add_argument("--ch-port", type=int); ap.add_argument("--ch-db")
    ap.add_argument("--ch-user"); ap.add_argument("--ch-password")
    args = ap.parse_args()

    config = load_config(args.config)
    ch = get_clickhouse_client(config, args)
    rest_url = args.rest_url or (config.get("universe", {}) or {}).get("rest_url", "https://fapi.binance.com")
    table = args.table or (config.get("open_interest", {}) or {}).get("table") or "market.fapi_oi_5m"

    print(f"{Colors.BOLD}{Colors.CYAN}=== Backfill missing 5m open interest ==={Colors.RESET}")
    print(f"ClickHouse: {ch.host}:{ch.port}  table: {table}   Binance: {rest_url}")
    print(f"Mode: {Colors.RED + 'APPLY (will INSERT)' if args.apply else Colors.GREEN + 'DRY RUN (nothing is written)'}{Colors.RESET}")
    ok, msg = ch.test_connection()
    if not ok:
        print(f"{Colors.RED}[ERROR] Cannot connect to ClickHouse: {msg}{Colors.RESET}")
        sys.exit(1)

    gaps, warns = read_gaps_csv(args.csv)
    for w in warns[:10]:
        print(f"{Colors.YELLOW}[WARN] {w}{Colors.RESET}")
    if len(warns) > 10:
        print(f"{Colors.YELLOW}[WARN] ... and {len(warns) - 10} more skipped line(s){Colors.RESET}")
    if args.symbol:
        gaps = {s: r for s, r in gaps.items() if s == args.symbol.upper()}
    total = sum((b - a) // BAR_MS for r in gaps.values() for a, b in r)
    print(f"Gaps file: {len(gaps)} symbol(s), {total:,} missing bar(s)\n")
    if not gaps:
        print("Nothing to do.")
        return

    fetcher = OIHist(rest_url, DataPacer(args.rps, args.window_cap))
    now_ms = int(time.time() * 1000)
    st = run(ch, table, gaps, fetcher, args.apply, now_ms, retention_days=args.retention_days,
             batch_rows=args.batch_rows)

    print(f"\n{Colors.BOLD}Summary{Colors.RESET}")
    print(f"  bars in file                : {st['requested_bars']:,}")
    print(f"  beyond Binance retention    : {st['beyond_retention']:,}   (cannot be fetched here: import the archive)")
    print(f"  already present in CH       : {st['already_present']:,}")
    print(f"  found at exchange           : {st['fetched']:,}")
    print(f"  empty at exchange (no data, cannot be filled): {st['empty_at_exchange']:,}")
    if args.apply:
        print(f"  {Colors.GREEN}inserted{Colors.RESET}                    : {st['inserted']:,}")
    else:
        print(f"  {Colors.YELLOW}would insert{Colors.RESET}                : {st['fetched']:,}   (re-run with --apply)")
    if st["invalid_symbols"]:
        print(f"  rejected by Binance ({len(st['invalid_symbols'])}): {', '.join(st['invalid_symbols'][:20])}")
    if st["failed_symbols"]:
        print(f"  {Colors.RED}failed symbols ({len(st['failed_symbols'])}){Colors.RESET}: {', '.join(st['failed_symbols'][:20])}")

    if args.apply and st["inserted"] and st["min_ms"] is not None:
        stale = rollup_hint(st["min_ms"], now_ms)
        if stale:
            print(f"\n{Colors.YELLOW}[NOTE] Repaired bars reach back {(now_ms - st['min_ms']) / 86_400_000:.1f} days, beyond the "
                  f"refresh lookback of the {', '.join(stale)} rollup(s). Those rollups will not pick the repair up by "
                  f"themselves; refold the months with:\n  ./rollup_oi_per_month.sh --password '<pw>' "
                  f"--from {month_start(st['min_ms'])} --to {next_month_start(st['max_ms'])}{Colors.RESET}")
        print("Verify:  python3 check_oi_consistency.py --start-date <date> --skip-coverage --gaps-csv oi_gaps_after.csv")
    sys.exit(1 if st["failed_symbols"] else 0)


if __name__ == "__main__":
    main()
