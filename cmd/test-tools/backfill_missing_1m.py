# -*- coding: utf-8 -*-
"""
Backfill missing 1m klines into ClickHouse from Binance REST.

Input is the CSV written by check_clickhouse_integrity.py --gaps-csv:

    python3 check_clickhouse_integrity.py --intervals 1m --gaps-csv gaps.csv
    python3 backfill_missing_1m.py gaps.csv            # dry run: fetch + report, write nothing
    python3 backfill_missing_1m.py gaps.csv --apply    # insert the bars that were found

What it does, per symbol:
  1. drops the parts of each gap that ClickHouse already has (safe to re-run),
  2. groups the remaining minutes into as few /fapi/v1/klines requests as is cheap,
  3. fetches them (paced against the per-IP weight limit, honours 429/418),
  4. inserts only closed bars that were asked for and are still missing.

Binance returns NO kline for a minute in which the symbol did not trade, so a gap can
legitimately stay a gap. Those minutes are counted as "empty at exchange" and are not
inserted (nothing to insert, and the integrity checker will keep listing them).

Only the 1m table is written. The 5m+ tables are ClickHouse rollups of 1m; they pick the
repaired minutes up on their next refresh if the repair is within their lookback (3 days for
5m/15m/1h, 7 for 4h, 10 for 1d). Older repairs need the rollups refolded, see the hint printed
at the end.
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import Colors, ClickHouseClient, get_clickhouse_client, load_config

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

MINUTE_MS = 60_000
MAX_LIMIT = 1500  # /fapi/v1/klines hard cap per request
# Binance lists some contracts with CJK names (e.g. 币安人生USDT), so accept any Unicode word
# characters; quotes, semicolons, spaces etc. stay rejected because the symbol is put into SQL.
SYMBOL_RE = re.compile(r"^\w{1,32}$")
INSERT_COLUMNS = [
    "symbol", "start_time", "end_time", "open", "high", "low", "close", "volume",
    "quote_volume", "taker_buy_volume", "taker_buy_quote_volume", "trades_count",
]
# Rollup lookbacks in days (deploy/clickhouse/002_kline_rollups.sql).
ROLLUP_LOOKBACK_DAYS = {"5m": 3, "15m": 3, "1h": 3, "4h": 7, "1d": 10}

Range = Tuple[int, int]  # [from_ms, to_ms) of bar-open times


# ----------------------------------------------------------------------------- pure helpers

def kline_weight(limit: int) -> int:
    """Request weight of /fapi/v1/klines by `limit`."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def read_gaps_csv(path: str) -> Tuple[Dict[str, List[Range]], List[str]]:
    """
    Reads the gaps CSV. Returns ({symbol: [(from_ms, to_ms), ...]}, [warnings]).
    Only 1m lines are used; malformed or misaligned lines are skipped with a warning.
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
            if row["interval"].strip() != "1m":
                warns.append(f"line {n}: interval {row['interval']!r} is not 1m; skipped")
                continue
            if not SYMBOL_RE.match(sym):
                warns.append(f"line {n}: bad symbol {sym!r}; skipped")
                continue
            try:
                a, b = int(row["from_ms"]), int(row["to_ms"])
            except ValueError:
                warns.append(f"line {n}: non-integer from_ms/to_ms; skipped")
                continue
            if a % MINUTE_MS or b % MINUTE_MS or b <= a:
                warns.append(f"line {n}: range [{a},{b}) is not a whole-minute non-empty range; skipped")
                continue
            out.setdefault(sym, []).append((a, b))
    return {s: merge_ranges(r) for s, r in out.items()}, warns


def merge_ranges(ranges: Iterable[Range]) -> List[Range]:
    """Sorts and merges overlapping or touching [from, to) ranges."""
    merged: List[Range] = []
    for a, b in sorted(ranges):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


def subtract_present(ranges: List[Range], present_ms: Iterable[int]) -> List[Range]:
    """Removes the minutes in `present_ms` from `ranges`; returns the missing sub-ranges."""
    present = set(present_ms)
    out: List[Range] = []
    for a, b in ranges:
        start: Optional[int] = None
        for t in range(a, b, MINUTE_MS):
            if t in present:
                if start is not None:
                    out.append((start, t))
                    start = None
            elif start is None:
                start = t
        if start is not None:
            out.append((start, b))
    return out


def plan_requests(ranges: List[Range]) -> List[Range]:
    """
    Groups sorted, disjoint missing ranges into request spans. Two neighbours share one
    request when the combined span fits one page (1500 minutes) and costs no more weight
    than asking for them separately; a range longer than one page is cut into pages.
    """
    pieces: List[Range] = []
    for a, b in ranges:
        while b - a > MAX_LIMIT * MINUTE_MS:
            pieces.append((a, a + MAX_LIMIT * MINUTE_MS))
            a += MAX_LIMIT * MINUTE_MS
        pieces.append((a, b))

    def w(r: Range) -> int:
        return kline_weight((r[1] - r[0]) // MINUTE_MS)

    spans: List[Range] = []
    for a, b in pieces:
        if spans:
            pa, pb = spans[-1]
            if b - pa <= MAX_LIMIT * MINUTE_MS and w((pa, b)) <= w((pa, pb)) + w((a, b)):
                spans[-1] = (pa, b)
                continue
        spans.append((a, b))
    return spans


def fmt_ts(ms: int) -> str:
    """ClickHouse DateTime64(3, 'UTC') text form."""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms % 1000:03d}"


def kline_to_row(symbol: str, k: List[Any]) -> Dict[str, Any]:
    """One /fapi/v1/klines array -> one fapi_kline_1m row (same mapping as the Go writer)."""
    return {
        "symbol": symbol,
        "start_time": fmt_ts(int(k[0])),
        "end_time": fmt_ts(int(k[6])),
        "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
        "volume": float(k[5]),
        "quote_volume": float(k[7]),
        "trades_count": int(k[8]),
        "taker_buy_volume": float(k[9]),
        "taker_buy_quote_volume": float(k[10]),
    }


def wanted(open_ms: int, ranges: List[Range]) -> bool:
    return any(a <= open_ms < b for a, b in ranges)


def rollup_hint(min_ms: int, now_ms: int) -> List[str]:
    """Which rollup tables no longer refresh over the repaired minutes."""
    age_days = (now_ms - min_ms) / 86_400_000.0
    return [iv for iv, d in ROLLUP_LOOKBACK_DAYS.items() if age_days > d]


# ----------------------------------------------------------------------------- Binance REST

class WeightPacer:
    """
    Keeps this process under `budget` weight per rolling minute and backs off to the next
    minute when Binance's own X-MBX-USED-WEIGHT-1M (which also counts other processes on
    this IP, e.g. the running collector) reaches `soft_limit`.
    """

    def __init__(self, budget: int, soft_limit: int,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep):
        self.budget, self.soft_limit = budget, soft_limit
        self.clock, self.sleep = clock, sleep
        self.events: List[Tuple[float, int]] = []
        self.server_used = 0

    def before(self, weight: int) -> None:
        while True:
            now = self.clock()
            self.events = [(t, w) for t, w in self.events if now - t < 60.0]
            used = sum(w for _, w in self.events)
            if self.server_used + weight > self.soft_limit:
                # Binance's counter is per calendar minute; wait it out, then re-read from scratch.
                self.sleep(min(60.0, 60.0 - (time.time() % 60.0) + 0.5))
                self.server_used = 0
                continue
            if used + weight <= self.budget or not self.events:
                self.events.append((now, weight))
                return
            self.sleep(max(0.05, 60.0 - (now - self.events[0][0])))

    def after(self, used_header: Optional[str]) -> None:
        if used_header and used_header.isdigit():
            self.server_used = int(used_header)


class BinanceKlines:
    def __init__(self, rest_url: str, pacer: WeightPacer, session: Any = None, timeout: int = 15,
                 sleep: Callable[[float], None] = time.sleep):
        if session is None and requests is None:
            raise RuntimeError("the 'requests' package is required for backfill_missing_1m.py")
        self.url = rest_url.rstrip("/") + "/fapi/v1/klines"
        self.pacer, self.timeout, self.sleep = pacer, timeout, sleep
        self.s = session or requests.Session()

    def fetch(self, symbol: str, start_ms: int, end_ms: int) -> List[List[Any]]:
        """Bars with open time in [start_ms, end_ms). One request; the span is at most one page."""
        limit = max(1, min(MAX_LIMIT, (end_ms - start_ms) // MINUTE_MS))
        params = {"symbol": symbol, "interval": "1m", "startTime": start_ms,
                  "endTime": end_ms - 1, "limit": limit}
        for attempt in range(8):
            self.pacer.before(kline_weight(limit))
            resp = self.s.get(self.url, params=params, timeout=self.timeout)
            self.pacer.after(resp.headers.get("X-MBX-USED-WEIGHT-1M"))
            if resp.status_code in (418, 429):
                wait = float(resp.headers.get("Retry-After", "0") or 0) or min(60.0, 2.0 * (attempt + 1))
                print(f"{Colors.YELLOW}  rate limited ({resp.status_code}); waiting {wait:.0f}s{Colors.RESET}")
                self.sleep(wait)
                continue
            if resp.status_code >= 500:
                self.sleep(min(10.0, 1.0 * (attempt + 1)))
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"{symbol}: klines request kept failing after retries")


# ----------------------------------------------------------------------------- ClickHouse

def existing_minutes(ch: ClickHouseClient, table: str, symbol: str, lo: int, hi: int) -> List[int]:
    sql = f"""
    SELECT toUnixTimestamp64Milli(start_time) AS t
    FROM {table} FINAL
    WHERE symbol = '{symbol}'
      AND start_time >= fromUnixTimestamp64Milli({lo}, 'UTC')
      AND start_time <  fromUnixTimestamp64Milli({hi}, 'UTC')
    """
    return [int(r["t"]) for r in ch.query(sql)]


# ----------------------------------------------------------------------------- main flow

def run(ch: ClickHouseClient, table: str, gaps: Dict[str, List[Range]], fetcher: BinanceKlines,
        apply: bool, now_ms: int, batch_rows: int = 2000, out: Callable[[str], None] = print) -> Dict[str, Any]:
    """Repairs every symbol in `gaps`. Returns a summary dict."""
    # Only closed bars: a bar is closed once its whole minute is in the past.
    closed_before = now_ms - now_ms % MINUTE_MS
    stats = {"symbols": len(gaps), "requested_minutes": 0, "already_present": 0, "fetched": 0,
             "inserted": 0, "empty_at_exchange": 0, "failed_symbols": [], "min_ms": None}
    pending: List[Dict[str, Any]] = []

    def flush() -> None:
        if pending and apply:
            stats["inserted"] += ch.insert_json_rows(table, INSERT_COLUMNS, pending)
        pending.clear()

    for i, (sym, ranges) in enumerate(sorted(gaps.items()), start=1):
        ranges = [(a, min(b, closed_before)) for a, b in ranges if a < closed_before]
        if not ranges:
            continue
        total = sum((b - a) // MINUTE_MS for a, b in ranges)
        try:
            present = existing_minutes(ch, table, sym, ranges[0][0], ranges[-1][1])
            todo = subtract_present(ranges, present)
            missing = sum((b - a) // MINUTE_MS for a, b in todo)
            stats["requested_minutes"] += total
            stats["already_present"] += total - missing
            got = 0
            for span in plan_requests(todo):
                for k in fetcher.fetch(sym, span[0], span[1]):
                    t = int(k[0])
                    if not wanted(t, todo) or int(k[6]) >= now_ms:
                        continue
                    pending.append(kline_to_row(sym, k))
                    got += 1
                    stats["min_ms"] = t if stats["min_ms"] is None else min(stats["min_ms"], t)
                if len(pending) >= batch_rows:
                    flush()
            empty = missing - got
            stats["fetched"] += got
            stats["empty_at_exchange"] += max(0, empty)
            out(f"  [{i}/{len(gaps)}] {sym}: missing {missing}, found at exchange {got}"
                + (f", empty at exchange {empty}" if empty else "")
                + (f" ({total - missing} were already present)" if total != missing else ""))
        except Exception as e:  # one bad symbol must not abort the run
            stats["failed_symbols"].append(sym)
            out(f"{Colors.RED}  [{i}/{len(gaps)}] {sym}: FAILED: {e}{Colors.RESET}")
    flush()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill missing 1m klines into ClickHouse from Binance REST, from a "
                    "check_clickhouse_integrity.py --gaps-csv file. Dry run unless --apply is given.")
    ap.add_argument("csv", help="Gaps CSV written by check_clickhouse_integrity.py --gaps-csv")
    ap.add_argument("--apply", action="store_true", help="Actually INSERT the bars (default: dry run, write nothing)")
    ap.add_argument("--config", help="Path to config.yaml (auto-discovered if omitted)")
    ap.add_argument("--table-prefix", default="market.fapi_kline", help="Table prefix; writes <prefix>_1m")
    ap.add_argument("--symbol", help="Only repair this symbol")
    ap.add_argument("--rest-url", help="Binance REST base URL (default: universe.rest_url from config)")
    ap.add_argument("--weight-budget", type=int, default=600,
                    help="Max request weight this script spends per minute (default 600 of the 2400/min IP limit)")
    ap.add_argument("--soft-limit", type=int, default=1800,
                    help="Pause until the next minute when Binance reports this much used weight (default 1800)")
    ap.add_argument("--batch-rows", type=int, default=2000, help="Rows per INSERT (default 2000)")
    ap.add_argument("--ch-host"); ap.add_argument("--ch-port", type=int); ap.add_argument("--ch-db")
    ap.add_argument("--ch-user"); ap.add_argument("--ch-password")
    args = ap.parse_args()

    config = load_config(args.config)
    ch = get_clickhouse_client(config, args)
    rest_url = args.rest_url or (config.get("universe", {}) or {}).get("rest_url", "https://fapi.binance.com")
    table = f"{args.table_prefix}_1m"

    print(f"{Colors.BOLD}{Colors.CYAN}=== Backfill missing 1m klines ==={Colors.RESET}")
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
    total_min = sum((b - a) // MINUTE_MS for r in gaps.values() for a, b in r)
    print(f"Gaps file: {len(gaps)} symbol(s), {total_min} missing minute(s)\n")
    if not gaps:
        print("Nothing to do.")
        return

    fetcher = BinanceKlines(rest_url, WeightPacer(args.weight_budget, args.soft_limit))
    now_ms = int(time.time() * 1000)
    st = run(ch, table, gaps, fetcher, args.apply, now_ms, batch_rows=args.batch_rows)

    print(f"\n{Colors.BOLD}Summary{Colors.RESET}")
    print(f"  minutes in file            : {st['requested_minutes']}")
    print(f"  already present in CH      : {st['already_present']}")
    print(f"  found at exchange          : {st['fetched']}")
    print(f"  empty at exchange (no trades, cannot be filled): {st['empty_at_exchange']}")
    if args.apply:
        print(f"  {Colors.GREEN}inserted{Colors.RESET}                   : {st['inserted']}")
    else:
        print(f"  {Colors.YELLOW}would insert{Colors.RESET}               : {st['fetched']}   (re-run with --apply)")
    if st["failed_symbols"]:
        print(f"  {Colors.RED}failed symbols ({len(st['failed_symbols'])}){Colors.RESET}: {', '.join(st['failed_symbols'][:20])}")

    if args.apply and st["inserted"] and st["min_ms"] is not None:
        stale = rollup_hint(st["min_ms"], now_ms)
        if stale:
            print(f"\n{Colors.YELLOW}[NOTE] Repaired minutes reach back {(now_ms - st['min_ms']) / 86_400_000:.1f} days, beyond the "
                  f"refresh lookback of the {', '.join(stale)} rollup(s). Those rollups will not pick the repair up "
                  f"by themselves; refold them with deploy/clickhouse-fixes/001_fix_kline_rollup_lookback.sh "
                  f"apply --from <YYYY-MM> --to <YYYY-MM>.{Colors.RESET}")
        print("Verify:  python3 check_clickhouse_integrity.py --intervals 1m")
    sys.exit(1 if st["failed_symbols"] else 0)


if __name__ == "__main__":
    main()
