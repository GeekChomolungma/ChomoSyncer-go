# -*- coding: utf-8 -*-
"""
ClickHouse vs Binance external-truth reconciliation.

Every table this project stores derives from one WebSocket feed:
`fapi_kline_1m` from the live collector, and `fapi_kline_5m/15m/1h/4h/1d` as
ClickHouse rollups of it. `e2e_reconciliation.py` only checks Redis against
ClickHouse -- i.e. the pipeline against itself -- so a systematic error (field
mapping, unit, rollup bucket alignment, an aggregation function bug) would pass
unnoticed.

This tool compares a ClickHouse kline table, bar by bar, against Binance's own
`/fapi/v1/klines` REST response for the same window:

  * start_time must line up exactly (bucket alignment)
  * OHLC must match to a tiny relative tolerance (argMin/argMax/min/max -> exact)
  * volume / quote_volume / taker_buy_* to a looser relative tolerance
    (float summation order differs; Binance sums from trades, we sum 1m bars)
  * trades_count must match exactly (integer sums)

Use it on:
  --intervals 1m         -> validates raw collector ingestion vs Binance
  --intervals 1h,4h,1d   -> validates the ClickHouse rollups (Phase B)

Sampling the whole universe over a recent window (instead of full history, which
would be slow and REST-heavy): --bars picks the last N *closed* bars ending at
now (not a date), so e.g. --intervals 1m --bars 720 covers the last 12h; pass
--limit-symbols with a value >= the active symbol count (sorted alphabetically,
see common.fetch_active_symbols) to cover every symbol instead of the default
sample of 8.

Exit code 0 = every checked bar matched within tolerance.

RATE LIMIT NOTE: every /fapi/v1/klines call here uses limit=1500 (weight 10,
regardless of --bars), and --symbol-delay paces the per-symbol loop (default
0.35s = ~2.9 req/s -> ~1700 weight/min, under Binance's 2400/min IP cap with
headroom for the live daemon's own REST use). Do not set it to 0 for a
--limit-symbols run covering the whole universe -- see docs/OPERATIONS.md's
backfill.rest_rps notes for the same weight-budget math.
"""
import os
import sys
import time
import argparse
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    Colors,
    load_config,
    get_clickhouse_client,
    fetch_active_symbols,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)

try:
    import requests
except ImportError:
    requests = None

# Binance USD-M futures: /fapi/v1/klines returns at most 1500 rows per call.
BINANCE_MAX_LIMIT = 1500


def binance_klines(rest_url: str, symbol: str, interval: str,
                   start_ms: int, end_ms: int, timeout: int = 15) -> List[List[Any]]:
    """
    Pages through /fapi/v1/klines for [start_ms, end_ms). Returns raw rows,
    ascending by open time.
    """
    if requests is None:
        raise RuntimeError("the 'requests' package is required for check_vs_binance.py")

    out: List[List[Any]] = []
    cur = start_ms
    url = f"{rest_url}/fapi/v1/klines"
    while cur < end_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": cur,
            "endTime": end_ms,
            "limit": BINANCE_MAX_LIMIT,
        }
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 429 or resp.status_code == 418:
            # Backoff on rate limit and retry the same page.
            time.sleep(2.0)
            continue
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        out.extend(page)
        last_open = int(page[-1][0])
        nxt = last_open + parse_interval_to_ms(interval)
        if nxt <= cur or len(page) < BINANCE_MAX_LIMIT:
            break
        cur = nxt
        time.sleep(0.15)  # gentle on the weight limit
    # Trim to the requested window and de-dup on open time.
    seen = set()
    trimmed = []
    for r in out:
        t = int(r[0])
        if t < start_ms or t >= end_ms or t in seen:
            continue
        seen.add(t)
        trimmed.append(r)
    trimmed.sort(key=lambda r: int(r[0]))
    return trimmed


def ch_bars(ch, table: str, symbol: str, start_ms: int, end_ms: int) -> Dict[int, Dict[str, Any]]:
    """
    Reads one symbol's bars in [start_ms, end_ms) from a kline table, keyed by
    open-time ms. FINAL for read-time dedup (raw and rollup tables both need it).
    """
    sql = f"""
    SELECT
        toUnixTimestamp64Milli(start_time) AS t,
        open, high, low, close,
        volume, quote_volume, taker_buy_volume, taker_buy_quote_volume,
        trades_count
    FROM {table} FINAL
    WHERE symbol = '{symbol}'
      AND start_time >= fromUnixTimestamp64Milli(toInt64({start_ms}))
      AND start_time <  fromUnixTimestamp64Milli(toInt64({end_ms}))
    ORDER BY start_time ASC
    """
    rows = ch.query(sql)
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        out[int(r["t"])] = r
    return out


def rel_close(a: float, b: float, tol: float) -> bool:
    if a == b:
        return True
    denom = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / denom <= tol


def compare_symbol(ch, rest_url: str, table: str, symbol: str, interval: str,
                   bars: int, price_tol: float, vol_tol: float,
                   http_timeout: int, settle_lag: int = 1) -> Dict[str, Any]:
    iv_ms = parse_interval_to_ms(interval)
    now_ms = int(time.time() * 1000)
    # Skip the bucket currently forming, AND `settle_lag` more closed ones after
    # it: the newest closed bar can briefly read as missing/mismatched even on a
    # perfectly healthy pipeline -- ClickHouse write latency (chwriter batches on
    # flush_interval, ~1-2s) and Binance's own WS-close-vs-REST-kline settling
    # (the REST /klines history can pick up a trade or two after the WS "x":true
    # close frame) both resolve within a few seconds. Comparing hundreds of
    # symbols one by one (see --symbol-delay) means `now` keeps advancing as the
    # run progresses, so without this margin every run flags a rotating set of
    # false positives on whatever was "newest" when each symbol was checked.
    last_closed_open = ((now_ms // iv_ms) * iv_ms) - iv_ms - settle_lag * iv_ms
    start_ms = last_closed_open - (bars - 1) * iv_ms
    end_ms = last_closed_open + iv_ms  # exclusive upper bound

    res = {
        "symbol": symbol, "checked": 0, "matched": 0,
        "missing_ch": 0, "missing_binance": 0,
        "price_mismatch": 0, "vol_mismatch": 0, "count_mismatch": 0,
        "status": "PASS", "notes": [],
    }

    try:
        b_rows = binance_klines(rest_url, symbol, interval, start_ms, end_ms, http_timeout)
    except Exception as e:
        res["status"] = "ERROR"
        res["notes"].append(f"Binance fetch failed: {e}")
        return res
    try:
        c_map = ch_bars(ch, table, symbol, start_ms, end_ms)
    except Exception as e:
        res["status"] = "ERROR"
        res["notes"].append(f"ClickHouse read failed: {e}")
        return res

    b_map = {int(r[0]): r for r in b_rows}
    all_ts = sorted(set(b_map) | set(c_map))

    for t in all_ts:
        b = b_map.get(t)
        c = c_map.get(t)
        if b is None:
            res["missing_binance"] += 1
            continue
        if c is None:
            res["missing_ch"] += 1
            if len(res["notes"]) < 4:
                res["notes"].append(f"{format_ms_to_utc(t)}: missing in ClickHouse")
            continue

        res["checked"] += 1
        b_o, b_h, b_l, b_c = float(b[1]), float(b[2]), float(b[3]), float(b[4])
        b_v, b_qv, b_tbv, b_tbqv = float(b[5]), float(b[7]), float(b[9]), float(b[10])
        b_n = int(b[8])

        ok = True
        if not (rel_close(b_o, float(c["open"]), price_tol) and
                rel_close(b_h, float(c["high"]), price_tol) and
                rel_close(b_l, float(c["low"]), price_tol) and
                rel_close(b_c, float(c["close"]), price_tol)):
            res["price_mismatch"] += 1
            ok = False
            if len(res["notes"]) < 4:
                res["notes"].append(
                    f"{format_ms_to_utc(t)}: OHLC CH=[{c['open']},{c['high']},{c['low']},{c['close']}] "
                    f"BN=[{b_o},{b_h},{b_l},{b_c}]")
        if not (rel_close(b_v, float(c["volume"]), vol_tol) and
                rel_close(b_qv, float(c["quote_volume"]), vol_tol) and
                rel_close(b_tbv, float(c["taker_buy_volume"]), vol_tol) and
                rel_close(b_tbqv, float(c["taker_buy_quote_volume"]), vol_tol)):
            res["vol_mismatch"] += 1
            ok = False
            if len(res["notes"]) < 4:
                res["notes"].append(
                    f"{format_ms_to_utc(t)}: volume CH={c['volume']} BN={b_v} "
                    f"(rel diff {abs(b_v - float(c['volume'])) / max(abs(b_v), 1e-12):.2e})")
        if int(c["trades_count"]) != b_n:
            res["count_mismatch"] += 1
            ok = False
            if len(res["notes"]) < 4:
                res["notes"].append(f"{format_ms_to_utc(t)}: trades_count CH={c['trades_count']} BN={b_n}")
        if ok:
            res["matched"] += 1

    mism = res["price_mismatch"] + res["vol_mismatch"] + res["count_mismatch"]
    if res["status"] != "ERROR":
        if res["missing_ch"] > 0 or mism > 0:
            res["status"] = "FAIL"
        elif res["checked"] == 0:
            res["status"] = "EMPTY"
        elif res["missing_binance"] > 0 and res["checked"] < bars:
            res["status"] = "WARN"
        else:
            res["status"] = "PASS"
    return res


def main():
    p = argparse.ArgumentParser(description="Reconcile ClickHouse kline tables against Binance /fapi/v1/klines.")
    p.add_argument("--config", help="Path to config.yaml")
    p.add_argument("--intervals", default="1h,4h,1d",
                   help="Comma-separated intervals to check (default: 1h,4h,1d). Add 1m to check raw ingestion.")
    p.add_argument("--symbol", help="Specific symbol (else a sample of the active universe)")
    p.add_argument("--limit-symbols", type=int, default=8, help="Symbols to sample when --symbol is omitted (default 8)")
    p.add_argument("--bars", type=int, default=48, help="Closed bars per (symbol,interval) to compare (default 48)")
    p.add_argument("--settle-lag", type=int, default=1,
                   help="Exclude this many of the newest closed bars from the comparison (default 1): "
                        "ClickHouse write latency and Binance's own WS-vs-REST settling can make the "
                        "very newest bar briefly read as missing/mismatched on an otherwise healthy "
                        "pipeline. Set to 0 to compare right up to the edge (noisier).")
    p.add_argument("--price-tol", type=float, default=1e-9, help="Relative tolerance for OHLC (default 1e-9)")
    p.add_argument("--volume-tol", type=float, default=1e-6, help="Relative tolerance for volume fields (default 1e-6)")
    p.add_argument("--table-prefix", default="market.fapi_kline", help="ClickHouse table prefix")
    p.add_argument("--http-timeout", type=int, default=15, help="Binance REST timeout seconds")
    p.add_argument("--symbol-delay", type=float, default=0.35,
                   help="Seconds to sleep between symbols (default 0.35 -> ~2.9 req/s, "
                        "stays under Binance's 2400 weight/min IP cap at weight=10/call). "
                        "Only lower this for small --limit-symbols runs.")
    p.add_argument("--show-only-failures", action="store_true")
    p.add_argument("--ch-host"); p.add_argument("--ch-port", type=int)
    p.add_argument("--ch-db"); p.add_argument("--ch-user"); p.add_argument("--ch-password")
    args = p.parse_args()

    config = load_config(args.config)
    ch = get_clickhouse_client(config, args)
    rest_url = config.get("universe", {}).get("rest_url", "https://fapi.binance.com")

    print(f"{Colors.BOLD}{Colors.CYAN}=== ClickHouse vs Binance Reconciliation ==={Colors.RESET}")
    print(f"ClickHouse: {ch.host}:{ch.port}/{ch.database}   Binance: {rest_url}")
    ok, msg = ch.test_connection()
    if not ok:
        print(f"{Colors.RED}[ERROR] ClickHouse: {msg}{Colors.RESET}")
        sys.exit(1)

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = fetch_active_symbols(config, ch_client=ch, limit=args.limit_symbols)
    intervals = [s.strip() for s in args.intervals.split(",") if s.strip()]
    eta_s = len(symbols) * len(intervals) * args.symbol_delay
    print(f"Symbols: {len(symbols)} | Intervals: {intervals} | Bars/each: {args.bars} | "
          f"tol price={args.price_tol:g} vol={args.volume_tol:g} | "
          f"symbol-delay={args.symbol_delay}s (ETA ~{eta_s:.0f}s)\n")

    overall_ok = True
    for interval in intervals:
        table = f"{args.table_prefix}_{interval}"
        print(f"{Colors.BOLD}--- {table} vs Binance {interval} ---{Colors.RESET}")
        headers = ["Symbol", "Checked", "Matched", "MissCH", "Px!=", "Vol!=", "Cnt!=", "Status", "Notes"]
        rows = []
        agg = {"PASS": 0, "WARN": 0, "FAIL": 0, "EMPTY": 0, "ERROR": 0}
        for i, sym in enumerate(symbols):
            if i > 0 and args.symbol_delay > 0:
                time.sleep(args.symbol_delay)
            r = compare_symbol(ch, rest_url, table, sym, interval, args.bars,
                               args.price_tol, args.volume_tol, args.http_timeout, args.settle_lag)
            agg[r["status"]] = agg.get(r["status"], 0) + 1
            if r["status"] in ("FAIL", "ERROR"):
                overall_ok = False
            if args.show_only_failures and r["status"] in ("PASS", "EMPTY", "WARN"):
                continue
            color = {"PASS": Colors.GREEN, "WARN": Colors.YELLOW, "EMPTY": Colors.YELLOW,
                     "FAIL": Colors.RED, "ERROR": Colors.RED}[r["status"]]
            rows.append([
                sym, r["checked"], r["matched"], r["missing_ch"],
                r["price_mismatch"], r["vol_mismatch"], r["count_mismatch"],
                f"{color}{r['status']}{Colors.RESET}", "; ".join(r["notes"]),
            ])
        if rows:
            print_table(headers, rows)
        print(f"Summary {interval}: {Colors.GREEN}{agg['PASS']} PASS{Colors.RESET}, "
              f"{Colors.YELLOW}{agg['WARN']} WARN, {agg['EMPTY']} EMPTY{Colors.RESET}, "
              f"{Colors.RED}{agg['FAIL']} FAIL, {agg['ERROR']} ERROR{Colors.RESET}\n")

    if overall_ok:
        print(f"{Colors.GREEN}{Colors.BOLD}OK — ClickHouse matches Binance within tolerance.{Colors.RESET}")
        sys.exit(0)
    print(f"{Colors.RED}{Colors.BOLD}MISMATCH — ClickHouse disagrees with Binance. Investigate ingestion / rollup.{Colors.RESET}")
    sys.exit(1)


if __name__ == "__main__":
    main()
