# -*- coding: utf-8 -*-
"""
End-to-End Storage Reconciliation Tool (Redis vs ClickHouse)

Cross-verifies data consistency between Redis sliding windows and ClickHouse tables:
1. Fetches rolling bars from Redis `kline:{SYMBOL}:{interval}`.
2. Queries the corresponding latest N bars from ClickHouse `market.fapi_kline_{interval}`.
3. Compares bar-by-bar:
   - Timestamp alignment
   - Open, High, Low, Close price equality
   - Volume and Taker volume equality
4. Highlights any discrepancies between cache and persistent storage.
"""
import os
import sys
import json
import math
import argparse
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    Colors,
    load_config,
    get_clickhouse_client,
    get_redis_client,
    fetch_active_symbols,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)


def compare_bars(redis_bars: List[List[Any]], ch_rows: List[Dict[str, Any]], epsilon: float = 1e-5) -> Dict[str, Any]:
    """
    Compares Redis compact bars (newest first) with ClickHouse rows (newest first).
    """
    res = {
        "redis_count": len(redis_bars),
        "ch_count": len(ch_rows),
        "compared_count": 0,
        "matched_count": 0,
        "ts_mismatches": 0,
        "price_mismatches": 0,
        "volume_mismatches": 0,
        "discrepancies": [],
    }

    n = min(len(redis_bars), len(ch_rows))
    res["compared_count"] = n

    for i in range(n):
        r_bar = redis_bars[i]
        c_row = ch_rows[i]

        r_t = int(r_bar[0])
        c_t = int(c_row["t"])

        if r_t != c_t:
            res["ts_mismatches"] += 1
            if len(res["discrepancies"]) < 3:
                res["discrepancies"].append(
                    f"Index {i} timestamp mismatch: Redis={format_ms_to_utc(r_t)} vs CH={format_ms_to_utc(c_t)}"
                )
            continue

        # Check OHLC
        r_o, r_h, r_l, r_c = float(r_bar[1]), float(r_bar[2]), float(r_bar[3]), float(r_bar[4])
        c_o, c_h, c_l, c_c = float(c_row["open"]), float(c_row["high"]), float(c_row["low"]), float(c_row["close"])

        price_diff = (
            abs(r_o - c_o) > epsilon
            or abs(r_h - c_h) > epsilon
            or abs(r_l - c_l) > epsilon
            or abs(r_c - c_c) > epsilon
        )
        if price_diff:
            res["price_mismatches"] += 1
            if len(res["discrepancies"]) < 3:
                res["discrepancies"].append(
                    f"Time {format_ms_to_utc(r_t)} price mismatch: "
                    f"Redis=[{r_o}, {r_h}, {r_l}, {r_c}] vs CH=[{c_o}, {c_h}, {c_l}, {c_c}]"
                )
            continue

        # Check Volume
        r_v, r_qv = float(r_bar[5]), float(r_bar[6])
        c_v, c_qv = float(c_row["volume"]), float(c_row["quote_volume"])

        vol_diff = abs(r_v - c_v) > epsilon or abs(r_qv - c_qv) > epsilon
        if vol_diff:
            res["volume_mismatches"] += 1
            if len(res["discrepancies"]) < 3:
                res["discrepancies"].append(
                    f"Time {format_ms_to_utc(r_t)} volume mismatch: Redis={r_v} vs CH={c_v}"
                )
            continue

        res["matched_count"] += 1

    return res


def reconcile_symbol(
    rdb,
    ch,
    symbol: str,
    interval: str,
    prefix: str,
    table: str,
    window_size: int,
) -> Dict[str, Any]:
    """
    Reconciles a single symbol between Redis and ClickHouse.
    """
    redis_key = f"{prefix}:{symbol.upper()}:{interval}"

    # 1. Fetch from Redis
    try:
        raw_list = rdb.lrange(redis_key, 0, window_size - 1)
    except Exception as e:
        return {"status": "ERROR", "notes": f"Redis error: {e}"}

    redis_bars = []
    for raw in raw_list:
        try:
            arr = json.loads(raw)
            if isinstance(arr, list) and len(arr) >= 7:
                redis_bars.append(arr)
        except Exception:
            pass

    # 2. Fetch from ClickHouse
    limit = len(redis_bars) if redis_bars else window_size
    ch_sql = f"""
    SELECT
        toUnixTimestamp64Milli(start_time) AS t,
        open, high, low, close, volume, quote_volume
    FROM {table} FINAL
    WHERE symbol = '{symbol}'
    ORDER BY start_time DESC
    LIMIT {limit}
    """

    try:
        ch_rows = ch.query(ch_sql)
    except Exception as e:
        return {"status": "ERROR", "notes": f"ClickHouse error: {e}"}

    # 3. Compare
    comp = compare_bars(redis_bars, ch_rows)

    status = "PASS"
    notes = []

    if comp["redis_count"] == 0 and comp["ch_count"] == 0:
        status = "EMPTY"
        notes.append("No data in either store")
    elif comp["redis_count"] == 0:
        status = "FAIL"
        notes.append(f"Missing in Redis (CH has {comp['ch_count']})")
    elif comp["ch_count"] == 0:
        status = "FAIL"
        notes.append(f"Missing in CH (Redis has {comp['redis_count']})")
    elif comp["ts_mismatches"] > 0 or comp["price_mismatches"] > 0 or comp["volume_mismatches"] > 0:
        status = "FAIL"
        notes.extend(comp["discrepancies"])
    elif comp["compared_count"] < window_size:
        status = "WARN"
        notes.append(f"Matched {comp['matched_count']} bars (< window_size {window_size})")
    else:
        status = "PASS"
        notes.append(f"100% match across {comp['matched_count']} bars")

    return {
        "symbol": symbol,
        "status": status,
        "redis_bars": comp["redis_count"],
        "ch_bars": comp["ch_count"],
        "matched": comp["matched_count"],
        "mismatches": comp["ts_mismatches"] + comp["price_mismatches"] + comp["volume_mismatches"],
        "notes": "; ".join(notes),
    }


def main():
    parser = argparse.ArgumentParser(description="End-to-End Reconciliation (Redis vs ClickHouse).")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--intervals", default="1m", help="Comma-separated intervals (default: 1m). Only the base interval has a Redis window; for coarser intervals reconcile ClickHouse against Binance with check_vs_binance.py.")
    parser.add_argument("--symbol", help="Specific symbol to check (e.g. BTCUSDT)")
    parser.add_argument("--limit-symbols", type=int, default=10, help="Limit number of symbols (default: 10)")
    parser.add_argument("--window-size", type=int, default=None, help="Bars to reconcile (default: 200)")
    parser.add_argument("--prefix", default=None, help="Redis key prefix override")
    parser.add_argument("--table-prefix", default="market.fapi_kline", help="ClickHouse table prefix")
    parser.add_argument("--redis-host", help="Redis host override")
    parser.add_argument("--redis-port", type=int, help="Redis port override")
    parser.add_argument("--redis-db", type=int, help="Redis DB override")
    parser.add_argument("--redis-password", help="Redis password override")
    parser.add_argument("--ch-host", help="ClickHouse host override")
    parser.add_argument("--ch-port", type=int, help="ClickHouse HTTP port override")
    parser.add_argument("--ch-db", help="ClickHouse database override")
    parser.add_argument("--ch-user", help="ClickHouse username override")
    parser.add_argument("--ch-password", help="ClickHouse password override")

    args = parser.parse_args()
    config = load_config(args.config)

    r_cfg = config.get("redis", {})
    win_cfg = r_cfg.get("window", {})
    prefix = args.prefix or win_cfg.get("key_prefix", "kline")
    window_size = args.window_size or win_cfg.get("window_size", 200)

    print(f"{Colors.BOLD}{Colors.CYAN}=== End-to-End Data Reconciliation (Redis vs ClickHouse) ==={Colors.RESET}")

    # Connect Redis
    try:
        rdb = get_redis_client(config, args, for_live=False)
        rdb.ping()
        print(f"{Colors.GREEN}[OK] Connected to Redis.{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Cannot connect to Redis: {e}{Colors.RESET}")
        sys.exit(1)

    # Connect ClickHouse
    ch = get_clickhouse_client(config, args)
    ok, msg = ch.test_connection()
    if not ok:
        print(f"{Colors.RED}[ERROR] Cannot connect to ClickHouse: {msg}{Colors.RESET}")
        sys.exit(1)
    print(f"{Colors.GREEN}[OK] Connected to ClickHouse.{Colors.RESET}")
    print()

    # Discover symbols
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = fetch_active_symbols(config, ch_client=ch, limit=args.limit_symbols)

    print(f"Reconciling {len(symbols)} symbol(s) (window_size={window_size})...")
    print()

    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
    overall_pass = True

    for interval in intervals:
        table = f"{args.table_prefix}_{interval}"
        print(f"{Colors.BOLD}--- Reconciling Interval: {interval} (Redis: {prefix}:<SYM>:{interval} vs CH: {table}) ---{Colors.RESET}")

        headers = ["Symbol", "Redis Bars", "CH Bars", "Matched", "Mismatches", "Status", "Notes"]
        table_rows = []

        pass_cnt = 0
        warn_cnt = 0
        fail_cnt = 0

        for sym in symbols:
            res = reconcile_symbol(
                rdb=rdb,
                ch=ch,
                symbol=sym,
                interval=interval,
                prefix=prefix,
                table=table,
                window_size=window_size,
            )

            status = res["status"]
            if status == "PASS":
                status_str = f"{Colors.GREEN}PASS{Colors.RESET}"
                pass_cnt += 1
            elif status in ("WARN", "EMPTY"):
                status_str = f"{Colors.YELLOW}{status}{Colors.RESET}"
                warn_cnt += 1
            else:
                status_str = f"{Colors.RED}{status}{Colors.RESET}"
                fail_cnt += 1
                overall_pass = False

            table_rows.append([
                sym,
                res.get("redis_bars", "-"),
                res.get("ch_bars", "-"),
                res.get("matched", "-"),
                res.get("mismatches", "-"),
                status_str,
                res.get("notes", ""),
            ])

        print_table(headers, table_rows)
        print(f"Summary for {interval}: {Colors.GREEN}{pass_cnt} PASS{Colors.RESET}, {Colors.YELLOW}{warn_cnt} WARN/EMPTY{Colors.RESET}, {Colors.RED}{fail_cnt} FAIL{Colors.RESET} (Total: {len(symbols)})")
        print()

    if overall_pass:
        print(f"{Colors.GREEN}{Colors.BOLD}? End-to-End reconciliation PASSED (Redis and ClickHouse are in sync).{Colors.RESET}")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}? End-to-End reconciliation found discrepancies between Redis and ClickHouse.{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
