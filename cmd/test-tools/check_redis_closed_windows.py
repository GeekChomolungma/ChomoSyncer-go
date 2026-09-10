# -*- coding: utf-8 -*-
"""
Redis Closed K-Line Rolling Windows Verification Tool

Inspects `kline:{SYMBOL}:{interval}` lists in Redis:
1. Validates key existence and list length (expected: window_size, e.g. 200).
2. Verifies head bar (index 0) freshness against the most recent closed boundary.
3. Checks strict monotonic decreasing timestamp ordering (newest first).
4. Verifies window continuity (no missing bars between adjacent elements in the list).
5. Validates 9-element compact bar schema and price/volume bounds.
"""
import os
import sys
import time
import json
import argparse
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    Colors,
    load_config,
    get_redis_client,
    fetch_active_symbols,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)


def validate_compact_bar(arr: Any) -> Tuple[bool, str]:
    """
    Validates [t, o, h, l, c, v, qv, tbv, tbqv] compact bar array.
    """
    if not isinstance(arr, list) or len(arr) != 9:
        return False, f"Expected 9 elements, got {len(arr) if isinstance(arr, list) else type(arr)}"

    try:
        t = int(arr[0])
        o, h, l, c = float(arr[1]), float(arr[2]), float(arr[3]), float(arr[4])
        v, qv, tbv, tbqv = float(arr[5]), float(arr[6]), float(arr[7]), float(arr[8])
    except Exception as e:
        return False, f"Number parse error: {e}"

    if t <= 0:
        return False, f"Invalid timestamp {t}"
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return False, f"Non-positive price: o={o}, h={h}, l={l}, c={c}"
    if h < l:
        return False, f"High ({h}) < Low ({l})"
    if h < o or h < c:
        return False, f"High ({h}) < max(Open, Close)"
    if l > o or l > c:
        return False, f"Low ({l}) > min(Open, Close)"
    if v < 0 or qv < 0:
        return False, "Negative volume"

    return True, ""


def inspect_symbol_window(
    raw_bars: List[str],
    interval_ms: int,
    now_ms: int,
    expected_size: int,
) -> Dict[str, Any]:
    """
    Analyzes one symbol's rolling window list.
    """
    res = {
        "actual_len": len(raw_bars),
        "head_time_ms": 0,
        "head_time_str": "-",
        "tail_time_ms": 0,
        "tail_time_str": "-",
        "head_lag_bars": 0,
        "window_gaps": 0,
        "missing_in_window": 0,
        "corrupt_bars": 0,
        "status": "PASS",
        "notes": [],
    }

    if not raw_bars:
        res["status"] = "EMPTY"
        res["notes"].append("Window list is empty")
        return res

    # 1. Parse and validate elements
    parsed_bars = []
    for idx, raw in enumerate(raw_bars):
        try:
            arr = json.loads(raw)
            ok, err_msg = validate_compact_bar(arr)
            if not ok:
                res["corrupt_bars"] += 1
                if len(res["notes"]) < 3:
                    res["notes"].append(f"Bar {idx} corrupt: {err_msg}")
            else:
                parsed_bars.append((int(arr[0]), arr))
        except Exception as e:
            res["corrupt_bars"] += 1
            if len(res["notes"]) < 3:
                res["notes"].append(f"JSON parse error at index {idx}: {e}")

    if not parsed_bars:
        res["status"] = "CORRUPT"
        return res

    head_t = parsed_bars[0][0]
    tail_t = parsed_bars[-1][0]
    res["head_time_ms"] = head_t
    res["head_time_str"] = format_ms_to_utc(head_t)
    res["tail_time_ms"] = tail_t
    res["tail_time_str"] = format_ms_to_utc(tail_t)

    # 2. Check head freshness
    # Current forming interval boundary
    current_forming_t = (now_ms // interval_ms) * interval_ms
    latest_expected_closed_t = current_forming_t - interval_ms

    if head_t >= latest_expected_closed_t:
        res["head_lag_bars"] = 0
    else:
        lag_bars = int((latest_expected_closed_t - head_t) // interval_ms)
        res["head_lag_bars"] = lag_bars
        if lag_bars > 1:
            res["notes"].append(f"Head lagging by {lag_bars} bar(s)")

    # 3. Check monotonicity and gap continuity inside window
    for i in range(len(parsed_bars) - 1):
        curr_t = parsed_bars[i][0]
        prev_t = parsed_bars[i + 1][0]

        if curr_t <= prev_t:
            res["corrupt_bars"] += 1
            res["notes"].append(f"Non-monotonic timestamps at index {i}->{i+1}: {curr_t} <= {prev_t}")
        else:
            diff = curr_t - prev_t
            if diff > interval_ms:
                gap_missing = int(diff // interval_ms) - 1
                res["window_gaps"] += 1
                res["missing_in_window"] += gap_missing
                if len(res["notes"]) < 3:
                    res["notes"].append(f"Gap of {gap_missing} bar(s) between idx {i} and {i+1}")

    # 4. Determine status
    if res["corrupt_bars"] > 0 or res["window_gaps"] > 0:
        res["status"] = "FAIL"
    elif res["head_lag_bars"] > 2:
        res["status"] = "STALE"
    elif res["actual_len"] < expected_size:
        res["status"] = "SHORT"
        res["notes"].append(f"Partial window ({res['actual_len']}/{expected_size})")
    elif res["head_lag_bars"] > 0:
        res["status"] = "WARN"
    else:
        res["status"] = "PASS"

    return res


def main():
    parser = argparse.ArgumentParser(description="Verify Redis closed rolling windows (kline:SYMBOL:interval).")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--intervals", default="1m", help="Comma-separated intervals (default: 1m; only the base interval has a Redis closed-window — coarser intervals live in ClickHouse rollup tables)")
    parser.add_argument("--symbol", help="Specific symbol to check (e.g. BTCUSDT)")
    parser.add_argument("--limit-symbols", type=int, default=None, help="Limit number of symbols to inspect")
    parser.add_argument("--window-size", type=int, default=None, help="Expected window size override (default: from config or 200)")
    parser.add_argument("--prefix", default=None, help="Key prefix override (default: from config or 'kline')")
    parser.add_argument("--show-only-failures", action="store_true", help="Only display failed/short/stale windows")
    parser.add_argument("--redis-host", help="Redis host override")
    parser.add_argument("--redis-port", type=int, help="Redis port override")
    parser.add_argument("--redis-db", type=int, help="Redis DB override")
    parser.add_argument("--redis-password", help="Redis password override")

    args = parser.parse_args()
    config = load_config(args.config)

    r_cfg = config.get("redis", {})
    win_cfg = r_cfg.get("window", {})
    prefix = args.prefix or win_cfg.get("key_prefix", "kline")
    expected_size = args.window_size or win_cfg.get("window_size", 200)

    print(f"{Colors.BOLD}{Colors.CYAN}=== Redis Closed K-Line Rolling Windows Checker ==={Colors.RESET}")

    try:
        rdb = get_redis_client(config, args, for_live=False)
        rdb.ping()
        info = rdb.info("server")
        print(f"{Colors.GREEN}[OK] Connected to Redis ({rdb.connection_pool.connection_kwargs.get('host')}:{rdb.connection_pool.connection_kwargs.get('port')}, redis_version={info.get('redis_version')}){Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Cannot connect to Redis: {e}{Colors.RESET}")
        sys.exit(1)

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        print("Discovering active universe symbols...")
        symbols = fetch_active_symbols(config, limit=args.limit_symbols)

    print(f"Target Symbols: {len(symbols)} | Key Prefix: '{prefix}' | Expected Size: {expected_size}\n")

    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
    overall_pass = True

    for interval in intervals:
        try:
            interval_ms = parse_interval_to_ms(interval)
        except ValueError as e:
            print(f"{Colors.RED}[ERROR] {e}{Colors.RESET}")
            continue

        now_ms = int(time.time() * 1000)
        print(f"{Colors.BOLD}--- Inspecting Closed Windows for Interval: {interval} ---{Colors.RESET}")

        headers = ["Symbol", "Key", "Length", "Head Time", "Head Lag", "Gaps", "Corrupt", "Status", "Notes"]
        table_rows = []
        stats = {"PASS": 0, "SHORT": 0, "WARN": 0, "STALE": 0, "FAIL": 0, "EMPTY": 0}

        # Pipeline LRANGE 0 -1 for all symbols in chunks
        chunk_size = 50
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i : i + chunk_size]
            pipe = rdb.pipeline(transaction=False)
            keys = [f"{prefix}:{sym.upper()}:{interval}" for sym in chunk]
            for k in keys:
                pipe.lrange(k, 0, -1)

            batch_raw = pipe.execute()

            for idx, sym in enumerate(chunk):
                raw_bars = batch_raw[idx]
                k = keys[idx]

                res = inspect_symbol_window(
                    raw_bars=raw_bars,
                    interval_ms=interval_ms,
                    now_ms=now_ms,
                    expected_size=expected_size,
                )

                status = res["status"]
                stats[status] = stats.get(status, 0) + 1

                if args.show_only_failures and status == "PASS":
                    continue

                if status == "PASS":
                    status_str = f"{Colors.GREEN}PASS{Colors.RESET}"
                elif status in ("SHORT", "WARN"):
                    status_str = f"{Colors.YELLOW}{status}{Colors.RESET}"
                elif status == "STALE":
                    status_str = f"{Colors.YELLOW}STALE{Colors.RESET}"
                elif status == "EMPTY":
                    status_str = f"{Colors.RED}EMPTY{Colors.RESET}"
                else:
                    status_str = f"{Colors.RED}FAIL{Colors.RESET}"

                len_str = f"{res['actual_len']}/{expected_size}"
                if res["actual_len"] == expected_size:
                    len_str = f"{Colors.GREEN}{len_str}{Colors.RESET}"
                elif res["actual_len"] > 0:
                    len_str = f"{Colors.YELLOW}{len_str}{Colors.RESET}"
                else:
                    len_str = f"{Colors.RED}0{Colors.RESET}"

                lag_str = "0" if res["head_lag_bars"] == 0 else f"{Colors.YELLOW}{res['head_lag_bars']}{Colors.RESET}"
                gap_str = "0" if res["window_gaps"] == 0 else f"{Colors.RED}{res['window_gaps']} ({res['missing_in_window']} bars){Colors.RESET}"
                corrupt_str = "0" if res["corrupt_bars"] == 0 else f"{Colors.RED}{res['corrupt_bars']}{Colors.RESET}"

                notes_str = "; ".join(res["notes"])

                table_rows.append([
                    sym,
                    k,
                    len_str,
                    res["head_time_str"],
                    lag_str,
                    gap_str,
                    corrupt_str,
                    status_str,
                    notes_str,
                ])

        print_table(headers, table_rows)

        total = len(symbols)
        p_cnt = stats.get("PASS", 0)
        sh_cnt = stats.get("SHORT", 0)
        w_cnt = stats.get("WARN", 0)
        st_cnt = stats.get("STALE", 0)
        f_cnt = stats.get("FAIL", 0)
        e_cnt = stats.get("EMPTY", 0)

        print(f"Summary for {interval}: {Colors.GREEN}{p_cnt} PASS{Colors.RESET}, "
              f"{Colors.YELLOW}{sh_cnt} SHORT, {w_cnt} WARN, {st_cnt} STALE{Colors.RESET}, "
              f"{Colors.RED}{f_cnt} FAIL, {e_cnt} EMPTY{Colors.RESET} (Total: {total})\n")

        if f_cnt > 0 or e_cnt > 0:
            overall_pass = False

    if overall_pass:
        print(f"{Colors.GREEN}{Colors.BOLD}? All Redis closed windows checks PASSED.{Colors.RESET}")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}? Redis closed windows check found FAIL or EMPTY windows.{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
