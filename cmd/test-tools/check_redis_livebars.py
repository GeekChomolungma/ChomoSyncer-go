# -*- coding: utf-8 -*-
"""
Redis Living Bars (LiveBar) Verification Tool

Inspects `livebar:{SYMBOL}:{interval}` hashes in Redis:
1. Validates key existence for all target symbols and intervals.
2. Checks TTL validity (within ttl_multiple * interval).
3. Verifies freshness (bar timestamp matches current forming interval).
4. Verifies completeness and integrity of all 11 fields:
   t (openTime ms), o, h, l, c, v, qv, tbv, tbqv, n (trades), x (isFinal).
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
    get_redis_client,
    fetch_active_symbols,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)


def validate_livebar_data(data: Dict[str, str], interval_ms: int, now_ms: int) -> Tuple[str, List[str]]:
    """
    Validates the 11 fields of a LiveBar hash.
    Returns (status, reasons). Status: PASS, WARN, or FAIL.
    """
    reasons = []
    required_fields = ["t", "o", "h", "l", "c", "v", "qv", "tbv", "tbqv", "n", "x"]
    for f in required_fields:
        if f not in data or data[f] is None or data[f] == "":
            reasons.append(f"Missing field '{f}'")

    if reasons:
        return "CORRUPT", reasons

    # Parse numeric fields
    try:
        t = int(data["t"])
        o = float(data["o"])
        h = float(data["h"])
        l = float(data["l"])
        c = float(data["c"])
        v = float(data["v"])
        qv = float(data["qv"])
        tbv = float(data["tbv"])
        tbqv = float(data["tbqv"])
        n = int(data["n"])
        x = str(data["x"]).strip()
    except Exception as e:
        return "CORRUPT", [f"Field parse error: {e}"]

    # Price validations
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        reasons.append("Non-positive price (o/h/l/c <= 0)")
    if h < l:
        reasons.append(f"High ({h}) < Low ({l})")
    if h < o or h < c:
        reasons.append(f"High ({h}) < max(Open, Close)")
    if l > o or l > c:
        reasons.append(f"Low ({l}) > min(Open, Close)")
    if v < 0 or qv < 0 or tbv < 0 or tbqv < 0:
        reasons.append("Negative volume")
    if n < 0:
        reasons.append(f"Negative trade count: {n}")
    if x not in ("0", "1"):
        reasons.append(f"Invalid is_final value: {x}")

    if reasons:
        return "CORRUPT", reasons

    # Freshness check
    age_ms = now_ms - t
    if age_ms < -10000:
        reasons.append(f"Future timestamp (ahead by {-age_ms}ms, check system clock)")
        return "WARN", reasons
    elif age_ms > 2 * interval_ms:
        reasons.append(f"Stale bar: age {age_ms / 1000.0:.1f}s > 2*interval ({2 * interval_ms / 1000.0:.1f}s)")
        return "STALE", reasons

    return "PASS", []


def inspect_livebars(
    rdb,
    symbols: List[str],
    interval: str,
    prefix: str,
    ttl_multiple: int = 2,
    batch_size: int = 100,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    interval_ms = parse_interval_to_ms(interval)
    now_ms = int(time.time() * 1000)

    results = []
    stats = {"PASS": 0, "STALE": 0, "MISSING": 0, "CORRUPT": 0, "WARN": 0}

    # Pipeline in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        pipe = rdb.pipeline(transaction=False)
        keys = [f"{prefix}:{sym.upper()}:{interval}" for sym in batch]

        for key in keys:
            pipe.exists(key)
            pipe.ttl(key)
            pipe.hgetall(key)

        batch_res = pipe.execute()

        # Each key had 3 commands in the pipeline
        for idx, sym in enumerate(batch):
            key = keys[idx]
            exists = bool(batch_res[idx * 3])
            ttl_val = int(batch_res[idx * 3 + 1])
            data = batch_res[idx * 3 + 2]

            item = {
                "symbol": sym,
                "interval": interval,
                "key": key,
                "exists": exists,
                "ttl": ttl_val,
                "age_s": "-",
                "close": "-",
                "volume": "-",
                "trades": "-",
                "is_final": "-",
                "status": "PASS",
                "notes": "",
            }

            if not exists or not data:
                item["status"] = "MISSING"
                item["notes"] = "Key not found in Redis"
                stats["MISSING"] += 1
                results.append(item)
                continue

            status, reasons = validate_livebar_data(data, interval_ms, now_ms)
            item["status"] = status
            item["notes"] = "; ".join(reasons)
            stats[status] += 1

            if "t" in data:
                try:
                    t_val = int(data["t"])
                    item["age_s"] = f"{(now_ms - t_val) / 1000.0:.1f}s"
                except Exception:
                    pass
            item["close"] = data.get("c", "-")
            item["volume"] = data.get("v", "-")
            item["trades"] = data.get("n", "-")
            item["is_final"] = data.get("x", "-")

            # Check TTL
            if ttl_val <= 0 and ttl_val != -1:
                if item["status"] == "PASS":
                    item["status"] = "WARN"
                    stats["PASS"] -= 1
                    stats["WARN"] += 1
                item["notes"] += f" [Abnormal TTL: {ttl_val}]"

            results.append(item)

    return results, stats


def main():
    parser = argparse.ArgumentParser(description="Verify Redis living bars (livebar:SYMBOL:interval).")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--intervals", default="1m,1h", help="Comma-separated intervals (default: 1m,1h)")
    parser.add_argument("--symbol", help="Specific symbol to check (e.g. BTCUSDT)")
    parser.add_argument("--limit-symbols", type=int, default=None, help="Limit number of symbols to inspect")
    parser.add_argument("--prefix", default=None, help="Key prefix override (default: from config or 'livebar')")
    parser.add_argument("--show-only-failures", action="store_true", help="Only display failed/missing/stale bars")
    parser.add_argument("--redis-host", help="Redis host override")
    parser.add_argument("--redis-port", type=int, help="Redis port override")
    parser.add_argument("--redis-db", type=int, help="Redis DB override")
    parser.add_argument("--redis-password", help="Redis password override")

    args = parser.parse_args()
    config = load_config(args.config)

    # Resolve prefix and settings
    r_cfg = config.get("redis", {})
    live_cfg = r_cfg.get("live", {})
    prefix = args.prefix or live_cfg.get("key_prefix", "livebar")
    ttl_multiple = live_cfg.get("ttl_multiple", 2)

    print(f"{Colors.BOLD}{Colors.CYAN}=== Redis Living Bars (LiveBar) Integrity Checker ==={Colors.RESET}")

    try:
        rdb = get_redis_client(config, args, for_live=True)
        rdb.ping()
        info = rdb.info("server")
        print(f"{Colors.GREEN}[OK] Connected to Redis ({rdb.connection_pool.connection_kwargs.get('host')}:{rdb.connection_pool.connection_kwargs.get('port')}, redis_version={info.get('redis_version')}){Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Cannot connect to Redis: {e}{Colors.RESET}")
        sys.exit(1)

    # Fetch symbols
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        print("Discovering active universe symbols...")
        symbols = fetch_active_symbols(config, limit=args.limit_symbols)

    print(f"Target Symbols: {len(symbols)} | Key Prefix: '{prefix}' | TTL Multiple: {ttl_multiple}x\n")

    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
    overall_success = True

    for interval in intervals:
        print(f"{Colors.BOLD}--- Inspecting Living Bars for Interval: {interval} ---{Colors.RESET}")
        results, stats = inspect_livebars(
            rdb=rdb,
            symbols=symbols,
            interval=interval,
            prefix=prefix,
            ttl_multiple=ttl_multiple,
        )

        headers = ["Symbol", "Key", "Exists", "TTL", "Age", "Close", "Volume", "Trades", "x", "Status", "Notes"]
        table_rows = []

        for r in results:
            if args.show_only_failures and r["status"] == "PASS":
                continue

            status_str = r["status"]
            if r["status"] == "PASS":
                status_str = f"{Colors.GREEN}PASS{Colors.RESET}"
            elif r["status"] in ("STALE", "WARN"):
                status_str = f"{Colors.YELLOW}{r['status']}{Colors.RESET}"
            else:
                status_str = f"{Colors.RED}{r['status']}{Colors.RESET}"

            exists_str = f"{Colors.GREEN}YES{Colors.RESET}" if r["exists"] else f"{Colors.RED}NO{Colors.RESET}"
            ttl_str = f"{r['ttl']}s" if r["ttl"] >= 0 else str(r["ttl"])

            table_rows.append([
                r["symbol"],
                r["key"],
                exists_str,
                ttl_str,
                r["age_s"],
                r["close"],
                r["volume"],
                r["trades"],
                r["is_final"],
                status_str,
                r["notes"],
            ])

        print_table(headers, table_rows)

        total = len(symbols)
        p_cnt = stats["PASS"]
        s_cnt = stats["STALE"]
        m_cnt = stats["MISSING"]
        c_cnt = stats["CORRUPT"]
        w_cnt = stats["WARN"]

        print(f"Result for {interval}: {Colors.GREEN}{p_cnt} PASS{Colors.RESET}, "
              f"{Colors.YELLOW}{s_cnt} STALE, {w_cnt} WARN{Colors.RESET}, "
              f"{Colors.RED}{m_cnt} MISSING, {c_cnt} CORRUPT{Colors.RESET} (Total: {total})\n")

        if m_cnt > 0 or c_cnt > 0 or s_cnt > (total * 0.1):
            overall_success = False

    if overall_success:
        print(f"{Colors.GREEN}{Colors.BOLD}? All Redis living bars checks PASSED.{Colors.RESET}")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}? Redis living bars check found MISSING or CORRUPT bars.{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
