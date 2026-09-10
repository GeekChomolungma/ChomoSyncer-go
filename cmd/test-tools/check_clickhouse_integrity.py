# -*- coding: utf-8 -*-
"""
ClickHouse K-Line Integrity and Continuity Checker

Verifies:
1. Timestamp continuity (detects historical gaps between adjacent bars).
2. Data content sanity (ensures bars have non-zero OHLC, valid high/low bounds,
   non-negative volume, positive trades_count, and aligned end_time).
"""
import os
import sys
import argparse
import time
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    Colors,
    ClickHouseClient,
    get_clickhouse_client,
    load_config,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)


def check_symbol_table(
    ch: ClickHouseClient,
    table: str,
    symbol: str,
    interval: str,
    interval_ms: int,
    time_filter: str = "",
    max_gap_display: int = 5,
) -> Dict[str, Any]:
    """
    Performs gap detection and content integrity validation on a single symbol in a table.
    """
    result = {
        "symbol": symbol,
        "total_bars": 0,
        "min_time_str": "-",
        "max_time_str": "-",
        "expected_bars": 0,
        "missing_bars": 0,
        "gap_count": 0,
        "gaps": [],
        "invalid_ohlc": 0,
        "invalid_hl_bounds": 0,
        "invalid_volume": 0,
        "zero_trades": 0,
        "invalid_timestamps": 0,
        "status": "PASS",
        "error_msg": "",
    }

    # 1. Aggregate stats and sanity checks
    agg_sql = f"""
    SELECT
        count() AS total_bars,
        min(start_time) AS min_time,
        max(start_time) AS max_time,
        toUnixTimestamp64Milli(min(start_time)) AS min_ms,
        toUnixTimestamp64Milli(max(start_time)) AS max_ms,
        countIf(open <= 0 OR high <= 0 OR low <= 0 OR close <= 0) AS invalid_ohlc,
        countIf(high < low OR high < open OR high < close OR low > open OR low > close) AS invalid_hl_bounds,
        countIf(volume < 0 OR quote_volume < 0) AS invalid_volume,
        countIf(trades_count = 0) AS zero_trades,
        countIf(toUnixTimestamp64Milli(end_time) <= toUnixTimestamp64Milli(start_time)) AS invalid_timestamps
    FROM {table} FINAL
    WHERE symbol = '{symbol}' {time_filter}
    """

    try:
        agg_rows = ch.query(agg_sql)
    except Exception as e:
        result["status"] = "ERROR"
        result["error_msg"] = str(e)
        return result

    if not agg_rows or agg_rows[0].get("total_bars", 0) == 0:
        result["status"] = "EMPTY"
        return result

    row = agg_rows[0]
    total_bars = int(row.get("total_bars", 0))
    min_ms = int(row.get("min_ms", 0))
    max_ms = int(row.get("max_ms", 0))

    result["total_bars"] = total_bars
    result["min_time_str"] = format_ms_to_utc(min_ms)
    result["max_time_str"] = format_ms_to_utc(max_ms)
    result["invalid_ohlc"] = int(row.get("invalid_ohlc", 0))
    result["invalid_hl_bounds"] = int(row.get("invalid_hl_bounds", 0))
    result["invalid_volume"] = int(row.get("invalid_volume", 0))
    result["zero_trades"] = int(row.get("zero_trades", 0))
    result["invalid_timestamps"] = int(row.get("invalid_timestamps", 0))

    expected_bars = int((max_ms - min_ms) / interval_ms) + 1 if max_ms >= min_ms else total_bars
    result["expected_bars"] = expected_bars

    # 2. Gap analysis via window function
    gap_sql = f"""
    SELECT
        prev_time,
        start_time,
        toUnixTimestamp64Milli(prev_time) AS prev_ms,
        toUnixTimestamp64Milli(start_time) AS curr_ms,
        diff_ms,
        intDiv(diff_ms, {interval_ms}) - 1 AS missing_count
    FROM (
        SELECT
            lagInFrame(start_time) OVER (ORDER BY start_time ASC) AS prev_time,
            start_time,
            toUnixTimestamp64Milli(start_time) - toUnixTimestamp64Milli(lagInFrame(start_time) OVER (ORDER BY start_time ASC)) AS diff_ms
        FROM {table} FINAL
        WHERE symbol = '{symbol}' {time_filter}
    )
    WHERE prev_ms > 0 AND diff_ms > {interval_ms}
    ORDER BY start_time ASC
    """

    try:
        gap_rows = ch.query(gap_sql)
        gaps = []
        total_missing = 0
        for g in gap_rows:
            p_ms = int(g["prev_ms"])
            c_ms = int(g["curr_ms"])
            m_cnt = int(g["missing_count"])
            total_missing += m_cnt
            gaps.append({
                "gap_start": format_ms_to_utc(p_ms + interval_ms),
                "gap_end": format_ms_to_utc(c_ms - 1),
                "missing_count": m_cnt,
            })

        result["gap_count"] = len(gaps)
        result["missing_bars"] = total_missing
        result["gaps"] = gaps
    except Exception as e:
        result["status"] = "ERROR"
        result["error_msg"] = f"Gap query error: {e}"
        return result

    # 3. Compute status
    anomalies = (
        result["invalid_ohlc"]
        + result["invalid_hl_bounds"]
        + result["invalid_volume"]
        + result["invalid_timestamps"]
    )
    if result["gap_count"] > 0 or anomalies > 0:
        result["status"] = "FAIL"
    elif result["zero_trades"] > 0:
        result["status"] = "WARN"
    else:
        result["status"] = "PASS"

    return result


def main():
    parser = argparse.ArgumentParser(description="Check ClickHouse K-Line data continuity and field integrity.")
    parser.add_argument("--config", help="Path to config.yaml (auto-discovered if omitted)")
    parser.add_argument("--intervals", default="1m,5m,15m,1h,4h,1d", help="Comma-separated intervals (default: 1m plus the standard rollup tables)")
    parser.add_argument("--symbol", help="Specific symbol to check (e.g. BTCUSDT), or check all if omitted")
    parser.add_argument("--limit-symbols", type=int, default=None, help="Limit number of symbols to inspect")
    parser.add_argument("--table-prefix", default="market.fapi_kline", help="Table prefix (default: market.fapi_kline)")
    parser.add_argument("--start-date", help="Optional filter: start date/time (e.g. '2024-01-01' or '2024-01-01 00:00:00')")
    parser.add_argument("--end-date", help="Optional filter: end date/time")
    parser.add_argument("--max-gaps", type=int, default=5, help="Max gaps to display per symbol (default: 5)")
    parser.add_argument("--show-all-gaps", action="store_true", help="Print all gaps without truncation")
    parser.add_argument("--ch-host", help="ClickHouse host override")
    parser.add_argument("--ch-port", type=int, help="ClickHouse HTTP port override (default: 8123)")
    parser.add_argument("--ch-db", help="ClickHouse database override")
    parser.add_argument("--ch-user", help="ClickHouse username override")
    parser.add_argument("--ch-password", help="ClickHouse password override")

    args = parser.parse_args()
    config = load_config(args.config)
    ch = get_clickhouse_client(config, args)

    print(f"{Colors.BOLD}{Colors.CYAN}=== ClickHouse Kline Integrity & Continuity Checker ==={Colors.RESET}")
    print(f"Target ClickHouse: {ch.host}:{ch.port} (database: {ch.database})")

    # Test connection
    ok, msg = ch.test_connection()
    if not ok:
        print(f"{Colors.RED}[ERROR] Cannot connect to ClickHouse: {msg}{Colors.RESET}")
        print("Please verify ClickHouse is running and credentials/port are correct.")
        sys.exit(1)

    print(f"{Colors.GREEN}[OK] Connected to ClickHouse successfully.{Colors.RESET}\n")

    # Time filter
    time_filter = ""
    if args.start_date:
        time_filter += f" AND start_time >= '{args.start_date}'"
    if args.end_date:
        time_filter += f" AND start_time <= '{args.end_date}'"

    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]

    overall_pass = True
    for interval in intervals:
        table = f"{args.table_prefix}_{interval}"
        try:
            interval_ms = parse_interval_to_ms(interval)
        except ValueError as e:
            print(f"{Colors.RED}[ERROR] {e}{Colors.RESET}")
            continue

        print(f"\n" + f"{Colors.BOLD}--- Inspecting Table: {table} (interval: {interval}, step: {interval_ms}ms) ---{Colors.RESET}")

        # Check if table exists
        try:
            check_table_sql = f"EXISTS TABLE {table}"
            res = ch.query(check_table_sql)
            if not res or res[0].get("result") == 0:
                print(f"{Colors.YELLOW}[WARN] Table {table} does not exist in ClickHouse. Skipping.{Colors.RESET}")
                continue
        except Exception as e:
            print(f"{Colors.RED}[ERROR] Failed checking table {table}: {e}{Colors.RESET}")
            continue

        # Fetch symbols to check
        if args.symbol:
            symbols = [args.symbol.upper()]
        else:
            sym_sql = f"SELECT DISTINCT symbol FROM {table} FINAL ORDER BY symbol"
            try:
                rows = ch.query(sym_sql)
                symbols = [r["symbol"] for r in rows if "symbol" in r]
            except Exception as e:
                print(f"{Colors.RED}[ERROR] Failed querying distinct symbols: {e}{Colors.RESET}")
                continue

        if not symbols:
            print(f"{Colors.YELLOW}[INFO] No data or symbols found in {table}.{Colors.RESET}")
            continue

        if args.limit_symbols and args.limit_symbols > 0:
            symbols = symbols[:args.limit_symbols]

        print(f"Checking {len(symbols)} symbol(s)...")

        headers = ["Symbol", "Total Bars", "Time Span", "Coverage", "Gaps", "Missing", "Anomalies", "Status"]
        table_rows = []
        all_symbol_gaps = []

        pass_count = 0
        fail_count = 0
        warn_count = 0

        for sym in symbols:
            res = check_symbol_table(
                ch=ch,
                table=table,
                symbol=sym,
                interval=interval,
                interval_ms=interval_ms,
                time_filter=time_filter,
                max_gap_display=args.max_gaps,
            )

            if res["status"] == "EMPTY":
                table_rows.append([sym, 0, "No records", "0.0%", 0, 0, 0, f"{Colors.YELLOW}EMPTY{Colors.RESET}"])
                warn_count += 1
                continue
            elif res["status"] == "ERROR":
                table_rows.append([sym, "-", "-", "-", "-", "-", "-", f"{Colors.RED}ERROR{Colors.RESET}"])
                fail_count += 1
                overall_pass = False
                continue

            time_span = f"{res['min_time_str']} -> {res['max_time_str']}"
            cov_pct = (res['total_bars'] / res['expected_bars'] * 100.0) if res['expected_bars'] > 0 else 100.0
            cov_str = f"{cov_pct:.2f}%"

            anomalies = (
                res["invalid_ohlc"]
                + res["invalid_hl_bounds"]
                + res["invalid_volume"]
                + res["invalid_timestamps"]
            )
            anom_str = str(anomalies)
            if anomalies > 0:
                anom_str = f"{Colors.RED}{anomalies}{Colors.RESET}"

            gap_str = str(res["gap_count"])
            if res["gap_count"] > 0:
                gap_str = f"{Colors.RED}{res['gap_count']}{Colors.RESET}"
                all_symbol_gaps.append((sym, res["gaps"]))

            missing_str = str(res["missing_bars"])
            if res["missing_bars"] > 0:
                missing_str = f"{Colors.RED}{res['missing_bars']}{Colors.RESET}"

            if res["status"] == "PASS":
                status_str = f"{Colors.GREEN}PASS{Colors.RESET}"
                pass_count += 1
            elif res["status"] == "WARN":
                status_str = f"{Colors.YELLOW}WARN{Colors.RESET}"
                warn_count += 1
            else:
                status_str = f"{Colors.RED}FAIL{Colors.RESET}"
                fail_count += 1
                overall_pass = False

            table_rows.append([
                sym,
                f"{res['total_bars']:,}",
                time_span,
                cov_str,
                gap_str,
                missing_str,
                anom_str,
                status_str,
            ])

        print_table(headers, table_rows)
        print(f"Summary for {table}: {Colors.GREEN}{pass_count} PASS{Colors.RESET}, "
              f"{Colors.YELLOW}{warn_count} WARN/EMPTY{Colors.RESET}, "
              f"{Colors.RED}{fail_count} FAIL{Colors.RESET} (Total: {len(symbols)})")

        # Display detailed gaps if any
        if all_symbol_gaps:
            print(f"\n{Colors.RED}{Colors.BOLD}>>> Detailed Gap Breakdown for {table} <<<{Colors.RESET}")
            for sym, gaps in all_symbol_gaps:
                print(f"  {Colors.BOLD}[{sym}]{Colors.RESET} total {len(gaps)} gap(s):")
                display_gaps = gaps if args.show_all_gaps else gaps[:args.max_gaps]
                for g in display_gaps:
                    print(f"    - Missing {g['missing_count']} bar(s) between {Colors.YELLOW}{g['gap_start']}{Colors.RESET} and {Colors.YELLOW}{g['gap_end']}{Colors.RESET}")
                if not args.show_all_gaps and len(gaps) > args.max_gaps:
                    print(f"    - ... and {len(gaps) - args.max_gaps} more gap(s) (use --show-all-gaps to see all)")

    if overall_pass:
        print(f"\n" + f"{Colors.GREEN}{Colors.BOLD}? All ClickHouse integrity checks PASSED.{Colors.RESET}")
        sys.exit(0)
    else:
        print(f"\n" + f"{Colors.RED}{Colors.BOLD}? ClickHouse integrity checks found anomalies or missing bars.{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
