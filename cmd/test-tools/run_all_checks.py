# -*- coding: utf-8 -*-
"""
ChomoSyncer Comprehensive Pre-Deployment Test Suite Runner

Runs full diagnostics across the entire data pipeline:
1. ClickHouse K-Line Integrity & Continuity Check
2. Redis Living Bars Check
3. Redis Closed Rolling Windows Check
4. Redis Kline-Ready Stream Inspection
5. End-to-End Cache vs Storage Reconciliation
6. ClickHouse vs Binance ground truth (optional, --vs-binance)
7. Open-interest table consistency (optional, --oi)

Generates a unified pre-deployment health scorecard.
"""
import os
import sys
import argparse
import subprocess
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import Colors, load_config, print_table


def run_subcommand(cmd: List[str]) -> Tuple[int, str]:
    """
    Executes a test script and captures exit code and output.
    """
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    return proc.returncode, proc.stdout


def main():
    parser = argparse.ArgumentParser(description="Run all pre-deployment integrity checks.")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--intervals", default="1m", help="Redis-facing intervals (default: 1m; only the base interval has Redis windows / live bars)")
    parser.add_argument("--ch-intervals", default="1m,5m,15m,1h,4h,1d", help="ClickHouse intervals for the integrity check (base + rollup tables)")
    parser.add_argument("--symbol", help="Specific symbol to check")
    parser.add_argument("--quick", action="store_true", help="Quick smoke check with first 5 symbols")
    parser.add_argument("--skip-ch", action="store_true", help="Skip ClickHouse checks")
    parser.add_argument("--skip-redis", action="store_true", help="Skip Redis checks")
    parser.add_argument("--skip-e2e", action="store_true", help="Skip E2E cross reconciliation")
    parser.add_argument("--vs-binance", action="store_true", help="Also reconcile ClickHouse rollups against Binance REST (slow; hits the exchange)")
    parser.add_argument("--oi", action="store_true", help="Also check the 5-minute open-interest table (needs the open_interest module to be running); with --vs-binance it also compares rows with Binance")
    parser.add_argument("--verbose", action="store_true", help="Print full output of all sub-checks")

    args = parser.parse_args()
    base_dir = os.path.dirname(os.path.abspath(__file__))

    limit_sym = "5" if args.quick else ""

    print(f"{Colors.BOLD}{Colors.CYAN}======================================================={Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}    ChomoSyncer Pre-Deployment Health Verification     {Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}======================================================={Colors.RESET}")
    print(f"Mode: {'Quick Smoke (5 symbols)' if args.quick else 'Standard'} | Intervals: {args.intervals}")
    print()

    scorecard = []
    overall_ok = True

    # 1. ClickHouse Integrity Check
    if not args.skip_ch:
        print(f"{Colors.BOLD}>>> [1/7] Running ClickHouse Continuity & Data Integrity Check...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "check_clickhouse_integrity.py"), "--intervals", args.ch_intervals]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        elif limit_sym:
            cmd.extend(["--limit-symbols", limit_sym])

        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)

        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "Continuous timestamps, 0 gaps, valid OHLCV"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "Anomalies or missing bars detected"
            overall_ok = False
        scorecard.append(["ClickHouse Integrity", f"fapi_kline_[{args.ch_intervals}]", status, detail])
    else:
        scorecard.append(["ClickHouse Integrity", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Skipped via flag"])

    # 2. Redis Living Bars Check
    if not args.skip_redis:
        print(f"{Colors.BOLD}>>> [2/7] Running Redis Living Bars (LiveBar) Check...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "check_redis_livebars.py"), "--intervals", args.intervals]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        elif limit_sym:
            cmd.extend(["--limit-symbols", limit_sym])

        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)

        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "All livebar hashes exist, fresh TTL & timestamp"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "Missing, stale, or corrupt livebar keys"
            overall_ok = False
        scorecard.append(["Redis Live Bars", f"livebar:*:{args.intervals}", status, detail])
    else:
        scorecard.append(["Redis Live Bars", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Skipped via flag"])

    # 3. Redis Closed Rolling Windows Check
    if not args.skip_redis:
        print(f"{Colors.BOLD}>>> [3/7] Running Redis Closed Rolling Windows Check...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "check_redis_closed_windows.py"), "--intervals", args.intervals]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        elif limit_sym:
            cmd.extend(["--limit-symbols", limit_sym])

        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)

        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "Windows valid length, head up-to-date, zero internal gaps"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "Empty/short windows, stale head, or internal gaps"
            overall_ok = False
        scorecard.append(["Redis Closed Windows", f"kline:*:{args.intervals}", status, detail])
    else:
        scorecard.append(["Redis Closed Windows", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Skipped via flag"])

    # 4. Redis Stream Notification Check
    if not args.skip_redis:
        print(f"{Colors.BOLD}>>> [4/7] Running Redis Kline-Ready Stream Inspection...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "monitor_redis_kline_ready.py"), "--recent", "10"]
        if args.config:
            cmd.extend(["--config", args.config])

        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)

        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "Stream events present, valid latency & coverage"
        else:
            status = f"{Colors.YELLOW}WARN{Colors.RESET}"
            detail = "No recent stream events or stream read failed"
        scorecard.append(["Redis Kline-Ready Stream", "stream:market:kline_ready", status, detail])
    else:
        scorecard.append(["Redis Kline-Ready Stream", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Skipped via flag"])

    # 5. E2E Cross Reconciliation
    if not args.skip_ch and not args.skip_redis and not args.skip_e2e:
        print(f"{Colors.BOLD}>>> [5/7] Running End-to-End Cache vs Storage Reconciliation...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "e2e_reconciliation.py"), "--intervals", args.intervals]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        else:
            cmd.extend(["--limit-symbols", "10" if not limit_sym else limit_sym])

        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)

        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "100% bar-by-bar match between Redis and ClickHouse"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "Discrepancy found between Redis cache and ClickHouse"
            overall_ok = False
        scorecard.append(["E2E Storage Consistency", "Redis <-> ClickHouse", status, detail])
    else:
        scorecard.append(["E2E Storage Consistency", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Skipped via flag"])

    # 6. ClickHouse vs Binance (optional; hits the exchange)
    if args.vs_binance and not args.skip_ch:
        print(f"{Colors.BOLD}>>> [6/7] Reconciling ClickHouse rollups against Binance REST...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "check_vs_binance.py"), "--intervals", "1m,1h,4h"]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        else:
            cmd.extend(["--limit-symbols", "5" if limit_sym else "8"])
        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)
        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "ClickHouse matches Binance within tolerance"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "ClickHouse disagrees with Binance (ingestion / rollup bug?)"
            overall_ok = False
        scorecard.append(["External Truth (Binance)", "ClickHouse <-> Binance REST", status, detail])
    else:
        scorecard.append(["External Truth (Binance)", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Pass --vs-binance to enable"])

    # 7. Open-interest table consistency (optional; ClickHouse-only unless --vs-binance)
    if args.oi and not args.skip_ch:
        print(f"{Colors.BOLD}>>> [7/7] Running Open-Interest Table Consistency Check...{Colors.RESET}")
        cmd = [sys.executable, os.path.join(base_dir, "check_oi_consistency.py"), "--hours", "6"]
        if args.config:
            cmd.extend(["--config", args.config])
        if args.symbol:
            cmd.extend(["--symbol", args.symbol])
        elif limit_sym:
            cmd.extend(["--limit-symbols", limit_sym])
        if args.vs_binance:
            cmd.extend(["--vs-binance", "--binance-symbols", "3"])
        code, out = run_subcommand(cmd)
        if args.verbose or code != 0:
            print(out)
        if code == 0:
            status = f"{Colors.GREEN}PASS{Colors.RESET}"
            detail = "OI series continuous, aligned to kline bars, consistent with hist"
        else:
            status = f"{Colors.RED}FAIL{Colors.RESET}"
            detail = "OI gaps, misaligned rows, stale data or disagreement with Binance"
            overall_ok = False
        scorecard.append(["Open Interest", "market.fapi_oi_5m", status, detail])
    else:
        scorecard.append(["Open Interest", "-", f"{Colors.DIM}SKIPPED{Colors.RESET}", "Pass --oi to enable"])

    # Summary Report
    print()
    print(f"{Colors.BOLD}{Colors.CYAN}======================================================={Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}            PRE-DEPLOYMENT HEALTH SCORECARD            {Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}======================================================={Colors.RESET}")
    headers = ["Component / Suite", "Target Resource", "Result", "Evaluation Detail"]
    print_table(headers, scorecard)
    print()

    if overall_ok:
        print(f"{Colors.GREEN}{Colors.BOLD}? READY FOR PRODUCTION DEPLOYMENT: All critical checks passed.{Colors.RESET}")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}? DEPLOYMENT BLOCKED: Please review failed checks above before releasing.{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
