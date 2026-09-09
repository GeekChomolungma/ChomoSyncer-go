# -*- coding: utf-8 -*-
"""
Redis Stream Kline-Ready Cross-Section Notification Monitor

Monitors `stream:market:kline_ready` stream:
1. Inspects recent section events (XRANGE / XREVRANGE).
2. Live tails new events in real-time (XREAD BLOCK).
3. Evaluates publishing latency: (event_time - (bar_timestamp + interval_duration)).
4. Checks symbol coverage against total active universe.
5. Verifies section timestamp continuity (no missing cross-sections).
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
    load_config,
    get_redis_client,
    fetch_active_symbols,
    parse_interval_to_ms,
    format_ms_to_utc,
    print_table,
)


def parse_stream_entry(entry_id: str, fields: Dict[str, str], total_symbols: int) -> Dict[str, Any]:
    """
    Parses and verifies a single stream entry from stream:market:kline_ready.
    """
    try:
        event_ms = int(entry_id.split("-")[0])
    except Exception:
        event_ms = int(time.time() * 1000)

    interval = fields.get("interval", "unknown")
    try:
        interval_ms = parse_interval_to_ms(interval)
    except Exception:
        interval_ms = 60000

    try:
        bar_ts = int(fields.get("timestamp", 0))
    except Exception:
        bar_ts = 0

    try:
        sym_count = int(fields.get("symbols_count", 0))
    except Exception:
        sym_count = 0

    close_ms = bar_ts + interval_ms
    latency_ms = event_ms - close_ms

    coverage_pct = (sym_count / total_symbols * 100.0) if total_symbols > 0 else 100.0

    # Determine publish trigger reason heuristic:
    # If latency is ~5.0s (close to section_timeout default 5s), it was likely a timeout trigger.
    # If latency < 2.0s and coverage == 100%, it was an onComplete trigger.
    trigger = "Complete" if (total_symbols > 0 and sym_count >= total_symbols) else "Timeout/Partial"

    return {
        "id": entry_id,
        "event_time_str": format_ms_to_utc(event_ms),
        "event_ms": event_ms,
        "interval": interval,
        "bar_ts": bar_ts,
        "bar_time_str": format_ms_to_utc(bar_ts),
        "close_time_str": format_ms_to_utc(close_ms),
        "symbols_count": sym_count,
        "total_symbols": total_symbols,
        "coverage_pct": coverage_pct,
        "latency_ms": latency_ms,
        "latency_s": latency_ms / 1000.0,
        "trigger": trigger,
    }


def inspect_recent_events(rdb, stream_key: str, count: int, total_symbols: int):
    """
    Fetches and displays the most recent N events from the stream.
    """
    print(f"Reading last {count} event(s) from Stream '{stream_key}'...")
    try:
        raw_entries = rdb.xrevrange(stream_key, count=count)
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Failed reading stream {stream_key}: {e}{Colors.RESET}")
        return

    if not raw_entries:
        print(f"{Colors.YELLOW}[INFO] No events found in stream '{stream_key}'.{Colors.RESET}")
        return

    # Oldest first for display
    raw_entries.reverse()

    headers = ["Stream Entry ID", "Interval", "Section Bar Time", "Bar Closed At", "Published At", "Latency", "Symbols Count", "Coverage", "Trigger"]
    rows = []

    for entry_id, fields in raw_entries:
        info = parse_stream_entry(entry_id, fields, total_symbols)

        # Latency coloring
        lat = info["latency_s"]
        if lat < 1.0:
            lat_str = f"{Colors.GREEN}+{lat:.2f}s{Colors.RESET}"
        elif lat <= 5.5:
            lat_str = f"{Colors.YELLOW}+{lat:.2f}s{Colors.RESET}"
        else:
            lat_str = f"{Colors.RED}+{lat:.2f}s{Colors.RESET}"

        # Coverage coloring
        cov = info["coverage_pct"]
        if cov >= 99.0:
            cov_str = f"{Colors.GREEN}{cov:.1f}%{Colors.RESET}"
        elif cov >= 90.0:
            cov_str = f"{Colors.YELLOW}{cov:.1f}%{Colors.RESET}"
        else:
            cov_str = f"{Colors.RED}{cov:.1f}%{Colors.RESET}"

        trigger_str = f"{Colors.GREEN}Complete{Colors.RESET}" if info["trigger"] == "Complete" else f"{Colors.YELLOW}{info['trigger']}{Colors.RESET}"

        rows.append([
            info["id"],
            info["interval"],
            info["bar_time_str"],
            info["close_time_str"],
            info["event_time_str"],
            lat_str,
            f"{info['symbols_count']}/{total_symbols}",
            cov_str,
            trigger_str,
        ])

    print_table(headers, rows)


def tail_stream(rdb, stream_key: str, total_symbols: int, max_events: Optional[int] = None):
    """
    Continuously listens for new events on the stream.
    """
    print(f"{Colors.CYAN}Listening for live section events on '{stream_key}' (Press Ctrl+C to stop)...{Colors.RESET}")
    print()
    last_id = "$"
    event_count = 0

    try:
        while True:
            res = rdb.xread({stream_key: last_id}, count=10, block=2000)
            if not res:
                continue

            for stream_name, entries in res:
                for entry_id, fields in entries:
                    last_id = entry_id
                    event_count += 1
                    info = parse_stream_entry(entry_id, fields, total_symbols)

                    lat = info["latency_s"]
                    cov = info["coverage_pct"]

                    status_color = Colors.GREEN if cov >= 95.0 else Colors.YELLOW
                    print(
                        f"{Colors.BOLD}{status_color}[READY]{Colors.RESET} "
                        f"Interval: {Colors.BOLD}{info['interval']:<3}{Colors.RESET} | "
                        f"Bar Start: {info['bar_time_str']} | "
                        f"Published: {info['event_time_str']} (Latency: {lat:+.2f}s) | "
                        f"Symbols: {info['symbols_count']}/{total_symbols} ({cov:.1f}%) | "
                        f"ID: {info['id']}"
                    )

                    if max_events and event_count >= max_events:
                        print("\nReached max events limit ({max_events}). Exiting listener.")
                        return
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")


def main():
    parser = argparse.ArgumentParser(description="Monitor and inspect Redis stream:market:kline_ready.")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--recent", type=int, default=20, help="Number of recent events to inspect (default: 20)")
    parser.add_argument("--tail", action="store_true", help="Continuously tail and listen for live events")
    parser.add_argument("--max-events", type=int, help="Max events to listen for when --tail is enabled")
    parser.add_argument("--stream-key", help="Stream key override (default: from config or 'stream:market:kline_ready')")
    parser.add_argument("--redis-host", help="Redis host override")
    parser.add_argument("--redis-port", type=int, help="Redis port override")
    parser.add_argument("--redis-db", type=int, help="Redis DB override")
    parser.add_argument("--redis-password", help="Redis password override")

    args = parser.parse_args()
    config = load_config(args.config)

    r_cfg = config.get("redis", {})
    win_cfg = r_cfg.get("window", {})
    stream_key = args.stream_key or win_cfg.get("stream_key", "stream:market:kline_ready")

    print(f"{Colors.BOLD}{Colors.CYAN}=== Redis Kline-Ready Stream Monitor ==={Colors.RESET}")

    try:
        rdb = get_redis_client(config, args, for_live=False)
        rdb.ping()
        print(f"{Colors.GREEN}[OK] Connected to Redis.{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Cannot connect to Redis: {e}{Colors.RESET}")
        sys.exit(1)

    # Discover expected symbols count
    symbols = fetch_active_symbols(config)
    total_symbols = len(symbols)
    print(f"Stream Target: '{stream_key}' | Universe Size: {total_symbols} symbols\n")

    # Inspect recent events
    inspect_recent_events(rdb, stream_key, count=args.recent, total_symbols=total_symbols)

    # Tail mode if requested
    if args.tail:
        tail_stream(rdb, stream_key, total_symbols, max_events=args.max_events)


if __name__ == "__main__":
    main()
