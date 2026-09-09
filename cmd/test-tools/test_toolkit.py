# -*- coding: utf-8 -*-
"""
Unit and Mock Tests for ChomoSyncer Test & Validation Toolkit
"""
import os
import sys
import unittest
import json
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse_interval_to_ms, format_ms_to_utc, Colors
from check_redis_livebars import validate_livebar_data
from check_redis_closed_windows import validate_compact_bar, inspect_symbol_window
from monitor_redis_kline_ready import parse_stream_entry
from e2e_reconciliation import compare_bars


class TestCommon(unittest.TestCase):
    def test_parse_interval_to_ms(self):
        self.assertEqual(parse_interval_to_ms("1m"), 60_000)
        self.assertEqual(parse_interval_to_ms("3m"), 180_000)
        self.assertEqual(parse_interval_to_ms("5m"), 300_000)
        self.assertEqual(parse_interval_to_ms("15m"), 900_000)
        self.assertEqual(parse_interval_to_ms("30m"), 1_800_000)
        self.assertEqual(parse_interval_to_ms("1h"), 3_600_000)
        self.assertEqual(parse_interval_to_ms("2h"), 7_200_000)
        self.assertEqual(parse_interval_to_ms("4h"), 14_400_000)
        self.assertEqual(parse_interval_to_ms("1d"), 86_400_000)
        self.assertEqual(parse_interval_to_ms("1w"), 604_800_000)

    def test_format_ms_to_utc(self):
        # 1719835200000 = 2024-07-01 12:00:00 UTC
        s = format_ms_to_utc(1719835200000)
        self.assertEqual(s, "2024-07-01 12:00:00")


class TestLiveBarValidation(unittest.TestCase):
    def setUp(self):
        self.now_ms = 1719835230000  # 30 seconds into the bar
        self.valid_data = {
            "t": "1719835200000",
            "o": "60000.5",
            "h": "60100.0",
            "l": "59950.0",
            "c": "60050.0",
            "v": "12.345",
            "qv": "740700.0",
            "tbv": "6.12",
            "tbqv": "367000.0",
            "n": "342",
            "x": "0",
        }

    def test_valid_livebar(self):
        status, reasons = validate_livebar_data(self.valid_data, interval_ms=60000, now_ms=self.now_ms)
        self.assertEqual(status, "PASS")
        self.assertEqual(len(reasons), 0)

    def test_missing_field(self):
        bad = dict(self.valid_data)
        del bad["c"]
        status, reasons = validate_livebar_data(bad, interval_ms=60000, now_ms=self.now_ms)
        self.assertEqual(status, "CORRUPT")
        self.assertTrue(any("Missing field 'c'" in r for r in reasons))

    def test_inverted_high_low(self):
        bad = dict(self.valid_data)
        bad["h"] = "59000.0"  # high < low
        status, reasons = validate_livebar_data(bad, interval_ms=60000, now_ms=self.now_ms)
        self.assertEqual(status, "CORRUPT")
        self.assertTrue(any("High" in r and "Low" in r for r in reasons))

    def test_stale_livebar(self):
        # Age > 2 * 60s
        stale_now_ms = 1719835200000 + 130000
        status, reasons = validate_livebar_data(self.valid_data, interval_ms=60000, now_ms=stale_now_ms)
        self.assertEqual(status, "STALE")
        self.assertTrue(any("Stale bar" in r for r in reasons))


class TestClosedWindowValidation(unittest.TestCase):
    def test_validate_compact_bar_valid(self):
        bar = [1719835200000, 60000.0, 60100.0, 59900.0, 60050.0, 10.5, 630000.0, 5.0, 300000.0]
        ok, msg = validate_compact_bar(bar)
        self.assertTrue(ok)
        self.assertEqual(msg, "")

    def test_validate_compact_bar_invalid_length(self):
        bar = [1719835200000, 60000.0]
        ok, msg = validate_compact_bar(bar)
        self.assertFalse(ok)
        self.assertIn("Expected 9 elements", msg)

    def test_inspect_symbol_window_continuous(self):
        interval_ms = 60000
        base_t = 1719835200000  # latest closed bar
        # Generate 5 continuous bars: newest first
        raw_bars = []
        for i in range(5):
            t = base_t - (i * interval_ms)
            bar = [t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0]
            raw_bars.append(json.dumps(bar))

        # current time is within next bar
        now_ms = base_t + interval_ms + 10000
        res = inspect_symbol_window(raw_bars, interval_ms=interval_ms, now_ms=now_ms, expected_size=5)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["actual_len"], 5)
        self.assertEqual(res["window_gaps"], 0)
        self.assertEqual(res["head_lag_bars"], 0)

    def test_inspect_symbol_window_with_gap(self):
        interval_ms = 60000
        base_t = 1719835200000
        # Missing bar between idx 0 and idx 1: base_t -> base_t - 2*interval_ms
        bars = [
            [base_t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0],
            [base_t - 2 * interval_ms, 59900.0, 60000.0, 59800.0, 59950.0, 8.0, 480000.0, 4.0, 240000.0],
        ]
        raw_bars = [json.dumps(b) for b in bars]
        now_ms = base_t + interval_ms + 10000
        res = inspect_symbol_window(raw_bars, interval_ms=interval_ms, now_ms=now_ms, expected_size=2)
        self.assertEqual(res["status"], "FAIL")
        self.assertEqual(res["window_gaps"], 1)
        self.assertEqual(res["missing_in_window"], 1)


class TestE2EReconciliation(unittest.TestCase):
    def test_compare_bars_identical(self):
        t = 1719835200000
        redis_bars = [[t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0]]
        ch_rows = [{
            "t": t,
            "open": 60000.0,
            "high": 60100.0,
            "low": 59900.0,
            "close": 60050.0,
            "volume": 10.0,
            "quote_volume": 600000.0,
        }]
        res = compare_bars(redis_bars, ch_rows)
        self.assertEqual(res["matched_count"], 1)
        self.assertEqual(res["ts_mismatches"], 0)
        self.assertEqual(res["price_mismatches"], 0)

    def test_compare_bars_price_mismatch(self):
        t = 1719835200000
        redis_bars = [[t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0]]
        ch_rows = [{
            "t": t,
            "open": 60000.0,
            "high": 60100.0,
            "low": 59900.0,
            "close": 60999.0,  # mismatch!
            "volume": 10.0,
            "quote_volume": 600000.0,
        }]
        res = compare_bars(redis_bars, ch_rows)
        self.assertEqual(res["matched_count"], 0)
        self.assertEqual(res["price_mismatches"], 1)


class TestStreamParser(unittest.TestCase):
    def test_parse_stream_entry(self):
        entry_id = "1719835262000-0"
        fields = {
            "interval": "1m",
            "timestamp": "1719835200000",
            "symbols_count": "245",
        }
        res = parse_stream_entry(entry_id, fields, total_symbols=250)
        self.assertEqual(res["interval"], "1m")
        self.assertEqual(res["bar_ts"], 1719835200000)
        self.assertEqual(res["symbols_count"], 245)
        self.assertEqual(res["total_symbols"], 250)
        self.assertEqual(res["coverage_pct"], 98.0)
        # close_ms = 1719835260000, event_ms = 1719835262000 -> latency = 2000ms = 2.0s
        self.assertAlmostEqual(res["latency_s"], 2.0, places=2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
