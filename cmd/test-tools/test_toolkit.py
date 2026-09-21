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
from check_vs_binance import rel_close
import backfill_missing_1m as bf
from check_clickhouse_integrity import write_gaps_csv, GAPS_CSV_HEADER
from check_oi_consistency import bar_window, evaluate_symbol, compare_with_binance, coverage_status, BAR_MS


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
        bar = [1719835200000, 60000.0, 60100.0, 59900.0, 60050.0, 10.5, 630000.0, 5.0, 300000.0, 42]
        ok, msg = validate_compact_bar(bar)
        self.assertTrue(ok)
        self.assertEqual(msg, "")

    def test_validate_compact_bar_invalid_length(self):
        bar = [1719835200000, 60000.0]
        ok, msg = validate_compact_bar(bar)
        self.assertFalse(ok)
        self.assertIn("Expected 10 elements", msg)

    def test_inspect_symbol_window_continuous(self):
        interval_ms = 60000
        base_t = 1719835200000  # latest closed bar
        # Generate 5 continuous bars: newest first
        raw_bars = []
        for i in range(5):
            t = base_t - (i * interval_ms)
            bar = [t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0, 42]
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
            [base_t, 60000.0, 60100.0, 59900.0, 60050.0, 10.0, 600000.0, 5.0, 300000.0, 42],
            [base_t - 2 * interval_ms, 59900.0, 60000.0, 59800.0, 59950.0, 8.0, 480000.0, 4.0, 240000.0, 31],
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


class TestVsBinance(unittest.TestCase):
    def test_rel_close_exact_and_tolerant(self):
        self.assertTrue(rel_close(100.0, 100.0, 1e-9))
        # ~1e-8 relative drift passes the volume tolerance but fails the price one
        self.assertTrue(rel_close(1234.56789, 1234.56789 * (1 + 5e-9), 1e-6))
        self.assertFalse(rel_close(1234.56789, 1234.56789 * (1 + 5e-9), 1e-12))
        # a real disagreement fails both
        self.assertFalse(rel_close(100.0, 100.5, 1e-6))
        # zero handling
        self.assertTrue(rel_close(0.0, 0.0, 1e-9))


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


class TestOIBarWindow(unittest.TestCase):
    def test_window_is_aligned_settled_and_the_right_size(self):
        # 2026-09-21 12:07:30 UTC
        now_ms = 1789992450000
        lo, hi = bar_window(now_ms, hours=1, settle_minutes=10)
        self.assertEqual(hi % BAR_MS, 0)
        self.assertEqual(lo % BAR_MS, 0)
        # 12:07:30 - 10m = 11:57:30 -> floor 11:55 -> the newest settled bar OPENS at 11:50
        self.assertEqual(format_ms_to_utc(hi), "2026-09-21 11:50:00")
        self.assertEqual((hi - lo) // BAR_MS + 1, 12)          # an hour of 5m bars
        self.assertLessEqual(hi + BAR_MS, now_ms - 10 * 60_000)  # its close is at least 10 minutes old

    def test_zero_settle_takes_the_last_closed_bar(self):
        now_ms = 1789992450000  # 12:07:30
        _, hi = bar_window(now_ms, hours=1, settle_minutes=0)
        self.assertEqual(format_ms_to_utc(hi), "2026-09-21 12:00:00")  # the 12:05 bar is still forming


def _oi_row(**over):
    row = {"symbol": "BTCUSDT", "n": 288, "min_ms": 0, "max_ms": 287 * BAR_MS, "off_grid": 0, "bad_value": 0,
           "zero_value": 0, "n_live": 0, "n_hist": 288, "n_archive": 0, "bad_rank": 0, "stale_live": 0,
           "bad_snap_live": 0, "bad_snap_cal": 0}
    row.update(over)
    return row


class TestOIEvaluateSymbol(unittest.TestCase):
    LO, HI = 0, 287 * BAR_MS

    def test_clean_series_passes(self):
        r = evaluate_symbol(_oi_row(), self.LO, self.HI)
        self.assertEqual((r["status"], r["missing"], r["lag_bars"]), ("PASS", 0, 0))

    def test_missing_bars_fail(self):
        r = evaluate_symbol(_oi_row(n=285), self.LO, self.HI)
        self.assertEqual(r["status"], "FAIL")
        self.assertEqual(r["missing"], 3)

    def test_off_grid_rows_fail_and_are_not_counted_as_bars(self):
        # 289 rows, 1 of them off the grid -> 288 on-grid rows: nothing missing, but still a FAIL
        r = evaluate_symbol(_oi_row(n=289, off_grid=1), self.LO, self.HI)
        self.assertEqual(r["status"], "FAIL")
        self.assertEqual(r["missing"], 0)

    def test_snap_time_violations_fail(self):
        self.assertEqual(evaluate_symbol(_oi_row(bad_snap_cal=2), self.LO, self.HI)["status"], "FAIL")
        self.assertEqual(evaluate_symbol(_oi_row(bad_snap_live=1), self.LO, self.HI)["status"], "FAIL")

    def test_stale_series_fails(self):
        r = evaluate_symbol(_oi_row(max_ms=287 * BAR_MS - 10 * BAR_MS), self.LO, self.HI, max_lag_bars=2)
        self.assertEqual(r["status"], "FAIL")
        self.assertEqual(r["lag_bars"], 10)

    def test_zero_value_is_only_a_warning(self):
        self.assertEqual(evaluate_symbol(_oi_row(zero_value=1), self.LO, self.HI)["status"], "WARN")

    def test_uncalibrated_live_rows_warn_unless_calibration_is_required(self):
        self.assertEqual(evaluate_symbol(_oi_row(stale_live=5), self.LO, self.HI)["status"], "WARN")
        self.assertEqual(evaluate_symbol(_oi_row(stale_live=5), self.LO, self.HI, require_calibration=True)["status"], "FAIL")

    def test_series_starting_late_warns(self):
        r = evaluate_symbol(_oi_row(n=248, min_ms=40 * BAR_MS), self.LO, self.HI)
        self.assertEqual(r["status"], "WARN")

    def test_bad_value_and_bad_rank_fail(self):
        self.assertEqual(evaluate_symbol(_oi_row(bad_value=1), self.LO, self.HI)["status"], "FAIL")
        self.assertEqual(evaluate_symbol(_oi_row(bad_rank=1), self.LO, self.HI)["status"], "FAIL")


class TestOICompareWithBinance(unittest.TestCase):
    LO, HI = 0, 100 * BAR_MS

    def test_a_label_is_stored_one_bar_earlier(self):
        rows = {10 * BAR_MS: {"oi": 100.0, "rank": 2}}
        # label T = 11 bars -> bar start = 10 bars
        r = compare_with_binance(rows, [(11 * BAR_MS, 100.0)], self.LO, self.HI)
        self.assertEqual((r["compared"], r["ok_cal"], r["missing"], r["mismatch"]), (1, 1, [], []))
        # the same value at the un-shifted position is reported missing, not silently accepted
        r = compare_with_binance(rows, [(10 * BAR_MS, 100.0)], self.LO, self.HI)
        self.assertEqual(r["missing"], [9 * BAR_MS])

    def test_calibrated_rows_must_match_exactly_live_rows_within_tolerance(self):
        rows = {10 * BAR_MS: {"oi": 100.0, "rank": 2}, 11 * BAR_MS: {"oi": 100.2, "rank": 1}}
        pts = [(11 * BAR_MS, 100.0001), (12 * BAR_MS, 100.0)]
        r = compare_with_binance(rows, pts, self.LO, self.HI, live_tol=0.005)
        self.assertEqual(r["ok_live"], 1)                      # 0.2% off but live -> within 0.5%
        self.assertEqual(len(r["mismatch"]), 1)                # hist row 1e-6 off -> must be exact
        self.assertEqual(r["mismatch"][0]["rank"], 2)

    def test_labels_outside_the_window_are_skipped(self):
        r = compare_with_binance({}, [(500 * BAR_MS, 1.0)], self.LO, self.HI)
        self.assertEqual((r["skipped"], r["compared"], r["missing"]), (1, 0, []))


class TestOICoverage(unittest.TestCase):
    def test_coverage_thresholds(self):
        self.assertEqual(coverage_status(288, 288, 0.995), (1.0, "PASS"))
        self.assertEqual(coverage_status(288, 287, 0.995)[1], "PASS")   # one bar of 288 is tolerated
        self.assertEqual(coverage_status(288, 285, 0.995)[1], "FAIL")
        self.assertEqual(coverage_status(0, 0, 0.995), (1.0, "PASS"))   # nothing to cover


M = bf.MINUTE_MS


class TestBackfillMissing1m(unittest.TestCase):
    def test_weight_table(self):
        self.assertEqual([bf.kline_weight(n) for n in (1, 99, 100, 499, 500, 1000, 1001, 1500)],
                         [1, 1, 2, 2, 5, 5, 10, 10])

    def test_merge_ranges(self):
        self.assertEqual(bf.merge_ranges([(5 * M, 6 * M), (1 * M, 2 * M), (2 * M, 3 * M), (10 * M, 11 * M)]),
                         [(1 * M, 3 * M), (5 * M, 6 * M), (10 * M, 11 * M)])

    def test_subtract_present(self):
        self.assertEqual(bf.subtract_present([(0, 5 * M)], [1 * M, 3 * M]),
                         [(0, 1 * M), (2 * M, 3 * M), (4 * M, 5 * M)])
        self.assertEqual(bf.subtract_present([(0, 2 * M)], [0, 1 * M]), [])

    def test_plan_requests_groups_close_gaps_and_splits_long_ones(self):
        # two 1-minute gaps 10 minutes apart cost 1+1 separately, 1 together -> one request
        self.assertEqual(bf.plan_requests([(0, 1 * M), (10 * M, 11 * M)]), [(0, 11 * M)])
        # far apart (span > 1500 min) stay separate
        far = 2000 * M
        self.assertEqual(bf.plan_requests([(0, 1 * M), (far, far + M)]), [(0, 1 * M), (far, far + M)])
        # a 3000-minute range is cut into pages of 1500
        self.assertEqual(bf.plan_requests([(0, 3000 * M)]), [(0, 1500 * M), (1500 * M, 3000 * M)])
        # merging must never cost more weight: 99+99 minutes (1+1) would become 200 (2) -> equal, allowed;
        # 90 + 90 with a 400-minute hole would become weight 2 vs 1+1 -> allowed; but 98,98 far apart 450 -> 5 > 2
        a, b = (0, 98 * M), (500 * M, 598 * M)
        self.assertEqual(bf.plan_requests([a, b]), [a, b])

    def test_kline_to_row_matches_go_mapping(self):
        k = [1789755240000, "1.5", "2", "1", "1.8", "10", 1789755299999, "18.5", 42, "4", "7.2", "0"]
        r = bf.kline_to_row("BTCUSDT", k)
        self.assertEqual(r["start_time"], "2026-09-18 18:14:00.000")
        self.assertEqual(r["end_time"], "2026-09-18 18:14:59.999")
        self.assertEqual((r["quote_volume"], r["trades_count"], r["taker_buy_volume"], r["taker_buy_quote_volume"]),
                         (18.5, 42, 4.0, 7.2))

    def test_read_gaps_csv_filters_and_merges(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as f:
            f.write("symbol,interval,from_ms,to_ms,from_utc,to_utc,missing_count\n")
            f.write(f"btcusdt,1m,{60 * M},{61 * M},x,x,1\n")
            f.write(f"BTCUSDT,1m,{61 * M},{63 * M},x,x,2\n")   # touches the previous one -> merged
            f.write(f"BTCUSDT,5m,{0},{300000},x,x,1\n")          # not 1m -> skipped
            f.write(f"币安人生USDT,1m,{60 * M},{61 * M},x,x,1\n")   # CJK symbol is valid
            f.write(f"BTC;DROP,1m,{60 * M},{61 * M},x,x,1\n")    # bad symbol -> skipped
            f.write(f"ETHUSDT,1m,{60 * M + 5},{61 * M},x,x,1\n") # misaligned -> skipped
            path = f.name
        try:
            gaps, warns = bf.read_gaps_csv(path)
        finally:
            os.unlink(path)
        self.assertEqual(gaps, {"BTCUSDT": [(60 * M, 63 * M)], "币安人生USDT": [(60 * M, 61 * M)]})
        self.assertEqual(len(warns), 3)

    def test_gaps_csv_roundtrip_from_integrity_checker(self):
        import tempfile
        g = {"from_ms": 60 * M, "to_ms": 62 * M, "missing_count": 2}
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            self.assertEqual(write_gaps_csv(path, [("BTCUSDT", "1m", g)]), 1)
            gaps, warns = bf.read_gaps_csv(path)
        finally:
            os.unlink(path)
        self.assertEqual(gaps, {"BTCUSDT": [(60 * M, 62 * M)]})
        self.assertEqual(warns, [])
        self.assertEqual(GAPS_CSV_HEADER[:4], ["symbol", "interval", "from_ms", "to_ms"])

    def test_rollup_hint(self):
        now = 100 * 86_400_000
        self.assertEqual(bf.rollup_hint(now - 2 * 86_400_000, now), [])
        self.assertEqual(bf.rollup_hint(now - 5 * 86_400_000, now), ["5m", "15m", "1h"])
        self.assertEqual(bf.rollup_hint(now - 20 * 86_400_000, now), ["5m", "15m", "1h", "4h", "1d"])

    def test_pacer_waits_when_budget_spent(self):
        t = [0.0]
        slept = []

        def sleep(d):
            slept.append(d)
            t[0] += d

        p = bf.WeightPacer(budget=10, soft_limit=1800, clock=lambda: t[0], sleep=sleep)
        p.before(6)
        p.before(4)          # exactly the budget: no wait
        self.assertEqual(slept, [])
        p.before(1)          # over budget: waits for the oldest event to leave the minute
        self.assertTrue(slept and slept[0] > 59)

    def test_run_inserts_only_missing_closed_wanted_bars(self):
        base = 1_000_000 * 60_000

        class FakeCH:
            def __init__(self):
                self.inserted = []
            def query(self, sql):
                return [{"t": base + 1 * M}]        # minute 1 is already present
            def insert_json_rows(self, table, cols, rows):
                self.inserted.extend(rows)
                return len(rows)

        class FakeFetch:
            def __init__(self):
                self.calls = []
            def fetch(self, sym, a, b):
                self.calls.append((sym, a, b))
                # exchange has minutes 0, 2 and an out-of-range extra (minute 5); minute 3 is empty
                mk = lambda m: [base + m * M, "1", "2", "1", "1", "1", base + m * M + 59999, "1", 1, "1", "1", "0"]
                return [mk(0), mk(2), mk(5)]

        ch, fe = FakeCH(), FakeFetch()
        now_ms = base + 60 * M
        st = bf.run(ch, "t", {"AAAUSDT": [(base, base + 4 * M)]}, fe, apply=True, now_ms=now_ms, out=lambda *_: None)
        self.assertEqual([r["start_time"] for r in ch.inserted],
                         [bf.fmt_ts(base), bf.fmt_ts(base + 2 * M)])
        self.assertEqual((st["fetched"], st["inserted"], st["empty_at_exchange"], st["already_present"]), (2, 2, 1, 1))

    def test_run_dry_run_writes_nothing(self):
        base = 1_000_000 * 60_000

        class FakeCH:
            def query(self, sql): return []
            def insert_json_rows(self, *a): raise AssertionError("dry run must not insert")

        class FakeFetch:
            def fetch(self, sym, a, b):
                return [[base, "1", "2", "1", "1", "1", base + 59999, "1", 1, "1", "1", "0"]]

        st = bf.run(FakeCH(), "t", {"AAAUSDT": [(base, base + M)]}, FakeFetch(), apply=False,
                    now_ms=base + 60 * M, out=lambda *_: None)
        self.assertEqual((st["fetched"], st["inserted"]), (1, 0))

    def test_run_skips_unclosed_minutes_and_isolates_failures(self):
        base = 1_000_000 * 60_000

        class FakeCH:
            def query(self, sql): return []
            def insert_json_rows(self, t, c, rows): return len(rows)

        class FakeFetch:
            def fetch(self, sym, a, b):
                if sym == "BADUSDT":
                    raise RuntimeError("boom")
                return []

        st = bf.run(FakeCH(), "t", {"BADUSDT": [(base, base + M)], "OKUSDT": [(base, base + M)]}, FakeFetch(),
                    apply=True, now_ms=base + 30_000, out=lambda *_: None)   # minute 0 not yet closed
        self.assertEqual(st["failed_symbols"], [])   # the open minute is dropped before any request
        st = bf.run(FakeCH(), "t", {"BADUSDT": [(base, base + M)], "OKUSDT": [(base, base + M)]}, FakeFetch(),
                    apply=True, now_ms=base + 5 * M, out=lambda *_: None)
        self.assertEqual(st["failed_symbols"], ["BADUSDT"])
        self.assertEqual(st["empty_at_exchange"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
