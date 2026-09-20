package openinterest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
)

// TestE2ERealBinance is a manual smoke test of the whole module against the REAL
// Binance REST API and a real ClickHouse scratch database. It exercises, in order:
//
//  1. cold start on an empty database  -> backfill from openInterestHist, checked
//     value-for-value against an independent request;
//  2. an immediate second pass          -> nothing is requested (already current);
//  3. a restart                         -> state re-read from ClickHouse, nothing to do;
//  4. a 3-hour "outage"                 -> exactly one small request per symbol;
//  5. a real live round at the next 5-minute boundary -> rows for the closing bar;
//  6. hist calibrating those live rows  -> the rows become src_rank 2, with the
//     live/hist deviation measured.
//
// It waits for a real boundary, so it takes 6-9 minutes:
//
//	CHOMO_TEST_E2E=1 CHOMO_TEST_CH_ADDR=127.0.0.1:9000 CHOMO_TEST_CH_PASSWORD=... \
//	  go test -run TestE2ERealBinance -v -timeout 20m ./internal/openinterest
func TestE2ERealBinance(t *testing.T) {
	if os.Getenv("CHOMO_TEST_E2E") == "" {
		t.Skip("set CHOMO_TEST_E2E=1 (and the CHOMO_TEST_CH_* variables) to run the real-Binance end-to-end test")
	}
	admin, db, chCfg := scratchDB(t)
	ctx := context.Background()
	syms := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	symsFn := func() []string { return syms }

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	gateReg := prometheus.NewRegistry()
	gate := weightgate.New(weightgate.Config{Registerer: gateReg})
	client := NewClient(ClientConfig{Gate: gate, Pool: NewDataPool(DataPoolConfig{Metrics: m}), Metrics: m})
	flusher, err := NewCHFlusher(chCfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewCHStore(chCfg)
	if err != nil {
		t.Fatal(err)
	}
	writer := NewWriter(WriterConfig{FlushInterval: 200 * time.Millisecond}, flusher, nil, m)
	t.Cleanup(func() { _ = writer.Close(); _ = store.Close() })
	cache := newLiveCache(24)

	histReqs := func() float64 { return testutil.ToFloat64(m.httpRequests.WithLabelValues("hist", "ok")) }
	dbCount := func(where string) uint64 {
		var n uint64
		q := fmt.Sprintf("SELECT count() FROM %s.fapi_oi_5m FINAL WHERE %s", db, where)
		if err := admin.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	flush := func() {
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// ---- 1. cold start ------------------------------------------------------
	rec := NewReconciler(HistConfig{}, client, store, writer, symsFn, cache, m, nil)
	if err := rec.LoadState(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := rec.Pass(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	flush()
	t.Logf("1. cold start: %+v", st)
	if st.ColdStart != 3 || st.Failed != 0 || st.Rows < 3*570 || !st.Committed {
		t.Fatalf("cold start stats unexpected: %+v (want 3 cold, 0 failed, ~576 rows each)", st)
	}
	if got := dbCount("src_rank = 2"); int(got) != st.Rows {
		t.Fatalf("rows in ClickHouse = %d, rows pushed = %d", got, st.Rows)
	}

	// independent check: the last 30 hist labels of BTCUSDT, straight from Binance, must be in the table one bar earlier
	raw := rawHist(t, "BTCUSDT", 30)
	for _, p := range raw {
		var oi float64
		start := p.label.Add(-BarInterval)
		q := fmt.Sprintf("SELECT sum_open_interest FROM %s.fapi_oi_5m FINAL WHERE symbol='BTCUSDT' AND start_time = ?", db)
		if err := admin.QueryRow(ctx, q, start).Scan(&oi); err != nil {
			t.Fatalf("label %v: no row at start %v: %v", p.label, start, err)
		}
		if oi != p.oi {
			t.Fatalf("label %v: table has %v, Binance says %v", p.label, oi, p.oi)
		}
	}
	t.Logf("   verified %d BTCUSDT labels value-for-value; each label T is stored at start_time T-5m", len(raw))

	// ---- 2. immediate second pass: nothing to ask for -----------------------
	before := histReqs()
	st2, _ := rec.Pass(ctx, 0)
	if st2.Skipped != 3 || histReqs() != before {
		t.Fatalf("second pass %+v made %v new requests; want all skipped", st2, histReqs()-before)
	}
	t.Logf("2. second pass: skipped=%d, requests made=%v", st2.Skipped, histReqs()-before)

	// ---- 3. restart: state comes back from ClickHouse ------------------------
	rec2 := NewReconciler(HistConfig{}, client, store, writer, symsFn, cache, m, nil)
	if err := rec2.LoadState(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range syms {
		if !rec2.last[s].Equal(rec.last[s]) || rec2.last[s].IsZero() {
			t.Fatalf("%s: state after restart = %v, before = %v", s, rec2.last[s], rec.last[s])
		}
	}
	before = histReqs()
	st3, _ := rec2.Pass(ctx, 0)
	if st3.Skipped != 3 || histReqs() != before {
		t.Fatalf("after a restart: %+v, %v requests; want nothing to fetch", st3, histReqs()-before)
	}
	t.Logf("3. restart: state re-read from ClickHouse, skipped=%d, requests made=%v", st3.Skipped, histReqs()-before)

	// ---- 4. a 3-hour outage: one small request per symbol -------------------
	rec3 := NewReconciler(HistConfig{}, client, store, writer, symsFn, cache, m, nil)
	for _, s := range syms {
		rec3.last[s] = rec.last[s].Add(-3 * time.Hour)
	}
	before = histReqs()
	st4, _ := rec3.Pass(ctx, 0)
	flush()
	if made := histReqs() - before; made != 3 || st4.Failed != 0 {
		t.Fatalf("3h gap: %v requests, stats %+v; want exactly one request per symbol", made, st4)
	}
	if int(st4.Rows) < 3*36 || int(st4.Rows) > 3*40 {
		t.Fatalf("3h gap rows = %d, want about 3 x 37", st4.Rows)
	}
	t.Logf("4. 3h outage: requests=%v rows=%d (one request per symbol)", histReqs()-before, st4.Rows)

	// ---- 5. a real live round ------------------------------------------------
	live := NewLive(LiveConfig{}, client, writer, symsFn, cache, m, nil)
	boundary, start := live.nextRound(time.Now())
	t.Logf("5. waiting until %s (boundary %s) for a real live round ...", start.Format("15:04:05"), boundary.Format("15:04:05"))
	time.Sleep(time.Until(start))
	rec.SetLiveSince(time.Now())
	lst := live.Cycle(ctx, boundary)
	flush()
	t.Logf("   live round: %+v", lst)
	if lst.OK != 3 {
		t.Fatalf("live round stats %+v, want all 3 symbols ok", lst)
	}
	bar := boundary.Add(-BarInterval)
	var liveRows uint64
	liveRows = dbCount(fmt.Sprintf("src_rank = 1 AND start_time = toDateTime64('%s', 3, 'UTC')", bar.Format("2006-01-02 15:04:05")))
	if liveRows != 3 {
		t.Fatalf("live rows for bar %v = %d, want 3", bar, liveRows)
	}
	var snap time.Time
	if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT snap_time FROM %s.fapi_oi_5m FINAL WHERE symbol='BTCUSDT' AND start_time = ?", db), bar).Scan(&snap); err != nil {
		t.Fatal(err)
	}
	t.Logf("   BTCUSDT live snap_time = %s, i.e. %v before the bar's close", snap.Format("15:04:05.000"), boundary.Sub(snap).Round(time.Millisecond))
	if d := boundary.Sub(snap); d < 0 || d > 45*time.Second {
		t.Fatalf("live snapshot was taken %v before the close; expected within the %v lead", d, 30*time.Second)
	}

	// ---- 6. hist calibrates the live rows ------------------------------------
	wait := time.Until(boundary.Add(3*time.Minute + 30*time.Second)) // labels are readable within ~3 minutes
	t.Logf("6. waiting %v for hist to publish label %s ...", wait.Round(time.Second), boundary.Format("15:04:05"))
	time.Sleep(wait)
	rec4 := NewReconciler(HistConfig{}, client, store, writer, symsFn, cache, m, nil)
	rec4.SetLiveSince(start)
	for _, s := range syms {
		rec4.last[s] = bar.Add(-2 * BarInterval) // as if calibrated up to two bars before
	}
	st6, err := rec4.Pass(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	flush()
	t.Logf("   calibration pass: %+v", st6)
	if n := dbCount(fmt.Sprintf("src_rank = 1 AND start_time = toDateTime64('%s', 3, 'UTC')", bar.Format("2006-01-02 15:04:05"))); n != 0 {
		t.Fatalf("%d live rows for bar %v survived the calibration", n, bar)
	}
	if n := dbCount(fmt.Sprintf("src_rank = 2 AND start_time = toDateTime64('%s', 3, 'UTC')", bar.Format("2006-01-02 15:04:05"))); n != 3 {
		t.Fatalf("hist rows for bar %v = %d, want 3 (each live row replaced by its hist row)", bar, n)
	}
	cnt := histogramCount(t, reg, "oi_live_vs_hist_rel_diff")
	if cnt < 3 {
		t.Fatalf("live-vs-hist comparisons = %d, want >= 3", cnt)
	}
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "oi_live_vs_hist_rel_diff" {
			h := mf.GetMetric()[0].GetHistogram()
			mean := h.GetSampleSum() / float64(h.GetSampleCount())
			t.Logf("   live vs hist: %d comparisons, mean |live-hist|/hist = %.5f%%", h.GetSampleCount(), mean*100)
			if mean > 0.005 {
				t.Fatalf("live and hist disagree by %.3f%% on average; expected far less than 0.5%%", mean*100)
			}
		}
	}
	t.Logf("done. weightgate used weight (1m) reported by Binance: %v", func() float64 {
		mfs, _ := gateReg.Gather()
		for _, mf := range mfs {
			if mf.GetName() == "weightgate_used_weight_1m" {
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return -1
	}())
}

type rawPoint struct {
	label time.Time
	oi    float64
}

// rawHist fetches the newest n hist points of a symbol with plain net/http, so it
// is independent of the module's Client.
func rawHist(t *testing.T, symbol string, n int) []rawPoint {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("https://fapi.binance.com/futures/data/openInterestHist?symbol=%s&period=5m&limit=%d", symbol, n))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var items []struct {
		SumOpenInterest string `json:"sumOpenInterest"`
		Timestamp       int64  `json:"timestamp"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("decode: %v: %.200s", err, body)
	}
	var out []rawPoint
	for _, it := range items {
		v, _ := strconv.ParseFloat(it.SumOpenInterest, 64)
		out = append(out, rawPoint{time.UnixMilli(it.Timestamp).UTC(), v})
	}
	return out
}
