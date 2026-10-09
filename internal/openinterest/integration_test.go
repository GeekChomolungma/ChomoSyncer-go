package openinterest

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// scratchDB creates a scratch database from deploy/clickhouse/004_fapi_oi.sql and
// returns an admin connection, the database name and a ready CHConfig. It is dropped
// when the test ends. Skips the test unless CHOMO_TEST_CH_ADDR is set.
func scratchDB(t *testing.T) (admin clickhouse.Conn, db string, cfg CHConfig) {
	t.Helper()
	return scratchDBFrom(t, "004_fapi_oi.sql", "fapi_oi_5m")
}

// scratchDBFrom is scratchDB for any DDL file of deploy/clickhouse; cfg.Table is
// <scratch db>.<table>.
func scratchDBFrom(t *testing.T, ddlFile, table string) (admin clickhouse.Conn, db string, cfg CHConfig) {
	t.Helper()
	addr := os.Getenv("CHOMO_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set CHOMO_TEST_CH_ADDR to run the ClickHouse integration tests")
	}
	user := os.Getenv("CHOMO_TEST_CH_USER")
	if user == "" {
		user = "default"
	}
	pass := os.Getenv("CHOMO_TEST_CH_PASSWORD")
	db = fmt.Sprintf("oi_it_%d", time.Now().UnixNano())

	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default", Username: user, Password: pass}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// t.Cleanup runs last-registered first: drop the scratch database, then close the connection.
	t.Cleanup(func() { _ = admin.Close() })
	t.Cleanup(func() { _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+db) })

	ddl, err := os.ReadFile("../../deploy/clickhouse/" + ddlFile)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(ddl), "\n") { // strip comments: some contain ';'
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	script := regexp.MustCompile(`\bmarket\b`).ReplaceAllString(strings.Join(lines, "\n"), db)
	for _, stmt := range strings.Split(script, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("applying %s: %v\n%s", ddlFile, err, stmt)
		}
	}
	return admin, db, CHConfig{Addrs: []string{addr}, Database: db, Username: user, Password: pass, Table: db + "." + table}
}

// Integration test against a real ClickHouse (see scratchDB); nothing in the real
// `market` database is touched:
//
//	CHOMO_TEST_CH_ADDR=127.0.0.1:9000 CHOMO_TEST_CH_PASSWORD=... go test -run TestIntegration ./internal/openinterest
func TestIntegrationClickHouse(t *testing.T) {
	admin, db, cfg := scratchDB(t)
	ctx := context.Background()
	fl, err := NewCHFlusher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	store, err := NewCHStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	bar := ts("2026-09-21 12:00:00.000")
	rows := []Row{
		{Symbol: "AAA", StartTime: bar, SumOpenInterest: 100, SnapTime: bar.Add(BarInterval - 20*time.Second), SrcRank: RankLive},
		{Symbol: "AAA", StartTime: bar.Add(BarInterval), SumOpenInterest: 101, SnapTime: bar.Add(2*BarInterval - 20*time.Second), SrcRank: RankLive},
		{Symbol: "BBB", StartTime: bar, SumOpenInterest: 7, SnapTime: bar.Add(BarInterval), SrcRank: RankHist},
	}
	if err := fl.Flush(ctx, rows); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// hist (rank 2) for the same key must win over live (rank 1), whatever the insert order
	if err := fl.Flush(ctx, []Row{{Symbol: "AAA", StartTime: bar, SumOpenInterest: 100.25, SnapTime: bar.Add(BarInterval), SrcRank: RankHist}}); err != nil {
		t.Fatal(err)
	}
	if err := fl.Flush(ctx, []Row{{Symbol: "AAA", StartTime: bar, SumOpenInterest: 999, SnapTime: bar.Add(BarInterval), SrcRank: RankLive}}); err != nil {
		t.Fatal(err)
	}

	// 1. times survive the round trip unchanged (server timezone must not shift them)
	var gotStart, gotSnap time.Time
	var gotOI float64
	var gotRank uint8
	row := admin.QueryRow(ctx, fmt.Sprintf("SELECT start_time, snap_time, sum_open_interest, src_rank FROM %s.fapi_oi_5m FINAL WHERE symbol='AAA' AND start_time = ?", db), bar)
	if err := row.Scan(&gotStart, &gotSnap, &gotOI, &gotRank); err != nil {
		t.Fatal(err)
	}
	if !gotStart.Equal(bar) || !gotSnap.Equal(bar.Add(BarInterval)) {
		t.Fatalf("times shifted: start=%v (want %v) snap=%v (want %v)", gotStart, bar, gotSnap, bar.Add(BarInterval))
	}
	// 2. precedence
	if gotOI != 100.25 || gotRank != RankHist {
		t.Fatalf("FINAL row = (oi %v, rank %d), want the hist row (100.25, 2) to win over both live inserts", gotOI, gotRank)
	}
	// 3. epoch check independent of any timezone handling
	var epoch uint32
	if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT toUnixTimestamp(start_time) FROM %s.fapi_oi_5m FINAL WHERE symbol='BBB'", db)).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if int64(epoch) != bar.Unix() {
		t.Fatalf("stored epoch = %d, want %d", epoch, bar.Unix())
	}

	// 4. LastStarts: newest overall vs newest calibrated
	got, err := store.LastStarts(ctx, []string{"AAA", "BBB", "NONE"})
	if err != nil {
		t.Fatal(err)
	}
	a := got["AAA"]
	if !a.Any.Equal(bar.Add(BarInterval)) || !a.Hist.Equal(bar) {
		t.Fatalf("AAA LastStarts = %+v, want Any=%v (a live row) Hist=%v", a, bar.Add(BarInterval), bar)
	}
	if b := got["BBB"]; !b.Any.Equal(bar) || !b.Hist.Equal(bar) {
		t.Fatalf("BBB LastStarts = %+v", b)
	}
	if _, ok := got["NONE"]; ok {
		t.Fatal("a symbol without rows must be absent")
	}
	// live rows only -> Hist is the zero time, not 1970
	if err := fl.Flush(ctx, []Row{{Symbol: "LIVEONLY", StartTime: bar, SumOpenInterest: 1, SnapTime: bar.Add(BarInterval), SrcRank: RankLive}}); err != nil {
		t.Fatal(err)
	}
	got, err = store.LastStarts(ctx, []string{"LIVEONLY"})
	if err != nil {
		t.Fatal(err)
	}
	if lo := got["LIVEONLY"]; !lo.Hist.IsZero() || !lo.Any.Equal(bar) {
		t.Fatalf("LIVEONLY LastStarts = %+v, want Hist zero, Any=%v", lo, bar)
	}
}

// TestIntegrationLSRatioClickHouse: NULL cells survive the round trip, archive (3)
// wins over hist (2) whatever the insert order, LastStarts works on the ratio table.
func TestIntegrationLSRatioClickHouse(t *testing.T) {
	admin, db, cfg := scratchDBFrom(t, "007_fapi_ls_ratio.sql", "fapi_ls_ratio_5m")
	ctx := context.Background()
	fl, err := NewLSFlusher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	store, err := NewCHStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	bar := ts("2026-09-21 12:00:00.000")
	archive := newLSRow("AAA", bar, RankArchive)
	archive.Ratios = [numRatios]float64{1.25, 2.5, 3.75}
	hist := newLSRow("AAA", bar, RankHist)
	hist.Ratios[RatioTopAccount] = 9 // the other two stay NULL
	holed := newLSRow("BBB", bar.Add(BarInterval), RankHist)
	holed.Ratios[RatioGlobalAccount] = 0.5
	for _, batch := range [][]LSRow{{archive}, {hist, holed}} {
		if err := fl.Flush(ctx, batch); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	var c1, c2, c3 *float64
	var rank uint8
	var start, snap time.Time
	q := fmt.Sprintf("SELECT start_time, snap_time, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, src_rank FROM %s.fapi_ls_ratio_5m FINAL WHERE symbol = ?", db)
	if err := admin.QueryRow(ctx, q, "AAA").Scan(&start, &snap, &c1, &c2, &c3, &rank); err != nil {
		t.Fatal(err)
	}
	if rank != RankArchive || c1 == nil || *c1 != 1.25 || *c2 != 2.5 || *c3 != 3.75 || !start.Equal(bar) || !snap.Equal(bar.Add(BarInterval)) {
		t.Fatalf("AAA FINAL row: rank=%d start=%v snap=%v; the archive row must win over the later hist insert", rank, start, snap)
	}
	if err := admin.QueryRow(ctx, q, "BBB").Scan(&start, &snap, &c1, &c2, &c3, &rank); err != nil {
		t.Fatal(err)
	}
	if c1 != nil || c2 != nil || c3 == nil || *c3 != 0.5 {
		t.Fatalf("BBB: NULL cells must stay NULL: %v %v %v", c1, c2, c3)
	}

	got, err := store.LastStarts(ctx, []string{"AAA", "BBB", "NONE"})
	if err != nil {
		t.Fatal(err)
	}
	if !got["AAA"].Hist.Equal(bar) || !got["BBB"].Hist.Equal(bar.Add(BarInterval)) {
		t.Fatalf("LastStarts = %+v", got)
	}
	if _, ok := got["NONE"]; ok {
		t.Fatal("a symbol without rows must be absent")
	}
}
