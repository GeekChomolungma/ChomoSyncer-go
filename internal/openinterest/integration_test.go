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

	ddl, err := os.ReadFile("../../deploy/clickhouse/004_fapi_oi.sql")
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
			t.Fatalf("applying 004: %v\n%s", err, stmt)
		}
	}
	return admin, db, CHConfig{Addrs: []string{addr}, Database: db, Username: user, Password: pass, Table: db + ".fapi_oi_5m"}
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
