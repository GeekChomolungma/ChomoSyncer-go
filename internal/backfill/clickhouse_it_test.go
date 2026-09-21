package backfill

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Integration test for CHStore.FindGaps against a real ClickHouse, in a scratch
// database that is dropped afterwards (nothing in `market` is touched):
//
//	CHOMO_TEST_CH_ADDR=127.0.0.1:9000 CHOMO_TEST_CH_PASSWORD=... go test -run FindGaps ./internal/backfill
func TestCHStoreFindGaps(t *testing.T) {
	addr := os.Getenv("CHOMO_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set CHOMO_TEST_CH_ADDR to run the ClickHouse integration tests")
	}
	user := os.Getenv("CHOMO_TEST_CH_USER")
	if user == "" {
		user = "default"
	}
	pass := os.Getenv("CHOMO_TEST_CH_PASSWORD")
	db := fmt.Sprintf("bf_it_%d", time.Now().UnixNano())
	ctx := context.Background()

	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default", Username: user, Password: pass}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	t.Cleanup(func() { _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+db) })
	for _, q := range []string{
		"CREATE DATABASE " + db,
		`CREATE TABLE ` + db + `.fapi_kline_1m (symbol LowCardinality(String), start_time DateTime64(3,'UTC'),
		   end_time DateTime64(3,'UTC'), open Float64, high Float64, low Float64, close Float64, volume Float64,
		   quote_volume Float64, taker_buy_volume Float64, taker_buy_quote_volume Float64, trades_count UInt32,
		   created_at DateTime DEFAULT now()) ENGINE = ReplacingMergeTree(created_at)
		   PARTITION BY toYYYYMM(start_time) ORDER BY (symbol, start_time)`,
	} {
		if err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	base := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	// AAA: minutes 0..9 except 3 and 6,7 (two holes; duplicate row for minute 4)
	// BBB: minutes 0..4 complete; CCC: starts at minute 5 (late listing) with a hole at 8
	present := map[string][]int{
		"AAA": {0, 1, 2, 4, 4, 5, 8, 9},
		"BBB": {0, 1, 2, 3, 4},
		"CCC": {5, 6, 7, 9},
	}
	for sym, mins := range present {
		for _, m := range mins {
			ts := base.Add(time.Duration(m) * time.Minute)
			if err := admin.Exec(ctx, fmt.Sprintf(
				`INSERT INTO %s.fapi_kline_1m (symbol,start_time,end_time,open,high,low,close,volume,quote_volume,taker_buy_volume,taker_buy_quote_volume,trades_count)
				 VALUES ('%s', fromUnixTimestamp64Milli(%d), fromUnixTimestamp64Milli(%d), 1,1,1,1,1,1,1,1,1)`,
				db, sym, ts.UnixMilli(), ts.UnixMilli()+59999)); err != nil {
				t.Fatal(err)
			}
		}
	}

	st, err := NewCHStore(CHStoreConfig{Addrs: []string{addr}, Database: db, Username: user, Password: pass, TablePrefix: db + ".fapi_kline"})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	got, err := st.FindGaps(ctx, "1m", base, base.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	want := map[string][]Range{
		"AAA": {{min(3), min(4)}, {min(6), min(8)}},
		"CCC": {{min(8), min(9)}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for sym, w := range want {
		g := got[sym]
		if len(g) != len(w) {
			t.Fatalf("%s: got %v want %v", sym, g, w)
		}
		for i := range w {
			if !g[i].From.Equal(w[i].From) || !g[i].To.Equal(w[i].To) {
				t.Fatalf("%s[%d]: got %v want %v", sym, i, g[i], w[i])
			}
		}
	}
}
