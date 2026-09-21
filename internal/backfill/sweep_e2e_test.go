package backfill

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
)

// direct archive writer: inserts each pushed row straight into the scratch table.
type directArchive struct {
	conn  driver.Conn
	table string
}

func (d *directArchive) Push(ctx context.Context, r chwriter.Row) error {
	return d.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (symbol,start_time,end_time,open,high,low,close,volume,quote_volume,
		taker_buy_volume,taker_buy_quote_volume,trades_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, d.table),
		r.Symbol, r.StartTime, r.EndTime, r.Open, r.High, r.Low, r.Close, r.Volume, r.QuoteVolume,
		r.TakerBuyVolume, r.TakerBuyQuoteVolume, r.TradesCount)
}
func (d *directArchive) Flush(context.Context) error { return nil }

// End-to-end: real Binance REST + real ClickHouse (scratch database, dropped after).
// Real bars are loaded, minutes are punched out, and one sweep must put back exactly
// those bars, identical to what Binance serves.
//
//	CHOMO_TEST_BINANCE=1 CHOMO_TEST_CH_ADDR=127.0.0.1:9000 CHOMO_TEST_CH_PASSWORD=... go test -run SweepE2E -v ./internal/backfill
func TestSweepE2EAgainstBinance(t *testing.T) {
	addr := os.Getenv("CHOMO_TEST_CH_ADDR")
	if addr == "" || os.Getenv("CHOMO_TEST_BINANCE") == "" {
		t.Skip("set CHOMO_TEST_CH_ADDR and CHOMO_TEST_BINANCE=1 to run the sweep E2E")
	}
	user := os.Getenv("CHOMO_TEST_CH_USER")
	if user == "" {
		user = "default"
	}
	pass := os.Getenv("CHOMO_TEST_CH_PASSWORD")
	db := fmt.Sprintf("bf_e2e_%d", time.Now().UnixNano())
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
	table := db + ".fapi_kline_1m"

	fetcher := NewBinanceFetcher(FetcherConfig{BaseURL: "https://fapi.binance.com", Registerer: prometheus.NewRegistry()})
	now := time.Now().UTC()
	to := now.Add(-10 * time.Minute).Truncate(time.Minute)
	from := to.Add(-3 * time.Hour)

	truth := map[string]map[int64]chwriter.Row{}
	punched := map[string][]time.Time{
		"BTCUSDT": {from.Add(30 * time.Minute), from.Add(90 * time.Minute), from.Add(91 * time.Minute), from.Add(92 * time.Minute)},
		"ETHUSDT": {from.Add(45 * time.Minute)},
	}
	da := &directArchive{conn: admin, table: table}
	for sym, drop := range punched {
		rows, err := fetcher.Fetch(ctx, sym, "1m", from, to)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) < 170 {
			t.Fatalf("%s: only %d bars from Binance for a 3h window", sym, len(rows))
		}
		truth[sym] = map[int64]chwriter.Row{}
		skip := map[int64]bool{}
		for _, d := range drop {
			skip[d.UnixMilli()] = true
		}
		for _, r := range rows {
			truth[sym][r.StartTime.UnixMilli()] = r
			if !skip[r.StartTime.UnixMilli()] {
				if err := da.Push(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	st, err := NewCHStore(CHStoreConfig{Addrs: []string{addr}, Database: db, Username: user, Password: pass, TablePrefix: db + ".fapi_kline"})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	bf, err := New(Config{Registerer: prometheus.NewRegistry(), GateTimeout: time.Minute},
		fetcher, map[string]ArchiveWriter{"1m": da}, st, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	bfCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	bf.Start(bfCtx)
	defer bf.Close()

	before, err := st.FindGaps(ctx, "1m", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(before["BTCUSDT"]) != 2 || len(before["ETHUSDT"]) != 1 {
		t.Fatalf("setup: gaps before = %v", before)
	}

	sw, err := NewSweeper(SweepConfig{Window: 4 * time.Hour, Settle: 5 * time.Minute, Registerer: prometheus.NewRegistry()}, st, bf)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := sw.SweepOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("sweep stats: %+v", stats)
	if stats.Repaired != 5 || stats.Empty != 0 || stats.Failed != 0 {
		t.Fatalf("stats = %+v, want 5 repaired", stats)
	}

	after, err := st.FindGaps(ctx, "1m", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("gaps remain after sweep: %v", after)
	}
	// every repaired bar must equal what Binance serves
	rows, err := admin.Query(ctx, fmt.Sprintf(`SELECT symbol, toUnixTimestamp64Milli(start_time), open, high, low, close, volume,
		quote_volume, taker_buy_volume, taker_buy_quote_volume, trades_count FROM %s FINAL WHERE start_time >= ? AND start_time < ?`, table), from, to)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var sym string
		var ms int64
		var o, h, l, c, v, q, tb, tbq float64
		var tc uint32
		if err := rows.Scan(&sym, &ms, &o, &h, &l, &c, &v, &q, &tb, &tbq, &tc); err != nil {
			t.Fatal(err)
		}
		w, ok := truth[sym][ms]
		if !ok || w.Open != o || w.High != h || w.Low != l || w.Close != c || w.Volume != v || w.QuoteVolume != q ||
			w.TakerBuyVolume != tb || w.TakerBuyQuoteVolume != tbq || w.TradesCount != tc {
			t.Fatalf("%s %d differs from Binance: got o=%v c=%v v=%v tc=%d want %+v", sym, ms, o, c, v, tc, w)
		}
		n++
	}
	if want := len(truth["BTCUSDT"]) + len(truth["ETHUSDT"]); n != want {
		t.Fatalf("rows after sweep = %d, want %d", n, want)
	}

	// a second sweep finds nothing to do
	stats, _ = sw.SweepOnce(ctx)
	if stats.Found != 0 {
		t.Fatalf("second sweep found %d holes", stats.Found)
	}
}
