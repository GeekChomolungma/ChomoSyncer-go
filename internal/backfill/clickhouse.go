package backfill

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
)

// CHStoreConfig configures the ClickHouse read-back connection (independent of
// the chwriter write connection).
type CHStoreConfig struct {
	Addrs       []string
	Database    string
	Username    string
	Password    string
	TablePrefix string // "<prefix>_<interval>", e.g. market.fapi_kline
	DialTimeout time.Duration
	TLS         bool
}

// CHStore implements backfill.KlineStore over the market.fapi_kline_<interval> tables.
type CHStore struct {
	conn   driver.Conn
	prefix string
}

// NewCHStore opens a read connection. The dial is lazy; queries reconnect.
func NewCHStore(cfg CHStoreConfig) (*CHStore, error) {
	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("backfill: at least one ClickHouse address is required")
	}
	dt := cfg.DialTimeout
	if dt <= 0 {
		dt = 5 * time.Second
	}
	opts := &clickhouse.Options{
		Addr: cfg.Addrs,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: dt,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	}
	if cfg.TLS {
		opts.TLS = &tls.Config{}
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("backfill: open clickhouse: %w", err)
	}
	prefix := cfg.TablePrefix
	if prefix == "" {
		prefix = "market.fapi_kline"
	}
	return &CHStore{conn: conn, prefix: prefix}, nil
}

// Close releases the connection.
func (s *CHStore) Close() error { return s.conn.Close() }

func (s *CHStore) table(interval string) string { return s.prefix + "_" + interval }

// MaxStartTime returns each symbol's newest start_time (absent from the map when
// the symbol has no rows).
func (s *CHStore) MaxStartTime(ctx context.Context, interval string, symbols []string) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(symbols))
	if len(symbols) == 0 {
		return out, nil
	}
	q := fmt.Sprintf("SELECT symbol, max(start_time) FROM %s WHERE symbol IN (?) GROUP BY symbol", s.table(interval))
	rows, err := s.conn.Query(ctx, q, symbols)
	if err != nil {
		return nil, fmt.Errorf("backfill: MaxStartTime %s: %w", interval, err)
	}
	defer rows.Close()
	for rows.Next() {
		var sym string
		var mx time.Time
		if err := rows.Scan(&sym, &mx); err != nil {
			return nil, err
		}
		out[sym] = mx
	}
	return out, rows.Err()
}

// LastBars returns up to n most-recent deduped bars per symbol, ascending by
// start_time.
func (s *CHStore) LastBars(ctx context.Context, interval string, symbols []string, n int) (map[string][]chwriter.Row, error) {
	out := make(map[string][]chwriter.Row, len(symbols))
	if len(symbols) == 0 || n <= 0 {
		return out, nil
	}
	q := fmt.Sprintf(`
SELECT symbol, start_time, end_time, open, high, low, close,
       volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, trades_count
FROM %s FINAL
WHERE symbol IN (?)
ORDER BY symbol ASC, start_time DESC
LIMIT %d BY symbol`, s.table(interval), n)

	rows, err := s.conn.Query(ctx, q, symbols)
	if err != nil {
		return nil, fmt.Errorf("backfill: LastBars %s: %w", interval, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			r  chwriter.Row
			tc uint32
		)
		if err := rows.Scan(&r.Symbol, &r.StartTime, &r.EndTime,
			&r.Open, &r.High, &r.Low, &r.Close,
			&r.Volume, &r.QuoteVolume, &r.TakerBuyVolume, &r.TakerBuyQuoteVolume, &tc); err != nil {
			return nil, err
		}
		r.TradesCount = tc
		out[r.Symbol] = append(out[r.Symbol], r) // arriving DESC
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for sym, rs := range out { // reverse to ascending
		for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
			rs[i], rs[j] = rs[j], rs[i]
		}
		out[sym] = rs
	}
	return out, nil
}

// FindGaps returns, per symbol, the interior holes in [from, to): stretches of
// missing bar-open times lying strictly between two bars that exist. Each Range is
// [prevBar+interval, nextBar). Holes before a symbol's first bar or after its last
// bar in the window are not reported: the former may just be a later listing, the
// latter is the tail that resume-mode backfill already covers.
func (s *CHStore) FindGaps(ctx context.Context, interval string, from, to time.Time) (map[string][]Range, error) {
	dur, ok := parseIntervalDuration(interval)
	if !ok {
		return nil, fmt.Errorf("backfill: FindGaps: unknown interval %q", interval)
	}
	secs := int64(dur / time.Second)
	// FINAL: the table is a ReplacingMergeTree; un-merged duplicates would show as
	// zero-width "gaps" of size 0 and are harmless, but a stale parts view is not.
	q := fmt.Sprintf(`
SELECT symbol, prev_start, start_time
FROM (
    SELECT symbol, start_time,
           lagInFrame(start_time) OVER (PARTITION BY symbol ORDER BY start_time
                                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev_start,
           row_number() OVER (PARTITION BY symbol ORDER BY start_time) AS rn
    FROM %s FINAL
    WHERE start_time >= ? AND start_time < ?
)
WHERE rn > 1 AND toUnixTimestamp(start_time) - toUnixTimestamp(prev_start) > %d
ORDER BY symbol, start_time`, s.table(interval), secs)
	rows, err := s.conn.Query(ctx, q, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("backfill: FindGaps %s: %w", interval, err)
	}
	defer rows.Close()
	out := map[string][]Range{}
	for rows.Next() {
		var sym string
		var prev, next time.Time
		if err := rows.Scan(&sym, &prev, &next); err != nil {
			return nil, err
		}
		out[sym] = append(out[sym], Range{From: prev.Add(dur), To: next})
	}
	return out, rows.Err()
}
