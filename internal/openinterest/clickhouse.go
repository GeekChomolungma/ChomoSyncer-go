package openinterest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultTable is the raw 5-minute open-interest table (deploy/clickhouse/004_fapi_oi.sql).
const DefaultTable = "market.fapi_oi_5m"

// CHConfig is the ClickHouse native-protocol connection used by the flusher and
// the store.
type CHConfig struct {
	Addrs       []string
	Database    string
	Username    string
	Password    string
	Table       string        // default DefaultTable
	DialTimeout time.Duration // default 5s
	TLS         bool
}

func (c CHConfig) table() string {
	if c.Table == "" {
		return DefaultTable
	}
	return c.Table
}

func (c CHConfig) options() (*clickhouse.Options, error) {
	if len(c.Addrs) == 0 {
		return nil, errors.New("openinterest: at least one ClickHouse address is required")
	}
	dial := c.DialTimeout
	if dial <= 0 {
		dial = 5 * time.Second
	}
	opts := &clickhouse.Options{
		Addr:        c.Addrs,
		Auth:        clickhouse.Auth{Database: c.Database, Username: c.Username, Password: c.Password},
		DialTimeout: dial,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	}
	if c.TLS {
		opts.TLS = &tls.Config{}
	}
	return opts, nil
}

// chConn is a lazily (re)connecting native connection: it pings before use and
// redials when the ping fails, and callers invalidate it after a send error.
type chConn struct {
	opts *clickhouse.Options
	mu   sync.Mutex
	conn driver.Conn
}

func (c *chConn) get(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		if err := c.conn.Ping(ctx); err == nil {
			return c.conn, nil
		}
		_ = c.conn.Close()
		c.conn = nil
	}
	conn, err := clickhouse.Open(c.opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	c.conn = conn
	return conn, nil
}

func (c *chConn) invalidate() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}

func (c *chConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// CHFlusher is the default Flusher: one native columnar INSERT per batch.
type CHFlusher struct {
	chConn
	table string
}

// NewCHFlusher builds a flusher. It does not connect until the first flush, so the
// service still starts when ClickHouse is briefly unavailable at boot.
func NewCHFlusher(cfg CHConfig) (*CHFlusher, error) {
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}
	return &CHFlusher{chConn: chConn{opts: opts}, table: cfg.table()}, nil
}

// Flush implements Flusher.
func (f *CHFlusher) Flush(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := f.get(ctx)
	if err != nil {
		return fmt.Errorf("acquire clickhouse connection: %w", err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+f.table+" "+insertColumns)
	if err != nil {
		f.invalidate()
		return fmt.Errorf("prepare batch: %w", err)
	}
	for i := range rows {
		if err := batch.Append(rows[i].appendArgs()...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %d/%d: %w", i, len(rows), err)
		}
	}
	if err := batch.Send(); err != nil {
		f.invalidate()
		return fmt.Errorf("send batch of %d rows: %w", len(rows), err)
	}
	return nil
}

// LastStarts is what the database already holds for one symbol. A zero time means
// "no such rows".
type LastStarts struct {
	// Any is the newest start_time of any source (live rows included).
	Any time.Time
	// Hist is the newest start_time written by hist or the archive (src_rank >= 2),
	// i.e. how far the series has been calibrated. Live rows after it still await
	// calibration.
	Hist time.Time
}

// Store reads what the database already holds; it drives the start-up decision of
// whether hist has to backfill a symbol.
type Store interface {
	LastStarts(ctx context.Context, symbols []string) (map[string]LastStarts, error)
	Close() error
}

// CHStore is the ClickHouse-backed Store.
type CHStore struct {
	chConn
	table string
}

// NewCHStore builds a store; it connects on first use.
func NewCHStore(cfg CHConfig) (*CHStore, error) {
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}
	return &CHStore{chConn: chConn{opts: opts}, table: cfg.table()}, nil
}

// LastStarts implements Store: the newest start_time overall and the newest one
// with src_rank >= 2, per symbol. Symbols without rows are absent from the result.
func (s *CHStore) LastStarts(ctx context.Context, symbols []string) (map[string]LastStarts, error) {
	out := make(map[string]LastStarts, len(symbols))
	if len(symbols) == 0 {
		return out, nil
	}
	conn, err := s.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("openinterest: LastStarts: %w", err)
	}
	q := fmt.Sprintf("SELECT symbol, max(start_time), maxIf(start_time, src_rank >= 2) FROM %s WHERE symbol IN (?) GROUP BY symbol", s.table)
	rows, err := conn.Query(ctx, q, symbols)
	if err != nil {
		s.invalidate()
		return nil, fmt.Errorf("openinterest: LastStarts query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sym string
		var anyT, histT time.Time
		if err := rows.Scan(&sym, &anyT, &histT); err != nil {
			return nil, err
		}
		out[sym] = LastStarts{Any: nonEpoch(anyT), Hist: nonEpoch(histT)}
	}
	return out, rows.Err()
}

// nonEpoch maps ClickHouse's "no value" (the Unix epoch, what maxIf returns when
// nothing matched) to the zero time.
func nonEpoch(t time.Time) time.Time {
	if t.Unix() <= 0 {
		return time.Time{}
	}
	return t.UTC()
}
