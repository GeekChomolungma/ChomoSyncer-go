package chwriter

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// clickHouseFlusher is the default Flusher. It owns a single native-protocol
// connection and transparently reconnects: before every flush it pings the
// connection and redials if the ping fails, and it drops the connection on any
// prepare/send error so the next flush starts fresh. Retry/backoff around the
// flush itself is handled one level up by BatchWriter.flushWithRetry.
type clickHouseFlusher struct {
	opts  *clickhouse.Options
	table string

	mu   sync.Mutex
	conn driver.Conn
}

func newClickHouseFlusher(cfg Config) (*clickHouseFlusher, error) {
	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("chwriter: at least one ClickHouse address is required")
	}
	opts := &clickhouse.Options{
		Addr: cfg.Addrs,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: cfg.dialTimeout(),
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	}
	if cfg.TLS {
		opts.TLS = &tls.Config{}
	}

	f := &clickHouseFlusher{opts: opts, table: cfg.table()}
	// Eager connect; ignore the error so the writer still starts when
	// ClickHouse is briefly unavailable at boot.
	if conn, err := f.dial(context.Background()); err == nil {
		f.conn = conn
	}
	return f, nil
}

func (f *clickHouseFlusher) dial(ctx context.Context) (driver.Conn, error) {
	conn, err := clickhouse.Open(f.opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

func (f *clickHouseFlusher) getConn(ctx context.Context) (driver.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		if err := f.conn.Ping(ctx); err == nil {
			return f.conn, nil
		}
		_ = f.conn.Close()
		f.conn = nil
	}
	conn, err := f.dial(ctx)
	if err != nil {
		return nil, err
	}
	f.conn = conn
	return conn, nil
}

func (f *clickHouseFlusher) invalidate() {
	f.mu.Lock()
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}
	f.mu.Unlock()
}

// Flush appends every row to a native columnar block and sends it in one shot.
func (f *clickHouseFlusher) Flush(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := f.getConn(ctx)
	if err != nil {
		return fmt.Errorf("acquire clickhouse connection: %w", err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+f.table)
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

// Close closes the underlying connection. Implements io.Closer so BatchWriter.Close
// tears it down.
func (f *clickHouseFlusher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		return nil
	}
	err := f.conn.Close()
	f.conn = nil
	return err
}
