// Package chwriter implements a thread-safe, buffered batch writer for the
// append-only ClickHouse time-series store described in the design doc
// (section 3.1). It never issues single-row INSERTs: rows are accumulated in
// memory and flushed as native columnar blocks when either the batch size or
// the flush interval threshold is reached.
package chwriter

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Defaults derived from the design doc (section 3.1 "写入技术要求").
const (
	// DefaultBatchSize is the row count that forces a flush.
	DefaultBatchSize = 5000
	// DefaultFlushInterval is the maximum time rows may sit in the buffer.
	DefaultFlushInterval = 1000 * time.Millisecond
	// DefaultChannelSize is the capacity of the ingest channel. It acts as
	// the "ring buffer" backlog; the design doc alerts once in-memory backlog
	// exceeds 20,000 rows, so that is the natural capacity.
	DefaultChannelSize = 20000

	DefaultMaxRetries      = 5
	DefaultRetryBackoff    = 200 * time.Millisecond
	DefaultMaxRetryBackoff = 5 * time.Second
	DefaultShutdownTimeout = 15 * time.Second

	// DefaultTable is the target for the 1m kline stream.
	DefaultTable = "market.fapi_kline_1m"
)

// Config configures a BatchWriter and, when the default flusher is used, the
// underlying ClickHouse connection.
type Config struct {
	// --- ClickHouse connection (only used by New / the default flusher) ---

	// Addrs is the list of ClickHouse native-protocol endpoints, e.g.
	// []string{"clickhouse-0:9000", "clickhouse-1:9000"}.
	Addrs []string
	// Database, Username, Password are the native-protocol credentials.
	Database string
	Username string
	Password string
	// Table is the fully qualified insert target, e.g. "market.fapi_kline_1m".
	// Defaults to DefaultTable when empty.
	Table string
	// DialTimeout bounds a single connection attempt. Defaults to 5s.
	DialTimeout time.Duration
	// TLS enables TLS on the native connection.
	TLS bool

	// --- Buffering / flushing behaviour ---

	// BatchSize forces a flush once this many rows are buffered.
	// Defaults to DefaultBatchSize.
	BatchSize int
	// FlushInterval forces a flush this long after the previous one even if
	// BatchSize has not been reached. Defaults to DefaultFlushInterval.
	FlushInterval time.Duration
	// ChannelSize is the ingest channel capacity. Defaults to DefaultChannelSize.
	ChannelSize int
	// MaxRetries is how many times a failed flush is retried before the batch
	// is dropped (and counted in clickhouse_rows_dropped_total).
	MaxRetries int
	// RetryBackoff is the initial retry delay; it doubles (with jitter) up to
	// MaxRetryBackoff.
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// ShutdownTimeout bounds the final flush performed during Close / context
	// cancellation. Defaults to DefaultShutdownTimeout.
	ShutdownTimeout time.Duration

	// --- Observability ---

	// Registerer receives the writer's Prometheus metrics. When nil a private
	// registry is used so metrics still function but are not exposed.
	Registerer prometheus.Registerer
	// Logger is used for operational logging. Defaults to slog.Default().
	Logger *slog.Logger
}

// resolvedConfig holds the subset of Config the buffering engine needs, with
// all defaults applied.
type resolvedConfig struct {
	batchSize       int
	flushInterval   time.Duration
	channelSize     int
	maxRetries      int
	retryBackoff    time.Duration
	maxRetryBackoff time.Duration
	shutdownTimeout time.Duration
}

func (c Config) resolve() resolvedConfig {
	rc := resolvedConfig{
		batchSize:       c.BatchSize,
		flushInterval:   c.FlushInterval,
		channelSize:     c.ChannelSize,
		maxRetries:      c.MaxRetries,
		retryBackoff:    c.RetryBackoff,
		maxRetryBackoff: c.MaxRetryBackoff,
		shutdownTimeout: c.ShutdownTimeout,
	}
	if rc.batchSize <= 0 {
		rc.batchSize = DefaultBatchSize
	}
	if rc.flushInterval <= 0 {
		rc.flushInterval = DefaultFlushInterval
	}
	if rc.channelSize <= 0 {
		rc.channelSize = DefaultChannelSize
	}
	if rc.maxRetries < 0 {
		rc.maxRetries = 0
	} else if rc.maxRetries == 0 {
		rc.maxRetries = DefaultMaxRetries
	}
	if rc.retryBackoff <= 0 {
		rc.retryBackoff = DefaultRetryBackoff
	}
	if rc.maxRetryBackoff <= 0 {
		rc.maxRetryBackoff = DefaultMaxRetryBackoff
	}
	if rc.maxRetryBackoff < rc.retryBackoff {
		rc.maxRetryBackoff = rc.retryBackoff
	}
	if rc.shutdownTimeout <= 0 {
		rc.shutdownTimeout = DefaultShutdownTimeout
	}
	return rc
}

func (c Config) table() string {
	if c.Table == "" {
		return DefaultTable
	}
	return c.Table
}

func (c Config) dialTimeout() time.Duration {
	if c.DialTimeout <= 0 {
		return 5 * time.Second
	}
	return c.DialTimeout
}
