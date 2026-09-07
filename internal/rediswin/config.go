// Package rediswin implements the Redis online sliding-window model from the
// design doc (section 3.2): every closed bar is LPUSH-ed onto a per-symbol List
// that is immediately LTRIM-med back to a fixed 200 entries, and a
// cross-section-ready event is XADD-ed to a notification Stream (section 4.1)
// for the Python strategy engine to consume.
package rediswin

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// Defaults from the design doc (sections 3.2 and 4.1).
const (
	// DefaultKeyPrefix is the namespace for window keys: <prefix>:<symbol>:<interval>.
	DefaultKeyPrefix = "kline"
	// DefaultWindowSize is the fixed List length maintained by LTRIM.
	DefaultWindowSize = 200
	// DefaultStreamKey is the cross-section notification stream.
	DefaultStreamKey = "stream:market:kline_ready"
	// DefaultStreamMaxLen caps the notification stream via approximate MAXLEN.
	DefaultStreamMaxLen = 10000
)

// Config configures a Writer.
type Config struct {
	// KeyPrefix overrides the "kline" namespace. Defaults to DefaultKeyPrefix.
	KeyPrefix string
	// WindowSize is the number of bars kept per symbol. Defaults to
	// DefaultWindowSize (200). LTRIM is issued as "0 WindowSize-1".
	WindowSize int
	// Atomic, when true, wraps LPUSH+LTRIM in a MULTI/EXEC transaction
	// (TxPipeline) instead of a plain pipeline. The design doc's skeleton
	// (section 5.2) uses a plain pipeline for lowest latency and that is the
	// default; enable this if multiple producers may write the same window.
	Atomic bool
	// StreamKey overrides the notification stream key. Defaults to DefaultStreamKey.
	StreamKey string
	// StreamMaxLen bounds the notification stream (approximate MAXLEN ~).
	// Defaults to DefaultStreamMaxLen. Set negative to disable trimming.
	StreamMaxLen int64

	// Registerer receives the writer's Prometheus metrics. When nil a private
	// registry is used so metrics still function but are not exposed.
	Registerer prometheus.Registerer
	// Logger is used for operational warnings. Defaults to slog.Default().
	Logger *slog.Logger
}

type resolvedConfig struct {
	keyPrefix    string
	trimStop     int64 // LTRIM stop index = WindowSize-1
	atomic       bool
	streamKey    string
	streamMaxLen int64 // 0 means "no MAXLEN"
}

func (c Config) resolve() resolvedConfig {
	rc := resolvedConfig{
		keyPrefix: c.KeyPrefix,
		atomic:    c.Atomic,
		streamKey: c.StreamKey,
	}
	if rc.keyPrefix == "" {
		rc.keyPrefix = DefaultKeyPrefix
	}
	ws := c.WindowSize
	if ws <= 0 {
		ws = DefaultWindowSize
	}
	rc.trimStop = int64(ws - 1)
	if rc.streamKey == "" {
		rc.streamKey = DefaultStreamKey
	}
	switch {
	case c.StreamMaxLen < 0:
		rc.streamMaxLen = 0
	case c.StreamMaxLen == 0:
		rc.streamMaxLen = DefaultStreamMaxLen
	default:
		rc.streamMaxLen = c.StreamMaxLen
	}
	return rc
}
