// Package app wires the whole ChomoSyncer-go pipeline together:
//
//	universe (symbol discovery)
//	   └─► collector (Binance WS shards)
//	          └─► dispatcher (fan-out)
//	                 ├─► rediswin.Writer        (closed 200-bar window + kline_ready)
//	                 ├─► rediswin.LiveBarWriter (live snapshot)
//	                 └─► chwriter.BatchWriter × interval (ClickHouse archive)
//	metrics serves the shared registry over /metrics.
package app

import (
	"log/slog"
	"time"
)

// Config is the fully-resolved runtime configuration. cmd/chomosyncer-go builds
// it from flags + env.
type Config struct {
	// --- Binance ---
	WSBaseURL         string   // wss://fstream.binance.com
	RESTBaseURL       string   // https://fapi.binance.com
	Intervals         []string // {"1m","1h"}
	ShardsPerInterval int

	// --- Redis: closed-bar window + kline_ready stream ---
	RedisAddr     string
	RedisDB       int
	RedisPassword string

	// --- Redis: live-bar snapshot (dedicated client) ---
	// Empty LiveRedisAddr mirrors the closed-bar Redis endpoint.
	LiveRedisAddr     string
	LiveRedisDB       int
	LiveRedisPassword string
	LivePublish       bool

	// --- ClickHouse ---
	CHAddrs       []string
	CHDatabase    string
	CHUsername    string
	CHPassword    string
	CHTablePrefix string // per interval: "<prefix>_<interval>" (e.g. market.fapi_kline_1m)
	CHDialTimeout time.Duration

	// --- dispatcher ---
	SectionTimeout time.Duration

	// --- historical gapfill (see docs/GAPFILL_DESIGN.md) ---
	Backfill            bool          // master switch; false => forward-only
	BackfillRestRPS     float64       // token-bucket rate for /fapi/v1/klines, default 20
	BackfillWorkers     int           // per-request parallel fetch, default 4
	BackfillGapDebounce time.Duration // coalesce repeated shard-reconnect gaps, default 30s
	BackfillGateTimeout time.Duration // force-release a stuck key after this, default 5m
	BackfillFlushWait   time.Duration // wait after archiving before CH read-back, default 2s

	// --- observability ---
	MetricsAddr string // ":9090" (default); "off" disables the HTTP server
	Version     string

	// --- shutdown ---
	ShutdownTimeout time.Duration

	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.WSBaseURL == "" {
		c.WSBaseURL = "wss://fstream.binance.com"
	}
	if c.RESTBaseURL == "" {
		c.RESTBaseURL = "https://fapi.binance.com"
	}
	if len(c.Intervals) == 0 {
		c.Intervals = []string{"1m", "1h"}
	}
	if c.ShardsPerInterval <= 0 {
		c.ShardsPerInterval = 4
	}
	if c.RedisAddr == "" {
		c.RedisAddr = "localhost:6379"
	}
	if c.LiveRedisAddr == "" {
		c.LiveRedisAddr = c.RedisAddr
		c.LiveRedisDB = c.RedisDB
		c.LiveRedisPassword = c.RedisPassword
	}
	if len(c.CHAddrs) == 0 {
		c.CHAddrs = []string{"localhost:9000"}
	}
	if c.CHDatabase == "" {
		c.CHDatabase = "market"
	}
	if c.CHUsername == "" {
		c.CHUsername = "default"
	}
	if c.CHTablePrefix == "" {
		c.CHTablePrefix = "market.fapi_kline"
	}
	if c.CHDialTimeout <= 0 {
		c.CHDialTimeout = 5 * time.Second
	}
	if c.SectionTimeout <= 0 {
		c.SectionTimeout = 5 * time.Second
	}
	if c.BackfillRestRPS <= 0 {
		c.BackfillRestRPS = 20
	}
	if c.BackfillWorkers <= 0 {
		c.BackfillWorkers = 4
	}
	if c.BackfillGapDebounce <= 0 {
		c.BackfillGapDebounce = 30 * time.Second
	}
	if c.BackfillGateTimeout <= 0 {
		c.BackfillGateTimeout = 5 * time.Minute
	}
	if c.BackfillFlushWait <= 0 {
		c.BackfillFlushWait = 2 * time.Second
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = ":9090"
	}
	if c.Version == "" {
		c.Version = "dev"
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}
