// Command chomosyncer-go is the ChomoSyncer-go daemon: it streams Binance
// USDⓈ-M futures klines and fans them out to Redis and ClickHouse.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HarvestStars/chomosyncer-go/internal/app"
)

// version is overridden at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	cli, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2)
	}
	logger := newLogger(cli.logLevel, cli.logFormat)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cli.toAppConfig(logger))
	if err != nil {
		logger.Error("startup failed", "err", err)
		os.Exit(1)
	}
	if err := a.Run(ctx); err != nil {
		logger.Error("run failed", "err", err)
		os.Exit(1)
	}
	logger.Info("stopped cleanly")
}

type cliConfig struct {
	metricsAddr string

	redisAddr     string
	redisDB       int
	redisPassword string

	liveRedisAddr     string
	liveRedisDB       int
	liveRedisPassword string
	livePublish       bool

	chAddr     string
	chDatabase string
	chUsername string
	chPassword string

	intervals      string
	shards         int
	wsURL          string
	restURL        string
	sectionTimeout time.Duration

	backfill            bool
	backfillRestRPS     float64
	backfillWorkers     int
	backfillGapDebounce time.Duration
	backfillGateTimeout time.Duration
	backfillFlushWait   time.Duration

	logLevel  string
	logFormat string
}

func (c cliConfig) toAppConfig(logger *slog.Logger) app.Config {
	return app.Config{
		WSBaseURL:         c.wsURL,
		RESTBaseURL:       c.restURL,
		Intervals:         splitCSV(c.intervals),
		ShardsPerInterval: c.shards,

		RedisAddr:     c.redisAddr,
		RedisDB:       c.redisDB,
		RedisPassword: c.redisPassword,

		LiveRedisAddr:     c.liveRedisAddr,
		LiveRedisDB:       c.liveRedisDB,
		LiveRedisPassword: c.liveRedisPassword,
		LivePublish:       c.livePublish,

		CHAddrs:    splitCSV(c.chAddr),
		CHDatabase: c.chDatabase,
		CHUsername: c.chUsername,
		CHPassword: c.chPassword,

		SectionTimeout: c.sectionTimeout,

		Backfill:            c.backfill,
		BackfillRestRPS:     c.backfillRestRPS,
		BackfillWorkers:     c.backfillWorkers,
		BackfillGapDebounce: c.backfillGapDebounce,
		BackfillGateTimeout: c.backfillGateTimeout,
		BackfillFlushWait:   c.backfillFlushWait,

		MetricsAddr: c.metricsAddr,
		Version:     version,
		Logger:      logger,
	}
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("chomosyncer-go", flag.ContinueOnError)
	var c cliConfig

	fs.StringVar(&c.metricsAddr, "metrics-addr", env("CHOMOSYNCER_METRICS_ADDR", ":9090"), `Prometheus /metrics listen address ("off" to disable)`)

	fs.StringVar(&c.redisAddr, "redis-addr", env("CHOMOSYNCER_REDIS_ADDR", "localhost:6379"), "Redis addr for the closed-bar window + kline_ready stream")
	fs.IntVar(&c.redisDB, "redis-db", envInt("CHOMOSYNCER_REDIS_DB", 0), "Redis DB index")
	fs.StringVar(&c.redisPassword, "redis-password", env("CHOMOSYNCER_REDIS_PASSWORD", ""), "Redis password")

	fs.StringVar(&c.liveRedisAddr, "live-redis-addr", env("CHOMOSYNCER_LIVE_REDIS_ADDR", ""), "Redis addr for the live-bar snapshot (default: -redis-addr)")
	fs.IntVar(&c.liveRedisDB, "live-redis-db", envInt("CHOMOSYNCER_LIVE_REDIS_DB", 0), "live Redis DB index")
	fs.StringVar(&c.liveRedisPassword, "live-redis-password", env("CHOMOSYNCER_LIVE_REDIS_PASSWORD", ""), "live Redis password")
	fs.BoolVar(&c.livePublish, "live-publish", envBool("CHOMOSYNCER_LIVE_PUBLISH", false), "also PUBLISH live bars to livebar.<interval>")

	fs.StringVar(&c.chAddr, "ch-addr", env("CHOMOSYNCER_CH_ADDR", "localhost:9000"), "ClickHouse native addresses (comma-separated)")
	fs.StringVar(&c.chDatabase, "ch-database", env("CHOMOSYNCER_CH_DATABASE", "market"), "ClickHouse database")
	fs.StringVar(&c.chUsername, "ch-username", env("CHOMOSYNCER_CH_USERNAME", "default"), "ClickHouse username")
	fs.StringVar(&c.chPassword, "ch-password", env("CHOMOSYNCER_CH_PASSWORD", ""), "ClickHouse password")

	fs.StringVar(&c.intervals, "intervals", env("CHOMOSYNCER_INTERVALS", "1m,1h"), "kline intervals (comma-separated)")
	fs.IntVar(&c.shards, "shards-per-interval", envInt("CHOMOSYNCER_SHARDS_PER_INTERVAL", 4), "WS shard connections per interval")
	fs.StringVar(&c.wsURL, "ws-url", env("CHOMOSYNCER_WS_URL", "wss://fstream.binance.com"), "Binance futures WebSocket base URL")
	fs.StringVar(&c.restURL, "rest-url", env("CHOMOSYNCER_REST_URL", "https://fapi.binance.com"), "Binance futures REST base URL")
	fs.DurationVar(&c.sectionTimeout, "section-timeout", envDur("CHOMOSYNCER_SECTION_TIMEOUT", 5*time.Second), "kline_ready cross-section fallback timeout")

	fs.BoolVar(&c.backfill, "backfill", envBool("CHOMOSYNCER_BACKFILL", true), "historical gapfill: cold-start + shard-reconnect backfill and CH->Redis window rebuild")
	fs.Float64Var(&c.backfillRestRPS, "backfill-rest-rps", envFloat("CHOMOSYNCER_BACKFILL_REST_RPS", 20), "token-bucket rate for /fapi/v1/klines")
	fs.IntVar(&c.backfillWorkers, "backfill-workers", envInt("CHOMOSYNCER_BACKFILL_WORKERS", 4), "per-request parallel REST fetch count")
	fs.DurationVar(&c.backfillGapDebounce, "backfill-gap-debounce", envDur("CHOMOSYNCER_BACKFILL_GAP_DEBOUNCE", 30*time.Second), "coalesce repeated shard-reconnect gaps")
	fs.DurationVar(&c.backfillGateTimeout, "backfill-gate-timeout", envDur("CHOMOSYNCER_BACKFILL_GATE_TIMEOUT", 5*time.Minute), "force-release a gapfill-held key after this")
	fs.DurationVar(&c.backfillFlushWait, "backfill-flush-wait", envDur("CHOMOSYNCER_BACKFILL_FLUSH_WAIT", 2*time.Second), "wait after archiving before reading ClickHouse back")

	fs.StringVar(&c.logLevel, "log-level", env("CHOMOSYNCER_LOG_LEVEL", "info"), "log level: debug|info|warn|error")
	fs.StringVar(&c.logFormat, "log-format", env("CHOMOSYNCER_LOG_FORMAT", "text"), "log format: text|json")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}
	return c, nil
}

func newLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.ToLower(format) == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

// --- small env helpers (flags default to env, env defaults to the literal) ---

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
