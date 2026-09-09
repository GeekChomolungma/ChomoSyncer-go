// Command chomosyncer-go is the ChomoSyncer-go daemon: it streams Binance
// USDⓈ-M futures klines and fans them out to Redis and ClickHouse.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HarvestStars/chomosyncer-go/internal/app"
	"github.com/HarvestStars/chomosyncer-go/internal/config"
)

// version is overridden at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	cli, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
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
	configFile string

	metricsAddr string

	redisAddr     string
	redisDB       int
	redisPassword string
	redisPoolSize int

	windowSize int

	liveRedisAddr     string
	liveRedisDB       int
	liveRedisPassword string
	livePublish       bool
	liveWorkers       int
	liveChannelSize   int

	chAddr          string
	chDatabase      string
	chUsername      string
	chPassword      string
	chBatchSize     int
	chFlushInterval time.Duration
	chChannelSize   int

	intervals             string
	shards                int
	wsURL                 string
	collectorStaleTimeout time.Duration

	dispatcherClosedWorkers   int
	dispatcherClosedQueueSize int
	sectionTimeout            time.Duration

	restURL                 string
	universeRefreshInterval time.Duration
	universeQuoteAssets     string

	backfill            bool
	backfillStartDate   string
	backfillRestRPS     float64
	backfillWorkers     int
	backfillGapDebounce time.Duration
	backfillGateTimeout time.Duration
	backfillFlushWait   time.Duration

	logLevel  string
	logFormat string

	appCfg app.Config
}

func (c cliConfig) toAppConfig(logger *slog.Logger) app.Config {
	cfg := c.appCfg
	cfg.Logger = logger
	return cfg
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("chomosyncer-go", flag.ContinueOnError)
	var c cliConfig

	fs.StringVar(&c.configFile, "config", env("CHOMOSYNCER_CONFIG", ""), "path to YAML configuration file")

	fs.StringVar(&c.metricsAddr, "metrics-addr", env("CHOMOSYNCER_METRICS_ADDR", ":9090"), `Prometheus /metrics listen address ("off" to disable)`)
	fs.StringVar(&c.logLevel, "log-level", env("CHOMOSYNCER_LOG_LEVEL", "info"), "log level: debug|info|warn|error")
	fs.StringVar(&c.logFormat, "log-format", env("CHOMOSYNCER_LOG_FORMAT", "text"), "log format: text|json")

	fs.StringVar(&c.redisAddr, "redis-addr", env("CHOMOSYNCER_REDIS_ADDR", "localhost:6379"), "Redis addr for the closed-bar window + kline_ready stream")
	fs.IntVar(&c.redisDB, "redis-db", envInt("CHOMOSYNCER_REDIS_DB", 0), "Redis DB index")
	fs.StringVar(&c.redisPassword, "redis-password", env("CHOMOSYNCER_REDIS_PASSWORD", ""), "Redis password")
	fs.IntVar(&c.redisPoolSize, "redis-pool-size", envInt("CHOMOSYNCER_REDIS_POOL_SIZE", 20), "Redis connection pool size")

	fs.IntVar(&c.windowSize, "window-size", envInt("CHOMOSYNCER_WINDOW_SIZE", 200), "Redis rolling window size (also bounds historical backfill)")

	fs.StringVar(&c.liveRedisAddr, "live-redis-addr", env("CHOMOSYNCER_LIVE_REDIS_ADDR", ""), "Redis addr for the live-bar snapshot (default: -redis-addr)")
	fs.IntVar(&c.liveRedisDB, "live-redis-db", envInt("CHOMOSYNCER_LIVE_REDIS_DB", 0), "live Redis DB index")
	fs.StringVar(&c.liveRedisPassword, "live-redis-password", env("CHOMOSYNCER_LIVE_REDIS_PASSWORD", ""), "live Redis password")
	fs.BoolVar(&c.livePublish, "live-publish", envBool("CHOMOSYNCER_LIVE_PUBLISH", false), "also PUBLISH live bars to livebar.<interval>")
	fs.IntVar(&c.liveWorkers, "live-workers", envInt("CHOMOSYNCER_LIVE_WORKERS", 2), "live bar writer worker count")
	fs.IntVar(&c.liveChannelSize, "live-channel-size", envInt("CHOMOSYNCER_LIVE_CHANNEL_SIZE", 8192), "live bar ingest channel buffer capacity")

	fs.StringVar(&c.chAddr, "ch-addr", env("CHOMOSYNCER_CH_ADDR", "localhost:9000"), "ClickHouse native addresses (comma-separated)")
	fs.StringVar(&c.chDatabase, "ch-database", env("CHOMOSYNCER_CH_DATABASE", "market"), "ClickHouse database")
	fs.StringVar(&c.chUsername, "ch-username", env("CHOMOSYNCER_CH_USERNAME", "default"), "ClickHouse username")
	fs.StringVar(&c.chPassword, "ch-password", env("CHOMOSYNCER_CH_PASSWORD", ""), "ClickHouse password")
	fs.IntVar(&c.chBatchSize, "ch-batch-size", envInt("CHOMOSYNCER_CH_BATCH_SIZE", 5000), "ClickHouse batch writer flush batch size")
	fs.DurationVar(&c.chFlushInterval, "ch-flush-interval", envDur("CHOMOSYNCER_CH_FLUSH_INTERVAL", 1000*time.Millisecond), "ClickHouse batch writer flush interval")
	fs.IntVar(&c.chChannelSize, "ch-channel-size", envInt("CHOMOSYNCER_CH_CHANNEL_SIZE", 20000), "ClickHouse batch writer ingest buffer capacity")

	fs.StringVar(&c.intervals, "intervals", env("CHOMOSYNCER_INTERVALS", "1m,1h"), "kline intervals (comma-separated)")
	fs.IntVar(&c.shards, "shards-per-interval", envInt("CHOMOSYNCER_SHARDS_PER_INTERVAL", 4), "WS shard connections per interval")
	fs.StringVar(&c.wsURL, "ws-url", env("CHOMOSYNCER_WS_URL", "wss://fstream.binance.com"), "Binance futures WebSocket base URL")
	fs.DurationVar(&c.collectorStaleTimeout, "collector-stale-timeout", envDur("CHOMOSYNCER_COLLECTOR_STALE_TIMEOUT", 60*time.Second), "watchdog staleness reconnect timeout")

	fs.IntVar(&c.dispatcherClosedWorkers, "dispatcher-closed-workers", envInt("CHOMOSYNCER_DISPATCHER_CLOSED_WORKERS", 4), "closed-bar dispatcher worker count")
	fs.IntVar(&c.dispatcherClosedQueueSize, "dispatcher-closed-queue-size", envInt("CHOMOSYNCER_DISPATCHER_CLOSED_QUEUE_SIZE", 4096), "closed-bar queue capacity")
	fs.DurationVar(&c.sectionTimeout, "section-timeout", envDur("CHOMOSYNCER_SECTION_TIMEOUT", 5*time.Second), "kline_ready cross-section fallback timeout")

	fs.StringVar(&c.restURL, "rest-url", env("CHOMOSYNCER_REST_URL", "https://fapi.binance.com"), "Binance futures REST base URL")
	fs.DurationVar(&c.universeRefreshInterval, "universe-refresh-interval", envDur("CHOMOSYNCER_UNIVERSE_REFRESH_INTERVAL", 24*time.Hour), "universe refresh period")
	fs.StringVar(&c.universeQuoteAssets, "universe-quote-assets", env("CHOMOSYNCER_UNIVERSE_QUOTE_ASSETS", "USDT"), "universe target quote assets (comma-separated, e.g. USDT,USDC)")

	fs.BoolVar(&c.backfill, "backfill", envBool("CHOMOSYNCER_BACKFILL", true), "historical gapfill: cold-start + shard-reconnect backfill and CH->Redis window rebuild")
	fs.StringVar(&c.backfillStartDate, "backfill-start-date", env("CHOMOSYNCER_BACKFILL_START_DATE", ""), "initial historical backfill start date (e.g. 2024-01-01 or RFC3339)")
	fs.Float64Var(&c.backfillRestRPS, "backfill-rest-rps", envFloat("CHOMOSYNCER_BACKFILL_REST_RPS", 20), "token-bucket rate for /fapi/v1/klines")
	fs.IntVar(&c.backfillWorkers, "backfill-workers", envInt("CHOMOSYNCER_BACKFILL_WORKERS", 4), "per-request parallel REST fetch count")
	fs.DurationVar(&c.backfillGapDebounce, "backfill-gap-debounce", envDur("CHOMOSYNCER_BACKFILL_GAP_DEBOUNCE", 30*time.Second), "coalesce repeated shard-reconnect gaps")
	fs.DurationVar(&c.backfillGateTimeout, "backfill-gate-timeout", envDur("CHOMOSYNCER_BACKFILL_GATE_TIMEOUT", 5*time.Minute), "force-release a gapfill-held key after this")
	fs.DurationVar(&c.backfillFlushWait, "backfill-flush-wait", envDur("CHOMOSYNCER_BACKFILL_FLUSH_WAIT", 2*time.Second), "wait after archiving before reading ClickHouse back")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	visitedFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visitedFlags[f.Name] = true
	})

	explicit := func(flagName, envKey string) bool {
		if visitedFlags[flagName] {
			return true
		}
		if envKey != "" {
			_, ok := os.LookupEnv(envKey)
			return ok
		}
		return false
	}

	baseCfg := config.DefaultConfig()
	cfgPath := c.configFile
	if cfgPath == "" {
		if _, err := os.Stat("config.yaml"); err == nil {
			cfgPath = "config.yaml"
		}
	}
	if cfgPath != "" {
		loaded, err := config.LoadYAML(cfgPath)
		if err != nil {
			return cliConfig{}, fmt.Errorf("load config %q: %w", cfgPath, err)
		}
		baseCfg = loaded
	}

	// Apply CLI / Env overrides on top of baseCfg
	if explicit("metrics-addr", "CHOMOSYNCER_METRICS_ADDR") {
		baseCfg.App.MetricsAddr = c.metricsAddr
	}
	if explicit("log-level", "CHOMOSYNCER_LOG_LEVEL") {
		baseCfg.App.LogLevel = c.logLevel
	}
	if explicit("log-format", "CHOMOSYNCER_LOG_FORMAT") {
		baseCfg.App.LogFormat = c.logFormat
	}

	if explicit("redis-addr", "CHOMOSYNCER_REDIS_ADDR") {
		baseCfg.Redis.Addr = c.redisAddr
	}
	if explicit("redis-db", "CHOMOSYNCER_REDIS_DB") {
		baseCfg.Redis.DB = c.redisDB
	}
	if explicit("redis-password", "CHOMOSYNCER_REDIS_PASSWORD") {
		baseCfg.Redis.Password = c.redisPassword
	}
	if explicit("redis-pool-size", "CHOMOSYNCER_REDIS_POOL_SIZE") {
		baseCfg.Redis.PoolSize = c.redisPoolSize
	}

	if explicit("window-size", "CHOMOSYNCER_WINDOW_SIZE") {
		baseCfg.Redis.Window.WindowSize = c.windowSize
	}

	if explicit("live-redis-addr", "CHOMOSYNCER_LIVE_REDIS_ADDR") {
		baseCfg.Redis.Live.Addr = c.liveRedisAddr
	}
	if explicit("live-redis-db", "CHOMOSYNCER_LIVE_REDIS_DB") {
		baseCfg.Redis.Live.DB = c.liveRedisDB
	}
	if explicit("live-redis-password", "CHOMOSYNCER_LIVE_REDIS_PASSWORD") {
		baseCfg.Redis.Live.Password = c.liveRedisPassword
	}
	if explicit("live-publish", "CHOMOSYNCER_LIVE_PUBLISH") {
		baseCfg.Redis.Live.Publish = c.livePublish
	}
	if explicit("live-workers", "CHOMOSYNCER_LIVE_WORKERS") {
		baseCfg.Redis.Live.Workers = c.liveWorkers
	}
	if explicit("live-channel-size", "CHOMOSYNCER_LIVE_CHANNEL_SIZE") {
		baseCfg.Redis.Live.ChannelSize = c.liveChannelSize
	}

	if explicit("ch-addr", "CHOMOSYNCER_CH_ADDR") {
		baseCfg.ClickHouse.Addrs = splitCSV(c.chAddr)
	}
	if explicit("ch-database", "CHOMOSYNCER_CH_DATABASE") {
		baseCfg.ClickHouse.Database = c.chDatabase
	}
	if explicit("ch-username", "CHOMOSYNCER_CH_USERNAME") {
		baseCfg.ClickHouse.Username = c.chUsername
	}
	if explicit("ch-password", "CHOMOSYNCER_CH_PASSWORD") {
		baseCfg.ClickHouse.Password = c.chPassword
	}
	if explicit("ch-batch-size", "CHOMOSYNCER_CH_BATCH_SIZE") {
		baseCfg.ClickHouse.BatchSize = c.chBatchSize
	}
	if explicit("ch-flush-interval", "CHOMOSYNCER_CH_FLUSH_INTERVAL") {
		baseCfg.ClickHouse.FlushInterval = c.chFlushInterval
	}
	if explicit("ch-channel-size", "CHOMOSYNCER_CH_CHANNEL_SIZE") {
		baseCfg.ClickHouse.ChannelSize = c.chChannelSize
	}

	if explicit("intervals", "CHOMOSYNCER_INTERVALS") {
		baseCfg.Collector.Intervals = splitCSV(c.intervals)
	}
	if explicit("shards-per-interval", "CHOMOSYNCER_SHARDS_PER_INTERVAL") {
		baseCfg.Collector.ShardsPerInterval = c.shards
	}
	if explicit("ws-url", "CHOMOSYNCER_WS_URL") {
		baseCfg.Collector.WSURL = c.wsURL
	}
	if explicit("collector-stale-timeout", "CHOMOSYNCER_COLLECTOR_STALE_TIMEOUT") {
		baseCfg.Collector.StaleTimeout = c.collectorStaleTimeout
	}

	if explicit("dispatcher-closed-workers", "CHOMOSYNCER_DISPATCHER_CLOSED_WORKERS") {
		baseCfg.Dispatcher.ClosedWorkers = c.dispatcherClosedWorkers
	}
	if explicit("dispatcher-closed-queue-size", "CHOMOSYNCER_DISPATCHER_CLOSED_QUEUE_SIZE") {
		baseCfg.Dispatcher.ClosedQueueSize = c.dispatcherClosedQueueSize
	}
	if explicit("section-timeout", "CHOMOSYNCER_SECTION_TIMEOUT") {
		baseCfg.Dispatcher.SectionTimeout = c.sectionTimeout
	}

	if explicit("rest-url", "CHOMOSYNCER_REST_URL") {
		baseCfg.Universe.RESTURL = c.restURL
	}
	if explicit("universe-refresh-interval", "CHOMOSYNCER_UNIVERSE_REFRESH_INTERVAL") {
		baseCfg.Universe.RefreshInterval = c.universeRefreshInterval
	}
	if explicit("universe-quote-assets", "CHOMOSYNCER_UNIVERSE_QUOTE_ASSETS") {
		baseCfg.Universe.QuoteAssets = splitCSV(c.universeQuoteAssets)
	}

	if explicit("backfill", "CHOMOSYNCER_BACKFILL") {
		baseCfg.Backfill.Enabled = c.backfill
	}
	if explicit("backfill-start-date", "CHOMOSYNCER_BACKFILL_START_DATE") {
		baseCfg.Backfill.ColdStartDate = c.backfillStartDate
	}
	if explicit("backfill-rest-rps", "CHOMOSYNCER_BACKFILL_REST_RPS") {
		baseCfg.Backfill.RestRPS = c.backfillRestRPS
	}
	if explicit("backfill-workers", "CHOMOSYNCER_BACKFILL_WORKERS") {
		baseCfg.Backfill.Workers = c.backfillWorkers
	}
	if explicit("backfill-gap-debounce", "CHOMOSYNCER_BACKFILL_GAP_DEBOUNCE") {
		baseCfg.Backfill.GapDebounce = c.backfillGapDebounce
	}
	if explicit("backfill-gate-timeout", "CHOMOSYNCER_BACKFILL_GATE_TIMEOUT") {
		baseCfg.Backfill.GateTimeout = c.backfillGateTimeout
	}
	if explicit("backfill-flush-wait", "CHOMOSYNCER_BACKFILL_FLUSH_WAIT") {
		baseCfg.Backfill.FlushWait = c.backfillFlushWait
	}

	// Synchronize backfill window size with redis window size
	baseCfg.Backfill.QueueSize = 256

	if err := baseCfg.Validate(); err != nil {
		return cliConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}

	// Mirror merged values back to cliConfig fields for inspection / testing
	c.metricsAddr = baseCfg.App.MetricsAddr
	c.logLevel = baseCfg.App.LogLevel
	c.logFormat = baseCfg.App.LogFormat
	c.redisAddr = baseCfg.Redis.Addr
	c.redisDB = baseCfg.Redis.DB
	c.redisPassword = baseCfg.Redis.Password
	c.redisPoolSize = baseCfg.Redis.PoolSize
	c.windowSize = baseCfg.Redis.Window.WindowSize
	c.liveRedisAddr = baseCfg.Redis.Live.Addr
	c.liveRedisDB = baseCfg.Redis.Live.DB
	c.liveRedisPassword = baseCfg.Redis.Live.Password
	c.livePublish = baseCfg.Redis.Live.Publish
	c.liveWorkers = baseCfg.Redis.Live.Workers
	c.liveChannelSize = baseCfg.Redis.Live.ChannelSize
	c.chAddr = strings.Join(baseCfg.ClickHouse.Addrs, ",")
	c.chDatabase = baseCfg.ClickHouse.Database
	c.chUsername = baseCfg.ClickHouse.Username
	c.chPassword = baseCfg.ClickHouse.Password
	c.chBatchSize = baseCfg.ClickHouse.BatchSize
	c.chFlushInterval = baseCfg.ClickHouse.FlushInterval
	c.chChannelSize = baseCfg.ClickHouse.ChannelSize
	c.intervals = strings.Join(baseCfg.Collector.Intervals, ",")
	c.shards = baseCfg.Collector.ShardsPerInterval
	c.wsURL = baseCfg.Collector.WSURL
	c.collectorStaleTimeout = baseCfg.Collector.StaleTimeout
	c.dispatcherClosedWorkers = baseCfg.Dispatcher.ClosedWorkers
	c.dispatcherClosedQueueSize = baseCfg.Dispatcher.ClosedQueueSize
	c.sectionTimeout = baseCfg.Dispatcher.SectionTimeout
	c.restURL = baseCfg.Universe.RESTURL
	c.universeRefreshInterval = baseCfg.Universe.RefreshInterval
	c.universeQuoteAssets = strings.Join(baseCfg.Universe.QuoteAssets, ",")
	c.backfill = baseCfg.Backfill.Enabled
	c.backfillStartDate = baseCfg.Backfill.ColdStartDate
	c.backfillRestRPS = baseCfg.Backfill.RestRPS
	c.backfillWorkers = baseCfg.Backfill.Workers
	c.backfillGapDebounce = baseCfg.Backfill.GapDebounce
	c.backfillGateTimeout = baseCfg.Backfill.GateTimeout
	c.backfillFlushWait = baseCfg.Backfill.FlushWait

	c.appCfg = app.Config{
		Config:  baseCfg,
		Version: version,
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
