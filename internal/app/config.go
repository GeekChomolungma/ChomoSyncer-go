package app

import (
	"log/slog"

	"github.com/HarvestStars/chomosyncer-go/internal/config"
)

// Config is the runtime configuration for the App, embedding the modular
// configuration plus process-level dependencies.
type Config struct {
	config.Config

	Version string
	Logger  *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Version == "" {
		c.Version = "dev"
	}

	def := config.DefaultConfig()
	if len(c.Collector.Intervals) == 0 {
		c.Collector.Intervals = def.Collector.Intervals
	}
	if len(c.Collector.ServeIntervals) == 0 {
		c.Collector.ServeIntervals = def.Collector.ServeIntervals
	}
	if c.Collector.ShardsPerInterval <= 0 {
		c.Collector.ShardsPerInterval = def.Collector.ShardsPerInterval
	}
	if c.Collector.WSURL == "" {
		c.Collector.WSURL = def.Collector.WSURL
	}
	if c.Collector.ConnectStagger <= 0 {
		c.Collector.ConnectStagger = def.Collector.ConnectStagger
	}
	if c.Collector.ConnectTimeout <= 0 {
		c.Collector.ConnectTimeout = def.Collector.ConnectTimeout
	}
	if c.Collector.ReconnectBase <= 0 {
		c.Collector.ReconnectBase = def.Collector.ReconnectBase
	}
	if c.Collector.ReconnectMax <= 0 {
		c.Collector.ReconnectMax = def.Collector.ReconnectMax
	}
	if c.Collector.StaleTimeout <= 0 {
		c.Collector.StaleTimeout = def.Collector.StaleTimeout
	}
	if c.Collector.WatchdogInterval <= 0 {
		c.Collector.WatchdogInterval = def.Collector.WatchdogInterval
	}

	if c.Dispatcher.ClosedWorkers <= 0 {
		c.Dispatcher.ClosedWorkers = def.Dispatcher.ClosedWorkers
	}
	if c.Dispatcher.ClosedQueueSize <= 0 {
		c.Dispatcher.ClosedQueueSize = def.Dispatcher.ClosedQueueSize
	}
	if c.Dispatcher.SectionTimeout <= 0 {
		c.Dispatcher.SectionTimeout = def.Dispatcher.SectionTimeout
	}
	if c.Dispatcher.PublishTimeout <= 0 {
		c.Dispatcher.PublishTimeout = def.Dispatcher.PublishTimeout
	}

	if c.Redis.Addr == "" {
		c.Redis.Addr = def.Redis.Addr
	}
	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = def.Redis.PoolSize
	}
	if c.Redis.DialTimeout <= 0 {
		c.Redis.DialTimeout = def.Redis.DialTimeout
	}
	if c.Redis.ReadTimeout <= 0 {
		c.Redis.ReadTimeout = def.Redis.ReadTimeout
	}
	if c.Redis.WriteTimeout <= 0 {
		c.Redis.WriteTimeout = def.Redis.WriteTimeout
	}
	if c.Redis.Window.WindowSize <= 0 {
		c.Redis.Window.WindowSize = def.Redis.Window.WindowSize
	}
	if c.Redis.Window.KeyPrefix == "" {
		c.Redis.Window.KeyPrefix = def.Redis.Window.KeyPrefix
	}
	if c.Redis.Window.StreamKey == "" {
		c.Redis.Window.StreamKey = def.Redis.Window.StreamKey
	}
	if c.Redis.Window.StreamMaxLen <= 0 {
		c.Redis.Window.StreamMaxLen = def.Redis.Window.StreamMaxLen
	}

	if c.Redis.Live.Addr == "" {
		c.Redis.Live.Addr = c.Redis.Addr
		c.Redis.Live.DB = c.Redis.DB
		c.Redis.Live.Password = c.Redis.Password
	}
	if c.Redis.Live.PoolSize <= 0 {
		c.Redis.Live.PoolSize = def.Redis.Live.PoolSize
	}
	if c.Redis.Live.Workers <= 0 {
		c.Redis.Live.Workers = def.Redis.Live.Workers
	}
	if c.Redis.Live.ChannelSize <= 0 {
		c.Redis.Live.ChannelSize = def.Redis.Live.ChannelSize
	}
	if c.Redis.Live.WriteTimeout <= 0 {
		c.Redis.Live.WriteTimeout = def.Redis.Live.WriteTimeout
	}
	if c.Redis.Live.TTLMultiple <= 0 {
		c.Redis.Live.TTLMultiple = def.Redis.Live.TTLMultiple
	}
	if c.Redis.Live.DefaultTTL <= 0 {
		c.Redis.Live.DefaultTTL = def.Redis.Live.DefaultTTL
	}
	if c.Redis.Live.KeyPrefix == "" {
		c.Redis.Live.KeyPrefix = def.Redis.Live.KeyPrefix
	}
	if c.Redis.Live.ChannelPrefix == "" {
		c.Redis.Live.ChannelPrefix = def.Redis.Live.ChannelPrefix
	}

	if len(c.ClickHouse.Addrs) == 0 {
		c.ClickHouse.Addrs = def.ClickHouse.Addrs
	}
	if c.ClickHouse.Database == "" {
		c.ClickHouse.Database = def.ClickHouse.Database
	}
	if c.ClickHouse.Username == "" {
		c.ClickHouse.Username = def.ClickHouse.Username
	}
	if c.ClickHouse.TablePrefix == "" {
		c.ClickHouse.TablePrefix = def.ClickHouse.TablePrefix
	}
	if c.ClickHouse.DialTimeout <= 0 {
		c.ClickHouse.DialTimeout = def.ClickHouse.DialTimeout
	}
	if c.ClickHouse.BatchSize <= 0 {
		c.ClickHouse.BatchSize = def.ClickHouse.BatchSize
	}
	if c.ClickHouse.FlushInterval <= 0 {
		c.ClickHouse.FlushInterval = def.ClickHouse.FlushInterval
	}
	if c.ClickHouse.ChannelSize <= 0 {
		c.ClickHouse.ChannelSize = def.ClickHouse.ChannelSize
	}
	if c.ClickHouse.MaxRetries <= 0 {
		c.ClickHouse.MaxRetries = def.ClickHouse.MaxRetries
	}
	if c.ClickHouse.RetryBackoff <= 0 {
		c.ClickHouse.RetryBackoff = def.ClickHouse.RetryBackoff
	}
	if c.ClickHouse.MaxRetryBackoff <= 0 {
		c.ClickHouse.MaxRetryBackoff = def.ClickHouse.MaxRetryBackoff
	}
	if c.ClickHouse.ShutdownTimeout <= 0 {
		c.ClickHouse.ShutdownTimeout = def.ClickHouse.ShutdownTimeout
	}

	if c.Universe.RESTURL == "" {
		c.Universe.RESTURL = def.Universe.RESTURL
	}
	if c.Universe.RefreshInterval <= 0 {
		c.Universe.RefreshInterval = def.Universe.RefreshInterval
	}
	if c.Universe.RefreshOffset <= 0 {
		c.Universe.RefreshOffset = def.Universe.RefreshOffset
	}
	if c.Universe.HTTPTimeout <= 0 {
		c.Universe.HTTPTimeout = def.Universe.HTTPTimeout
	}
	if len(c.Universe.QuoteAssets) == 0 {
		c.Universe.QuoteAssets = def.Universe.QuoteAssets
	}
	if c.Universe.ContractType == "" {
		c.Universe.ContractType = def.Universe.ContractType
	}
	if c.Universe.Status == "" {
		c.Universe.Status = def.Universe.Status
	}

	if c.Backfill.Workers <= 0 {
		c.Backfill.Workers = def.Backfill.Workers
	}
	if c.Backfill.RestRPS <= 0 {
		c.Backfill.RestRPS = def.Backfill.RestRPS
	}
	if c.Backfill.QueueSize <= 0 {
		c.Backfill.QueueSize = def.Backfill.QueueSize
	}
	if c.Backfill.GapDebounce <= 0 {
		c.Backfill.GapDebounce = def.Backfill.GapDebounce
	}
	if c.Backfill.GateTimeout <= 0 {
		c.Backfill.GateTimeout = def.Backfill.GateTimeout
	}
	if c.Backfill.FlushWait <= 0 {
		c.Backfill.FlushWait = def.Backfill.FlushWait
	}

	if c.App.MetricsAddr == "" {
		c.App.MetricsAddr = def.App.MetricsAddr
	}
	if c.App.ShutdownTimeout <= 0 {
		c.App.ShutdownTimeout = def.App.ShutdownTimeout
	}
	return c
}
