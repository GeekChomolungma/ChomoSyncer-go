package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for ChomoSyncer-go.
type Config struct {
	App        AppConfig        `yaml:"app"`
	Collector  CollectorConfig  `yaml:"collector"`
	Dispatcher DispatcherConfig `yaml:"dispatcher"`
	Redis      RedisConfig      `yaml:"redis"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Universe   UniverseConfig   `yaml:"universe"`
	Backfill   BackfillConfig   `yaml:"backfill"`
}

// AppConfig contains process-level settings.
type AppConfig struct {
	LogLevel        string        `yaml:"log_level"`
	LogFormat       string        `yaml:"log_format"`
	MetricsAddr     string        `yaml:"metrics_addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// CollectorConfig configures WebSocket connections and shards.
type CollectorConfig struct {
	Intervals         []string      `yaml:"intervals"`
	ShardsPerInterval int           `yaml:"shards_per_interval"`
	WSURL             string        `yaml:"ws_url"`
	ConnectStagger    time.Duration `yaml:"connect_stagger"`
	ConnectTimeout    time.Duration `yaml:"connect_timeout"`
	ReconnectBase     time.Duration `yaml:"reconnect_base"`
	ReconnectMax      time.Duration `yaml:"reconnect_max"`
	StaleTimeout      time.Duration `yaml:"stale_timeout"`
	WatchdogInterval  time.Duration `yaml:"watchdog_interval"`
}

// DispatcherConfig configures the event fan-out and worker pools.
type DispatcherConfig struct {
	ClosedWorkers   int           `yaml:"closed_workers"`
	ClosedQueueSize int           `yaml:"closed_queue_size"`
	SectionTimeout  time.Duration `yaml:"section_timeout"`
	PublishTimeout  time.Duration `yaml:"publish_timeout"`
}

// RedisConfig configures Redis connections and window settings.
type RedisConfig struct {
	Addr         string        `yaml:"addr"`
	DB           int           `yaml:"db"`
	Password     string        `yaml:"password"`
	PoolSize     int           `yaml:"pool_size"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	Window       WindowConfig  `yaml:"window"`
	Live         LiveConfig    `yaml:"live"`
}

// WindowConfig configures the closed-bar sliding window.
type WindowConfig struct {
	KeyPrefix    string `yaml:"key_prefix"`
	WindowSize   int    `yaml:"window_size"`
	StreamKey    string `yaml:"stream_key"`
	StreamMaxLen int64  `yaml:"stream_maxlen"`
	Atomic       bool   `yaml:"atomic"`
}

// LiveConfig configures live unclosed bar writes and pub/sub.
type LiveConfig struct {
	Addr          string        `yaml:"addr"`
	DB            int           `yaml:"db"`
	Password      string        `yaml:"password"`
	PoolSize      int           `yaml:"pool_size"`
	Workers       int           `yaml:"workers"`
	ChannelSize   int           `yaml:"channel_size"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
	TTLMultiple   int           `yaml:"ttl_multiple"`
	DefaultTTL    time.Duration `yaml:"default_ttl"`
	Publish       bool          `yaml:"publish"`
	KeyPrefix     string        `yaml:"key_prefix"`
	ChannelPrefix string        `yaml:"channel_prefix"`
}

// ClickHouseConfig configures ClickHouse connections and batch writing.
type ClickHouseConfig struct {
	Addrs           []string      `yaml:"addrs"`
	Database        string        `yaml:"database"`
	Username        string        `yaml:"username"`
	Password        string        `yaml:"password"`
	TablePrefix     string        `yaml:"table_prefix"`
	DialTimeout     time.Duration `yaml:"dial_timeout"`
	TLS             bool          `yaml:"tls"`
	BatchSize       int           `yaml:"batch_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	ChannelSize     int           `yaml:"channel_size"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`
	MaxRetryBackoff time.Duration `yaml:"max_retry_backoff"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// UniverseConfig configures Binance perpetual contract discovery and filtering.
type UniverseConfig struct {
	RESTURL         string        `yaml:"rest_url"`
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	RefreshOffset   time.Duration `yaml:"refresh_offset"`
	HTTPTimeout     time.Duration `yaml:"http_timeout"`
	QuoteAssets     []string      `yaml:"quote_assets"`
	ContractType    string        `yaml:"contract_type"`
	Status          string        `yaml:"status"`
}

// BackfillConfig configures historical gapfill and window rebuild.
type BackfillConfig struct {
	Enabled bool `yaml:"enabled"`
	// OfflineOnly runs a one-shot historical backfill into ClickHouse and then
	// exits, without starting the live WebSocket collector, dispatcher or Redis
	// writers. Use it for phase one of a two-phase cold start ("load deep
	// history offline, then start the live service"). Default false.
	OfflineOnly   bool          `yaml:"offline_only"`
	ColdStartDate string        `yaml:"cold_start_date"` // e.g. "2024-01-01" or RFC3339
	Workers       int           `yaml:"workers"`
	RestRPS       float64       `yaml:"rest_rps"`
	QueueSize     int           `yaml:"queue_size"`
	GapDebounce   time.Duration `yaml:"gap_debounce"`
	GateTimeout   time.Duration `yaml:"gate_timeout"`
	FlushWait     time.Duration `yaml:"flush_wait"`
}

// ParseColdStartTime parses ColdStartDate into time.Time (UTC). Supports "2006-01-02",
// "2006-01-02 15:04:05", and RFC3339. Returns zero time if ColdStartDate is empty.
func (b BackfillConfig) ParseColdStartTime() (time.Time, error) {
	if b.ColdStartDate == "" {
		return time.Time{}, nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, b.ColdStartDate); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid cold_start_date %q: must be YYYY-MM-DD or RFC3339", b.ColdStartDate)
}

// DefaultConfig returns complete production defaults.
func DefaultConfig() Config {
	return Config{
		App: AppConfig{
			LogLevel:        "info",
			LogFormat:       "text",
			MetricsAddr:     ":9090",
			ShutdownTimeout: 30 * time.Second,
		},
		Collector: CollectorConfig{
			Intervals:         []string{"1m", "1h"},
			ShardsPerInterval: 4,
			WSURL:             "wss://fstream.binance.com",
			ConnectStagger:    300 * time.Millisecond,
			ConnectTimeout:    20 * time.Second,
			ReconnectBase:     1 * time.Second,
			ReconnectMax:      30 * time.Second,
			StaleTimeout:      60 * time.Second,
			WatchdogInterval:  10 * time.Second,
		},
		Dispatcher: DispatcherConfig{
			ClosedWorkers:   4,
			ClosedQueueSize: 4096,
			SectionTimeout:  5 * time.Second,
			PublishTimeout:  5 * time.Second,
		},
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			DB:           0,
			Password:     "",
			PoolSize:     20,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			Window: WindowConfig{
				KeyPrefix:    "kline",
				WindowSize:   200,
				StreamKey:    "stream:market:kline_ready",
				StreamMaxLen: 10000,
				Atomic:       false,
			},
			Live: LiveConfig{
				Addr:          "",
				DB:            0,
				Password:      "",
				PoolSize:      8,
				Workers:       2,
				ChannelSize:   8192,
				WriteTimeout:  200 * time.Millisecond,
				TTLMultiple:   2,
				DefaultTTL:    2 * time.Hour,
				Publish:       false,
				KeyPrefix:     "livebar",
				ChannelPrefix: "livebar",
			},
		},
		ClickHouse: ClickHouseConfig{
			Addrs:           []string{"localhost:9000"},
			Database:        "market",
			Username:        "default",
			Password:        "",
			TablePrefix:     "market.fapi_kline",
			DialTimeout:     5 * time.Second,
			TLS:             false,
			BatchSize:       5000,
			FlushInterval:   1000 * time.Millisecond,
			ChannelSize:     20000,
			MaxRetries:      5,
			RetryBackoff:    200 * time.Millisecond,
			MaxRetryBackoff: 5 * time.Second,
			ShutdownTimeout: 15 * time.Second,
		},
		Universe: UniverseConfig{
			RESTURL:         "https://fapi.binance.com",
			RefreshInterval: 24 * time.Hour,
			RefreshOffset:   2 * time.Minute,
			HTTPTimeout:     20 * time.Second,
			QuoteAssets:     []string{"USDT"},
			ContractType:    "PERPETUAL",
			Status:          "TRADING",
		},
		Backfill: BackfillConfig{
			Enabled:       true,
			OfflineOnly:   false,
			ColdStartDate: "",
			Workers:       4,
			RestRPS:       20,
			QueueSize:     256,
			GapDebounce:   30 * time.Second,
			GateTimeout:   5 * time.Minute,
			FlushWait:     2 * time.Second,
		},
	}
}

// LoadYAML reads and decodes a YAML config file on top of the default configuration.
func LoadYAML(path string) (Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("decode config file: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// Validate verifies that required parameters and sanity ranges are met.
func (c *Config) Validate() error {
	if len(c.Collector.Intervals) == 0 {
		return fmt.Errorf("collector.intervals must not be empty")
	}
	if c.Collector.ShardsPerInterval <= 0 {
		return fmt.Errorf("collector.shards_per_interval must be > 0")
	}
	if c.Dispatcher.ClosedWorkers <= 0 {
		return fmt.Errorf("dispatcher.closed_workers must be > 0")
	}
	if c.Dispatcher.ClosedQueueSize <= 0 {
		return fmt.Errorf("dispatcher.closed_queue_size must be > 0")
	}
	if c.Redis.Window.WindowSize <= 0 {
		return fmt.Errorf("redis.window.window_size must be > 0")
	}
	if c.ClickHouse.BatchSize <= 0 {
		return fmt.Errorf("clickhouse.batch_size must be > 0")
	}
	if c.ClickHouse.ChannelSize <= 0 {
		return fmt.Errorf("clickhouse.channel_size must be > 0")
	}
	if c.Backfill.Enabled {
		if c.Backfill.Workers <= 0 {
			return fmt.Errorf("backfill.workers must be > 0")
		}
		if _, err := c.Backfill.ParseColdStartTime(); err != nil {
			return fmt.Errorf("backfill.cold_start_date: %w", err)
		}
	}
	if c.Backfill.OfflineOnly && !c.Backfill.Enabled {
		return fmt.Errorf("backfill.offline_only requires backfill.enabled = true")
	}
	return nil
}
