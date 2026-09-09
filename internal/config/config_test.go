package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig() should be valid, got: %v", err)
	}
	if cfg.Redis.Window.WindowSize != 200 {
		t.Fatalf("expected WindowSize 200, got %d", cfg.Redis.Window.WindowSize)
	}
	if cfg.ClickHouse.BatchSize != 5000 {
		t.Fatalf("expected BatchSize 5000, got %d", cfg.ClickHouse.BatchSize)
	}
	if len(cfg.Universe.QuoteAssets) != 1 || cfg.Universe.QuoteAssets[0] != "USDT" {
		t.Fatalf("expected QuoteAssets [USDT], got %v", cfg.Universe.QuoteAssets)
	}
}

func TestLoadYAMLOverrides(t *testing.T) {
	yamlContent := `
app:
  log_level: "debug"

redis:
  window:
    window_size: 500

dispatcher:
  closed_workers: 8

clickhouse:
  batch_size: 2000

universe:
  quote_assets:
    - "USDT"
    - "USDC"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := LoadYAML(cfgPath)
	if err != nil {
		t.Fatalf("LoadYAML failed: %v", err)
	}

	if cfg.App.LogLevel != "debug" {
		t.Errorf("LogLevel = %s, want debug", cfg.App.LogLevel)
	}
	if cfg.Redis.Window.WindowSize != 500 {
		t.Errorf("WindowSize = %d, want 500", cfg.Redis.Window.WindowSize)
	}
	if cfg.Dispatcher.ClosedWorkers != 8 {
		t.Errorf("ClosedWorkers = %d, want 8", cfg.Dispatcher.ClosedWorkers)
	}
	if cfg.ClickHouse.BatchSize != 2000 {
		t.Errorf("BatchSize = %d, want 2000", cfg.ClickHouse.BatchSize)
	}
	// Non-overridden fields should retain defaults
	if cfg.Collector.ShardsPerInterval != 4 {
		t.Errorf("Collector.ShardsPerInterval = %d, want default 4", cfg.Collector.ShardsPerInterval)
	}
	if cfg.Backfill.GateTimeout != 5*time.Minute {
		t.Errorf("Backfill.GateTimeout = %v, want default 5m", cfg.Backfill.GateTimeout)
	}
	if len(cfg.Universe.QuoteAssets) != 2 || cfg.Universe.QuoteAssets[1] != "USDC" {
		t.Errorf("QuoteAssets = %v, want [USDT, USDC]", cfg.Universe.QuoteAssets)
	}
}

func TestValidateCatchesInvalid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Redis.Window.WindowSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero WindowSize")
	}

	cfg = DefaultConfig()
	cfg.Collector.Intervals = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty Intervals")
	}

	cfg = DefaultConfig()
	cfg.Backfill.ColdStartDate = "invalid-date"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid cold_start_date")
	}

	cfg = DefaultConfig()
	cfg.Backfill.ColdStartDate = "2024-01-01"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid for 2024-01-01, got %v", err)
	}
	tParsed, err := cfg.Backfill.ParseColdStartTime()
	if err != nil || tParsed.Year() != 2024 {
		t.Fatalf("ParseColdStartTime got %v, err: %v", tParsed, err)
	}

	cfg = DefaultConfig()
	cfg.Backfill.Enabled = false
	cfg.Backfill.OfflineOnly = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for offline_only without enabled")
	}

	cfg = DefaultConfig()
	cfg.Backfill.OfflineOnly = true // Enabled defaults to true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid for offline_only + enabled, got %v", err)
	}
}
