package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIsDerivableInterval(t *testing.T) {
	ok := []string{"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "300s"}
	bad := []string{"1m", "1s", "0m", "1w", "2d", "3d", "1M", "", "h", "1.5h", "90s" /* not a multiple of 60s */}
	for _, s := range ok {
		if !IsDerivableInterval(s) {
			t.Errorf("IsDerivableInterval(%q) = false, want true", s)
		}
	}
	for _, s := range bad {
		if IsDerivableInterval(s) {
			t.Errorf("IsDerivableInterval(%q) = true, want false", s)
		}
	}
}

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
	cfg.Collector.Intervals = []string{"1m", "1h"}
	if err := cfg.Validate(); err == nil {
		t.Fatal(`expected error: collector.intervals must be exactly ["1m"]`)
	}

	cfg = DefaultConfig()
	cfg.Collector.ServeIntervals = []string{"1h", "1w"} // 1w not derivable
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-derivable serve interval 1w")
	}

	cfg = DefaultConfig()
	cfg.Collector.ServeIntervals = []string{"5m", "15m", "1h", "4h", "1d"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid for standard serve intervals, got %v", err)
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

func TestWeightGateDefaultsAndYAMLOverride(t *testing.T) {
	def := DefaultConfig().WeightGate
	if def.LiveBudget != 600 || def.BulkBudget != 1200 || def.MiscBudget != 100 || def.SoftLimit != 1800 || def.HardLimit != 2300 {
		t.Fatalf("unexpected default weight_gate: %+v", def)
	}

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("weight_gate:\n  bulk_budget: 900\n  soft_limit: 1500\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadYAML(cfgPath)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	if cfg.WeightGate.BulkBudget != 900 || cfg.WeightGate.SoftLimit != 1500 {
		t.Errorf("overrides not applied: %+v", cfg.WeightGate)
	}
	if cfg.WeightGate.LiveBudget != 600 || cfg.WeightGate.HardLimit != 2300 {
		t.Errorf("non-overridden fields should keep defaults: %+v", cfg.WeightGate)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("a valid override must pass Validate: %v", err)
	}
}

func TestValidateRejectsBadWeightGate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WeightGate.BulkBudget = 2000 // 600 + 2000 + 100 > 2400
	if err := cfg.Validate(); err == nil {
		t.Error("expected error: budgets exceed the 2400/min exchange cap")
	}

	cfg = DefaultConfig()
	cfg.WeightGate.SoftLimit, cfg.WeightGate.HardLimit = 2200, 2000
	if err := cfg.Validate(); err == nil {
		t.Error("expected error: soft_limit above hard_limit")
	}

	cfg = DefaultConfig()
	cfg.WeightGate.LiveBudget = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error: negative budget")
	}
}

func TestOpenInterestDefaultsAreOffAndValid(t *testing.T) {
	cfg := DefaultConfig()
	oi := cfg.OpenInterest
	if oi.HistEnabled || oi.LiveEnabled {
		t.Fatal("open_interest must be off by default")
	}
	if oi.Table != "market.fapi_oi_5m" || oi.LiveLead != 30*time.Second || oi.HistReconcileSpread != 20*time.Minute ||
		oi.HistReconcileInterval != time.Hour || oi.DataWindowCap != 900 || oi.HistMaxLimit != 500 {
		t.Fatalf("unexpected defaults: %+v", oi)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	m := oi.ToModule()
	if m.Live.Lead != 30*time.Second || m.Hist.Spread != 20*time.Minute || m.DataRPS != 2 || m.Live.RPS != 25 {
		t.Fatalf("ToModule mapping wrong: %+v", m)
	}
}

func TestOpenInterestYAMLOverride(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	yamlContent := "open_interest:\n  hist_enabled: true\n  live_enabled: true\n  live_lead: 20s\n  hist_reconcile_spread: 10m\n  hist_max_backfill: 72h\n"
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadYAML(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	oi := cfg.OpenInterest
	if !oi.HistEnabled || !oi.LiveEnabled || oi.LiveLead != 20*time.Second || oi.HistReconcileSpread != 10*time.Minute || oi.HistMaxBackfill != 72*time.Hour {
		t.Fatalf("overrides not applied: %+v", oi)
	}
	if oi.FapiRPS != 25 || oi.HistWorkers != 4 { // untouched fields keep their defaults
		t.Fatalf("defaults lost: %+v", oi)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid override rejected: %v", err)
	}
}

func TestValidateRejectsBadOpenInterest(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"lead as long as the bar":    func(c *Config) { c.OpenInterest.LiveLead = 5 * time.Minute },
		"accept window too wide":     func(c *Config) { c.OpenInterest.LiveAcceptWindow = 3 * time.Minute },
		"offset beyond the interval": func(c *Config) { c.OpenInterest.HistReconcileOffset = 2 * time.Hour },
		"limit above Binance's max":  func(c *Config) { c.OpenInterest.HistMaxLimit = 1000 },
		"backfill beyond retention":  func(c *Config) { c.OpenInterest.HistMaxBackfill = 60 * 24 * time.Hour },
		"cold start beyond the cap":  func(c *Config) { c.OpenInterest.HistColdStartWindow = 200 * time.Hour },
	} {
		cfg := DefaultConfig()
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}
