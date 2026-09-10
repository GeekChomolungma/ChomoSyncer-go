package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.metricsAddr != ":9090" || c.redisAddr != "localhost:6379" || c.chAddr != "localhost:9000" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.intervals != "1m" || c.shards != 4 {
		t.Fatalf("interval/shard defaults wrong: %+v", c)
	}
	if c.serveIntervals != "5m,15m,1h,4h,1d" {
		t.Fatalf("serve-intervals default wrong: %+v", c)
	}
	if c.sectionTimeout != 5*time.Second {
		t.Fatalf("section timeout default = %v", c.sectionTimeout)
	}
	if c.windowSize != 200 {
		t.Fatalf("window size default = %d, want 200", c.windowSize)
	}
}

func TestParseFlagsOverride(t *testing.T) {
	c, err := parseFlags([]string{
		"-metrics-addr", "off",
		"-redis-addr", "redis:6380",
		"-ch-addr", "ch-a:9000,ch-b:9000",
		"-serve-intervals", "5m, 15m ,1h",
		"-shards-per-interval", "8",
		"-live-publish",
		"-section-timeout", "3s",
		"-log-format", "json",
		"-window-size", "500",
		"-dispatcher-closed-workers", "12",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.metricsAddr != "off" || c.redisAddr != "redis:6380" || !c.livePublish {
		t.Fatalf("overrides not applied: %+v", c)
	}
	if c.shards != 8 || c.sectionTimeout != 3*time.Second || c.logFormat != "json" {
		t.Fatalf("overrides wrong: %+v", c)
	}
	if c.windowSize != 500 || c.dispatcherClosedWorkers != 12 {
		t.Fatalf("new high-priority overrides wrong: %+v", c)
	}

	ac := c.toAppConfig(newLogger("info", "text"))
	if !reflect.DeepEqual(ac.ClickHouse.Addrs, []string{"ch-a:9000", "ch-b:9000"}) {
		t.Fatalf("ClickHouse.Addrs = %v", ac.ClickHouse.Addrs)
	}
	if !reflect.DeepEqual(ac.Collector.ServeIntervals, []string{"5m", "15m", "1h"}) {
		t.Fatalf("ServeIntervals = %v (CSV must trim spaces)", ac.Collector.ServeIntervals)
	}
	if ac.App.MetricsAddr != "off" || ac.Version != version {
		t.Fatalf("toAppConfig mismatch: %+v", ac)
	}
	if ac.Redis.Window.WindowSize != 500 || ac.Dispatcher.ClosedWorkers != 12 {
		t.Fatalf("ac overrides mismatch: %+v", ac)
	}
}

func TestEnvOverridesDefault(t *testing.T) {
	t.Setenv("CHOMOSYNCER_REDIS_ADDR", "envredis:6379")
	t.Setenv("CHOMOSYNCER_SHARDS_PER_INTERVAL", "6")
	t.Setenv("CHOMOSYNCER_LIVE_PUBLISH", "true")
	t.Setenv("CHOMOSYNCER_WINDOW_SIZE", "350")

	c, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.redisAddr != "envredis:6379" || c.shards != 6 || !c.livePublish || c.windowSize != 350 {
		t.Fatalf("env not honored: %+v", c)
	}

	// explicit flag still beats env
	c, err = parseFlags([]string{"-redis-addr", "flagredis:6379", "-window-size", "400"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.redisAddr != "flagredis:6379" {
		t.Fatalf("flag should override env, got %q", c.redisAddr)
	}
	if c.windowSize != 400 {
		t.Fatalf("flag should override env, got %d", c.windowSize)
	}
}

func TestYAMLConfigLoading(t *testing.T) {
	yamlContent := `
redis:
  window:
    window_size: 600
dispatcher:
  closed_workers: 16
clickhouse:
  batch_size: 3000
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test_config.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}

	c, err := parseFlags([]string{"-config", cfgPath})
	if err != nil {
		t.Fatalf("parseFlags with -config: %v", err)
	}

	if c.windowSize != 600 {
		t.Fatalf("windowSize = %d, want 600 from YAML", c.windowSize)
	}
	if c.dispatcherClosedWorkers != 16 {
		t.Fatalf("dispatcherClosedWorkers = %d, want 16 from YAML", c.dispatcherClosedWorkers)
	}
	if c.chBatchSize != 3000 {
		t.Fatalf("chBatchSize = %d, want 3000 from YAML", c.chBatchSize)
	}
}

func TestCLIOverridesYAML(t *testing.T) {
	yamlContent := `
redis:
  window:
    window_size: 600
dispatcher:
  closed_workers: 16
clickhouse:
  batch_size: 3000
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test_config.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}

	// Override window-size on CLI, but keep closed_workers from YAML
	c, err := parseFlags([]string{"-config", cfgPath, "-window-size", "800"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	if c.windowSize != 800 {
		t.Fatalf("windowSize = %d, want 800 (CLI override)", c.windowSize)
	}
	if c.dispatcherClosedWorkers != 16 {
		t.Fatalf("dispatcherClosedWorkers = %d, want 16 (from YAML)", c.dispatcherClosedWorkers)
	}
	if c.chBatchSize != 3000 {
		t.Fatalf("chBatchSize = %d, want 3000 (from YAML)", c.chBatchSize)
	}
}

func TestParseFlagsHelpAndBadFlag(t *testing.T) {
	if _, err := parseFlags([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h err = %v, want flag.ErrHelp", err)
	}
	if _, err := parseFlags([]string{"-nope"}); err == nil {
		t.Fatal("unknown flag should error")
	}
}

func TestNewLoggerLevels(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error", "bogus"} {
		if newLogger(lvl, "text") == nil {
			t.Fatalf("newLogger(%q) returned nil", lvl)
		}
	}
	if newLogger("info", "json") == nil {
		t.Fatal("json logger nil")
	}
}

func TestParseFlagsBackfillDefaults(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !c.backfill || c.backfillRestRPS != 20 || c.backfillWorkers != 4 {
		t.Fatalf("backfill defaults: %+v", c)
	}
	if c.backfillGapDebounce != 30*time.Second || c.backfillGateTimeout != 5*time.Minute {
		t.Fatalf("backfill duration defaults: %+v", c)
	}
	if c.backfillStartDate != "" {
		t.Fatalf("backfill start date default: %+v", c)
	}
	if c.backfillOfflineOnly {
		t.Fatalf("backfill offline-only default should be false: %+v", c)
	}

	c2, err := parseFlags([]string{
		"-backfill=false",
		"-backfill-rest-rps", "50",
		"-backfill-start-date", "2024-01-01",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c2.backfill || c2.backfillRestRPS != 50 || c2.backfillStartDate != "2024-01-01" {
		t.Fatalf("backfill overrides: %+v", c2)
	}

	c4, err := parseFlags([]string{"-backfill-offline-only", "-backfill-start-date", "2024-01-01"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !c4.backfillOfflineOnly {
		t.Fatalf("-backfill-offline-only not honored: %+v", c4)
	}

	t.Setenv("CHOMOSYNCER_BACKFILL", "false")
	t.Setenv("CHOMOSYNCER_BACKFILL_START_DATE", "2024-06-01")
	c3, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c3.backfill {
		t.Fatal("env CHOMOSYNCER_BACKFILL=false not honored")
	}
	if c3.backfillStartDate != "2024-06-01" {
		t.Fatalf("env CHOMOSYNCER_BACKFILL_START_DATE not honored: %v", c3.backfillStartDate)
	}
}
