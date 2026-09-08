package main

import (
	"errors"
	"flag"
	"reflect"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	c, _ := parseFlags(nil)
	if c.metricsAddr != ":9090" || c.redisAddr != "localhost:6379" || c.chAddr != "localhost:9000" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.intervals != "1m,1h" || c.shards != 4 {
		t.Fatalf("interval/shard defaults wrong: %+v", c)
	}
	if c.sectionTimeout != 5*time.Second {
		t.Fatalf("section timeout default = %v", c.sectionTimeout)
	}
}

func TestParseFlagsOverride(t *testing.T) {
	c, _ := parseFlags([]string{
		"-metrics-addr", "off",
		"-redis-addr", "redis:6380",
		"-ch-addr", "ch-a:9000,ch-b:9000",
		"-intervals", "1m, 5m ,1h",
		"-shards-per-interval", "8",
		"-live-publish",
		"-section-timeout", "3s",
		"-log-format", "json",
	})
	if c.metricsAddr != "off" || c.redisAddr != "redis:6380" || !c.livePublish {
		t.Fatalf("overrides not applied: %+v", c)
	}
	if c.shards != 8 || c.sectionTimeout != 3*time.Second || c.logFormat != "json" {
		t.Fatalf("overrides wrong: %+v", c)
	}

	ac := c.toAppConfig(newLogger("info", "text"))
	if !reflect.DeepEqual(ac.CHAddrs, []string{"ch-a:9000", "ch-b:9000"}) {
		t.Fatalf("CHAddrs = %v", ac.CHAddrs)
	}
	if !reflect.DeepEqual(ac.Intervals, []string{"1m", "5m", "1h"}) {
		t.Fatalf("Intervals = %v (CSV must trim spaces)", ac.Intervals)
	}
	if ac.MetricsAddr != "off" || ac.Version != version {
		t.Fatalf("toAppConfig mismatch: %+v", ac)
	}
}

func TestEnvOverridesDefault(t *testing.T) {
	t.Setenv("CHOMOSYNCER_REDIS_ADDR", "envredis:6379")
	t.Setenv("CHOMOSYNCER_SHARDS_PER_INTERVAL", "6")
	t.Setenv("CHOMOSYNCER_LIVE_PUBLISH", "true")

	c, _ := parseFlags(nil)
	if c.redisAddr != "envredis:6379" || c.shards != 6 || !c.livePublish {
		t.Fatalf("env not honored: %+v", c)
	}

	// explicit flag still beats env
	c, _ = parseFlags([]string{"-redis-addr", "flagredis:6379"})
	if c.redisAddr != "flagredis:6379" {
		t.Fatalf("flag should override env, got %q", c.redisAddr)
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
	c, _ := parseFlags(nil)
	if !c.backfill || c.backfillRestRPS != 20 || c.backfillWorkers != 4 {
		t.Fatalf("backfill defaults: %+v", c)
	}
	if c.backfillGapDebounce != 30*time.Second || c.backfillGateTimeout != 5*time.Minute {
		t.Fatalf("backfill duration defaults: %+v", c)
	}

	c2, _ := parseFlags([]string{"-backfill=false", "-backfill-rest-rps", "50"})
	if c2.backfill || c2.backfillRestRPS != 50 {
		t.Fatalf("backfill overrides: %+v", c2)
	}

	t.Setenv("CHOMOSYNCER_BACKFILL", "false")
	c3, _ := parseFlags(nil)
	if c3.backfill {
		t.Fatal("env CHOMOSYNCER_BACKFILL=false not honored")
	}
}
