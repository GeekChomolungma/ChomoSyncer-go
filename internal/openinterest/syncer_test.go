package openinterest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("zero config must be valid (all defaults): %v", err)
	}
	bad := map[string]Config{
		"lead >= bar":               {Live: LiveConfig{Lead: 5 * time.Minute}},
		"accept window too wide":    {Live: LiveConfig{AcceptWindow: 5 * time.Minute}},
		"lead below min remaining":  {Live: LiveConfig{Lead: 3 * time.Second}},
		"offset >= interval":        {Hist: HistConfig{Interval: time.Hour, Offset: time.Hour}},
		"spread > interval":         {Hist: HistConfig{Interval: time.Hour, Spread: 2 * time.Hour}},
		"limit > 500":               {Hist: HistConfig{MaxLimit: 600}},
		"cold start > max backfill": {Hist: HistConfig{ColdStartWindow: 200 * time.Hour, MaxBackfill: 100 * time.Hour}},
		"backfill beyond retention": {Hist: HistConfig{MaxBackfill: 40 * 24 * time.Hour}},
	}
	for name, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestNewRequiresCollaborators(t *testing.T) {
	syms := func() []string { return []string{"AAA"} }
	sink := &fakeSink{}
	if _, err := New(Config{LiveEnabled: true}, Deps{Symbols: syms, Sink: sink}); err == nil {
		t.Error("live without a SnapshotSource must fail")
	}
	if _, err := New(Config{HistEnabled: true}, Deps{Symbols: syms, Sink: sink, History: newFakeAPI()}); err == nil {
		t.Error("hist without a Store must fail")
	}
	if _, err := New(Config{}, Deps{Sink: sink}); err == nil {
		t.Error("missing Symbols must fail")
	}
	if _, err := New(Config{Live: LiveConfig{Lead: time.Hour}}, Deps{Symbols: syms, Sink: sink}); err == nil {
		t.Error("an invalid config must fail")
	}
}

func TestBuildReturnsNilWhenDisabled(t *testing.T) {
	s, err := Build(Config{}, CHConfig{Addrs: []string{"127.0.0.1:1"}}, "", nil, func() []string { return nil }, nil, nil)
	if s != nil || err != nil {
		t.Fatalf("Build with nothing enabled = (%v, %v), want (nil, nil)", s, err)
	}
}

type orderCloser struct {
	name string
	mu   *sync.Mutex
	log  *[]string
}

func (c orderCloser) Close() error {
	c.mu.Lock()
	*c.log = append(*c.log, c.name)
	c.mu.Unlock()
	return nil
}

func TestSyncerLifecycle(t *testing.T) {
	// real clock: the live loop just waits for the next boundary; hist runs its start-up pass
	api := newFakeAPI()
	now := time.Now().UTC()
	floor := floorBar(now)
	api.setSeries("AAA", floor.Add(-6*time.Hour).Format("2006-01-02 15:04:05.000"), floor.Format("2006-01-02 15:04:05.000"), 1)
	store := &fakeStore{data: map[string]LastStarts{"AAA": {Hist: floor.Add(-3 * time.Hour), Any: floor.Add(-3 * time.Hour)}}}
	sink := &fakeSink{}
	var mu sync.Mutex
	var order []string

	s, err := New(Config{LiveEnabled: true, HistEnabled: true}, Deps{
		Symbols: func() []string { return []string{"AAA"} }, Snapshots: &fakeSnaps{fn: func(string) (Snapshot, error) { return Snapshot{}, errors.New("not reached") }},
		History: api, Store: store, Sink: sink,
		Closers: []io.Closer{orderCloser{"writer", &mu, &order}, orderCloser{"store", &mu, &order}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("a second Start must fail")
	}
	eventually(t, 3*time.Second, func() bool { return sink.count() > 0 }) // the hist start-up pass wrote rows
	for _, r := range sink.forSymbol("AAA") {
		if r.SrcRank != RankHist {
			t.Fatalf("unexpected rank %d", r.SrcRank)
		}
	}

	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung although the live loop was only waiting for the next boundary")
	}
	if strings.Join(order, ",") != "writer,store" {
		t.Fatalf("closers ran in order %v, want writer then store", order)
	}
	if err := s.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("Start after Close must fail")
	}
}
