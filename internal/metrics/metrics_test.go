package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegistererCollectsModuleMetrics(t *testing.T) {
	m, err := New(Config{Addr: ""})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// A "module" registers via the shared Registerer.
	c := promauto.With(m.Registerer()).NewCounter(prometheus.CounterOpts{
		Name: "fake_module_events_total", Help: "x",
	})
	c.Add(3)

	if got := testutil.ToFloat64(c); got != 3 {
		t.Fatalf("counter = %v", got)
	}

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{
		"fake_module_events_total",
		"chomosyncer_build_info",
		"go_goroutines",              // Go collector
		"process_start_time_seconds", // process collector
	} {
		if !names[want] {
			t.Errorf("missing metric family %q", want)
		}
	}
}

func TestHTTPServerServesMetrics(t *testing.T) {
	m, err := New(Config{Addr: "127.0.0.1:0", Version: "test-1.2.3"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	promauto.With(m.Registerer()).NewGauge(prometheus.GaugeOpts{Name: "probe_gauge", Help: "x"}).Set(1)

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr := m.Addr()
	if addr == "" {
		t.Fatal("Addr empty after Start")
	}

	body := httpGet(t, "http://"+addr+"/metrics")
	for _, want := range []string{"probe_gauge", `chomosyncer_build_info{go_version=`, `version="test-1.2.3"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}

	if code, _ := httpGetCode(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz code = %d", code)
	}

	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Server is really down.
	if _, err := http.Get("http://" + addr + "/metrics"); err == nil {
		t.Fatal("expected connection error after Close")
	}
}

func TestHandlerEmbeddable(t *testing.T) {
	m, err := New(Config{Addr: ""}) // server disabled; use Handler directly
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	promauto.With(m.Registerer()).NewCounter(prometheus.CounterOpts{Name: "embed_total", Help: "x"}).Inc()

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	if !strings.Contains(httpGet(t, srv.URL), "embed_total") {
		t.Fatal("embedded handler did not serve the metric")
	}
}

func TestDisabledServer(t *testing.T) {
	m, err := New(Config{Addr: ""})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start (disabled) should be nil, got %v", err)
	}
	if m.Addr() != "" {
		t.Fatalf("Addr = %q, want empty", m.Addr())
	}
	if m.Registerer() == nil {
		t.Fatal("Registerer nil")
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close (disabled) should be nil, got %v", err)
	}
}

func TestStartPortClashFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	m, err := New(Config{Addr: ln.Addr().String()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.Start(); err == nil {
		t.Fatal("expected Start to fail on a taken port")
		_ = m.Close(context.Background())
	}
}

func TestCloseIdempotent(t *testing.T) {
	m, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
}

func TestDuplicateRuntimeCollectorsDisabled(t *testing.T) {
	m, err := New(Config{Addr: "", DisableRuntimeMetrics: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	families, _ := m.Gatherer().Gather()
	for _, f := range families {
		if f.GetName() == "go_goroutines" {
			t.Fatal("go collector should be absent when DisableRuntimeMetrics")
		}
	}
}

// --- helpers ---

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	return string(b)
}

func httpGetCode(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}
