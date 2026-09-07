// Package metrics owns the single Prometheus registry the whole service shares
// and serves it over HTTP.
//
// Every other module (chwriter, rediswin, dispatcher, collector, universe)
// takes a prometheus.Registerer in its Config and self-registers its metrics
// via promauto. cmd/chomosyncer-go passes Metrics.Registerer() to all of them, so
// one /metrics endpoint exposes the union of their metrics plus the Go runtime
// and process collectors.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Defaults.
const (
	DefaultAddr            = ":9090"
	DefaultPath            = "/metrics"
	DefaultShutdownTimeout = 5 * time.Second
)

// Config configures the metrics module.
type Config struct {
	// Addr is the listen address for the HTTP server, e.g. ":9090". Empty
	// disables the server; Registerer / Handler still work (useful in tests or
	// when embedding /metrics in another mux).
	Addr string
	// Path is the metrics endpoint. Defaults to "/metrics".
	Path string
	// ShutdownTimeout bounds graceful shutdown when Close's ctx has no deadline.
	// Defaults to 5s.
	ShutdownTimeout time.Duration
	// DisableRuntimeMetrics skips the Go runtime and process collectors.
	DisableRuntimeMetrics bool
	// Version is stamped into chomosyncer_build_info; falls back to the module
	// build version when empty.
	Version string
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Metrics is the shared registry plus its HTTP server.
type Metrics struct {
	reg     *prometheus.Registry
	handler http.Handler
	cfg     Config
	log     *slog.Logger

	srv     *http.Server
	ln      net.Listener
	serveWG sync.WaitGroup

	startOnce sync.Once
	closeOnce sync.Once
}

// New builds the registry, registers the runtime/process/build-info collectors,
// and prepares (but does not start) the HTTP server.
func New(cfg Config) (*Metrics, error) {
	if cfg.Path == "" {
		cfg.Path = DefaultPath
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "metrics")

	reg := prometheus.NewRegistry()
	if !cfg.DisableRuntimeMetrics {
		if err := reg.Register(collectors.NewGoCollector()); err != nil {
			return nil, fmt.Errorf("metrics: register go collector: %w", err)
		}
		if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
			return nil, fmt.Errorf("metrics: register process collector: %w", err)
		}
	}

	m := &Metrics{reg: reg, cfg: cfg, log: log}
	m.registerBuildInfo()

	// Build the instrumented handler exactly once: InstrumentMetricHandler
	// registers counters on reg and panics if called twice.
	base := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		ErrorLog:          slogErrorLog{log},
	})
	m.handler = promhttp.InstrumentMetricHandler(reg, base)
	return m, nil
}

func (m *Metrics) registerBuildInfo() {
	version := m.cfg.Version
	goVersion := "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		goVersion = bi.GoVersion
		if version == "" && bi.Main.Version != "" {
			version = bi.Main.Version
		}
	}
	if version == "" {
		version = "dev"
	}
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "chomosyncer_build_info",
		Help: "Build metadata; value is always 1.",
	}, []string{"version", "go_version"})
	g.WithLabelValues(version, goVersion).Set(1)
	m.reg.MustRegister(g)
}

// Registerer is handed to every module's Config.Registerer.
func (m *Metrics) Registerer() prometheus.Registerer { return m.reg }

// Gatherer exposes the registry (tests, custom scraping).
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.reg }

// Handler is the instrumented promhttp handler for the shared registry. Use it
// to embed /metrics in an existing mux instead of calling Start.
func (m *Metrics) Handler() http.Handler { return m.handler }

// Addr is the bound listen address once Start has run ("" if disabled / not started).
func (m *Metrics) Addr() string {
	if m.ln == nil {
		return ""
	}
	return m.ln.Addr().String()
}

// Start binds the listener synchronously (a port clash fails fast) then serves
// in the background. No-op when Addr is empty. Idempotent.
func (m *Metrics) Start() error {
	var startErr error
	m.startOnce.Do(func() {
		if m.cfg.Addr == "" {
			m.log.Info("metrics HTTP server disabled (empty Addr)")
			return
		}
		ln, err := net.Listen("tcp", m.cfg.Addr)
		if err != nil {
			startErr = fmt.Errorf("metrics: listen %q: %w", m.cfg.Addr, err)
			return
		}

		mux := http.NewServeMux()
		mux.Handle(m.cfg.Path, m.handler)
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		})

		m.ln = ln
		m.srv = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		m.serveWG.Add(1)
		go func() {
			defer m.serveWG.Done()
			m.log.Info("metrics HTTP server listening", "addr", ln.Addr().String(), "path", m.cfg.Path)
			if err := m.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				m.log.Error("metrics HTTP server terminated", "err", err)
			}
		}()
	})
	return startErr
}

// Close gracefully shuts the server down, bounded by ctx (or ShutdownTimeout
// when ctx has no deadline), and waits for the serve goroutine to exit. No-op
// when the server never started. Idempotent.
func (m *Metrics) Close(ctx context.Context) error {
	var err error
	m.closeOnce.Do(func() {
		if m.srv == nil {
			return
		}
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, m.cfg.ShutdownTimeout)
			defer cancel()
		}
		err = m.srv.Shutdown(ctx)
		m.serveWG.Wait()
		m.log.Info("metrics HTTP server stopped")
	})
	return err
}

type slogErrorLog struct{ l *slog.Logger }

func (s slogErrorLog) Println(v ...any) { s.l.Error(fmt.Sprint(v...)) }
