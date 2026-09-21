package openinterest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics is shared by every component of one Syncer.
type metrics struct {
	// live
	liveSnapshots     *prometheus.CounterVec // {result="ok|error"}
	liveCycleSeconds  prometheus.Histogram
	liveCycleComplete prometheus.Gauge // ok snapshots / symbols in the last cycle

	// hist
	histRequests      *prometheus.CounterVec // {result="ok|empty|error|invalid_symbol"}
	histRows          prometheus.Counter
	histSymbols       *prometheus.CounterVec // {outcome="fetched|skipped|failed"}
	histPassSeconds   prometheus.Histogram
	histLastPass      prometheus.Gauge // unix seconds of the last finished pass
	histBeyondCap     prometheus.Counter
	crossSection      prometheus.Gauge // symbols whose newest hist row is current / universe size
	liveVsHistRelDiff prometheus.Histogram
	liveGapBars       prometheus.Counter

	// http / pools
	httpRequests *prometheus.CounterVec // {endpoint, result}
	rateLimited  *prometheus.CounterVec // {pool, code}
	dataWindow   prometheus.Gauge       // /futures/data requests in the current 5-minute window

	// writer
	rowsWritten  prometheus.Counter
	rowsDropped  prometheus.Counter
	flushErrors  prometheus.Counter
	flushSeconds prometheus.Histogram
	bufferRows   prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		liveSnapshots: f.NewCounterVec(prometheus.CounterOpts{
			Name: "oi_live_snapshots_total",
			Help: "Live open-interest snapshots by outcome.",
		}, []string{"result"}),
		liveCycleSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "oi_live_cycle_seconds",
			Help:    "Wall-clock duration of one whole-market live snapshot round.",
			Buckets: []float64{2, 5, 10, 15, 20, 30, 45, 60, 90},
		}),
		liveCycleComplete: f.NewGauge(prometheus.GaugeOpts{
			Name: "oi_live_cycle_complete_ratio",
			Help: "Fraction of the universe that got an accepted live snapshot in the last round.",
		}),
		histRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "oi_hist_requests_total",
			Help: "openInterestHist requests by outcome.",
		}, []string{"result"}),
		histRows: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_hist_rows_total",
			Help: "Rows produced from openInterestHist.",
		}),
		histSymbols: f.NewCounterVec(prometheus.CounterOpts{
			Name: "oi_hist_symbols_total",
			Help: "Per-symbol outcome of hist passes (skipped = already up to date).",
		}, []string{"outcome"}),
		histPassSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "oi_hist_pass_seconds",
			Help:    "Duration of one hist pass (start-up catch-up or hourly calibration).",
			Buckets: []float64{5, 30, 120, 300, 600, 1200, 1800, 3600},
		}),
		histLastPass: f.NewGauge(prometheus.GaugeOpts{
			Name: "oi_hist_last_pass_timestamp_seconds",
			Help: "Unix time the last hist pass finished.",
		}),
		histBeyondCap: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_hist_gap_beyond_cap_total",
			Help: "Symbols whose gap was older than hist_max_backfill; the remainder needs the archive.",
		}),
		crossSection: f.NewGauge(prometheus.GaugeOpts{
			Name: "oi_cross_section_complete_ratio",
			Help: "After a hist pass: symbols whose newest hist row is current / universe size.",
		}),
		liveVsHistRelDiff: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "oi_live_vs_hist_rel_diff",
			Help:    "|live - hist| / hist for the same (symbol, bar), observed when hist calibrates a live row.",
			Buckets: []float64{1e-5, 5e-5, 1e-4, 2e-4, 5e-4, 1e-3, 2e-3, 5e-3, 1e-2, 5e-2},
		}),
		liveGapBars: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_live_gap_bars_total",
			Help: "Bars that hist returned but live had no snapshot for (while live was running).",
		}),
		httpRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "oi_http_requests_total",
			Help: "Binance REST requests made by the open-interest module.",
		}, []string{"endpoint", "result"}),
		rateLimited: f.NewCounterVec(prometheus.CounterOpts{
			Name: "oi_rate_limited_total",
			Help: "429 / 418 responses seen, by pool.",
		}, []string{"pool", "code"}),
		dataWindow: f.NewGauge(prometheus.GaugeOpts{
			Name: "oi_data_window_used",
			Help: "Requests to /futures/data/* in the current sliding 5-minute window (limit 1000 per IP).",
		}),
		rowsWritten: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_rows_written_total",
			Help: "Rows flushed to ClickHouse.",
		}),
		rowsDropped: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_writer_rows_dropped_total",
			Help: "Rows dropped after exhausting flush retries.",
		}),
		flushErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "oi_writer_flush_errors_total",
			Help: "Failed ClickHouse flush attempts.",
		}),
		flushSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "oi_writer_flush_seconds",
			Help:    "ClickHouse flush duration.",
			Buckets: prometheus.DefBuckets,
		}),
		bufferRows: f.NewGauge(prometheus.GaugeOpts{
			Name: "oi_writer_buffer_rows",
			Help: "Rows queued or buffered in the writer.",
		}),
	}
}
