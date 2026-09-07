package dispatcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	events         *prometheus.CounterVec   // {interval,kind}  kind=live|closed
	dropped        *prometheus.CounterVec   // {sink}           sink=live|closed|archive
	sinkErrors     *prometheus.CounterVec   // {sink}           sink=window
	parseErrors    prometheus.Counter       // event field parse failures
	outOfOrder     *prometheus.CounterVec   // {interval}       monotonic-guard rejections
	sectionPub     *prometheus.CounterVec   // {interval,reason} reason=complete|timeout
	sectionErrors  prometheus.Counter       // kline_ready publish failures
	sectionPending prometheus.Gauge         // open (unpublished + retained) sections
	sectionSize    *prometheus.HistogramVec // {interval} symbols_count at publish
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		events: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_events_total",
			Help: "Kline events handled by the dispatcher.",
		}, []string{"interval", "kind"}),
		dropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_events_dropped_total",
			Help: "Events dropped because a downstream sink's buffer was full.",
		}, []string{"sink"}),
		sinkErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_sink_errors_total",
			Help: "Downstream sink write errors (non-buffer-full).",
		}, []string{"sink"}),
		parseErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_parse_errors_total",
			Help: "Kline events dropped because a numeric field failed to parse.",
		}),
		outOfOrder: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_out_of_order_total",
			Help: "Closed bars dropped by the monotonic guard (replay / reorder).",
		}, []string{"interval"}),
		sectionPub: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_section_published_total",
			Help: "kline_ready cross-section events published.",
		}, []string{"interval", "reason"}),
		sectionErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_section_publish_errors_total",
			Help: "kline_ready publish failures.",
		}),
		sectionPending: f.NewGauge(prometheus.GaugeOpts{
			Name: "dispatcher_section_pending",
			Help: "Cross-sections currently tracked (awaiting completion or in post-publish retention).",
		}),
		sectionSize: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dispatcher_section_symbols",
			Help:    "symbols_count carried by each published kline_ready event.",
			Buckets: []float64{1, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300},
		}, []string{"interval"}),
	}
}
