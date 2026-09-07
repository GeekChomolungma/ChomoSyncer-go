package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	wsStatus       *prometheus.GaugeVec   // {shard} 1 up / 0 down
	wsReconnects   *prometheus.CounterVec // {shard}
	wsMessages     *prometheus.CounterVec // {shard}
	lastMsgAge     *prometheus.GaugeVec   // {shard} seconds since last frame
	subUpdates     *prometheus.CounterVec // {shard} subscription-delta applications
	shardsActive   prometheus.Gauge
	klineIngested  *prometheus.CounterVec // {symbol,interval} closed bars forwarded
	dispatchErrors *prometheus.CounterVec // {reason} busy|closed|other
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		wsStatus: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_connection_status",
			Help: "WebSocket shard liveness: 1 connected, 0 down.",
		}, []string{"shard"}),
		wsReconnects: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_reconnects_total",
			Help: "WebSocket shard reconnect attempts.",
		}, []string{"shard"}),
		wsMessages: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_messages_total",
			Help: "Kline stream messages received per shard.",
		}, []string{"shard"}),
		lastMsgAge: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_last_message_age_seconds",
			Help: "Seconds since the shard last received any frame (staleness watchdog input).",
		}, []string{"shard"}),
		subUpdates: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_subscription_updates_total",
			Help: "Times a shard applied a SUBSCRIBE/UNSUBSCRIBE delta after a universe change.",
		}, []string{"shard"}),
		shardsActive: f.NewGauge(prometheus.GaugeOpts{
			Name: "ws_shards_active",
			Help: "Number of shard connections the collector is managing.",
		}),
		klineIngested: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kline_ingested_total",
			Help: "Closed (k.x==true) bars forwarded downstream.",
		}, []string{"symbol", "interval"}),
		dispatchErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "collector_dispatch_errors_total",
			Help: "Downstream HandleKlineEvent errors, partitioned by reason.",
		}, []string{"reason"}),
	}
}
