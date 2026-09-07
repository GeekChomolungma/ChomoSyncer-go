package backfill

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

// FetcherConfig configures the Binance REST kline fetcher.
type FetcherConfig struct {
	BaseURL     string  // https://fapi.binance.com
	RPS         float64 // token-bucket rate, default 20
	Burst       int     // default = ceil(RPS)
	HTTPClient  *http.Client
	Registerer  prometheus.Registerer
	Logger      *slog.Logger
	Clock       func() time.Time
	MaxPageSpan int // /klines limit param, default 1500
}

// BinanceFetcher pulls closed klines from GET /fapi/v1/klines, rate-limited,
// paginated, and filtered to bars that are actually closed.
type BinanceFetcher struct {
	base    string
	http    *http.Client
	lim     *rate.Limiter
	pageLim int
	now     func() time.Time
	log     *slog.Logger

	weight  prometheus.Gauge
	httpErr *prometheus.CounterVec // {code}
}

// NewBinanceFetcher builds the fetcher.
func NewBinanceFetcher(cfg FetcherConfig) *BinanceFetcher {
	base := cfg.BaseURL
	if base == "" {
		base = "https://fapi.binance.com"
	}
	rps := cfg.RPS
	if rps <= 0 {
		rps = 20
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = int(rps) + 1
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 20 * time.Second}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	nowFn := cfg.Clock
	if nowFn == nil {
		nowFn = time.Now
	}
	pageLim := cfg.MaxPageSpan
	if pageLim <= 0 || pageLim > 1500 {
		pageLim = 1500
	}
	reg := cfg.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &BinanceFetcher{
		base:    base,
		http:    hc,
		lim:     rate.NewLimiter(rate.Limit(rps), burst),
		pageLim: pageLim,
		now:     nowFn,
		log:     log.With("component", "backfill_rest"),
		weight: f.NewGauge(prometheus.GaugeOpts{
			Name: "backfill_rest_weight_used",
			Help: "Most recent X-MBX-USED-WEIGHT-1M response header value.",
		}),
		httpErr: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_rest_http_errors_total",
			Help: "Binance REST errors by HTTP status (429 rate-limit, 418 ban, 5xx, other).",
		}, []string{"code"}),
	}
}

// Fetch returns closed bars with open time in [since, until), ascending. A bar
// is "closed" only if its closeTime is at least 1s in the past. Implements
// backfill.KlineFetcher.
func (f *BinanceFetcher) Fetch(ctx context.Context, symbol, interval string, since, until time.Time) ([]chwriter.Row, error) {
	sinceMs := since.UnixMilli()
	untilMs := until.UnixMilli()
	closedBefore := f.now().Add(-time.Second).UnixMilli()

	var out []chwriter.Row
	cursor := sinceMs
	for cursor < untilMs {
		rows, lastOpen, err := f.page(ctx, symbol, interval, cursor, untilMs)
		if err != nil {
			return out, err
		}
		for _, r := range rows {
			if r.openMs < sinceMs || r.openMs >= untilMs || r.closeMs >= closedBefore {
				continue
			}
			row, cerr := buildRow(symbol, r)
			if cerr != nil {
				f.log.Warn("skip malformed kline row", "symbol", symbol, "interval", interval, "err", cerr)
				continue
			}
			out = append(out, row)
		}
		if len(rows) == 0 || lastOpen < cursor {
			break
		}
		next := lastOpen + 1
		if next <= cursor {
			break
		}
		cursor = next
		if len(rows) < f.pageLim {
			break // last page
		}
	}
	return out, nil
}

type rawKline struct {
	openMs              int64
	closeMs             int64
	open, high, low, c  string
	volume, quoteVol    string
	takerBuy, takerBuyQ string
	trades              int64
}

func (f *BinanceFetcher) page(ctx context.Context, symbol, interval string, startMs, endMs int64) ([]rawKline, int64, error) {
	if err := f.lim.Wait(ctx); err != nil {
		return nil, 0, err
	}
	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("interval", interval)
	q.Set("startTime", strconv.FormatInt(startMs, 10))
	q.Set("endTime", strconv.FormatInt(endMs, 10))
	q.Set("limit", strconv.Itoa(f.pageLim))
	u := f.base + "/fapi/v1/klines?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))

	if w := resp.Header.Get("X-MBX-USED-WEIGHT-1M"); w != "" {
		if v, e := strconv.ParseFloat(w, 64); e == nil {
			f.weight.Set(v)
		}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusTooManyRequests:
		f.httpErr.WithLabelValues("429").Inc()
		f.backoff(ctx, resp.Header.Get("Retry-After"), 5*time.Second)
		return nil, 0, fmt.Errorf("binance 429 rate limited: %.120s", body)
	case resp.StatusCode == http.StatusTeapot: // 418 IP ban
		f.httpErr.WithLabelValues("418").Inc()
		f.backoff(ctx, resp.Header.Get("Retry-After"), 60*time.Second)
		return nil, 0, fmt.Errorf("binance 418 banned: %.120s", body)
	case resp.StatusCode >= 500:
		f.httpErr.WithLabelValues("5xx").Inc()
		return nil, 0, fmt.Errorf("binance %d: %.120s", resp.StatusCode, body)
	default:
		f.httpErr.WithLabelValues("other").Inc()
		return nil, 0, fmt.Errorf("binance %d: %.120s", resp.StatusCode, body)
	}

	var raw [][]any
	if err := sonic.Unmarshal(body, &raw); err != nil {
		return nil, 0, fmt.Errorf("decode klines: %w", err)
	}
	rows := make([]rawKline, 0, len(raw))
	var lastOpen int64
	for _, r := range raw {
		if len(r) < 11 {
			continue
		}
		k := rawKline{
			openMs:    asInt64(r[0]),
			open:      asStr(r[1]),
			high:      asStr(r[2]),
			low:       asStr(r[3]),
			c:         asStr(r[4]),
			volume:    asStr(r[5]),
			closeMs:   asInt64(r[6]),
			quoteVol:  asStr(r[7]),
			trades:    asInt64(r[8]),
			takerBuy:  asStr(r[9]),
			takerBuyQ: asStr(r[10]),
		}
		rows = append(rows, k)
		lastOpen = k.openMs
	}
	return rows, lastOpen, nil
}

func (f *BinanceFetcher) backoff(ctx context.Context, retryAfter string, fallback time.Duration) {
	d := fallback
	if retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
			d = time.Duration(secs) * time.Second
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func buildRow(symbol string, k rawKline) (chwriter.Row, error) {
	return chwriter.NewRow(symbol, k.openMs, k.closeMs,
		k.open, k.high, k.low, k.c,
		k.volume, k.quoteVol,
		k.takerBuy, k.takerBuyQ,
		k.trades)
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		x, _ := strconv.ParseInt(n, 10, 64)
		return x
	default:
		return 0
	}
}
