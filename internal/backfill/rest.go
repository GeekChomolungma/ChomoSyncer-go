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
	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
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
	MaxPageSpan int // upper bound of the /klines limit param, default 1500

	// Gate is the shared /fapi request-weight gate. Every page waits at it (as
	// weightgate.ClassBulk) before being sent and reports the response's
	// X-MBX-USED-WEIGHT-1M back. nil = a private gate with default budgets, so a
	// fetcher built without one (tests) is still weight-aware, just not shared.
	Gate *weightgate.Gate
}

// BinanceFetcher pulls closed klines from GET /fapi/v1/klines, rate-limited,
// paginated, and filtered to bars that are actually closed.
type BinanceFetcher struct {
	base    string
	http    *http.Client
	lim     *rate.Limiter // request-rate cap; the weight budget is enforced by gate
	gate    *weightgate.Gate
	pageLim int
	now     func() time.Time
	log     *slog.Logger

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
	gate := cfg.Gate
	if gate == nil {
		gate = weightgate.New(weightgate.Config{Registerer: reg, Logger: log})
	}
	f := promauto.With(reg)
	return &BinanceFetcher{
		base:    base,
		http:    hc,
		lim:     rate.NewLimiter(rate.Limit(rps), burst),
		gate:    gate,
		pageLim: pageLim,
		now:     nowFn,
		log:     log.With("component", "backfill_rest"),
		httpErr: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_rest_http_errors_total",
			Help: "Binance REST errors by HTTP status (429 rate-limit, 418 ban, 5xx, other).",
		}, []string{"code"}),
	}
}

// FetchStream calls onBatch with each page of closed rows as soon as it is fetched.
// If onBatch returns an error, pagination terminates early and returns that error.
// Implements backfill.StreamFetcher.
func (f *BinanceFetcher) FetchStream(ctx context.Context, symbol, interval string, since, until time.Time, onBatch func([]chwriter.Row) error) error {
	sinceMs := since.UnixMilli()
	untilMs := until.UnixMilli()
	closedBefore := f.now().Add(-time.Second).UnixMilli()

	cursor := sinceMs
	for cursor < untilMs {
		limit := f.pageLimitFor(interval, cursor, untilMs)
		rows, lastOpen, err := f.page(ctx, symbol, interval, cursor, untilMs, limit)
		if err != nil {
			return err
		}
		var batch []chwriter.Row
		for _, r := range rows {
			if r.openMs < sinceMs || r.openMs >= untilMs || r.closeMs >= closedBefore {
				continue
			}
			row, cerr := buildRow(symbol, r)
			if cerr != nil {
				f.log.Warn("skip malformed kline row", "symbol", symbol, "interval", interval, "err", cerr)
				continue
			}
			batch = append(batch, row)
		}
		if len(batch) > 0 && onBatch != nil {
			if err := onBatch(batch); err != nil {
				return err
			}
		}
		if len(rows) == 0 || lastOpen < cursor {
			break
		}
		next := lastOpen + 1
		if next <= cursor {
			break
		}
		cursor = next
		if len(rows) < limit {
			break // last page: the exchange had fewer bars than we allowed for
		}
	}
	return nil
}

// Fetch returns closed bars with open time in [since, until), ascending. A bar
// is "closed" only if its closeTime is at least 1s in the past. Implements
// backfill.KlineFetcher.
func (f *BinanceFetcher) Fetch(ctx context.Context, symbol, interval string, since, until time.Time) ([]chwriter.Row, error) {
	var out []chwriter.Row
	err := f.FetchStream(ctx, symbol, interval, since, until, func(batch []chwriter.Row) error {
		out = append(out, batch...)
		return nil
	})
	return out, err
}

type rawKline struct {
	openMs              int64
	closeMs             int64
	open, high, low, c  string
	volume, quoteVol    string
	takerBuy, takerBuyQ string
	trades              int64
}

func (f *BinanceFetcher) page(ctx context.Context, symbol, interval string, startMs, endMs int64, limit int) ([]rawKline, int64, error) {
	if err := f.lim.Wait(ctx); err != nil {
		return nil, 0, err
	}
	// Admission against the shared /fapi weight pool. The weight is fixed by the
	// `limit` we send, not by how many bars come back.
	if err := f.gate.Wait(ctx, weightgate.ClassBulk, klineWeight(limit)); err != nil {
		return nil, 0, err
	}
	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("interval", interval)
	q.Set("startTime", strconv.FormatInt(startMs, 10))
	q.Set("endTime", strconv.FormatInt(endMs, 10))
	q.Set("limit", strconv.Itoa(limit))
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

	f.gate.ObserveHeader(resp.Header)

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusTooManyRequests:
		f.httpErr.WithLabelValues("429").Inc()
		f.gate.PauseFor(weightgate.RetryAfter(resp.Header, 5*time.Second))
		return nil, 0, fmt.Errorf("binance 429 rate limited: %.120s", body)
	case resp.StatusCode == http.StatusTeapot: // 418 IP ban
		f.httpErr.WithLabelValues("418").Inc()
		f.gate.PauseFor(weightgate.RetryAfter(resp.Header, 60*time.Second))
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

// pageLimitFor sizes one request's `limit` to the bars still needed in
// [cursorMs, untilMs], capped at the configured page span.
//
// This matters because Binance weights /fapi/v1/klines by the `limit` we send,
// not by the number of bars returned: a 3-minute gap asked for with limit=1500
// returns ~3 bars yet costs weight 10, while limit=100 returns the same bars for
// weight 1. The +2 covers an unaligned first bar and one bar of slack, so a
// response shorter than `limit` reliably means "no more bars in range".
func (f *BinanceFetcher) pageLimitFor(interval string, cursorMs, untilMs int64) int {
	dur, ok := parseIntervalDuration(interval)
	if !ok || dur <= 0 || untilMs <= cursorMs {
		return f.pageLim
	}
	need := (untilMs-cursorMs)/dur.Milliseconds() + 2
	if need > int64(f.pageLim) {
		return f.pageLim
	}
	return int(need)
}

// klineWeight is the request weight of GET /fapi/v1/klines for a given limit.
// Binance documents [1,100)->1, [100,500)->2, [500,1000]->5, >1000->10. A probe
// in 2026-09 measured 100->1, 500->2, 1000->5, 1500->10 (never above the
// documented table), so the documented table is a safe upper bound. The
// exchange's own X-MBX-USED-WEIGHT-1M header, reported to the gate on every
// response, remains the source of truth.
func klineWeight(limit int) int {
	switch {
	case limit < 100:
		return 1
	case limit < 500:
		return 2
	case limit <= 1000:
		return 5
	default:
		return 10
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
