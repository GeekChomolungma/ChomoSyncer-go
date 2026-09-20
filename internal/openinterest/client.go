package openinterest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
)

// DefaultBaseURL is the USDⓈ-M futures REST root.
const DefaultBaseURL = "https://fapi.binance.com"

// Snapshot is one GET /fapi/v1/openInterest response.
type Snapshot struct {
	OpenInterest float64
	Time         time.Time // when the exchange took the value (response `time`), UTC
}

// HistPoint is one GET /futures/data/openInterestHist item.
type HistPoint struct {
	Label        time.Time // the snapshot instant T, always a whole 5-minute boundary
	OpenInterest float64
}

// SnapshotSource returns the current open interest of a symbol.
type SnapshotSource interface {
	Snapshot(ctx context.Context, symbol string) (Snapshot, error)
}

// HistSource returns Binance's 5-minute open-interest history for a symbol, oldest
// first. It returns the NEWEST `limit` points inside [start, end] — start alone
// cannot page forward — so callers page backwards by moving end.
type HistSource interface {
	History(ctx context.Context, symbol string, start, end time.Time, limit int) ([]HistPoint, error)
}

// ErrInvalidSymbol is returned when Binance rejects the symbol (HTTP 400, -1121).
var ErrInvalidSymbol = errors.New("openinterest: invalid symbol")

// RateLimitError is a 429 (rate limited) or 418 (IP banned) response.
type RateLimitError struct {
	Status     int
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("binance %d rate limited (retry after %v)", e.Status, e.RetryAfter)
}

// ClientConfig configures a Client.
type ClientConfig struct {
	BaseURL    string // default DefaultBaseURL
	HTTPClient *http.Client
	// Gate is the shared /fapi weight gate; Snapshot requests wait at it as
	// weightgate.ClassLive (weight 1) and report X-MBX-USED-WEIGHT-1M back. nil = a
	// private gate with default budgets.
	Gate *weightgate.Gate
	// Pool admits /futures/data requests. nil = a private pool with defaults.
	Pool *DataPool

	Metrics *metrics
	Logger  *slog.Logger
}

// Client is the hand-written REST client for the two open-interest endpoints, in
// the style of internal/backfill.BinanceFetcher.
type Client struct {
	base string
	http *http.Client
	gate *weightgate.Gate
	pool *DataPool
	m    *metrics
	log  *slog.Logger
}

var (
	_ SnapshotSource = (*Client)(nil)
	_ HistSource     = (*Client)(nil)
)

// NewClient builds a Client.
func NewClient(cfg ClientConfig) *Client {
	c := &Client{base: cfg.BaseURL, http: cfg.HTTPClient, gate: cfg.Gate, pool: cfg.Pool, m: cfg.Metrics, log: cfg.Logger}
	if c.base == "" {
		c.base = DefaultBaseURL
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 20 * time.Second}
	}
	if c.m == nil {
		c.m = newMetrics(nil)
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	if c.gate == nil {
		c.gate = weightgate.New(weightgate.Config{})
	}
	if c.pool == nil {
		c.pool = NewDataPool(DataPoolConfig{Metrics: c.m, Logger: c.log})
	}
	c.log = c.log.With("component", "oi_client")
	return c
}

type snapshotJSON struct {
	OpenInterest string `json:"openInterest"`
	Time         int64  `json:"time"`
}

type histJSON struct {
	SumOpenInterest string `json:"sumOpenInterest"`
	Timestamp       int64  `json:"timestamp"`
}

type errorJSON struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Snapshot implements SnapshotSource: GET /fapi/v1/openInterest (weight 1).
func (c *Client) Snapshot(ctx context.Context, symbol string) (Snapshot, error) {
	if err := c.gate.Wait(ctx, weightgate.ClassLive, 1); err != nil {
		return Snapshot{}, err
	}
	q := url.Values{"symbol": {symbol}}
	status, header, body, err := c.get(ctx, "/fapi/v1/openInterest", q)
	if err != nil {
		c.m.httpRequests.WithLabelValues("snapshot", "error").Inc()
		return Snapshot{}, err
	}
	c.gate.ObserveHeader(header)
	if err := c.checkStatus("snapshot", "fapi", status, header, body, func(d time.Duration) { c.gate.PauseFor(d) }); err != nil {
		return Snapshot{}, err
	}
	var r snapshotJSON
	if err := sonic.Unmarshal(body, &r); err != nil {
		c.m.httpRequests.WithLabelValues("snapshot", "error").Inc()
		return Snapshot{}, fmt.Errorf("decode openInterest: %w", err)
	}
	oi, err := strconv.ParseFloat(r.OpenInterest, 64)
	if err != nil || r.Time <= 0 {
		c.m.httpRequests.WithLabelValues("snapshot", "error").Inc()
		return Snapshot{}, fmt.Errorf("malformed openInterest response for %s: %.120s", symbol, body)
	}
	c.m.httpRequests.WithLabelValues("snapshot", "ok").Inc()
	return Snapshot{OpenInterest: oi, Time: time.UnixMilli(r.Time).UTC()}, nil
}

// History implements HistSource: GET /futures/data/openInterestHist, period=5m.
// Both startTime and endTime are always sent; see HistSource for why.
func (c *Client) History(ctx context.Context, symbol string, start, end time.Time, limit int) ([]HistPoint, error) {
	if err := c.pool.Wait(ctx); err != nil {
		return nil, err
	}
	q := url.Values{
		"symbol":    {symbol},
		"period":    {"5m"},
		"startTime": {strconv.FormatInt(start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(end.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(limit)},
	}
	status, header, body, err := c.get(ctx, "/futures/data/openInterestHist", q)
	if err != nil {
		c.m.httpRequests.WithLabelValues("hist", "error").Inc()
		return nil, err
	}
	if err := c.checkStatus("hist", "data", status, header, body, func(d time.Duration) { c.pool.PauseFor(d) }); err != nil {
		return nil, err
	}
	var items []histJSON
	if err := sonic.Unmarshal(body, &items); err != nil {
		c.m.httpRequests.WithLabelValues("hist", "error").Inc()
		return nil, fmt.Errorf("decode openInterestHist: %w", err)
	}
	out := make([]HistPoint, 0, len(items))
	for _, it := range items {
		oi, err := strconv.ParseFloat(it.SumOpenInterest, 64)
		if err != nil || it.Timestamp <= 0 {
			c.log.Warn("skipping malformed openInterestHist item", "symbol", symbol, "ts", it.Timestamp, "value", it.SumOpenInterest)
			continue
		}
		out = append(out, HistPoint{Label: time.UnixMilli(it.Timestamp).UTC(), OpenInterest: oi})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label.Before(out[j].Label) })
	c.m.httpRequests.WithLabelValues("hist", "ok").Inc()
	return out, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+q.Encode(), nil)
	if err != nil {
		return 0, nil, nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// checkStatus turns a non-200 response into an error and, for 429/418, pauses the
// right pool via pause.
func (c *Client) checkStatus(endpoint, pool string, status int, h http.Header, body []byte, pause func(time.Duration)) error {
	switch {
	case status == http.StatusOK:
		return nil
	case status == http.StatusTooManyRequests:
		d := weightgate.RetryAfter(h, 5*time.Second)
		pause(d)
		c.m.rateLimited.WithLabelValues(pool, "429").Inc()
		c.m.httpRequests.WithLabelValues(endpoint, "rate_limited").Inc()
		return &RateLimitError{Status: status, RetryAfter: d}
	case status == http.StatusTeapot: // 418: IP ban
		d := weightgate.RetryAfter(h, 60*time.Second)
		pause(d)
		c.m.rateLimited.WithLabelValues(pool, "418").Inc()
		c.m.httpRequests.WithLabelValues(endpoint, "rate_limited").Inc()
		return &RateLimitError{Status: status, RetryAfter: d}
	case status == http.StatusBadRequest:
		var e errorJSON
		if sonic.Unmarshal(body, &e) == nil && e.Code == -1121 {
			c.m.httpRequests.WithLabelValues(endpoint, "invalid_symbol").Inc()
			return ErrInvalidSymbol
		}
	}
	c.m.httpRequests.WithLabelValues(endpoint, "error").Inc()
	return fmt.Errorf("binance %s: status %d: %.160s", endpoint, status, body)
}
