package openinterest

// Long/short ratios: market.fapi_ls_ratio_5m (deploy/clickhouse/007_fapi_ls_ratio.sql).
//
// Three /futures/data endpoints, one column each. There is no live (/fapi/v1)
// counterpart, so the table only has hist (src_rank 2) and archive (3) rows, kept up
// to date by LSReconciler. The endpoints share the /futures/data pool (DataPool)
// with openInterestHist: one IP, one 1000-requests-per-5-minutes budget.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
)

// DefaultLSTable is the raw 5-minute long/short ratio table.
const DefaultLSTable = "market.fapi_ls_ratio_5m"

// RatioKind selects one of the three long/short ratio endpoints.
type RatioKind int

const (
	RatioTopAccount    RatioKind = iota // topLongShortAccountRatio    -> count_toptrader_long_short_ratio
	RatioTopPosition                    // topLongShortPositionRatio   -> sum_toptrader_long_short_ratio
	RatioGlobalAccount                  // globalLongShortAccountRatio -> count_long_short_ratio
	numRatios
)

var ratioSpecs = [numRatios]struct {
	path   string // under /futures/data/
	column string // fapi_ls_ratio_5m column
}{
	RatioTopAccount:    {"topLongShortAccountRatio", "count_toptrader_long_short_ratio"},
	RatioTopPosition:   {"topLongShortPositionRatio", "sum_toptrader_long_short_ratio"},
	RatioGlobalAccount: {"globalLongShortAccountRatio", "count_long_short_ratio"},
}

func (k RatioKind) String() string { return ratioSpecs[k].path }

// RatioPoint is one item of a long/short ratio endpoint.
type RatioPoint struct {
	Label time.Time // the snapshot instant T, always a whole 5-minute boundary
	Ratio float64   // longShortRatio
}

// RatioHistSource returns Binance's 5-minute history of one long/short ratio for a
// symbol, oldest first, with the same paging semantics as HistSource: the NEWEST
// `limit` points inside [start, end].
type RatioHistSource interface {
	RatioHistory(ctx context.Context, kind RatioKind, symbol string, start, end time.Time, limit int) ([]RatioPoint, error)
}

var _ RatioHistSource = (*Client)(nil)

type ratioJSON struct {
	LongShortRatio string `json:"longShortRatio"`
	Timestamp      int64  `json:"timestamp"`
}

// RatioHistory implements RatioHistSource: GET /futures/data/<endpoint>, period=5m,
// admitted by the shared /futures/data pool. Both startTime and endTime are always
// sent (see HistSource).
func (c *Client) RatioHistory(ctx context.Context, kind RatioKind, symbol string, start, end time.Time, limit int) ([]RatioPoint, error) {
	if kind < 0 || kind >= numRatios {
		return nil, fmt.Errorf("openinterest: unknown ratio kind %d", kind)
	}
	endpoint := kind.String()
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
	status, header, body, err := c.get(ctx, "/futures/data/"+endpoint, q)
	if err != nil {
		c.m.httpRequests.WithLabelValues(endpoint, "error").Inc()
		return nil, err
	}
	if err := c.checkStatus(endpoint, "data", status, header, body, func(d time.Duration) { c.pool.PauseFor(d) }); err != nil {
		return nil, err
	}
	var items []ratioJSON
	if err := sonic.Unmarshal(body, &items); err != nil {
		c.m.httpRequests.WithLabelValues(endpoint, "error").Inc()
		return nil, fmt.Errorf("decode %s: %w", endpoint, err)
	}
	out := make([]RatioPoint, 0, len(items))
	for _, it := range items {
		v, err := strconv.ParseFloat(it.LongShortRatio, 64)
		if err != nil || it.Timestamp <= 0 || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			c.log.Warn("skipping malformed long/short ratio item", "endpoint", endpoint, "symbol", symbol, "ts", it.Timestamp, "value", it.LongShortRatio)
			continue
		}
		out = append(out, RatioPoint{Label: time.UnixMilli(it.Timestamp).UTC(), Ratio: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label.Before(out[j].Label) })
	c.m.httpRequests.WithLabelValues(endpoint, "ok").Inc()
	return out, nil
}

// LSRow is one fapi_ls_ratio_5m row: a symbol's three long/short ratios at the close
// of the 5-minute kline that OPENS at StartTime.
type LSRow struct {
	Symbol    string
	StartTime time.Time          // kline open time, UTC (same meaning as fapi_kline_5m.start_time)
	Ratios    [numRatios]float64 // indexed by RatioKind; NaN = no value (stored as NULL)
	SnapTime  time.Time          // the snapshot instant (= StartTime + 5m)
	SrcRank   uint8              // RankHist / RankArchive
}

// newLSRow returns a row with every ratio missing.
func newLSRow(symbol string, start time.Time, rank uint8) LSRow {
	r := LSRow{Symbol: symbol, StartTime: start, SnapTime: start.Add(BarInterval), SrcRank: rank}
	for i := range r.Ratios {
		r.Ratios[i] = math.NaN()
	}
	return r
}

func (r LSRow) validate() error {
	switch {
	case r.Symbol == "":
		return errors.New("empty symbol")
	case r.StartTime.IsZero() || !r.StartTime.Equal(r.StartTime.Truncate(BarInterval)):
		return fmt.Errorf("start_time %v is not on a 5m boundary", r.StartTime)
	case r.SrcRank != RankHist && r.SrcRank != RankArchive:
		return fmt.Errorf("invalid src_rank %d for a long/short ratio row", r.SrcRank)
	}
	present := 0
	for k, v := range r.Ratios {
		if math.IsNaN(v) {
			continue
		}
		if math.IsInf(v, 0) || v < 0 {
			return fmt.Errorf("invalid %s %v", ratioSpecs[k].column, v)
		}
		present++
	}
	if present == 0 {
		return errors.New("long/short ratio row without any value")
	}
	return nil
}

// lsInsertColumns must match appendArgs.
const lsInsertColumns = "(symbol, start_time, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, snap_time, src_rank)"

func (r LSRow) appendArgs() []any {
	return []any{r.Symbol, r.StartTime.UTC(),
		nullable(r.Ratios[RatioTopAccount]), nullable(r.Ratios[RatioTopPosition]), nullable(r.Ratios[RatioGlobalAccount]),
		r.SnapTime.UTC(), r.SrcRank}
}

// nullable maps NaN to a NULL of a Nullable(Float64) column.
func nullable(v float64) *float64 {
	if math.IsNaN(v) {
		return nil
	}
	return &v
}

// LSRowSink accepts long/short ratio rows for persistence. *LSWriter implements it.
type LSRowSink = rowSink[LSRow]

// LSWriter is the batching writer of fapi_ls_ratio_5m rows.
type LSWriter = batchWriter[LSRow]

// newLSWriter starts the long/short ratio writer's run loop. Close it to drain and stop.
func newLSWriter(cfg WriterConfig, flusher rowFlusher[LSRow], log *slog.Logger, m *metrics) *LSWriter {
	if m == nil {
		m = newMetrics(nil)
	}
	return newBatchWriter[LSRow](cfg, flusher, log, "ls_ratio_writer", &m.ls.writer)
}

// LSFlusher writes fapi_ls_ratio_5m batches: one native columnar INSERT per batch,
// on the same lazily (re)connecting connection as CHFlusher.
type LSFlusher struct {
	chConn
	table string
}

// NewLSFlusher builds a flusher for cfg.Table (default DefaultLSTable). It does not
// connect until the first flush.
func NewLSFlusher(cfg CHConfig) (*LSFlusher, error) {
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}
	table := cfg.Table
	if table == "" {
		table = DefaultLSTable
	}
	return &LSFlusher{chConn: chConn{opts: opts}, table: table}, nil
}

// Flush implements rowFlusher[LSRow].
func (f *LSFlusher) Flush(ctx context.Context, rows []LSRow) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := f.get(ctx)
	if err != nil {
		return fmt.Errorf("acquire clickhouse connection: %w", err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+f.table+" "+lsInsertColumns)
	if err != nil {
		f.invalidate()
		return fmt.Errorf("prepare batch: %w", err)
	}
	for i := range rows {
		if err := batch.Append(rows[i].appendArgs()...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %d/%d: %w", i, len(rows), err)
		}
	}
	if err := batch.Send(); err != nil {
		f.invalidate()
		return fmt.Errorf("send batch of %d rows: %w", len(rows), err)
	}
	return nil
}
