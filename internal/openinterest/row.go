package openinterest

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Row is one fapi_oi_5m row: a symbol's open interest at the close of the 5-minute
// kline that OPENS at StartTime.
type Row struct {
	Symbol          string
	StartTime       time.Time // kline open time, UTC (same meaning as fapi_kline_5m.start_time)
	SumOpenInterest float64   // contracts, in the base asset
	SnapTime        time.Time // instant the value was actually observed: live = response `time`, hist = label
	SrcRank         uint8     // RankLive / RankHist / RankArchive
}

// validate rejects rows that must never reach the table.
func (r Row) validate() error {
	switch {
	case r.Symbol == "":
		return errors.New("empty symbol")
	case r.StartTime.IsZero() || !r.StartTime.Equal(r.StartTime.Truncate(BarInterval)):
		return fmt.Errorf("start_time %v is not on a 5m boundary", r.StartTime)
	case math.IsNaN(r.SumOpenInterest) || math.IsInf(r.SumOpenInterest, 0) || r.SumOpenInterest < 0:
		return fmt.Errorf("invalid open interest %v", r.SumOpenInterest)
	case r.SrcRank < RankLive || r.SrcRank > RankArchive:
		return fmt.Errorf("invalid src_rank %d", r.SrcRank)
	}
	return nil
}

// insertColumns must match appendArgs.
const insertColumns = "(symbol, start_time, sum_open_interest, snap_time, src_rank)"

// appendArgs returns the values in insertColumns order. Times are passed as
// time.Time (never raw epoch numbers, which ClickHouse would misread as seconds).
func (r Row) appendArgs() []any {
	return []any{r.Symbol, r.StartTime.UTC(), r.SumOpenInterest, r.SnapTime.UTC(), r.SrcRank}
}
