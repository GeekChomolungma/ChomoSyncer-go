package chwriter

import (
	"fmt"
	"strconv"
	"time"
)

// Row is one raw kline fact, matching the market.fapi_kline_1m DDL column order
// (design doc section 3.1) minus created_at, which ClickHouse fills via DEFAULT
// now(). All timestamps are UTC.
type Row struct {
	Symbol              string
	StartTime           time.Time // k.t — canonical bar timestamp
	EndTime             time.Time // k.T
	Open                float64
	High                float64
	Low                 float64
	Close               float64
	Volume              float64
	QuoteVolume         float64
	TakerBuyVolume      float64
	TakerBuyQuoteVolume float64
	TradesCount         uint32
}

// insertColumns is the explicit column list for PrepareBatch, in the exact
// order appendArgs emits values. It MUST stay in sync with appendArgs and the
// market.fapi_kline_1m DDL. Naming the columns lets ClickHouse fill created_at
// from its DEFAULT instead of demanding a 13th value from the driver.
const insertColumns = "(symbol, start_time, end_time, open, high, low, close, " +
	"volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, trades_count)"

// appendArgs returns the row's fields in DDL column order for driver.Batch.Append.
func (r Row) appendArgs() []any {
	return []any{
		r.Symbol,
		r.StartTime,
		r.EndTime,
		r.Open,
		r.High,
		r.Low,
		r.Close,
		r.Volume,
		r.QuoteVolume,
		r.TakerBuyVolume,
		r.TakerBuyQuoteVolume,
		r.TradesCount,
	}
}

// NewRow builds a Row from the raw wire representation Binance uses: millisecond
// epoch timestamps and decimal strings for every numeric quantity. It is the
// bridge the upstream WebSocket handler (Task 3) uses before calling
// BatchWriter.Push.
func NewRow(
	symbol string,
	startMs, endMs int64,
	open, high, low, closePrice string,
	volume, quoteVolume string,
	takerBuyVolume, takerBuyQuoteVolume string,
	tradesCount int64,
) (Row, error) {
	parse := func(name, s string) (float64, error) {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("chwriter: parse %s=%q: %w", name, s, err)
		}
		return v, nil
	}

	var (
		r   Row
		err error
	)
	r.Symbol = symbol
	r.StartTime = time.UnixMilli(startMs).UTC()
	r.EndTime = time.UnixMilli(endMs).UTC()
	if r.Open, err = parse("open", open); err != nil {
		return Row{}, err
	}
	if r.High, err = parse("high", high); err != nil {
		return Row{}, err
	}
	if r.Low, err = parse("low", low); err != nil {
		return Row{}, err
	}
	if r.Close, err = parse("close", closePrice); err != nil {
		return Row{}, err
	}
	if r.Volume, err = parse("volume", volume); err != nil {
		return Row{}, err
	}
	if r.QuoteVolume, err = parse("quote_volume", quoteVolume); err != nil {
		return Row{}, err
	}
	if r.TakerBuyVolume, err = parse("taker_buy_volume", takerBuyVolume); err != nil {
		return Row{}, err
	}
	if r.TakerBuyQuoteVolume, err = parse("taker_buy_quote_volume", takerBuyQuoteVolume); err != nil {
		return Row{}, err
	}
	if tradesCount < 0 {
		tradesCount = 0
	}
	r.TradesCount = uint32(tradesCount)
	return r, nil
}
