// Package dispatcher is the orchestration layer (design doc section 5.2). It consumes
// decoded kline events from the collector, converts their string-typed numeric
// fields, and fans each one out:
//
//   - every event (closed or not) -> Redis live snapshot layer  (rediswin.LiveBarWriter)
//   - closed events (k.x == true) -> Redis 200-bar rolling window (rediswin.Writer)
//   - closed events               -> ClickHouse archive          (chwriter.BatchWriter)
//   - closed events               -> cross-section aggregator -> kline_ready event
//
// The dispatcher never imports the Binance SDK: the collector decodes WS frames
// into KlineEvent, the dispatcher only converts and routes.
package dispatcher

import (
	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// KlineEvent is one decoded kline update from the collector. Numeric quantities
// stay as the decimal strings Binance sends; the dispatcher parses them once.
type KlineEvent struct {
	Symbol              string
	Interval            string
	OpenTime            int64 // k.t, ms — canonical bar timestamp
	CloseTime           int64 // k.T, ms
	Open                string
	High                string
	Low                 string
	Close               string
	Volume              string
	QuoteVolume         string
	TakerBuyVolume      string
	TakerBuyQuoteVolume string
	TradeCount          int64
	IsFinal             bool // k.x
}

func (e KlineEvent) toLiveBar() (rediswin.LiveBar, error) {
	return rediswin.NewLiveBar(
		e.OpenTime,
		e.Open, e.High, e.Low, e.Close,
		e.Volume, e.QuoteVolume,
		e.TakerBuyVolume, e.TakerBuyQuoteVolume,
		e.TradeCount, e.IsFinal,
	)
}

func (e KlineEvent) toCompactBar() (rediswin.CompactBar, error) {
	return rediswin.NewCompactBar(
		e.OpenTime,
		e.Open, e.High, e.Low, e.Close,
		e.Volume, e.QuoteVolume,
		e.TakerBuyVolume, e.TakerBuyQuoteVolume,
	)
}

func (e KlineEvent) toRow() (chwriter.Row, error) {
	return chwriter.NewRow(
		e.Symbol,
		e.OpenTime, e.CloseTime,
		e.Open, e.High, e.Low, e.Close,
		e.Volume, e.QuoteVolume,
		e.TakerBuyVolume, e.TakerBuyQuoteVolume,
		e.TradeCount,
	)
}
