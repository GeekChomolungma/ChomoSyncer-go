package rediswin

import (
	"fmt"
	"strconv"

	"github.com/bytedance/sonic"
)

// CompactBar is one closed kline in the positional layout the Python strategy
// engine expects (design doc section 3.2 "序列化协议 (Payload)"):
//
//	[ start_time, open, high, low, close, volume, quote_volume,
//	  taker_buy_volume, taker_buy_quote_volume ]
//
// start_time is emitted as an integer (milliseconds, k.t); every other field is
// a JSON number. A keyless array is used instead of an object to cut Redis
// memory and Python parse cost.
type CompactBar struct {
	StartTime           int64 // milliseconds, k.t
	Open                float64
	High                float64
	Low                 float64
	Close               float64
	Volume              float64
	QuoteVolume         float64
	TakerBuyVolume      float64
	TakerBuyQuoteVolume float64
}

// compactBarLen is the number of positions in the serialized array.
const compactBarLen = 9

// Marshal renders the bar as a compact, keyless JSON array via sonic.
func (b CompactBar) Marshal() ([]byte, error) {
	arr := [compactBarLen]any{
		b.StartTime,
		b.Open,
		b.High,
		b.Low,
		b.Close,
		b.Volume,
		b.QuoteVolume,
		b.TakerBuyVolume,
		b.TakerBuyQuoteVolume,
	}
	p, err := sonic.Marshal(&arr)
	if err != nil {
		return nil, fmt.Errorf("rediswin: marshal compact bar: %w", err)
	}
	return p, nil
}

// NewCompactBar builds a CompactBar from Binance's raw wire representation: a
// millisecond epoch start time and decimal strings for every quantity. It is
// the bridge the upstream WebSocket handler (Task 3) uses before calling
// Writer.PushBarAndTrim.
func NewCompactBar(
	startMs int64,
	open, high, low, closePrice string,
	volume, quoteVolume string,
	takerBuyVolume, takerBuyQuoteVolume string,
) (CompactBar, error) {
	parse := func(name, s string) (float64, error) {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("rediswin: parse %s=%q: %w", name, s, err)
		}
		return v, nil
	}

	var (
		b   CompactBar
		err error
	)
	b.StartTime = startMs
	if b.Open, err = parse("open", open); err != nil {
		return CompactBar{}, err
	}
	if b.High, err = parse("high", high); err != nil {
		return CompactBar{}, err
	}
	if b.Low, err = parse("low", low); err != nil {
		return CompactBar{}, err
	}
	if b.Close, err = parse("close", closePrice); err != nil {
		return CompactBar{}, err
	}
	if b.Volume, err = parse("volume", volume); err != nil {
		return CompactBar{}, err
	}
	if b.QuoteVolume, err = parse("quote_volume", quoteVolume); err != nil {
		return CompactBar{}, err
	}
	if b.TakerBuyVolume, err = parse("taker_buy_volume", takerBuyVolume); err != nil {
		return CompactBar{}, err
	}
	if b.TakerBuyQuoteVolume, err = parse("taker_buy_quote_volume", takerBuyQuoteVolume); err != nil {
		return CompactBar{}, err
	}
	return b, nil
}
