package rediswin

import (
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
)

// LiveBar is a snapshot of the currently forming (or just-closed) kline for one
// symbol/interval — the "A mode" real-time state from design doc section 3.3. It
// carries two fields CompactBar does not: TradesCount (k.n) and IsFinal (k.x).
type LiveBar struct {
	StartTime           int64 // milliseconds, k.t
	Open                float64
	High                float64
	Low                 float64
	Close               float64
	Volume              float64
	QuoteVolume         float64
	TakerBuyVolume      float64
	TakerBuyQuoteVolume float64
	TradesCount         int64
	IsFinal             bool
}

// liveBarCompactLen is the element count of the Pub/Sub array payload:
// the 9 CompactBar positions plus trades_count and is_final.
const liveBarCompactLen = 11

func (b LiveBar) finalFlag() int {
	if b.IsFinal {
		return 1
	}
	return 0
}

// hashFields returns the HSET argument list in the design-doc field order
// (t o h l c v qv tbv tbqv n x).
func (b LiveBar) hashFields() []any {
	return []any{
		"t", b.StartTime,
		"o", b.Open,
		"h", b.High,
		"l", b.Low,
		"c", b.Close,
		"v", b.Volume,
		"qv", b.QuoteVolume,
		"tbv", b.TakerBuyVolume,
		"tbqv", b.TakerBuyQuoteVolume,
		"n", b.TradesCount,
		"x", b.finalFlag(),
	}
}

// marshalCompact renders the 11-element keyless array used for the optional
// Pub/Sub feed: [t,o,h,l,c,v,qv,tbv,tbqv,n,x].
func (b LiveBar) marshalCompact() ([]byte, error) {
	arr := [liveBarCompactLen]any{
		b.StartTime,
		b.Open,
		b.High,
		b.Low,
		b.Close,
		b.Volume,
		b.QuoteVolume,
		b.TakerBuyVolume,
		b.TakerBuyQuoteVolume,
		b.TradesCount,
		b.finalFlag(),
	}
	p, err := sonic.Marshal(&arr)
	if err != nil {
		return nil, fmt.Errorf("rediswin: marshal live bar: %w", err)
	}
	return p, nil
}

// NewLiveBar builds a LiveBar from Binance's raw wire representation.
func NewLiveBar(
	startMs int64,
	open, high, low, closePrice string,
	volume, quoteVolume string,
	takerBuyVolume, takerBuyQuoteVolume string,
	tradesCount int64,
	isFinal bool,
) (LiveBar, error) {
	parse := func(name, s string) (float64, error) {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("rediswin: parse %s=%q: %w", name, s, err)
		}
		return v, nil
	}

	var (
		b   LiveBar
		err error
	)
	b.StartTime = startMs
	b.TradesCount = tradesCount
	b.IsFinal = isFinal
	if b.Open, err = parse("open", open); err != nil {
		return LiveBar{}, err
	}
	if b.High, err = parse("high", high); err != nil {
		return LiveBar{}, err
	}
	if b.Low, err = parse("low", low); err != nil {
		return LiveBar{}, err
	}
	if b.Close, err = parse("close", closePrice); err != nil {
		return LiveBar{}, err
	}
	if b.Volume, err = parse("volume", volume); err != nil {
		return LiveBar{}, err
	}
	if b.QuoteVolume, err = parse("quote_volume", quoteVolume); err != nil {
		return LiveBar{}, err
	}
	if b.TakerBuyVolume, err = parse("taker_buy_volume", takerBuyVolume); err != nil {
		return LiveBar{}, err
	}
	if b.TakerBuyQuoteVolume, err = parse("taker_buy_quote_volume", takerBuyQuoteVolume); err != nil {
		return LiveBar{}, err
	}
	return b, nil
}

// parseIntervalDuration converts a Binance kline interval ("1m", "15m", "1h",
// "4h", "1d", "1w", "1M", ...) into a time.Duration. Month is approximated as
// 30 days. ok is false for anything unrecognized.
func parseIntervalDuration(interval string) (d time.Duration, ok bool) {
	if len(interval) < 2 {
		return 0, false
	}
	unit := interval[len(interval)-1]
	n, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || n <= 0 {
		return 0, false
	}
	switch unit {
	case 's':
		return time.Duration(n) * time.Second, true
	case 'm': // minutes (lowercase)
		return time.Duration(n) * time.Minute, true
	case 'h':
		return time.Duration(n) * time.Hour, true
	case 'd':
		return time.Duration(n) * 24 * time.Hour, true
	case 'w':
		return time.Duration(n) * 7 * 24 * time.Hour, true
	case 'M': // months (uppercase)
		return time.Duration(n) * 30 * 24 * time.Hour, true
	default:
		return 0, false
	}
}
