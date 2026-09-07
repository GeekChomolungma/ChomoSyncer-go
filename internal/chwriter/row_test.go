package chwriter

import (
	"testing"
	"time"
)

func TestNewRow(t *testing.T) {
	r, err := NewRow(
		"ethusdt",
		1_719_835_200_000, 1_719_835_259_999,
		"60250.5", "60800.0", "60100.2", "60720.0",
		"12450.85", "75602300.5",
		"6120.40", "37150000.2",
		1234,
	)
	if err != nil {
		t.Fatalf("NewRow: %v", err)
	}

	if r.Symbol != "ethusdt" {
		t.Errorf("symbol: got %q", r.Symbol)
	}
	if !r.StartTime.Equal(time.UnixMilli(1_719_835_200_000).UTC()) {
		t.Errorf("start time: got %v", r.StartTime)
	}
	if r.StartTime.Location() != time.UTC {
		t.Errorf("start time not UTC: %v", r.StartTime.Location())
	}
	if r.Close != 60720.0 || r.High != 60800.0 || r.TakerBuyQuoteVolume != 37150000.2 {
		t.Errorf("numeric parse mismatch: %+v", r)
	}
	if r.TradesCount != 1234 {
		t.Errorf("trades: got %d", r.TradesCount)
	}
}

func TestNewRowBadNumber(t *testing.T) {
	_, err := NewRow("BTCUSDT", 1, 2, "not-a-number", "1", "1", "1", "1", "1", "1", "1", 0)
	if err == nil {
		t.Fatal("expected parse error for malformed open price")
	}
}

func TestRowAppendArgsOrder(t *testing.T) {
	r := Row{
		Symbol:              "BTCUSDT",
		StartTime:           time.UnixMilli(1000).UTC(),
		EndTime:             time.UnixMilli(2000).UTC(),
		Open:                1,
		High:                2,
		Low:                 3,
		Close:               4,
		Volume:              5,
		QuoteVolume:         6,
		TakerBuyVolume:      7,
		TakerBuyQuoteVolume: 8,
		TradesCount:         9,
	}
	args := r.appendArgs()
	if len(args) != 12 {
		t.Fatalf("want 12 columns, got %d", len(args))
	}
	if args[0] != "BTCUSDT" {
		t.Errorf("col0 symbol: %v", args[0])
	}
	if args[11] != uint32(9) {
		t.Errorf("col11 trades_count: %v", args[11])
	}
}
