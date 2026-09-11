package rediswin

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bytedance/sonic"
)

func sampleBar() CompactBar {
	return CompactBar{
		StartTime:           1_719_835_200_000,
		Open:                60250.5,
		High:                60800.0,
		Low:                 60100.2,
		Close:               60720.0,
		Volume:              12450.85,
		QuoteVolume:         75602300.5,
		TakerBuyVolume:      6120.40,
		TakerBuyQuoteVolume: 37150000.2,
		TradesCount:         1420,
	}
}

func TestCompactBarMarshalShape(t *testing.T) {
	p, err := sampleBar().Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if p[0] != '[' || p[len(p)-1] != ']' {
		t.Fatalf("not a JSON array: %s", p)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(p, &raw); err != nil {
		t.Fatalf("unmarshal array: %v", err)
	}
	if len(raw) != compactBarLen {
		t.Fatalf("want %d elements, got %d: %s", compactBarLen, len(raw), p)
	}
	// start_time must be an integer literal, not a float.
	if bytes.ContainsRune(raw[0], '.') || bytes.ContainsRune(raw[0], 'e') {
		t.Fatalf("start_time must be an integer literal, got %s", raw[0])
	}
}

func TestCompactBarMarshalMatchesStdlib(t *testing.T) {
	b := sampleBar()
	got, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	std, err := json.Marshal([compactBarLen]any{
		b.StartTime, b.Open, b.High, b.Low, b.Close,
		b.Volume, b.QuoteVolume, b.TakerBuyVolume, b.TakerBuyQuoteVolume,
		b.TradesCount,
	})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if string(got) != string(std) {
		t.Fatalf("sonic output %s\ndiffers from stdlib %s", got, std)
	}
}

func TestCompactBarRoundTrip(t *testing.T) {
	b := sampleBar()
	p, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got [compactBarLen]float64
	if err := sonic.Unmarshal(p, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := [compactBarLen]float64{
		float64(b.StartTime), b.Open, b.High, b.Low, b.Close,
		b.Volume, b.QuoteVolume, b.TakerBuyVolume, b.TakerBuyQuoteVolume,
		float64(b.TradesCount),
	}
	if got != want {
		t.Fatalf("round trip mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestNewCompactBar(t *testing.T) {
	b, err := NewCompactBar(
		1_719_835_200_000,
		"60250.5", "60800.0", "60100.2", "60720.0",
		"12450.85", "75602300.5",
		"6120.40", "37150000.2",
		1420,
	)
	if err != nil {
		t.Fatalf("NewCompactBar: %v", err)
	}
	if b != sampleBar() {
		t.Fatalf("got %+v\nwant %+v", b, sampleBar())
	}
}

func TestNewCompactBarBadNumber(t *testing.T) {
	_, err := NewCompactBar(1, "1", "not-a-number", "1", "1", "1", "1", "1", "1", 9)
	if err == nil {
		t.Fatal("expected parse error for malformed high price")
	}
}
