package collector

import (
	"reflect"
	"testing"
)

func TestStreamName(t *testing.T) {
	if got := streamName("BTCUSDT", "1m"); got != "btcusdt@kline_1m" {
		t.Fatalf("streamName = %q", got)
	}
}

func TestShardID(t *testing.T) {
	if got := shardID("1h", 3); got != "kline_1h_3" {
		t.Fatalf("shardID = %q", got)
	}
}

func TestBucketOfStableAndDistributed(t *testing.T) {
	if bucketOf("BTCUSDT", 4) != bucketOf("btcusdt", 4) {
		t.Fatal("bucketOf must be case-insensitive / stable")
	}
	for _, n := range []int{2, 4, 8} {
		seen := map[int]int{}
		for _, s := range []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "LTCUSDT"} {
			b := bucketOf(s, n)
			if b < 0 || b >= n {
				t.Fatalf("bucketOf(%s,%d) = %d out of range", s, n, b)
			}
			seen[b]++
		}
		if len(seen) < 2 {
			t.Fatalf("shards=%d: all 8 symbols landed in one bucket (%v)", n, seen)
		}
	}
	if bucketOf("ANYTHING", 1) != 0 {
		t.Fatal("single shard must always be bucket 0")
	}
}

func TestPlanStreams(t *testing.T) {
	plan := planStreams([]string{"BTCUSDT", "ETHUSDT"}, []string{"1m", "1h"}, 1)
	// 1 bucket per interval => exactly 2 shards, each with both symbols.
	if len(plan) != 2 {
		t.Fatalf("want 2 shards, got %d: %v", len(plan), keysOf(plan))
	}
	m1 := plan["kline_1m_0"]
	if _, ok := m1["btcusdt@kline_1m"]; !ok {
		t.Fatalf("kline_1m_0 missing btcusdt stream: %v", m1)
	}
	if _, ok := m1["ethusdt@kline_1m"]; !ok {
		t.Fatalf("kline_1m_0 missing ethusdt stream: %v", m1)
	}
	if _, ok := plan["kline_1h_0"]["ethusdt@kline_1h"]; !ok {
		t.Fatal("kline_1h_0 missing ethusdt@kline_1h")
	}
}

func TestPlanStreamsEmptyBucketsAbsent(t *testing.T) {
	// One symbol, 8 buckets -> only the one bucket it hashes to exists.
	plan := planStreams([]string{"BTCUSDT"}, []string{"1m"}, 8)
	if len(plan) != 1 {
		t.Fatalf("want 1 non-empty shard, got %d: %v", len(plan), keysOf(plan))
	}
}

func TestDiffSets(t *testing.T) {
	cur := map[string]struct{}{"a": {}, "b": {}, "c": {}}
	want := map[string]struct{}{"b": {}, "c": {}, "d": {}, "e": {}}
	add, remove := diffSets(cur, want)
	if !reflect.DeepEqual(add, []string{"d", "e"}) {
		t.Fatalf("add = %v, want [d e]", add)
	}
	if !reflect.DeepEqual(remove, []string{"a"}) {
		t.Fatalf("remove = %v, want [a]", remove)
	}
}

func keysOf(m map[string]map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
