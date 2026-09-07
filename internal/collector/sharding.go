package collector

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// streamName is the Binance combined-stream name for a symbol/interval kline,
// e.g. "btcusdt@kline_1m" (symbol lower-cased).
func streamName(symbol, interval string) string {
	return strings.ToLower(symbol) + "@kline_" + interval
}

// shardID names a shard: interval plus its bucket index, e.g. "kline_1m_2".
func shardID(interval string, bucket int) string {
	return "kline_" + interval + "_" + strconv.Itoa(bucket)
}

// bucketOf maps a symbol to a stable bucket in [0, shards). Stable under
// universe churn: adding or removing a symbol only ever touches its own bucket,
// so an hourly refresh yields a handful of SUBSCRIBE/UNSUBSCRIBE deltas rather
// than a full re-shard.
func bucketOf(symbol string, shards int) int {
	if shards <= 1 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(strings.ToUpper(symbol)))) % shards
}

// planStreams distributes (symbol × interval) kline streams across shards:
// primarily by interval, then by a stable symbol hash within each interval.
// Empty buckets simply do not appear in the result.
func planStreams(symbols, intervals []string, shardsPerInterval int) map[string]map[string]struct{} {
	if shardsPerInterval < 1 {
		shardsPerInterval = 1
	}
	out := make(map[string]map[string]struct{})
	for _, iv := range intervals {
		for _, sym := range symbols {
			id := shardID(iv, bucketOf(sym, shardsPerInterval))
			m := out[id]
			if m == nil {
				m = make(map[string]struct{})
				out[id] = m
			}
			m[streamName(sym, iv)] = struct{}{}
		}
	}
	return out
}

// diffSets returns the streams to add (in want, not in cur) and to remove (in
// cur, not in want), each sorted for deterministic subscribe ordering.
func diffSets(cur, want map[string]struct{}) (add, remove []string) {
	for s := range want {
		if _, ok := cur[s]; !ok {
			add = append(add, s)
		}
	}
	for s := range cur {
		if _, ok := want[s]; !ok {
			remove = append(remove, s)
		}
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cloneSet(m map[string]struct{}) map[string]struct{} {
	cp := make(map[string]struct{}, len(m))
	for k := range m {
		cp[k] = struct{}{}
	}
	return cp
}
