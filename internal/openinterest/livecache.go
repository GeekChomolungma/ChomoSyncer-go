package openinterest

import (
	"sync"
	"time"
)

// liveCache remembers the last few live values per symbol, in memory only. When
// hist later returns the same (symbol, bar) it lets the reconciler compare the two
// without querying ClickHouse: the live/hist deviation histogram and the count of
// bars live missed.
type liveCache struct {
	mu   sync.Mutex
	keep int
	per  map[string]map[int64]float64 // symbol -> bar start (unix s) -> value
}

func newLiveCache(keep int) *liveCache {
	if keep <= 0 {
		keep = 24
	}
	return &liveCache{keep: keep, per: map[string]map[int64]float64{}}
}

func (c *liveCache) put(symbol string, start time.Time, v float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.per[symbol]
	if m == nil {
		m = make(map[int64]float64, c.keep+1)
		c.per[symbol] = m
	}
	m[start.Unix()] = v
	for len(m) > c.keep { // drop the oldest
		oldest := int64(1<<63 - 1)
		for k := range m {
			if k < oldest {
				oldest = k
			}
		}
		delete(m, oldest)
	}
}

func (c *liveCache) get(symbol string, start time.Time) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.per[symbol][start.Unix()]
	return v, ok
}
