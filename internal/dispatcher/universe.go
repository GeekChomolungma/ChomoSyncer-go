package dispatcher

import "strings"

// UniverseProvider supplies the set of symbols the cross-section aggregator
// expects to report a closed bar each period. It is read concurrently by the
// dispatcher's closed-bar workers; implementations must be safe for concurrent
// use. The hourly-refreshing implementation lives in the universe module (3b);
// until then a StaticUniverse (or nil) is enough.
type UniverseProvider interface {
	// Size is the number of symbols in the active universe. 0 means "unknown":
	// the aggregator then publishes kline_ready on the timeout fallback only.
	Size() int
	// Has reports whether symbol belongs to the active universe. Symbols that
	// return false are still archived and windowed, but are not counted toward
	// a cross-section.
	Has(symbol string) bool
}

// StaticUniverse is an immutable UniverseProvider for tests and bring-up.
type StaticUniverse struct {
	set map[string]struct{}
}

// NewStaticUniverse builds a StaticUniverse from a symbol list (case-insensitive).
func NewStaticUniverse(symbols ...string) *StaticUniverse {
	set := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		set[strings.ToUpper(s)] = struct{}{}
	}
	return &StaticUniverse{set: set}
}

func (u *StaticUniverse) Size() int { return len(u.set) }

func (u *StaticUniverse) Has(symbol string) bool {
	_, ok := u.set[strings.ToUpper(symbol)]
	return ok
}
