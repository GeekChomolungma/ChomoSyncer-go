package backfill

import (
	"strconv"
	"time"
)

// parseIntervalDuration converts a Binance kline interval ("1m", "15m", "1h",
// "4h", "1d", "1w", "1M", ...) to a time.Duration. Month is approximated as 30
// days. ok is false for anything unrecognized.
//
// (Local copy; the same helper exists unexported in dispatcher and rediswin.
// TODO: extract to a shared internal package if a 4th copy appears.)
func parseIntervalDuration(interval string) (time.Duration, bool) {
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
	case 'm':
		return time.Duration(n) * time.Minute, true
	case 'h':
		return time.Duration(n) * time.Hour, true
	case 'd':
		return time.Duration(n) * 24 * time.Hour, true
	case 'w':
		return time.Duration(n) * 7 * 24 * time.Hour, true
	case 'M':
		return time.Duration(n) * 30 * 24 * time.Hour, true
	default:
		return 0, false
	}
}
