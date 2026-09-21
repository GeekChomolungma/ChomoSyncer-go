// Package openinterest keeps the 5-minute open-interest table
// (market.fapi_oi_5m, design: new_requirements/oi.md) up to date from two REST
// sources:
//
//   - live: GET /fapi/v1/openInterest, snapshotted shortly before each 5-minute
//     kline closes (src_rank 1);
//   - hist: GET /futures/data/openInterestHist, used at start-up to backfill
//     whatever the database is missing and every hour to calibrate the live rows
//     (src_rank 2).
//
// A row's start_time is the OPEN time of the 5-minute kline whose CLOSE the value
// belongs to, so it joins fapi_kline_5m on (symbol, start_time) (oi.md §3).
package openinterest

import "time"

// BarInterval is the kline interval this package aligns to.
const BarInterval = 5 * time.Minute

// Source ranks stored in fapi_oi_5m.src_rank. On a merge the highest rank wins.
const (
	RankLive    uint8 = 1
	RankHist    uint8 = 2
	RankArchive uint8 = 3
)

// histBarStart maps an openInterestHist label T (the snapshot instant, always a
// whole 5-minute boundary) to the kline it belongs to: the one that closes at T,
// i.e. the one starting at T-5m. A label that is not on a boundary is rejected.
func histBarStart(label time.Time) (time.Time, bool) {
	label = label.UTC()
	if !label.Equal(label.Truncate(BarInterval)) {
		return time.Time{}, false
	}
	return label.Add(-BarInterval), true
}

// floorBar returns the start of the 5-minute bucket containing t.
func floorBar(t time.Time) time.Time { return t.UTC().Truncate(BarInterval) }

// newestPublishedStart is the start_time of the newest kline whose openInterestHist
// label can already be expected to exist at now. A label T is readable up to ~3
// minutes after T (measured 56-178s), so lag (default 4m) is subtracted first.
func newestPublishedStart(now time.Time, lag time.Duration) time.Time {
	return floorBar(now.Add(-lag)).Add(-BarInterval)
}
