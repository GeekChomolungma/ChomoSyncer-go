package collector

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
)

type reason int

const (
	reasonStopped reason = iota // ctx cancelled or stop() called
	reasonError                 // streamClient reported a fatal error
	reasonClosed                // streamClient error channel closed
	reasonStale                 // no frame within StaleTimeout
	reasonConnectFailed
)

func (r reason) String() string {
	switch r {
	case reasonStopped:
		return "stopped"
	case reasonError:
		return "error"
	case reasonClosed:
		return "closed"
	case reasonStale:
		return "stale"
	case reasonConnectFailed:
		return "connect_failed"
	default:
		return "unknown"
	}
}

// shard supervises one WebSocket connection carrying a slice of kline streams.
type shard struct {
	id         string
	c          *Collector
	startDelay time.Duration

	mu      sync.Mutex
	desired map[string]struct{}
	version uint64 // bumped on every setDesired

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func newShard(c *Collector, id string, streams map[string]struct{}, startDelay time.Duration) *shard {
	return &shard{
		id:         id,
		c:          c,
		startDelay: startDelay,
		desired:    cloneSet(streams),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

func (s *shard) setDesired(streams map[string]struct{}) {
	s.mu.Lock()
	s.desired = cloneSet(streams)
	s.version++
	s.mu.Unlock()
}

func (s *shard) snapshotDesired() (map[string]struct{}, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSet(s.desired), s.version
}

func (s *shard) stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

func (s *shard) onEvent(ev dispatcher.KlineEvent) {
	s.c.metrics.wsMessages.WithLabelValues(s.id).Inc()
	if ev.IsFinal {
		s.c.metrics.klineIngested.WithLabelValues(ev.Symbol, ev.Interval).Inc()
	}
	if err := s.c.sink.HandleKlineEvent(ev); err != nil {
		s.c.metrics.dispatchErrors.WithLabelValues(dispatchErrReason(err)).Inc()
	}
}

func (s *shard) run(ctx context.Context) {
	defer close(s.doneCh)

	if s.startDelay > 0 && !s.sleep(ctx, s.startDelay) {
		return
	}

	backoff := s.c.cfg.reconnectBase
	var downAt time.Time // last-frame time of a drop, reported to OnGap on the next successful connect
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		default:
		}

		r, d := s.connectAndPump(ctx, downAt)
		s.c.metrics.wsStatus.WithLabelValues(s.id).Set(0)
		if r == reasonStopped {
			return
		}
		downAt = d
		s.c.metrics.wsReconnects.WithLabelValues(s.id).Inc()
		s.c.log.Warn("shard down; reconnecting", "shard", s.id, "reason", r.String(), "retry_in", backoff)
		if !s.sleep(ctx, jitter(backoff)) {
			return
		}
		backoff *= 2
		if backoff > s.c.cfg.reconnectMax {
			backoff = s.c.cfg.reconnectMax
		}
	}
}

// connectAndPump establishes one connection and services it until it drops or
// the shard is told to stop. prevDownAt, if non-zero, is the last-frame time of
// the earlier drop that led here; once this connection is up, the gap
// [prevDownAt, now] is reported to OnGap. Returns why it ended plus the
// last-frame time to carry forward as the next prevDownAt (zero on a clean stop).
func (s *shard) connectAndPump(ctx context.Context, prevDownAt time.Time) (reason, time.Time) {
	cl := s.c.factory(s.id, s.onEvent)

	want, ver := s.snapshotDesired()
	streams := sortedKeys(want)

	cctx, cancel := context.WithTimeout(ctx, s.c.cfg.connectTimeout)
	err := cl.Connect(cctx, streams)
	cancel()
	if err != nil {
		_ = cl.Close()
		if ctx.Err() != nil {
			return reasonStopped, time.Time{}
		}
		s.c.log.Warn("shard connect failed", "shard", s.id, "err", err)
		return reasonConnectFailed, prevDownAt // carry the drop forward
	}
	defer func() { _ = cl.Close() }()

	s.c.metrics.wsStatus.WithLabelValues(s.id).Set(1)
	s.c.log.Info("shard connected", "shard", s.id, "streams", len(streams))
	if !prevDownAt.IsZero() {
		s.reportGap(prevDownAt, time.Now(), streams)
	}

	subscribed := want
	localVer := ver

	wd := time.NewTicker(s.c.cfg.watchdogInterval)
	defer wd.Stop()

	for {
		select {
		case <-ctx.Done():
			return reasonStopped, time.Time{}
		case <-s.stopCh:
			return reasonStopped, time.Time{}

		case e, ok := <-cl.Errors():
			if !ok {
				return reasonClosed, cl.LastMessageAt()
			}
			s.c.log.Warn("shard stream error", "shard", s.id, "err", e)
			return reasonError, cl.LastMessageAt()

		case <-wd.C:
			// Apply a subscription delta if the desired set changed.
			if cur, v := s.snapshotDesired(); v != localVer {
				add, remove := diffSets(subscribed, cur)
				if len(add) > 0 {
					if err := cl.Subscribe(ctx, add); err != nil {
						s.c.log.Warn("shard subscribe delta failed", "shard", s.id, "err", err)
						return reasonError, cl.LastMessageAt()
					}
				}
				if len(remove) > 0 {
					if err := cl.Unsubscribe(ctx, remove); err != nil {
						s.c.log.Warn("shard unsubscribe delta failed", "shard", s.id, "err", err)
					}
				}
				subscribed = cur
				localVer = v
				s.c.metrics.subUpdates.WithLabelValues(s.id).Inc()
				s.c.log.Info("shard subscription updated", "shard", s.id, "added", len(add), "removed", len(remove))
			}

			// Staleness watchdog: defend against a half-open socket the
			// ping/pong missed.
			age := time.Since(cl.LastMessageAt())
			s.c.metrics.lastMsgAge.WithLabelValues(s.id).Set(age.Seconds())
			if age > s.c.cfg.staleTimeout {
				s.c.log.Warn("shard stale; forcing reconnect", "shard", s.id, "age", age.Round(time.Second))
				return reasonStale, cl.LastMessageAt()
			}
		}
	}
}

// reportGap notifies the collector's OnGap callback (if set) that this shard
// missed data in [downAt, upAt] for the given streams.
func (s *shard) reportGap(downAt, upAt time.Time, streams []string) {
	if s.c.onGap == nil || downAt.IsZero() || !upAt.After(downAt) {
		return
	}
	cp := make([]string, len(streams))
	copy(cp, streams)
	s.c.onGap(GapEvent{
		ShardID:     s.id,
		Streams:     cp,
		LastMsgAt:   downAt,
		ReconnectAt: upAt,
	})
}

func (s *shard) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-s.stopCh:
		return false
	}
}

// jitter applies half jitter: d/2 + rand[0, d/2].
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func dispatchErrReason(err error) string {
	switch {
	case errors.Is(err, dispatcher.ErrBusy):
		return "busy"
	case errors.Is(err, dispatcher.ErrClosed):
		return "closed"
	default:
		return "other"
	}
}
