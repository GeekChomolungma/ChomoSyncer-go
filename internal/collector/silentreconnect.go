package collector

import (
	"time"

	"github.com/binance/binance-connector-go/common/v2/common"
)

// GapWindow is a stretch of stream time during which a connection was not
// receiving data although it never surfaced an error: the connector's own
// scheduled reconnect.
type GapWindow struct {
	LastMsgAt   time.Time // last frame seen before the connection left OPEN
	ReconnectAt time.Time // when it was OPEN again
}

// silentReconnects is optionally implemented by a streamClient whose underlying
// library reconnects by itself without reporting an error.
type silentReconnects interface {
	// Reconnects delivers one GapWindow per silent reconnect. It is never closed.
	Reconnects() <-chan GapWindow
}

// connStatus is the slice of *common.WebSocketConnection the watcher reads.
type connStatus interface {
	Status() common.WebsocketStatus
}

// watchStatus polls conns until stop is closed and emits a GapWindow every time
// one goes OPEN -> (CLOSING|CONNECTING|CLOSED) -> OPEN.
//
// Why polling: binance-connector-go re-creates every connection each 23 hours
// (KeepAlive). It marks the connection CLOSING, sleeps reconnectDelay, closes the
// socket and dials a new one. A normal close is swallowed inside the connector,
// so neither ErrorChan nor any callback fires, yet frames sent between the old
// socket's close and the new socket's first read are lost. Status() is the only
// public signal of that window, and CLOSING lasts at least reconnectDelay (1s),
// far longer than the poll period, so no transition is missed.
func watchStatus(stop <-chan struct{}, conns []connStatus, lastMsg func() time.Time,
	out chan<- GapWindow, poll time.Duration, now func() time.Time) {
	type st struct {
		down   bool
		lastAt time.Time
	}
	state := make([]st, len(conns))
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		for i, c := range conns {
			if c == nil {
				continue
			}
			open := c.Status() == common.OPEN
			switch {
			case !open && !state[i].down:
				state[i] = st{down: true, lastAt: lastMsg()}
			case open && state[i].down:
				state[i].down = false
				select {
				case out <- GapWindow{LastMsgAt: state[i].lastAt, ReconnectAt: now()}:
				default: // a slow consumer must not stall the watcher; the sweeper is the backstop
				}
			}
		}
	}
}
