package collector

import (
	"sync"
	"testing"
	"time"

	"github.com/binance/binance-connector-go/common/v2/common"
)

type fakeConn struct {
	mu sync.Mutex
	st common.WebsocketStatus
}

func (f *fakeConn) Status() common.WebsocketStatus { f.mu.Lock(); defer f.mu.Unlock(); return f.st }
func (f *fakeConn) set(s common.WebsocketStatus)   { f.mu.Lock(); f.st = s; f.mu.Unlock() }

func TestWatchStatusEmitsOnCycle(t *testing.T) {
	fc := &fakeConn{st: common.OPEN}
	stop := make(chan struct{})
	defer close(stop)
	out := make(chan GapWindow, 4)
	lastMsg := time.Now().Add(-time.Second)
	go watchStatus(stop, []connStatus{fc}, func() time.Time { return lastMsg }, out, time.Millisecond, time.Now)

	time.Sleep(10 * time.Millisecond)
	select {
	case <-out:
		t.Fatal("no cycle yet, nothing must be emitted")
	default:
	}
	// The connector's sequence: OPEN -> CLOSING -> CONNECTING -> OPEN.
	fc.set(common.CLOSING)
	time.Sleep(10 * time.Millisecond)
	fc.set(common.CONNECTING)
	time.Sleep(10 * time.Millisecond)
	fc.set(common.OPEN)
	select {
	case g := <-out:
		if !g.LastMsgAt.Equal(lastMsg) || !g.ReconnectAt.After(g.LastMsgAt) {
			t.Fatalf("gap = %+v", g)
		}
	case <-time.After(time.Second):
		t.Fatal("no gap emitted after OPEN->CLOSING->CONNECTING->OPEN")
	}
	time.Sleep(10 * time.Millisecond)
	if len(out) != 0 {
		t.Fatal("exactly one gap per cycle expected")
	}
}

func TestWatchStatusNoEmitWhileDown(t *testing.T) {
	fc := &fakeConn{st: common.OPEN}
	stop := make(chan struct{})
	defer close(stop)
	out := make(chan GapWindow, 4)
	go watchStatus(stop, []connStatus{fc}, time.Now, out, time.Millisecond, time.Now)
	time.Sleep(5 * time.Millisecond)
	fc.set(common.CLOSED)
	time.Sleep(20 * time.Millisecond)
	if len(out) != 0 {
		t.Fatal("must not emit until the connection is OPEN again")
	}
}

// The real connector type must satisfy connStatus and report transitions through
// SetStatus, the same call KeepAlive makes.
func TestRealConnectionStatusCompat(t *testing.T) {
	c := &common.WebSocketConnection{}
	c.SetStatus(common.OPEN)
	var cs connStatus = c
	stop := make(chan struct{})
	defer close(stop)
	out := make(chan GapWindow, 1)
	go watchStatus(stop, []connStatus{cs}, time.Now, out, time.Millisecond, time.Now)
	time.Sleep(5 * time.Millisecond)
	c.SetStatus(common.CLOSING)
	time.Sleep(10 * time.Millisecond)
	c.SetStatus(common.OPEN)
	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("real WebSocketConnection transitions not detected")
	}
}
