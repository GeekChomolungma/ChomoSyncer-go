package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	fclient "github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures"
	wsstreams "github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures/src/websocketstreams"
	"github.com/binance/binance-connector-go/common/v2/common"

	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
)

var errNotConnected = errors.New("collector: binance stream client not connected")

// binanceStreamClient adapts binance-connector-go's WebSocket Streams client to
// the streamClient interface. It runs one SINGLE-mode market connection; the
// connector handles auto-pong and its own (bounded) reconnect, while the shard
// supervisor adds unlimited backoff and staleness detection on top.
type binanceStreamClient struct {
	shardID string
	baseURL string
	onEvent func(dispatcher.KlineEvent)
	log     *slog.Logger

	fc      *wsstreams.WebsocketStreamsClient
	errCh   chan error
	lastMsg atomic.Int64 // unixnano

	closeOnce sync.Once
	closed    atomic.Bool
	errWG     sync.WaitGroup
}

func newBinanceStreamClient(shardID, baseURL string, onEvent func(dispatcher.KlineEvent), log *slog.Logger) streamClient {
	return &binanceStreamClient{
		shardID: shardID,
		baseURL: baseURL,
		onEvent: onEvent,
		log:     log.With("shard", shardID),
		errCh:   make(chan error, 4),
	}
}

func (b *binanceStreamClient) Connect(ctx context.Context, streams []string) error {
	cfg := common.NewConfigurationWebsocketStreams(
		common.WithWsStreamsBasePath(b.baseURL),
		common.WithWsStreamsReconnectDelay(time.Second),
		common.WithWsStreamsMaxReconnectAttempts(5),
	)
	top := fclient.NewBinanceDerivativesTradingUsdsFuturesClient(fclient.WithWebsocketStreams(cfg))
	b.fc = top.WebsocketStreams

	if err := b.fc.ConnectMarket(streams); err != nil {
		return err
	}
	b.lastMsg.Store(time.Now().UnixNano())

	for _, s := range streams {
		if err := b.fc.OnMarket(s, b.handleRaw); err != nil {
			b.log.Warn("OnMarket registration failed", "stream", s, "err", err)
		}
	}
	b.drainConnErrors()
	_ = ctx
	return nil
}

func (b *binanceStreamClient) Subscribe(_ context.Context, streams []string) error {
	if b.fc == nil {
		return errNotConnected
	}
	if err := b.fc.SubscribeMarket(streams, nil); err != nil {
		return err
	}
	for _, s := range streams {
		if err := b.fc.OnMarket(s, b.handleRaw); err != nil {
			b.log.Warn("OnMarket registration failed", "stream", s, "err", err)
		}
	}
	return nil
}

func (b *binanceStreamClient) Unsubscribe(_ context.Context, streams []string) error {
	if b.fc == nil {
		return errNotConnected
	}
	return b.fc.UnsubscribeMarket(streams)
}

func (b *binanceStreamClient) LastMessageAt() time.Time {
	return time.Unix(0, b.lastMsg.Load())
}

func (b *binanceStreamClient) Errors() <-chan error { return b.errCh }

func (b *binanceStreamClient) Close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		if b.fc != nil {
			_ = b.fc.WsMarket.CloseWebSocketStreamConnection()
		}
		b.errWG.Wait()
		close(b.errCh)
	})
	return nil
}

// handleRaw is the connector callback. The connector delivers the full combined
// message {"stream":..., "data":{...}} and spawns a goroutine per callback, so
// this may run concurrently for the same stream.
func (b *binanceStreamClient) handleRaw(msg map[string]any) {
	b.lastMsg.Store(time.Now().UnixNano())
	ev, ok := mapToEvent(msg)
	if !ok {
		return
	}
	b.onEvent(ev)
}

// drainConnErrors forwards the connector's per-connection ErrorChan (where
// ErrReconnectAttemptsExhausted lands) onto our errCh.
func (b *binanceStreamClient) drainConnErrors() {
	if b.fc == nil || b.fc.WsMarket == nil || b.fc.WsMarket.WsCommon == nil {
		return
	}
	for _, conn := range b.fc.WsMarket.WsCommon.Connections {
		if conn == nil {
			continue
		}
		b.errWG.Add(1)
		go func(ec <-chan error) {
			defer b.errWG.Done()
			for err := range ec {
				if b.closed.Load() {
					return
				}
				select {
				case b.errCh <- err:
				default:
				}
			}
		}(conn.ErrorChan)
	}
}

func mapToEvent(msg map[string]any) (dispatcher.KlineEvent, bool) {
	d, _ := msg["data"].(map[string]any)
	if d == nil {
		d = msg // some paths deliver the payload unwrapped
	}
	k, _ := d["k"].(map[string]any)
	if k == nil {
		return dispatcher.KlineEvent{}, false
	}
	ev := dispatcher.KlineEvent{
		Symbol:              mapStr(k, "s"),
		Interval:            mapStr(k, "i"),
		OpenTime:            mapI64(k, "t"),
		CloseTime:           mapI64(k, "T"),
		Open:                mapStr(k, "o"),
		High:                mapStr(k, "h"),
		Low:                 mapStr(k, "l"),
		Close:               mapStr(k, "c"),
		Volume:              mapStr(k, "v"),
		QuoteVolume:         mapStr(k, "q"),
		TakerBuyVolume:      mapStr(k, "V"),
		TakerBuyQuoteVolume: mapStr(k, "Q"),
		TradeCount:          mapI64(k, "n"),
		IsFinal:             mapBool(k, "x"),
	}
	if ev.Symbol == "" || ev.Interval == "" {
		return dispatcher.KlineEvent{}, false
	}
	return ev, true
}

func mapStr(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func mapI64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

func mapBool(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}
