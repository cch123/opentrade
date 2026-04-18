// Package engine wires per-symbol Kline + Depth state and the stateless
// PublicTrade forwarder into a single Handle(TradeEvent) function that
// returns the market-data events to emit.
//
// Engine is safe for concurrent use: Handle and SnapshotAll take an internal
// mutex so a snapshot ticker can run alongside the trade-event consumer.
package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/quote/internal/depth"
	"github.com/xargin/opentrade/quote/internal/kline"
	"github.com/xargin/opentrade/quote/internal/trades"
)

// Config configures a new Engine.
type Config struct {
	ProducerID string
	Intervals  []kline.IntervalSpec
	// Clock supplies wall-clock ms for EventMeta.ts_unix_ms. Defaults to
	// time.Now().UnixMilli().
	Clock func() int64
}

// Engine owns per-symbol Kline aggregators and Depth books and stamps the
// EventMeta of every emitted MarketDataEvent.
type Engine struct {
	cfg    Config
	logger *zap.Logger

	mu     sync.Mutex
	klines map[string]*kline.Aggregator
	books  map[string]*depth.Book

	seq atomic.Uint64
}

// New constructs an Engine. Pass a zap.NewNop() in tests.
func New(cfg Config, logger *zap.Logger) *Engine {
	if cfg.Clock == nil {
		cfg.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	if len(cfg.Intervals) == 0 {
		cfg.Intervals = kline.DefaultIntervals()
	}
	return &Engine{
		cfg:    cfg,
		logger: logger,
		klines: make(map[string]*kline.Aggregator),
		books:  make(map[string]*depth.Book),
	}
}

// Handle consumes a trade-event and returns the fan-out of MarketDataEvents
// to publish. Each returned event already carries its EventMeta.
func (e *Engine) Handle(evt *eventpb.TradeEvent) []*eventpb.MarketDataEvent {
	if evt == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch p := evt.Payload.(type) {
	case *eventpb.TradeEvent_Trade:
		return e.handleTrade(evt, p.Trade)
	case *eventpb.TradeEvent_Accepted:
		return e.handleAccepted(p.Accepted)
	case *eventpb.TradeEvent_Cancelled:
		if p.Cancelled == nil {
			return nil
		}
		return e.handleClosed(p.Cancelled.Symbol, p.Cancelled.OrderId)
	case *eventpb.TradeEvent_Expired:
		if p.Expired == nil {
			return nil
		}
		return e.handleClosed(p.Expired.Symbol, p.Expired.OrderId)
	default:
		// Rejected orders never rested — no market-data effect.
		return nil
	}
}

// SnapshotAll returns a DepthSnapshot for every symbol that has any book
// state. Call on a periodic ticker.
func (e *Engine) SnapshotAll() []*eventpb.MarketDataEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.books) == 0 {
		return nil
	}
	out := make([]*eventpb.MarketDataEvent, 0, len(e.books))
	for _, b := range e.books {
		snap := b.Snapshot()
		if snap == nil {
			continue
		}
		e.stamp(snap)
		out = append(out, snap)
	}
	return out
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (e *Engine) handleTrade(evt *eventpb.TradeEvent, trade *eventpb.Trade) []*eventpb.MarketDataEvent {
	if trade == nil {
		return nil
	}
	symbol := trade.Symbol
	var out []*eventpb.MarketDataEvent

	// 1. PublicTrade forward.
	if pt := trades.FromTrade(evt); pt != nil {
		out = append(out, &eventpb.MarketDataEvent{
			Symbol:  symbol,
			Payload: &eventpb.MarketDataEvent_PublicTrade{PublicTrade: pt},
		})
	}

	// 2. Kline aggregation (uses trade ts if present, engine clock otherwise).
	ts := int64(0)
	if evt.Meta != nil {
		ts = evt.Meta.TsUnixMs
	}
	if ts <= 0 {
		ts = e.cfg.Clock()
	}
	kEvents, err := e.klineFor(symbol).OnTrade(trade.Price, trade.Qty, ts)
	if err != nil {
		e.logger.Error("kline update",
			zap.String("symbol", symbol), zap.String("trade_id", trade.TradeId),
			zap.Error(err))
	}
	out = append(out, kEvents...)

	// 3. Depth: maker level shrinks.
	dEvent, err := e.bookFor(symbol).OnTrade(trade.MakerOrderId, trade.Qty)
	if err != nil {
		e.logger.Error("depth update (trade)",
			zap.String("symbol", symbol), zap.Uint64("maker_order_id", trade.MakerOrderId),
			zap.Error(err))
	}
	if dEvent != nil {
		out = append(out, dEvent)
	}

	for _, m := range out {
		e.stamp(m)
	}
	return out
}

func (e *Engine) handleAccepted(acc *eventpb.OrderAccepted) []*eventpb.MarketDataEvent {
	if acc == nil {
		return nil
	}
	ev, err := e.bookFor(acc.Symbol).OnOrderAccepted(acc.OrderId, acc.Side, acc.Price, acc.RemainingQty)
	if err != nil {
		e.logger.Error("depth accepted",
			zap.String("symbol", acc.Symbol), zap.Uint64("order_id", acc.OrderId),
			zap.Error(err))
		return nil
	}
	if ev == nil {
		return nil
	}
	e.stamp(ev)
	return []*eventpb.MarketDataEvent{ev}
}

func (e *Engine) handleClosed(symbol string, orderID uint64) []*eventpb.MarketDataEvent {
	ev, err := e.bookFor(symbol).OnOrderClosed(orderID)
	if err != nil {
		e.logger.Error("depth closed",
			zap.String("symbol", symbol), zap.Uint64("order_id", orderID),
			zap.Error(err))
		return nil
	}
	if ev == nil {
		return nil
	}
	e.stamp(ev)
	return []*eventpb.MarketDataEvent{ev}
}

func (e *Engine) klineFor(symbol string) *kline.Aggregator {
	a, ok := e.klines[symbol]
	if !ok {
		a = kline.New(symbol, e.cfg.Intervals)
		e.klines[symbol] = a
	}
	return a
}

func (e *Engine) bookFor(symbol string) *depth.Book {
	b, ok := e.books[symbol]
	if !ok {
		b = depth.New(symbol)
		e.books[symbol] = b
	}
	return b
}

func (e *Engine) stamp(m *eventpb.MarketDataEvent) {
	m.Meta = &eventpb.EventMeta{
		SeqId:      e.seq.Add(1),
		TsUnixMs:   e.cfg.Clock(),
		ProducerId: e.cfg.ProducerID,
	}
}
