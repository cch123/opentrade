// Package engine wires per-symbol Kline + Depth state and the stateless
// PublicTrade forwarder into a single Handle(TradeEvent) function that
// returns the market-data events to emit.
//
// Engine is safe for concurrent use: Handle and SnapshotAll take an internal
// mutex so a snapshot ticker can run alongside the trade-event consumer.
package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/quote/internal/depth"
	"github.com/xargin/opentrade/quote/internal/kline"
	"github.com/xargin/opentrade/quote/internal/snapshot"
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
	// offsets tracks the next-to-consume offset per trade-event partition.
	// Updated under mu alongside state mutation in HandleRecord so Capture
	// sees state and offsets that are consistent with one another.
	offsets map[int32]int64

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
		cfg:     cfg,
		logger:  logger,
		klines:  make(map[string]*kline.Aggregator),
		books:   make(map[string]*depth.Book),
		offsets: make(map[int32]int64),
	}
}

// Handle consumes a trade-event and returns the fan-out of MarketDataEvents
// to publish. Each returned event already carries its EventMeta.
//
// Handle does NOT update the per-partition offset bookkeeping — use
// HandleRecord from the consumer loop so state and offsets advance together.
// Handle remains on the API surface for tests that feed synthetic events
// without Kafka wiring.
func (e *Engine) Handle(evt *eventpb.TradeEvent) []*eventpb.MarketDataEvent {
	if evt == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dispatch(evt)
}

// HandleRecord is Handle plus an atomic update of the offset bookkeeping.
// Capturing a snapshot under the same mutex sees state and offsets whose
// latest advance are paired: everything we report through offset `offset+1`
// has been baked into state. On restart the consumer resumes from that
// "next" offset so we never re-apply a record.
func (e *Engine) HandleRecord(evt *eventpb.TradeEvent, partition int32, offset int64) []*eventpb.MarketDataEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.dispatch(evt)
	e.offsets[partition] = offset + 1
	return out
}

// dispatch is the mutex-less inner handler.
func (e *Engine) dispatch(evt *eventpb.TradeEvent) []*eventpb.MarketDataEvent {
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

// Capture returns a deep copy of the engine's state, offsets and emit seq
// suitable for serialization. Safe to call concurrently with HandleRecord.
func (e *Engine) Capture() *snapshot.Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	snap := &snapshot.Snapshot{
		Version: snapshot.Version,
		Seq:     e.seq.Load(),
		Offsets: make(map[int32]int64, len(e.offsets)),
		Symbols: make(map[string]*snapshot.SymbolSnapshot),
	}
	for p, off := range e.offsets {
		snap.Offsets[p] = off
	}
	// Collect symbols from both maps — a symbol may exist in only one of
	// them (depth-only or kline-only state is possible mid-run).
	symbols := make(map[string]struct{})
	for s := range e.books {
		symbols[s] = struct{}{}
	}
	for s := range e.klines {
		symbols[s] = struct{}{}
	}
	for sym := range symbols {
		ss := &snapshot.SymbolSnapshot{}
		if book, ok := e.books[sym]; ok {
			st := book.Capture()
			ss.Depth = &snapshot.DepthSnapshot{
				Symbol: sym,
				Bids:   st.Bids,
				Asks:   st.Asks,
				Prices: st.Prices,
			}
			if len(st.Orders) > 0 {
				ss.Depth.Orders = make([]snapshot.OrderRefSnap, len(st.Orders))
				for i, o := range st.Orders {
					ss.Depth.Orders[i] = snapshot.OrderRefSnap{
						OrderID:   o.OrderID,
						Side:      o.Side,
						PriceKey:  o.PriceKey,
						Remaining: o.Remaining,
					}
				}
			}
		}
		if agg, ok := e.klines[sym]; ok {
			st := agg.Capture()
			ks := &snapshot.KlineSnapshot{Symbol: sym, Bars: make(map[int32]snapshot.BarSnap, len(st.Bars))}
			for iv, b := range st.Bars {
				ks.Bars[int32(iv)] = snapshot.BarSnap{
					OpenTimeMs:  b.OpenTimeMs,
					CloseTimeMs: b.CloseTimeMs,
					Open:        b.Open,
					High:        b.High,
					Low:         b.Low,
					Close:       b.Close,
					Volume:      b.Volume,
					QuoteVolume: b.QuoteVolume,
					Count:       b.Count,
				}
			}
			ss.Kline = ks
		}
		snap.Symbols[sym] = ss
	}
	return snap
}

// Restore replaces engine state with the snapshot. Must be called before the
// consumer loop starts; caller is responsible for passing a fresh Engine.
func (e *Engine) Restore(snap *snapshot.Snapshot) error {
	if snap == nil {
		return nil
	}
	if snap.Version != snapshot.Version {
		return fmt.Errorf("snapshot version mismatch: got %d want %d", snap.Version, snapshot.Version)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq.Store(snap.Seq)
	e.offsets = make(map[int32]int64, len(snap.Offsets))
	for p, off := range snap.Offsets {
		e.offsets[p] = off
	}
	e.books = make(map[string]*depth.Book)
	e.klines = make(map[string]*kline.Aggregator)
	for sym, ss := range snap.Symbols {
		if ss == nil {
			continue
		}
		if ss.Depth != nil {
			book := depth.New(sym)
			orders := make([]depth.OrderRef, len(ss.Depth.Orders))
			for i, o := range ss.Depth.Orders {
				orders[i] = depth.OrderRef{
					OrderID:   o.OrderID,
					Side:      o.Side,
					PriceKey:  o.PriceKey,
					Remaining: o.Remaining,
				}
			}
			if err := book.Restore(depth.State{
				Bids:   ss.Depth.Bids,
				Asks:   ss.Depth.Asks,
				Prices: ss.Depth.Prices,
				Orders: orders,
			}); err != nil {
				return fmt.Errorf("restore depth %s: %w", sym, err)
			}
			e.books[sym] = book
		}
		if ss.Kline != nil {
			agg := kline.New(sym, e.cfg.Intervals)
			bars := make(map[eventpb.KlineInterval]kline.Bar, len(ss.Kline.Bars))
			for iv, b := range ss.Kline.Bars {
				bars[eventpb.KlineInterval(iv)] = kline.Bar{
					OpenTimeMs:  b.OpenTimeMs,
					CloseTimeMs: b.CloseTimeMs,
					Open:        b.Open,
					High:        b.High,
					Low:         b.Low,
					Close:       b.Close,
					Volume:      b.Volume,
					QuoteVolume: b.QuoteVolume,
					Count:       b.Count,
				}
			}
			if err := agg.Restore(kline.State{Bars: bars}); err != nil {
				return fmt.Errorf("restore kline %s: %w", sym, err)
			}
			e.klines[sym] = agg
		}
	}
	return nil
}

// Offsets returns a copy of the consumer's per-partition watermark.
// Used by main to set the consumer's initial offsets on restart.
func (e *Engine) Offsets() map[int32]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[int32]int64, len(e.offsets))
	for p, off := range e.offsets {
		out[p] = off
	}
	return out
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
