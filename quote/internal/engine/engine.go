// Package engine wires per-symbol Kline state and the stateless PublicTrade
// forwarder into a single Handle(TradeEvent) function that returns the
// market-data events to emit.
//
// ADR-0055 removed depth responsibilities — orderbook state is owned by
// Match and published directly to market-data as OrderBook frames. Quote
// now only handles PublicTrade forwarding and Kline aggregation.
//
// Engine is safe for concurrent use: Handle and Capture take an internal
// mutex so a snapshot ticker can run alongside the trade-event consumer.
package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
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

// Engine owns per-symbol Kline aggregators and stamps the EventMeta of every
// emitted MarketDataEvent.
type Engine struct {
	cfg    Config
	logger *zap.Logger

	mu     sync.Mutex
	klines map[string]*kline.Aggregator
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

// dispatch is the mutex-less inner handler. Only Trade payloads matter now
// that depth lives in Match; other payloads have no Quote-side projection.
func (e *Engine) dispatch(evt *eventpb.TradeEvent) []*eventpb.MarketDataEvent {
	if p, ok := evt.Payload.(*eventpb.TradeEvent_Trade); ok {
		return e.handleTrade(evt, p.Trade)
	}
	return nil
}

// Capture returns a deep copy of the engine's state, offsets and emit seq
// suitable for serialization. Safe to call concurrently with HandleRecord.
func (e *Engine) Capture() *snapshot.Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	snap := &snapshot.Snapshot{
		Version:  snapshot.Version,
		QuoteSeq: e.seq.Load(),
		Offsets:  make(map[int32]int64, len(e.offsets)),
		Symbols:  make(map[string]*snapshot.SymbolSnapshot),
	}
	for p, off := range e.offsets {
		snap.Offsets[p] = off
	}
	for sym, agg := range e.klines {
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
		snap.Symbols[sym] = &snapshot.SymbolSnapshot{Kline: ks}
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
	e.seq.Store(snap.QuoteSeq)
	e.offsets = make(map[int32]int64, len(snap.Offsets))
	for p, off := range snap.Offsets {
		e.offsets[p] = off
	}
	e.klines = make(map[string]*kline.Aggregator)
	for sym, ss := range snap.Symbols {
		if ss == nil || ss.Kline == nil {
			continue
		}
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

	for _, m := range out {
		e.stamp(m)
	}
	return out
}

func (e *Engine) klineFor(symbol string) *kline.Aggregator {
	a, ok := e.klines[symbol]
	if !ok {
		a = kline.New(symbol, e.cfg.Intervals)
		e.klines[symbol] = a
	}
	return a
}

func (e *Engine) stamp(m *eventpb.MarketDataEvent) {
	m.Meta = &eventpb.EventMeta{
		TsUnixMs:   e.cfg.Clock(),
		ProducerId: e.cfg.ProducerID,
	}
	m.QuoteSeqId = e.seq.Add(1)
}
