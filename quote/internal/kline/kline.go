// Package kline aggregates trade flows into OHLCV candlesticks.
//
// The aggregator is single-threaded per symbol. It maintains one open bar per
// configured interval and emits KlineUpdate on every trade plus KlineClosed
// when a bar rolls to the next bucket. When trades skip one or more interval
// buckets entirely the aggregator also emits empty KlineClosed events for
// every intermediate bucket (O=H=L=C = previous close, volume = 0, count =
// 0) so downstream kline streams stay dense. This is the "gap fill" behaviour.
//
// Gap filling scales with the duration of the gap and the interval size: a
// trade arriving one week after the last one produces 10 080 empty 1m bars.
// The cost is bounded by the gap; there is no internal cap in this revision.
package kline

import (
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// IntervalSpec pairs a KlineInterval enum with its duration in milliseconds.
type IntervalSpec struct {
	Interval eventpb.KlineInterval
	Millis   int64
}

// DefaultIntervals returns the MVP set: 1m / 5m / 15m / 1h / 1d.
func DefaultIntervals() []IntervalSpec {
	return []IntervalSpec{
		{eventpb.KlineInterval_KLINE_INTERVAL_1M, 60_000},
		{eventpb.KlineInterval_KLINE_INTERVAL_5M, 5 * 60_000},
		{eventpb.KlineInterval_KLINE_INTERVAL_15M, 15 * 60_000},
		{eventpb.KlineInterval_KLINE_INTERVAL_1H, 60 * 60_000},
		{eventpb.KlineInterval_KLINE_INTERVAL_1D, 24 * 60 * 60_000},
	}
}

// bar is the mutable open candlestick we carry between trades.
type bar struct {
	openTime    int64
	closeTime   int64
	open        dec.Decimal
	high        dec.Decimal
	low         dec.Decimal
	close       dec.Decimal
	volume      dec.Decimal
	quoteVolume dec.Decimal
	count       uint64
}

// Aggregator tracks candlesticks for one symbol across several intervals.
type Aggregator struct {
	symbol    string
	intervals []IntervalSpec
	bars      map[eventpb.KlineInterval]*bar // nil when no trade has been seen yet for that interval
}

// New returns an Aggregator for symbol with the given intervals.
func New(symbol string, intervals []IntervalSpec) *Aggregator {
	return &Aggregator{
		symbol:    symbol,
		intervals: intervals,
		bars:      make(map[eventpb.KlineInterval]*bar, len(intervals)),
	}
}

// Symbol returns the symbol this Aggregator is bound to.
func (a *Aggregator) Symbol() string { return a.symbol }

// OnTrade feeds a trade into every configured interval and returns the set of
// market-data events to emit, in the order: closed bars first (from longest
// to shortest — see note below), then updates.
//
// If the trade's ts is strictly before the currently open bar's open time we
// treat it as out-of-order and skip it with a warning (the caller supplies a
// handler). trade-event is single-producer per symbol (ADR-0019), so this
// should only happen on a bug or after a replay.
func (a *Aggregator) OnTrade(priceStr, qtyStr string, tsUnixMs int64) ([]*eventpb.MarketDataEvent, error) {
	if tsUnixMs <= 0 {
		return nil, fmt.Errorf("kline: ts must be > 0, got %d", tsUnixMs)
	}
	price, err := dec.Parse(priceStr)
	if err != nil {
		return nil, fmt.Errorf("kline: bad price: %w", err)
	}
	qty, err := dec.Parse(qtyStr)
	if err != nil {
		return nil, fmt.Errorf("kline: bad qty: %w", err)
	}
	if !dec.IsPositive(price) || !dec.IsPositive(qty) {
		return nil, fmt.Errorf("kline: price and qty must be positive")
	}

	var out []*eventpb.MarketDataEvent
	for _, spec := range a.intervals {
		closed, updated := a.stepOne(spec, price, qty, tsUnixMs)
		for _, c := range closed {
			out = append(out, a.wrapClosed(spec, c))
		}
		if updated != nil {
			out = append(out, a.wrapUpdate(spec, updated))
		}
	}
	return out, nil
}

// stepOne folds the current trade into one interval. It returns the set of
// bars that just closed (typically one; more than one if gap filling fired)
// and the open bar that is now tracking the current price. Either return can
// be nil for "nothing to emit" (first-ever trade; out-of-order skip).
func (a *Aggregator) stepOne(spec IntervalSpec, price, qty dec.Decimal, ts int64) (closed []*bar, updated *bar) {
	bucket := (ts / spec.Millis) * spec.Millis
	current, ok := a.bars[spec.Interval]
	switch {
	case !ok:
		// First trade ever for this interval.
		nb := newBar(bucket, spec.Millis, price, qty)
		a.bars[spec.Interval] = nb
		return nil, nb
	case ts < current.openTime:
		// Out-of-order; keep current unchanged, signal via nil outputs so the
		// caller can decide what to log.
		return nil, nil
	case bucket == current.openTime:
		// Same bar: extend it.
		current.update(price, qty)
		return nil, current
	default:
		// ts advanced into a later bucket: close current, fill any empty
		// intermediate buckets, then open the new bar.
		closed = []*bar{current}
		for fill := current.openTime + spec.Millis; fill < bucket; fill += spec.Millis {
			closed = append(closed, newEmptyBar(fill, spec.Millis, current.close))
		}
		nb := newBar(bucket, spec.Millis, price, qty)
		a.bars[spec.Interval] = nb
		return closed, nb
	}
}

func newBar(openTime, millis int64, price, qty dec.Decimal) *bar {
	return &bar{
		openTime:    openTime,
		closeTime:   openTime + millis,
		open:        price,
		high:        price,
		low:         price,
		close:       price,
		volume:      qty,
		quoteVolume: price.Mul(qty),
		count:       1,
	}
}

// newEmptyBar builds a zero-volume gap-fill bar carrying the previous bar's
// close as open/high/low/close. This matches Binance's behaviour: a bucket
// with no trades still has a bar, and its price fields equal the last print.
func newEmptyBar(openTime, millis int64, lastClose dec.Decimal) *bar {
	return &bar{
		openTime:    openTime,
		closeTime:   openTime + millis,
		open:        lastClose,
		high:        lastClose,
		low:         lastClose,
		close:       lastClose,
		volume:      dec.Zero,
		quoteVolume: dec.Zero,
		count:       0,
	}
}

func (b *bar) update(price, qty dec.Decimal) {
	if price.Cmp(b.high) > 0 {
		b.high = price
	}
	if price.Cmp(b.low) < 0 {
		b.low = price
	}
	b.close = price
	b.volume = b.volume.Add(qty)
	b.quoteVolume = b.quoteVolume.Add(price.Mul(qty))
	b.count++
}

// State is the serializable form of an Aggregator. Shape mirrors the
// in-memory bar directly so JSON round-trips are lossless.
type State struct {
	Bars map[eventpb.KlineInterval]Bar
}

// Bar is the exported form of the internal bar, byte-for-byte compatible
// with the snapshot on-disk shape.
type Bar struct {
	OpenTimeMs  int64
	CloseTimeMs int64
	Open        string
	High        string
	Low         string
	Close       string
	Volume      string
	QuoteVolume string
	Count       uint64
}

// Capture returns a snapshot of the current open bars.
func (a *Aggregator) Capture() State {
	out := State{Bars: make(map[eventpb.KlineInterval]Bar, len(a.bars))}
	for iv, b := range a.bars {
		out.Bars[iv] = Bar{
			OpenTimeMs:  b.openTime,
			CloseTimeMs: b.closeTime,
			Open:        b.open.String(),
			High:        b.high.String(),
			Low:         b.low.String(),
			Close:       b.close.String(),
			Volume:      b.volume.String(),
			QuoteVolume: b.quoteVolume.String(),
			Count:       b.count,
		}
	}
	return out
}

// Restore replaces the aggregator's open bars with s.Bars.
func (a *Aggregator) Restore(s State) error {
	bars := make(map[eventpb.KlineInterval]*bar, len(s.Bars))
	for iv, b := range s.Bars {
		open, err := dec.Parse(b.Open)
		if err != nil {
			return fmt.Errorf("kline restore: open: %w", err)
		}
		high, err := dec.Parse(b.High)
		if err != nil {
			return fmt.Errorf("kline restore: high: %w", err)
		}
		low, err := dec.Parse(b.Low)
		if err != nil {
			return fmt.Errorf("kline restore: low: %w", err)
		}
		closeD, err := dec.Parse(b.Close)
		if err != nil {
			return fmt.Errorf("kline restore: close: %w", err)
		}
		vol, err := dec.Parse(b.Volume)
		if err != nil {
			return fmt.Errorf("kline restore: volume: %w", err)
		}
		qv, err := dec.Parse(b.QuoteVolume)
		if err != nil {
			return fmt.Errorf("kline restore: quote_volume: %w", err)
		}
		bars[iv] = &bar{
			openTime:    b.OpenTimeMs,
			closeTime:   b.CloseTimeMs,
			open:        open,
			high:        high,
			low:         low,
			close:       closeD,
			volume:      vol,
			quoteVolume: qv,
			count:       b.Count,
		}
	}
	a.bars = bars
	return nil
}

func (a *Aggregator) wrapUpdate(spec IntervalSpec, b *bar) *eventpb.MarketDataEvent {
	return &eventpb.MarketDataEvent{
		Symbol:  a.symbol,
		Payload: &eventpb.MarketDataEvent_KlineUpdate{KlineUpdate: &eventpb.KlineUpdate{Kline: a.asProto(spec, b)}},
	}
}

func (a *Aggregator) wrapClosed(spec IntervalSpec, b *bar) *eventpb.MarketDataEvent {
	return &eventpb.MarketDataEvent{
		Symbol:  a.symbol,
		Payload: &eventpb.MarketDataEvent_KlineClosed{KlineClosed: &eventpb.KlineClosed{Kline: a.asProto(spec, b)}},
	}
}

func (a *Aggregator) asProto(spec IntervalSpec, b *bar) *eventpb.Kline {
	return &eventpb.Kline{
		Symbol:      a.symbol,
		Interval:    spec.Interval,
		OpenTimeMs:  b.openTime,
		CloseTimeMs: b.closeTime,
		Open:        b.open.String(),
		High:        b.high.String(),
		Low:         b.low.String(),
		Close:       b.close.String(),
		Volume:      b.volume.String(),
		QuoteVolume: b.quoteVolume.String(),
		TradeCount:  b.count,
	}
}
