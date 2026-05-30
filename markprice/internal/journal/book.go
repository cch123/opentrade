// Package journal wires markprice to Kafka: it consumes the spot + perp
// market-data streams (ADR-0055 OrderBook frames) to track each symbol's
// depth, and produces the mark-price topic (MarkTick / FundingTick, ADR-0068
// §5) that perp-counter keys unrealized PnL, liquidation, and funding off of.
//
// This file holds the order-book depth tracker. Like BFF's marketcache it keys
// off the authoritative Full frame and ignores Delta: the Full carries the
// exact Top-N, so the mid AND the depth-weighted impact prices are correct at
// each Full (just at the Full cadence). The mid feeds the index / mark basis;
// the impact prices (depth-weighted average fill over a fixed notional) feed
// the funding premium index — using impact rather than top-of-book is what
// stops a thin best-level quote from moving the funding rate (Binance method).
package journal

import (
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

var (
	zero = dec.FromInt(0)
	two  = dec.FromInt(2)
)

// level is one parsed order-book price level.
type level struct {
	price dec.Decimal
	size  dec.Decimal
}

// sideLevels holds a symbol's two book sides. bids are descending by price,
// asks ascending (ADR-0055), i.e. each slice is already in fill order.
type sideLevels struct {
	bids []level
	asks []level
}

// Book tracks each symbol's latest Full-frame depth. Safe for concurrent use:
// consumer goroutines write, the tick loop reads.
type Book struct {
	mu    sync.RWMutex
	books map[string]*sideLevels
}

// NewBook returns an empty depth tracker.
func NewBook() *Book { return &Book{books: map[string]*sideLevels{}} }

// ApplyFull replaces symbol's stored depth from a Full frame. An entirely empty
// Full is ignored (a transient empty book should not erase a usable price).
func (b *Book) ApplyFull(symbol string, full *eventpb.OrderBookFull) {
	if full == nil || symbol == "" {
		return
	}
	bids := parseLevels(full.GetBids())
	asks := parseLevels(full.GetAsks())
	if len(bids) == 0 && len(asks) == 0 {
		return
	}
	b.mu.Lock()
	b.books[symbol] = &sideLevels{bids: bids, asks: asks}
	b.mu.Unlock()
}

// Mid returns symbol's top-of-book mid (one-sided fallback) and whether a book
// has been observed.
func (b *Book) Mid(symbol string) (dec.Decimal, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sl := b.books[symbol]
	if sl == nil {
		return zero, false
	}
	hasBid, hasAsk := len(sl.bids) > 0, len(sl.asks) > 0
	switch {
	case hasBid && hasAsk:
		return sl.bids[0].price.Add(sl.asks[0].price).Div(two), true
	case hasBid:
		return sl.bids[0].price, true
	case hasAsk:
		return sl.asks[0].price, true
	}
	return zero, false
}

// ImpactPrices returns symbol's depth-weighted impact bid/ask: the volume-
// weighted average price to fill `notional` worth of quote on each side
// (ADR-0068 §5 / Binance impact-price method). ok is true only when both sides
// have depth — a one-sided book contributes no premium sample that tick.
func (b *Book) ImpactPrices(symbol string, notional dec.Decimal) (impactBid, impactAsk dec.Decimal, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sl := b.books[symbol]
	if sl == nil {
		return zero, zero, false
	}
	bid, okBid := impactFill(sl.bids, notional)
	ask, okAsk := impactFill(sl.asks, notional)
	return bid, ask, okBid && okAsk
}

// impactFill returns the volume-weighted average price to consume `notional`
// worth of quote across levels (already in fill order). When the book is
// shallower than notional it falls back to the VWAP of all available depth.
// A non-positive notional also falls back to the full-book VWAP. ok=false when
// there are no levels.
func impactFill(levels []level, notional dec.Decimal) (dec.Decimal, bool) {
	if len(levels) == 0 {
		return zero, false
	}
	spent := zero // quote consumed
	qty := zero   // base filled
	for _, lv := range levels {
		levelVal := lv.price.Mul(lv.size)
		if notional.Sign() > 0 && spent.Add(levelVal).Cmp(notional) >= 0 {
			remain := notional.Sub(spent)       // quote left to fill
			qty = qty.Add(remain.Div(lv.price)) // base bought/sold for it
			spent = notional
			break
		}
		spent = spent.Add(levelVal)
		qty = qty.Add(lv.size)
	}
	if qty.Sign() <= 0 {
		return zero, false
	}
	return spent.Div(qty), true
}

// parseLevels converts wire levels to internal ones, dropping malformed /
// non-positive entries. Order is preserved (the producer already sorts).
func parseLevels(raw []*eventpb.OrderBookLevel) []level {
	out := make([]level, 0, len(raw))
	for _, l := range raw {
		if l == nil {
			continue
		}
		p, err := dec.Parse(l.GetPrice())
		if err != nil || p.Sign() <= 0 {
			continue
		}
		s, err := dec.Parse(l.GetQty())
		if err != nil || s.Sign() <= 0 {
			continue
		}
		out = append(out, level{price: p, size: s})
	}
	return out
}
