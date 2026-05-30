// Package journal wires markprice to Kafka: it consumes the spot + perp
// market-data streams (ADR-0055 OrderBook frames) to track each symbol's
// mid-price, and produces the mark-price topic (MarkTick / FundingTick,
// ADR-0068 §5) that perp-counter keys unrealized PnL, liquidation, and funding
// off of.
//
// This file holds the mid-price tracker. Like BFF's marketcache it keys off the
// authoritative Full frame and ignores Delta: the Full carries the exact Top-N,
// so best-bid/ask (and therefore the mid) are correct at each Full, just at the
// Full cadence rather than per-tick. The mark EMA smooths the rest, so a
// Full-only mid is enough for the MVP (ADR-0068 §5); a Delta-applied book is a
// later refinement if sub-Full freshness is ever needed.
package journal

import (
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// MidBook tracks the latest mid-price per symbol from OrderBook Full frames.
// Safe for concurrent use: consumer goroutines write, the tick loop reads.
type MidBook struct {
	mu   sync.RWMutex
	mids map[string]dec.Decimal
}

// NewMidBook returns an empty tracker.
func NewMidBook() *MidBook { return &MidBook{mids: map[string]dec.Decimal{}} }

// ApplyFull recomputes symbol's mid from a Full frame's top of book. Bids are
// descending, asks ascending (ADR-0055), so the best levels are index 0. When
// only one side has liquidity the mid falls back to that side; an empty book
// leaves the prior mid untouched (a transient empty Full should not erase a
// usable price).
func (b *MidBook) ApplyFull(symbol string, full *eventpb.OrderBookFull) {
	if full == nil || symbol == "" {
		return
	}
	bestBid, okBid := firstPrice(full.GetBids())
	bestAsk, okAsk := firstPrice(full.GetAsks())
	var mid dec.Decimal
	switch {
	case okBid && okAsk:
		mid = bestBid.Add(bestAsk).Div(dec.FromInt(2))
	case okBid:
		mid = bestBid
	case okAsk:
		mid = bestAsk
	default:
		return // empty book — keep the last known mid
	}
	b.mu.Lock()
	b.mids[symbol] = mid
	b.mu.Unlock()
}

// Mid returns symbol's latest mid and whether one has been observed.
func (b *MidBook) Mid(symbol string) (dec.Decimal, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.mids[symbol]
	return m, ok
}

// firstPrice parses the price of the first level, reporting ok=false for an
// empty side or an unparseable / non-positive price.
func firstPrice(levels []*eventpb.OrderBookLevel) (dec.Decimal, bool) {
	if len(levels) == 0 || levels[0] == nil {
		return dec.Decimal{}, false
	}
	p, err := dec.Parse(levels[0].GetPrice())
	if err != nil || p.Sign() <= 0 {
		return dec.Decimal{}, false
	}
	return p, true
}
