// Package depth projects trade-event lifecycle events onto a per-symbol
// order-book view and emits DepthUpdate (incremental) / DepthSnapshot (full)
// market-data messages.
//
// Book state is kept in memory only; on restart Quote replays trade-event from
// the last committed offset (at-least-once) and idempotent ops rebuild it.
// See ADR-0021 + ADR-0024.
package depth

import (
	"errors"
	"fmt"
	"sort"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// Side is the side of a resting order.
type Side uint8

const (
	SideBuy  Side = 1 // bids
	SideSell Side = 2 // asks
)

// orderRef tracks the information we need to reverse a resting order when
// its cancel / expire event arrives (those events carry only order_id).
type orderRef struct {
	side      Side
	priceKey  string // normalized price key (from dec.Decimal.String())
	price     dec.Decimal
	remaining dec.Decimal
}

// Book is the per-symbol projection.
type Book struct {
	symbol string
	bids   map[string]dec.Decimal
	asks   map[string]dec.Decimal
	// priceOf stores the original decimal keyed by the normalized price key so
	// DepthUpdate emissions carry a canonical string form.
	priceOf map[string]dec.Decimal
	orders  map[uint64]*orderRef
}

// New returns an empty Book for symbol.
func New(symbol string) *Book {
	return &Book{
		symbol:  symbol,
		bids:    make(map[string]dec.Decimal),
		asks:    make(map[string]dec.Decimal),
		priceOf: make(map[string]dec.Decimal),
		orders:  make(map[uint64]*orderRef),
	}
}

// Symbol returns the symbol this book is bound to.
func (b *Book) Symbol() string { return b.symbol }

// ---------------------------------------------------------------------------
// Event handlers — each returns a DepthUpdate if any level actually moved, or
// nil when the event is a no-op (unknown order id, etc.).
// ---------------------------------------------------------------------------

// OnOrderAccepted ingests a resting order entering the book.
func (b *Book) OnOrderAccepted(orderID uint64, protoSide eventpb.Side, priceStr, remainingStr string) (*eventpb.MarketDataEvent, error) {
	if orderID == 0 {
		return nil, errors.New("depth: order_id required")
	}
	if _, exists := b.orders[orderID]; exists {
		// Defensive: match dedup means we should not see two Accepted events
		// for the same order id. Skip rather than double-add.
		return nil, nil
	}
	side, err := sideFromProto(protoSide)
	if err != nil {
		return nil, err
	}
	price, err := dec.Parse(priceStr)
	if err != nil {
		return nil, fmt.Errorf("depth: bad price: %w", err)
	}
	remaining, err := dec.Parse(remainingStr)
	if err != nil {
		return nil, fmt.Errorf("depth: bad remaining: %w", err)
	}
	if !dec.IsPositive(price) || !dec.IsPositive(remaining) {
		return nil, nil // zero-remaining rest is meaningless
	}

	key := price.String()
	b.priceOf[key] = price
	b.addToLevel(side, key, remaining)
	b.orders[orderID] = &orderRef{
		side:      side,
		priceKey:  key,
		price:     price,
		remaining: remaining,
	}
	return b.makeUpdate(side, key), nil
}

// OnTrade ingests a match that consumed (part of) a maker on the book.
// If the maker is unknown (e.g. a Trade arrived before the matching Accepted
// because of replay ordering glitches) we skip with no emission.
func (b *Book) OnTrade(makerOrderID uint64, qtyStr string) (*eventpb.MarketDataEvent, error) {
	if makerOrderID == 0 {
		return nil, nil
	}
	qty, err := dec.Parse(qtyStr)
	if err != nil {
		return nil, fmt.Errorf("depth: bad trade qty: %w", err)
	}
	if !dec.IsPositive(qty) {
		return nil, nil
	}
	ref, ok := b.orders[makerOrderID]
	if !ok {
		return nil, nil
	}
	if err := b.subtractFromLevel(ref.side, ref.priceKey, qty); err != nil {
		return nil, err
	}
	ref.remaining = ref.remaining.Sub(qty)
	if ref.remaining.Sign() <= 0 {
		delete(b.orders, makerOrderID)
	}
	return b.makeUpdate(ref.side, ref.priceKey), nil
}

// OnOrderClosed removes the order from the book. Used for both OrderCancelled
// and OrderExpired — both produce identical depth effects (the remaining qty
// leaves the book).
func (b *Book) OnOrderClosed(orderID uint64) (*eventpb.MarketDataEvent, error) {
	ref, ok := b.orders[orderID]
	if !ok {
		return nil, nil
	}
	if err := b.subtractFromLevel(ref.side, ref.priceKey, ref.remaining); err != nil {
		return nil, err
	}
	delete(b.orders, orderID)
	return b.makeUpdate(ref.side, ref.priceKey), nil
}

// Snapshot returns a DepthSnapshot MarketDataEvent with every live level.
// bids are returned in descending price order, asks ascending.
func (b *Book) Snapshot() *eventpb.MarketDataEvent {
	return &eventpb.MarketDataEvent{
		Symbol: b.symbol,
		Payload: &eventpb.MarketDataEvent_DepthSnapshot{DepthSnapshot: &eventpb.DepthSnapshot{
			Symbol: b.symbol,
			Bids:   b.snapshotSide(b.bids, true),
			Asks:   b.snapshotSide(b.asks, false),
		}},
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (b *Book) sideMap(s Side) map[string]dec.Decimal {
	if s == SideBuy {
		return b.bids
	}
	return b.asks
}

func (b *Book) addToLevel(s Side, key string, qty dec.Decimal) {
	m := b.sideMap(s)
	cur, ok := m[key]
	if !ok {
		m[key] = qty
		return
	}
	m[key] = cur.Add(qty)
}

// subtractFromLevel reduces the level by qty. Returns error only on
// structural bugs (trying to subtract from a missing level, or going negative
// by more than rounding — which for decimal means any negative). The caller
// has already checked that qty > 0.
func (b *Book) subtractFromLevel(s Side, key string, qty dec.Decimal) error {
	m := b.sideMap(s)
	cur, ok := m[key]
	if !ok {
		return fmt.Errorf("depth: subtract from missing level %s on side %d", key, s)
	}
	nxt := cur.Sub(qty)
	if nxt.Sign() <= 0 {
		delete(m, key)
		// priceOf may still be referenced by other orders at the same price
		// on the opposite side; don't drop it. We trade a tiny bit of memory
		// for simplicity.
		return nil
	}
	m[key] = nxt
	return nil
}

func (b *Book) makeUpdate(s Side, key string) *eventpb.MarketDataEvent {
	price, ok := b.priceOf[key]
	if !ok {
		return nil
	}
	qtyStr := "0"
	if v, ok := b.sideMap(s)[key]; ok {
		qtyStr = v.String()
	}
	level := &eventpb.DepthLevel{Price: price.String(), Qty: qtyStr}
	upd := &eventpb.DepthUpdate{Symbol: b.symbol}
	if s == SideBuy {
		upd.Bids = []*eventpb.DepthLevel{level}
	} else {
		upd.Asks = []*eventpb.DepthLevel{level}
	}
	return &eventpb.MarketDataEvent{
		Symbol:  b.symbol,
		Payload: &eventpb.MarketDataEvent_DepthUpdate{DepthUpdate: upd},
	}
}

func (b *Book) snapshotSide(m map[string]dec.Decimal, descending bool) []*eventpb.DepthLevel {
	if len(m) == 0 {
		return nil
	}
	out := make([]*eventpb.DepthLevel, 0, len(m))
	for key, qty := range m {
		price := b.priceOf[key]
		out = append(out, &eventpb.DepthLevel{Price: price.String(), Qty: qty.String()})
	}
	sort.Slice(out, func(i, j int) bool {
		pi := dec.New(out[i].Price)
		pj := dec.New(out[j].Price)
		if descending {
			return pi.Cmp(pj) > 0
		}
		return pi.Cmp(pj) < 0
	})
	return out
}

func sideFromProto(s eventpb.Side) (Side, error) {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return SideBuy, nil
	case eventpb.Side_SIDE_SELL:
		return SideSell, nil
	default:
		return 0, fmt.Errorf("depth: unspecified side")
	}
}
