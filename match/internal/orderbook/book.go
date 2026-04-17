package orderbook

import (
	"container/list"
	"errors"

	"github.com/xargin/opentrade/pkg/dec"
)

// Common errors.
var (
	ErrDuplicateOrderID = errors.New("orderbook: duplicate order id")
	ErrOrderNotFound    = errors.New("orderbook: order id not found")
	ErrFillExceedsQty   = errors.New("orderbook: fill qty exceeds order remaining")
)

// Book is the two-sided order book for a single symbol. It is NOT thread-safe;
// the SymbolWorker provides the serialization (ADR-0016, 0019).
type Book struct {
	symbol string
	bids   *sideBook
	asks   *sideBook
	index  map[uint64]*orderRef
}

// orderRef ties an Order to its position in the side book.
type orderRef struct {
	order *Order
	level *priceLevel
	elem  *list.Element
}

// NewBook constructs an empty book for the given symbol.
func NewBook(symbol string) *Book {
	return &Book{
		symbol: symbol,
		bids:   newSideBook(true),
		asks:   newSideBook(false),
		index:  make(map[uint64]*orderRef),
	}
}

// Symbol returns the symbol this book is for.
func (b *Book) Symbol() string { return b.symbol }

// Len returns the total number of live orders across both sides.
func (b *Book) Len() int { return len(b.index) }

// Has reports whether an order with the given id is on the book.
func (b *Book) Has(orderID uint64) bool { _, ok := b.index[orderID]; return ok }

// Get returns the live order with the given id, or nil.
func (b *Book) Get(orderID uint64) *Order {
	if ref, ok := b.index[orderID]; ok {
		return ref.order
	}
	return nil
}

// Insert adds a live order to the book. Returns ErrDuplicateOrderID if an
// order with the same id already exists. Callers must not insert Market
// orders.
func (b *Book) Insert(o *Order) error {
	if _, dup := b.index[o.ID]; dup {
		return ErrDuplicateOrderID
	}
	side := b.sideFor(o.Side)
	lvl := side.getOrCreate(o.Price)
	elem := lvl.pushBack(o)
	b.index[o.ID] = &orderRef{order: o, level: lvl, elem: elem}
	return nil
}

// Cancel removes the order with the given id from the book and returns it.
// Returns ErrOrderNotFound if no such order is live.
func (b *Book) Cancel(orderID uint64) (*Order, error) {
	ref, ok := b.index[orderID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	ref.level.remove(ref.elem)
	if ref.level.empty() {
		b.sideFor(ref.order.Side).removeLevel(ref.level)
	}
	delete(b.index, orderID)
	return ref.order, nil
}

// Best returns the top-of-book order for the given side and whether one exists.
func (b *Book) Best(side Side) (*Order, bool) {
	sb := b.sideFor(side)
	lvl := sb.best()
	if lvl == nil {
		return nil, false
	}
	return lvl.front().Value.(*Order), true
}

// BestPrice returns the top-of-book price for the given side and whether one
// exists.
func (b *Book) BestPrice(side Side) (dec.Decimal, bool) {
	sb := b.sideFor(side)
	lvl := sb.best()
	if lvl == nil {
		return dec.Zero, false
	}
	return lvl.price, true
}

// Depth returns the total remaining qty across all levels on the given side.
// O(number of levels).
func (b *Book) Depth(side Side) dec.Decimal {
	sum := dec.Zero
	b.sideFor(side).walkLevels(func(lvl *priceLevel) bool {
		sum = sum.Add(lvl.qty)
		return true
	})
	return sum
}

// Fill applies a partial fill of qty to the order with the given id. If the
// order's Remaining becomes zero, it is removed from the book.
//
// Returns the (possibly removed) order, whether the fill exhausted it, and
// any error.
func (b *Book) Fill(orderID uint64, qty dec.Decimal) (*Order, bool, error) {
	ref, ok := b.index[orderID]
	if !ok {
		return nil, false, ErrOrderNotFound
	}
	if qty.Cmp(ref.order.Remaining) > 0 {
		return ref.order, false, ErrFillExceedsQty
	}
	ref.order.Remaining = ref.order.Remaining.Sub(qty)
	ref.level.reduceQty(qty)
	if dec.IsPositive(ref.order.Remaining) {
		return ref.order, false, nil
	}
	// Remaining == 0: remove from queue / index / (maybe) level.
	ref.level.orders.Remove(ref.elem)
	if ref.level.empty() {
		b.sideFor(ref.order.Side).removeLevel(ref.level)
	}
	delete(b.index, orderID)
	return ref.order, true, nil
}

// Walk iterates orders on the given side in priority order (best level first,
// FIFO within a level). Callback returns false to stop.
func (b *Book) Walk(side Side, fn func(*Order) bool) {
	b.sideFor(side).walkLevels(func(lvl *priceLevel) bool {
		for e := lvl.orders.Front(); e != nil; e = e.Next() {
			if !fn(e.Value.(*Order)) {
				return false
			}
		}
		return true
	})
}

// LevelQty returns the total remaining qty at the given price on the given side.
// Returns zero if no such level exists.
func (b *Book) LevelQty(side Side, price dec.Decimal) dec.Decimal {
	if lvl := b.sideFor(side).lookup(price); lvl != nil {
		return lvl.qty
	}
	return dec.Zero
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (b *Book) sideFor(s Side) *sideBook {
	if s == Bid {
		return b.bids
	}
	return b.asks
}
