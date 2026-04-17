package orderbook

import (
	"container/list"

	"github.com/xargin/opentrade/pkg/dec"
)

// priceLevel is the FIFO queue of orders at a single price.
//
// Invariant: qty == Σ (order.Remaining for order in orders).
type priceLevel struct {
	price  dec.Decimal
	qty    dec.Decimal
	orders *list.List // elements are *Order, front is the oldest (highest priority)
}

func newPriceLevel(price dec.Decimal) *priceLevel {
	return &priceLevel{
		price:  price,
		qty:    dec.Zero,
		orders: list.New(),
	}
}

// pushBack appends an order to the tail of the queue.
func (lvl *priceLevel) pushBack(o *Order) *list.Element {
	lvl.qty = lvl.qty.Add(o.Remaining)
	return lvl.orders.PushBack(o)
}

// remove unlinks an order from the queue and decreases the level qty by
// the order's current Remaining.
func (lvl *priceLevel) remove(e *list.Element) {
	o := e.Value.(*Order)
	lvl.qty = lvl.qty.Sub(o.Remaining)
	lvl.orders.Remove(e)
}

// reduceQty decreases the level total qty by delta (used when the maker at
// the front is partially filled).
func (lvl *priceLevel) reduceQty(delta dec.Decimal) {
	lvl.qty = lvl.qty.Sub(delta)
}

// front returns the element at the head of the queue, or nil when empty.
func (lvl *priceLevel) front() *list.Element { return lvl.orders.Front() }

// empty reports whether the level has no orders.
func (lvl *priceLevel) empty() bool { return lvl.orders.Len() == 0 }
