package orderbook

import (
	"github.com/google/btree"

	"github.com/xargin/opentrade/pkg/dec"
)

// sideBook is one side of the order book (bids or asks). Levels are stored in
// a Google B-tree keyed by price; the Less function flips direction so that
// tree.Min() always returns the top-of-book level:
//
//   - bids: "less" means higher price, so Min() = highest bid (best)
//   - asks: "less" means lower price,  so Min() = lowest  ask (best)
//
// Walking the tree via Ascend then visits levels in best-first priority.
type sideBook struct {
	bid  bool
	tree *btree.BTreeG[*priceLevel]
}

const btreeDegree = 32

func newSideBook(bid bool) *sideBook {
	var less btree.LessFunc[*priceLevel]
	if bid {
		less = func(a, b *priceLevel) bool { return a.price.Cmp(b.price) > 0 }
	} else {
		less = func(a, b *priceLevel) bool { return a.price.Cmp(b.price) < 0 }
	}
	return &sideBook{bid: bid, tree: btree.NewG[*priceLevel](btreeDegree, less)}
}

// best returns the top-of-book level, or nil if empty.
func (s *sideBook) best() *priceLevel {
	lvl, ok := s.tree.Min()
	if !ok {
		return nil
	}
	return lvl
}

// lookup returns the level at the exact price, or nil.
func (s *sideBook) lookup(price dec.Decimal) *priceLevel {
	probe := &priceLevel{price: price}
	if lvl, ok := s.tree.Get(probe); ok {
		return lvl
	}
	return nil
}

// getOrCreate returns the level at price, creating it if absent.
func (s *sideBook) getOrCreate(price dec.Decimal) *priceLevel {
	if lvl := s.lookup(price); lvl != nil {
		return lvl
	}
	lvl := newPriceLevel(price)
	s.tree.ReplaceOrInsert(lvl)
	return lvl
}

// removeLevel removes a level (called when the level becomes empty).
func (s *sideBook) removeLevel(lvl *priceLevel) {
	s.tree.Delete(lvl)
}

// empty reports whether the side has no levels.
func (s *sideBook) empty() bool { return s.tree.Len() == 0 }

// walkLevels visits levels in best-first priority order. The callback returns
// false to stop.
func (s *sideBook) walkLevels(fn func(*priceLevel) bool) {
	s.tree.Ascend(func(lvl *priceLevel) bool { return fn(lvl) })
}
