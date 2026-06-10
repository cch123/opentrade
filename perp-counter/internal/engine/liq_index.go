package engine

import (
	"hash/fnv"
	"strconv"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// liqIndex is ADR-0072's derived liquidation-price index. It deliberately
// lives inside Engine and is protected by Engine.mu: the index is a cache over
// authoritative positions, so keeping updates in the same critical section as
// position mutations is more important than introducing a separately
// concurrent data structure.
type liqIndex struct {
	symbols    map[string]*symbolLiqIndex
	byPosition map[liqPositionKey]liqIndexEntry
}

type symbolLiqIndex struct {
	long  sideLiqTree
	short sideLiqTree
}

type sideLiqTree struct {
	root *liqNode
}

// The side tree is a deterministic treap keyed by (liqPrice,user,symbol). A
// balanced stdlib tree does not exist in Go, and a heap alone cannot delete or
// update arbitrary positions cheaply. The treap keeps O(log n) expected
// upsert/delete/range behavior while the hash-derived priority makes snapshot
// restore and tests stable.
type liqNode struct {
	entry    liqIndexEntry
	priority uint64
	left     *liqNode
	right    *liqNode
}

type liqPositionKey struct {
	userID uint64
	symbol string
}

type liqIndexEntry struct {
	userID   uint64
	symbol   string
	side     perpstate.Side
	liqPrice dec.Decimal
}

func newLiqIndex() *liqIndex {
	return &liqIndex{
		symbols:    map[string]*symbolLiqIndex{},
		byPosition: map[liqPositionKey]liqIndexEntry{},
	}
}

func (idx *liqIndex) rebuild(positions map[uint64]map[string]*perpstate.Position, mmrFor func(*perpstate.Position) perpstate.MMRFunc) {
	idx.symbols = map[string]*symbolLiqIndex{}
	idx.byPosition = map[liqPositionKey]liqIndexEntry{}
	if mmrFor == nil {
		return
	}
	for user, bySym := range positions {
		for symbol, p := range bySym {
			idx.upsert(user, symbol, p, mmrFor(p))
		}
	}
}

func (idx *liqIndex) upsert(user uint64, symbol string, p *perpstate.Position, mmrOf perpstate.MMRFunc) {
	idx.remove(user, symbol)
	if p == nil || p.IsFlat() || mmrOf == nil {
		return
	}
	// Cross positions have no per-position liquidation price — the account
	// pool is the trigger unit (ADR-0074 §4 rule #6). Indexing them would
	// produce wrong candidates from isolated-style math.
	if p.Mode == perpstate.MarginCross {
		return
	}
	if p.Side != perpstate.SideBuy && p.Side != perpstate.SideSell {
		return
	}
	// The index stores the trigger price, not the current health. That keeps
	// mark ticks cheap; the caller still rechecks the full CollateralPool before
	// acting so stale tier config or mark gaps only create harmless candidates.
	entry := liqIndexEntry{
		userID: user, symbol: symbol, side: p.Side,
		liqPrice: p.LiqPrice(mmrOf),
	}
	idx.bySymbol(symbol).treeFor(p.Side).insert(entry)
	idx.byPosition[liqPositionKey{userID: user, symbol: symbol}] = entry
}

func (idx *liqIndex) remove(user uint64, symbol string) {
	key := liqPositionKey{userID: user, symbol: symbol}
	old, ok := idx.byPosition[key]
	if !ok {
		return
	}
	if symIdx := idx.symbols[old.symbol]; symIdx != nil {
		symIdx.treeFor(old.side).delete(old)
		if symIdx.long.root == nil && symIdx.short.root == nil {
			delete(idx.symbols, old.symbol)
		}
	}
	delete(idx.byPosition, key)
}

func (idx *liqIndex) crossed(symbol string, mark dec.Decimal) []liqIndexEntry {
	symIdx := idx.symbols[symbol]
	if symIdx == nil {
		return nil
	}
	var out []liqIndexEntry
	// Longs are liquidatable when mark falls through their liq price; shorts
	// are liquidatable when mark rises through it. The query returns the full
	// current crossed set, so mark gaps do not require replaying intermediate
	// ticks.
	symIdx.long.rangeGreaterEqual(mark, func(entry liqIndexEntry) {
		out = append(out, entry)
	})
	symIdx.short.rangeLessEqual(mark, func(entry liqIndexEntry) {
		out = append(out, entry)
	})
	return out
}

func (idx *liqIndex) bySymbol(symbol string) *symbolLiqIndex {
	symIdx := idx.symbols[symbol]
	if symIdx == nil {
		symIdx = &symbolLiqIndex{}
		idx.symbols[symbol] = symIdx
	}
	return symIdx
}

func (idx *symbolLiqIndex) treeFor(side perpstate.Side) *sideLiqTree {
	if side == perpstate.SideBuy {
		return &idx.long
	}
	return &idx.short
}

func (t *sideLiqTree) insert(entry liqIndexEntry) {
	t.root = insertLiqNode(t.root, &liqNode{entry: entry, priority: liqPriority(entry)})
}

func insertLiqNode(root, n *liqNode) *liqNode {
	if root == nil {
		return n
	}
	cmp := compareLiqEntry(n.entry, root.entry)
	if cmp == 0 {
		root.entry = n.entry
		return root
	}
	if cmp < 0 {
		root.left = insertLiqNode(root.left, n)
		if root.left.priority < root.priority {
			return rotateLiqRight(root)
		}
		return root
	}
	root.right = insertLiqNode(root.right, n)
	if root.right.priority < root.priority {
		return rotateLiqLeft(root)
	}
	return root
}

func (t *sideLiqTree) delete(entry liqIndexEntry) {
	t.root = deleteLiqNode(t.root, entry)
}

func deleteLiqNode(root *liqNode, entry liqIndexEntry) *liqNode {
	if root == nil {
		return nil
	}
	cmp := compareLiqEntry(entry, root.entry)
	if cmp < 0 {
		root.left = deleteLiqNode(root.left, entry)
		return root
	}
	if cmp > 0 {
		root.right = deleteLiqNode(root.right, entry)
		return root
	}
	return mergeLiqNodes(root.left, root.right)
}

func mergeLiqNodes(left, right *liqNode) *liqNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority < right.priority {
		left.right = mergeLiqNodes(left.right, right)
		return left
	}
	right.left = mergeLiqNodes(left, right.left)
	return right
}

func rotateLiqLeft(root *liqNode) *liqNode {
	next := root.right
	root.right = next.left
	next.left = root
	return next
}

func rotateLiqRight(root *liqNode) *liqNode {
	next := root.left
	root.left = next.right
	next.right = root
	return next
}

func (t *sideLiqTree) rangeLessEqual(max dec.Decimal, fn func(liqIndexEntry)) {
	rangeLiqLessEqual(t.root, max, fn)
}

func rangeLiqLessEqual(root *liqNode, max dec.Decimal, fn func(liqIndexEntry)) {
	if root == nil {
		return
	}
	if root.entry.liqPrice.Cmp(max) <= 0 {
		rangeLiqLessEqual(root.left, max, fn)
		fn(root.entry)
		rangeLiqLessEqual(root.right, max, fn)
		return
	}
	rangeLiqLessEqual(root.left, max, fn)
}

func (t *sideLiqTree) rangeGreaterEqual(min dec.Decimal, fn func(liqIndexEntry)) {
	rangeLiqGreaterEqual(t.root, min, fn)
}

func rangeLiqGreaterEqual(root *liqNode, min dec.Decimal, fn func(liqIndexEntry)) {
	if root == nil {
		return
	}
	if root.entry.liqPrice.Cmp(min) >= 0 {
		rangeLiqGreaterEqual(root.left, min, fn)
		fn(root.entry)
		rangeLiqGreaterEqual(root.right, min, fn)
		return
	}
	rangeLiqGreaterEqual(root.right, min, fn)
}

func compareLiqEntry(a, b liqIndexEntry) int {
	if cmp := a.liqPrice.Cmp(b.liqPrice); cmp != 0 {
		return cmp
	}
	// userID/symbol make the tree key total. Without this tie-breaker, two
	// positions at the same liq price would overwrite each other and a scan
	// could silently miss one account.
	if a.userID < b.userID {
		return -1
	}
	if a.userID > b.userID {
		return 1
	}
	if a.symbol < b.symbol {
		return -1
	}
	if a.symbol > b.symbol {
		return 1
	}
	return 0
}

func liqPriority(entry liqIndexEntry) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(entry.symbol))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatUint(entry.userID, 10)))
	_, _ = h.Write([]byte{0, byte(entry.side)})
	return h.Sum64()
}
