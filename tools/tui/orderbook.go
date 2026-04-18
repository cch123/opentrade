package main

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

// level represents one price level. priceNum mirrors price (parsed once at
// insertion) so render sorting doesn't re-parse on every frame.
type level struct {
	price    string
	qty      string
	priceNum float64
}

// orderBook keeps the top-of-book state. WS depth@ frames upsert levels;
// qty=0 deletes. Treats snapshot and update frames identically (ADR-0037
// style), accepting a slightly stale tail in exchange for a simpler TUI.
type orderBook struct {
	symbol string
	bids   map[string]level // keyed by price string
	asks   map[string]level
	at     time.Time
}

func newOrderBook(symbol string) *orderBook {
	return &orderBook{
		symbol: symbol,
		bids:   make(map[string]level),
		asks:   make(map[string]level),
	}
}

// applyDepthJSON parses a depth@ WS payload (DepthSnapshot or DepthUpdate —
// same wire shape) and upserts levels. qty=0 deletes the level.
func (b *orderBook) applyDepthJSON(data json.RawMessage) {
	var p struct {
		Symbol string `json:"symbol"`
		Bids   []struct {
			Price string `json:"price"`
			Qty   string `json:"qty"`
		} `json:"bids"`
		Asks []struct {
			Price string `json:"price"`
			Qty   string `json:"qty"`
		} `json:"asks"`
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return
	}
	if p.Symbol != "" {
		b.symbol = p.Symbol
	}
	for _, l := range p.Bids {
		b.applyLevel(b.bids, l.Price, l.Qty)
	}
	for _, l := range p.Asks {
		b.applyLevel(b.asks, l.Price, l.Qty)
	}
	b.at = time.Now()
}

func (b *orderBook) applyLevel(side map[string]level, price, qty string) {
	if price == "" {
		return
	}
	q, _ := strconv.ParseFloat(qty, 64)
	if q <= 0 {
		delete(side, price)
		return
	}
	pn, _ := strconv.ParseFloat(price, 64)
	side[price] = level{price: price, qty: qty, priceNum: pn}
}

// topBids returns at most n best bids (highest first).
func (b *orderBook) topBids(n int) []level {
	out := make([]level, 0, len(b.bids))
	for _, l := range b.bids {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].priceNum > out[j].priceNum })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// topAsks returns at most n best asks (lowest first).
func (b *orderBook) topAsks(n int) []level {
	out := make([]level, 0, len(b.asks))
	for _, l := range b.asks {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].priceNum < out[j].priceNum })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// bestBid / bestAsk are used to shape the mid-price indicator.
func (b *orderBook) bestBid() (level, bool) {
	top := b.topBids(1)
	if len(top) == 0 {
		return level{}, false
	}
	return top[0], true
}

func (b *orderBook) bestAsk() (level, bool) {
	top := b.topAsks(1)
	if len(top) == 0 {
		return level{}, false
	}
	return top[0], true
}
