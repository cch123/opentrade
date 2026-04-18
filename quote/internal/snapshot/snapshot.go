// Package snapshot serializes Quote's in-memory engine state (per-symbol
// depth book + kline aggregators + emit sequence + per-partition
// trade-event offsets) to local disk, so a restart can resume from the
// saved offsets instead of rescanning the entire topic (ADR-0025 §未来
// 工作, superseded by ADR-0036).
//
// MVP scope: single JSON file per instance, atomic write (tmp + rename).
// S3 / multi-file sharding land later.
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Version of the on-disk format.
const Version = 1

// Snapshot is the full on-disk representation of the engine.
type Snapshot struct {
	Version   int                        `json:"version"`
	TakenAtMs int64                      `json:"taken_at_ms"`
	Seq       uint64                     `json:"seq"`
	Offsets   map[int32]int64            `json:"offsets"`           // partition → next-to-consume
	Symbols   map[string]*SymbolSnapshot `json:"symbols,omitempty"` // symbol → state
}

// SymbolSnapshot captures everything engine tracks for one symbol.
type SymbolSnapshot struct {
	Depth *DepthSnapshot `json:"depth,omitempty"`
	Kline *KlineSnapshot `json:"kline,omitempty"`
}

// DepthSnapshot captures depth.Book state.
type DepthSnapshot struct {
	Symbol string           `json:"symbol"`
	Bids   map[string]string `json:"bids,omitempty"`   // priceKey → qty
	Asks   map[string]string `json:"asks,omitempty"`   // priceKey → qty
	Prices map[string]string `json:"prices,omitempty"` // priceKey → price canonical string
	Orders []OrderRefSnap    `json:"orders,omitempty"`
}

// OrderRefSnap is a resting order as tracked by depth.Book.
type OrderRefSnap struct {
	OrderID   uint64 `json:"order_id"`
	Side      uint8  `json:"side"` // 1=buy/bid, 2=sell/ask
	PriceKey  string `json:"price_key"`
	Remaining string `json:"remaining"`
}

// KlineSnapshot captures kline.Aggregator state.
type KlineSnapshot struct {
	Symbol string            `json:"symbol"`
	Bars   map[int32]BarSnap `json:"bars,omitempty"` // KlineInterval enum int → bar
}

// BarSnap is an open candlestick.
type BarSnap struct {
	OpenTimeMs  int64  `json:"open_time_ms"`
	CloseTimeMs int64  `json:"close_time_ms"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	QuoteVolume string `json:"quote_volume"`
	Count       uint64 `json:"count"`
}

// -----------------------------------------------------------------------------
// Disk I/O
// -----------------------------------------------------------------------------

// Save writes snap to path atomically (tmp + rename + fsync).
func Save(path string, snap *Snapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snap); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Load reads a snapshot from path. Returns (nil, nil) when the file does
// not exist so callers can treat absence as cold start.
func Load(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var snap Snapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if snap.Version != Version {
		return nil, fmt.Errorf("snapshot version mismatch: got %d want %d", snap.Version, Version)
	}
	return &snap, nil
}
