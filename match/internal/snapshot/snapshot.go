// Package snapshot serializes a SymbolWorker's orderbook state to local disk
// and restores it on startup.
//
// MVP-1 scope: single-file JSON per symbol, atomic write (tmp + rename).
// Future: protobuf format, S3/EFS upload, rotation (ADR-0006).
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// Version marks the snapshot format version. Bump when the schema changes.
const Version = 1

// KafkaOffset records the consumer position for a (topic, partition).
// Included in the snapshot so that recovery can resume from the exact point
// the snapshot was taken.
type KafkaOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OrderSnapshot is the serialized form of an orderbook.Order.
type OrderSnapshot struct {
	ID        uint64 `json:"id"`
	UserID    string `json:"user_id"`
	ClientID  string `json:"client_id,omitempty"`
	Side      uint8  `json:"side"`
	Type      uint8  `json:"type"`
	TIF       uint8  `json:"tif"`
	Price     string `json:"price"`
	Qty       string `json:"qty"`
	Remaining string `json:"remaining"`
	CreatedAt int64  `json:"created_at"`
}

// SymbolSnapshot is the full on-disk representation of one SymbolWorker.
type SymbolSnapshot struct {
	Version     int             `json:"version"`
	Symbol      string          `json:"symbol"`
	SeqID       uint64          `json:"seq_id"`
	Offsets     []KafkaOffset   `json:"offsets"`
	Orders      []OrderSnapshot `json:"orders"`
	TimestampMS int64           `json:"ts_unix_ms"`
}

// -----------------------------------------------------------------------------
// Capture / restore between Worker <-> Snapshot
// -----------------------------------------------------------------------------

// Capture extracts the current state of w into a SymbolSnapshot. offsets is
// the set of consumer positions the worker has fully processed and is safe to
// resume from. timestampMS is the wall-clock time at which the snapshot is
// taken (informational).
//
// Must be called from the worker goroutine (or while the worker is stopped)
// to avoid reading a partially-updated Book.
func Capture(w *sequencer.SymbolWorker, offsets []KafkaOffset, timestampMS int64) *SymbolSnapshot {
	snap := &SymbolSnapshot{
		Version:     Version,
		Symbol:      w.Symbol(),
		SeqID:       w.SeqID(),
		Offsets:     append([]KafkaOffset(nil), offsets...),
		TimestampMS: timestampMS,
	}
	w.Book().Walk(orderbook.Bid, func(o *orderbook.Order) bool {
		snap.Orders = append(snap.Orders, toSnapshot(o))
		return true
	})
	w.Book().Walk(orderbook.Ask, func(o *orderbook.Order) bool {
		snap.Orders = append(snap.Orders, toSnapshot(o))
		return true
	})
	return snap
}

// Restore rebuilds the worker's state from snap. The worker MUST be freshly
// constructed (empty book, default seq_id); calling Restore on a worker that
// has already processed events panics.
func Restore(w *sequencer.SymbolWorker, snap *SymbolSnapshot) error {
	if w.Book().Len() != 0 || w.SeqID() != 0 {
		return fmt.Errorf("snapshot.Restore: worker is not empty")
	}
	if snap.Symbol != w.Symbol() {
		return fmt.Errorf("snapshot.Restore: symbol mismatch: %q vs %q", snap.Symbol, w.Symbol())
	}
	if snap.Version != Version {
		return fmt.Errorf("snapshot.Restore: version mismatch: got %d want %d", snap.Version, Version)
	}
	for i := range snap.Orders {
		o, err := fromSnapshot(snap.Symbol, snap.Orders[i])
		if err != nil {
			return fmt.Errorf("snapshot.Restore: order %d: %w", snap.Orders[i].ID, err)
		}
		if err := w.Book().Insert(o); err != nil {
			return fmt.Errorf("snapshot.Restore: insert order %d: %w", o.ID, err)
		}
	}
	w.SetSeqID(snap.SeqID)
	return nil
}

// -----------------------------------------------------------------------------
// Disk I/O
// -----------------------------------------------------------------------------

// Save writes snap to path atomically (tmp + rename).
func Save(path string, snap *SymbolSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("snapshot.Save: mkdir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("snapshot.Save: create tmp: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snap); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot.Save: encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot.Save: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot.Save: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot.Save: rename: %w", err)
	}
	return nil
}

// Load reads a snapshot from path.
func Load(path string) (*SymbolSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var snap SymbolSnapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return nil, fmt.Errorf("snapshot.Load: decode: %w", err)
	}
	return &snap, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func toSnapshot(o *orderbook.Order) OrderSnapshot {
	return OrderSnapshot{
		ID:        o.ID,
		UserID:    o.UserID,
		ClientID:  o.ClientID,
		Side:      uint8(o.Side),
		Type:      uint8(o.Type),
		TIF:       uint8(o.TIF),
		Price:     o.Price.String(),
		Qty:       o.Qty.String(),
		Remaining: o.Remaining.String(),
		CreatedAt: o.CreatedAt,
	}
}

func fromSnapshot(symbol string, s OrderSnapshot) (*orderbook.Order, error) {
	price, err := dec.Parse(s.Price)
	if err != nil {
		return nil, fmt.Errorf("price: %w", err)
	}
	qty, err := dec.Parse(s.Qty)
	if err != nil {
		return nil, fmt.Errorf("qty: %w", err)
	}
	rem, err := dec.Parse(s.Remaining)
	if err != nil {
		return nil, fmt.Errorf("remaining: %w", err)
	}
	return &orderbook.Order{
		ID:        s.ID,
		UserID:    s.UserID,
		ClientID:  s.ClientID,
		Symbol:    symbol,
		Side:      orderbook.Side(s.Side),
		Type:      orderbook.OrderType(s.Type),
		TIF:       orderbook.TIF(s.TIF),
		Price:     price,
		Qty:       qty,
		Remaining: rem,
		CreatedAt: s.CreatedAt,
	}, nil
}
