// Package snapshot serializes the conditional engine's pending /
// terminal records and per-partition offsets to local disk, so a restart
// resumes where the last primary stopped instead of losing every in-
// flight stop order (ADR-0040 §Persistence).
//
// Format: single JSON file, atomic tmp + rename. Same shape as
// counter/internal/snapshot (so ops can eyeball it the same way).
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xargin/opentrade/conditional/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
)

// Version of the on-disk format.
const Version = 1

// Snapshot is the root on-disk document.
type Snapshot struct {
	Version      int                `json:"version"`
	TakenAtMs    int64              `json:"taken_at_ms"`
	Offsets      map[int32]int64    `json:"offsets,omitempty"`
	Pending      []ConditionalSnap  `json:"pending,omitempty"`
	Terminals    []ConditionalSnap  `json:"terminals,omitempty"`
}

// ConditionalSnap is the persisted shape of one conditional record. Stored
// as strings so a manual `jq` inspection is legible.
type ConditionalSnap struct {
	ID            uint64 `json:"id"`
	ClientCondID  string `json:"client_cond_id,omitempty"`
	UserID        string `json:"user_id"`
	Symbol        string `json:"symbol"`
	Side          uint8  `json:"side"`
	Type          uint8  `json:"type"`
	StopPrice     string `json:"stop_price"`
	LimitPrice    string `json:"limit_price,omitempty"`
	Qty           string `json:"qty,omitempty"`
	QuoteQty      string `json:"quote_qty,omitempty"`
	TIF           uint8  `json:"tif,omitempty"`
	Status        uint8  `json:"status"`
	CreatedAtMs   int64  `json:"created_at_ms"`
	TriggeredAtMs int64  `json:"triggered_at_ms,omitempty"`
	PlacedOrderID uint64 `json:"placed_order_id,omitempty"`
	RejectReason  string `json:"reject_reason,omitempty"`
	ExpiresAtMs   int64  `json:"expires_at_ms,omitempty"` // ADR-0043
}

// Capture builds a Snapshot from the engine. Caller stamps TakenAtMs.
func Capture(eng *engine.Engine) *Snapshot {
	pending, terminals, offsets := eng.Snapshot()
	return &Snapshot{
		Version:   Version,
		Offsets:   offsets,
		Pending:   toSnapSlice(pending),
		Terminals: toSnapSlice(terminals),
	}
}

// Restore hydrates engine from snap. Caller must supply a fresh Engine.
func Restore(eng *engine.Engine, snap *Snapshot) error {
	if snap == nil {
		return nil
	}
	if snap.Version != Version {
		return fmt.Errorf("conditional snapshot version mismatch: got %d want %d", snap.Version, Version)
	}
	pending, err := fromSnapSlice(snap.Pending)
	if err != nil {
		return fmt.Errorf("pending: %w", err)
	}
	terminals, err := fromSnapSlice(snap.Terminals)
	if err != nil {
		return fmt.Errorf("terminals: %w", err)
	}
	eng.Restore(pending, terminals, snap.Offsets)
	return nil
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

// Load reads a snapshot from path. Missing file → (nil, nil) so callers
// can treat absence as cold start.
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
		return nil, fmt.Errorf("conditional snapshot version mismatch: got %d want %d", snap.Version, Version)
	}
	return &snap, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func toSnapSlice(in []*engine.Conditional) []ConditionalSnap {
	if len(in) == 0 {
		return nil
	}
	out := make([]ConditionalSnap, len(in))
	for i, c := range in {
		out[i] = ConditionalSnap{
			ID:            c.ID,
			ClientCondID:  c.ClientCondID,
			UserID:        c.UserID,
			Symbol:        c.Symbol,
			Side:          uint8(c.Side),
			Type:          uint8(c.Type),
			StopPrice:     c.StopPrice.String(),
			LimitPrice:    decOrEmpty(c.LimitPrice),
			Qty:           decOrEmpty(c.Qty),
			QuoteQty:      decOrEmpty(c.QuoteQty),
			TIF:           uint8(c.TIF),
			Status:        uint8(c.Status),
			CreatedAtMs:   c.CreatedAtMs,
			TriggeredAtMs: c.TriggeredAtMs,
			PlacedOrderID: c.PlacedOrderID,
			RejectReason:  c.RejectReason,
			ExpiresAtMs:   c.ExpiresAtMs,
		}
	}
	return out
}

func fromSnapSlice(in []ConditionalSnap) ([]*engine.Conditional, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*engine.Conditional, 0, len(in))
	for _, s := range in {
		stop, err := dec.Parse(s.StopPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d stop_price: %w", s.ID, err)
		}
		limit, err := dec.Parse(s.LimitPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d limit_price: %w", s.ID, err)
		}
		qty, err := dec.Parse(s.Qty)
		if err != nil {
			return nil, fmt.Errorf("id %d qty: %w", s.ID, err)
		}
		qq, err := dec.Parse(s.QuoteQty)
		if err != nil {
			return nil, fmt.Errorf("id %d quote_qty: %w", s.ID, err)
		}
		out = append(out, &engine.Conditional{
			ID:            s.ID,
			ClientCondID:  s.ClientCondID,
			UserID:        s.UserID,
			Symbol:        s.Symbol,
			Side:          eventpb.Side(s.Side),
			Type:          condrpc.ConditionalType(s.Type),
			StopPrice:     stop,
			LimitPrice:    limit,
			Qty:           qty,
			QuoteQty:      qq,
			TIF:           eventpb.TimeInForce(s.TIF),
			Status:        condrpc.ConditionalStatus(s.Status),
			CreatedAtMs:   s.CreatedAtMs,
			TriggeredAtMs: s.TriggeredAtMs,
			PlacedOrderID: s.PlacedOrderID,
			RejectReason:  s.RejectReason,
			ExpiresAtMs:   s.ExpiresAtMs,
		})
	}
	return out, nil
}

func decOrEmpty(d dec.Decimal) string {
	if dec.IsZero(d) {
		return ""
	}
	return d.String()
}
