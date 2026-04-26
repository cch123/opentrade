// Package snapshot is trigger's consumer-side adapter over the
// trigger snapshot wire format (api/gen/snapshot.TriggerSnapshot).
// trade-dump's shadow pipeline produces these snapshots
// (pkg/snapshot/trigger.Save, ADR-0067); this package converts the
// proto back into engine state on startup.
//
// After ADR-0067 M6 trigger no longer self-produces snapshots — the
// Capture / Save path used to live here and was removed once the
// trade-dump shadow pipeline was verified end-to-end.
package snapshot

import (
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/trigger/engine"
)

// Version of the on-disk format.
const Version = 1

// Snapshot is the Go-side mirror of snapshotpb.TriggerSnapshot.
// Returned from RestoreFromProto so callers can read fields like
// Offsets / lengths / TakenAtMs without re-decoding.
type Snapshot struct {
	Version   int             `json:"version"`
	TakenAtMs int64           `json:"taken_at_ms"`
	Offsets   map[int32]int64 `json:"offsets,omitempty"`
	Pending   []TriggerSnap   `json:"pending,omitempty"`
	Terminals []TriggerSnap   `json:"terminals,omitempty"`
	// OCOByClient maps client-supplied OCO idempotency keys to their
	// engine-assigned group ids. Restored alongside pending/terminals so
	// PlaceOCO dedup survives restart (ADR-0044).
	OCOByClient map[string]string `json:"oco_by_client,omitempty"`
}

// TriggerSnap is the persisted shape of one trigger record. Stored
// as strings so a manual `jq` inspection is legible.
type TriggerSnap struct {
	ID              uint64 `json:"id"`
	ClientTriggerID string `json:"client_trigger_id,omitempty"`
	UserID          string `json:"user_id"`
	Symbol          string `json:"symbol"`
	Side            uint8  `json:"side"`
	Type            uint8  `json:"type"`
	StopPrice       string `json:"stop_price"`
	LimitPrice      string `json:"limit_price,omitempty"`
	Qty             string `json:"qty,omitempty"`
	QuoteQty        string `json:"quote_qty,omitempty"`
	TIF             uint8  `json:"tif,omitempty"`
	Status          uint8  `json:"status"`
	CreatedAtMs     int64  `json:"created_at_ms"`
	TriggeredAtMs   int64  `json:"triggered_at_ms,omitempty"`
	PlacedOrderID   uint64 `json:"placed_order_id,omitempty"`
	RejectReason    string `json:"reject_reason,omitempty"`
	ExpiresAtMs     int64  `json:"expires_at_ms,omitempty"` // ADR-0043
	OCOGroupID      string `json:"oco_group_id,omitempty"`  // ADR-0044
	// Trailing-stop state (ADR-0045).
	TrailingDeltaBps  int32  `json:"trailing_delta_bps,omitempty"`
	ActivationPrice   string `json:"activation_price,omitempty"`
	TrailingWatermark string `json:"trailing_watermark,omitempty"`
	TrailingActive    bool   `json:"trailing_active,omitempty"`
}

// restore hydrates eng from the Go-side Snapshot. Caller must supply
// a fresh Engine.
func restore(eng *engine.Engine, snap *Snapshot) error {
	if snap == nil {
		return nil
	}
	if snap.Version != Version {
		return fmt.Errorf("trigger snapshot version mismatch: got %d want %d", snap.Version, Version)
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
	eng.SetOCOByClient(snap.OCOByClient)
	return nil
}

// RestoreFromProto restores eng from a proto-form TriggerSnapshot —
// the shape trade-dump's shadow pipeline produces (ADR-0067). Returns
// the converted Go-side Snapshot so callers can read its fields
// (Offsets for consumer wiring, lengths / TakenAtMs for logging)
// without re-decoding.
func RestoreFromProto(eng *engine.Engine, pb *snapshotpb.TriggerSnapshot) (*Snapshot, error) {
	s := fromProto(pb)
	if s == nil {
		return nil, nil
	}
	if err := restore(eng, s); err != nil {
		return nil, err
	}
	return s, nil
}

// -----------------------------------------------------------------------------
// Proto → Snapshot mapping
// -----------------------------------------------------------------------------

func fromProto(pb *snapshotpb.TriggerSnapshot) *Snapshot {
	if pb == nil {
		return nil
	}
	s := &Snapshot{
		Version:     int(pb.Version),
		TakenAtMs:   pb.TakenAtMs,
		Offsets:     pb.Offsets,
		OCOByClient: pb.OcoByClient,
	}
	if n := len(pb.Pending); n > 0 {
		s.Pending = make([]TriggerSnap, 0, n)
		for _, r := range pb.Pending {
			s.Pending = append(s.Pending, recordFromProto(r))
		}
	}
	if n := len(pb.Terminals); n > 0 {
		s.Terminals = make([]TriggerSnap, 0, n)
		for _, r := range pb.Terminals {
			s.Terminals = append(s.Terminals, recordFromProto(r))
		}
	}
	return s
}

func recordFromProto(r *snapshotpb.TriggerRecord) TriggerSnap {
	return TriggerSnap{
		ID:                r.Id,
		ClientTriggerID:   r.ClientTriggerId,
		UserID:            r.UserId,
		Symbol:            r.Symbol,
		Side:              uint8(r.Side),
		Type:              uint8(r.Type),
		StopPrice:         r.StopPrice,
		LimitPrice:        r.LimitPrice,
		Qty:               r.Qty,
		QuoteQty:          r.QuoteQty,
		TIF:               uint8(r.Tif),
		Status:            uint8(r.Status),
		CreatedAtMs:       r.CreatedAtMs,
		TriggeredAtMs:     r.TriggeredAtMs,
		PlacedOrderID:     r.PlacedOrderId,
		RejectReason:      r.RejectReason,
		ExpiresAtMs:       r.ExpiresAtMs,
		OCOGroupID:        r.OcoGroupId,
		TrailingDeltaBps:  r.TrailingDeltaBps,
		ActivationPrice:   r.ActivationPrice,
		TrailingWatermark: r.TrailingWatermark,
		TrailingActive:    r.TrailingActive,
	}
}

// fromSnapSlice converts a slice of TriggerSnap (string-encoded
// decimals) back into engine.Trigger instances ready to install via
// engine.Restore.
func fromSnapSlice(in []TriggerSnap) ([]*engine.Trigger, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*engine.Trigger, 0, len(in))
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
		activation, err := dec.Parse(s.ActivationPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d activation_price: %w", s.ID, err)
		}
		watermark, err := dec.Parse(s.TrailingWatermark)
		if err != nil {
			return nil, fmt.Errorf("id %d trailing_watermark: %w", s.ID, err)
		}
		out = append(out, &engine.Trigger{
			ID:                s.ID,
			ClientTriggerID:   s.ClientTriggerID,
			UserID:            s.UserID,
			Symbol:            s.Symbol,
			Side:              eventpb.Side(s.Side),
			Type:              condrpc.TriggerType(s.Type),
			StopPrice:         stop,
			LimitPrice:        limit,
			Qty:               qty,
			QuoteQty:          qq,
			TIF:               eventpb.TimeInForce(s.TIF),
			Status:            condrpc.TriggerStatus(s.Status),
			CreatedAtMs:       s.CreatedAtMs,
			TriggeredAtMs:     s.TriggeredAtMs,
			PlacedOrderID:     s.PlacedOrderID,
			RejectReason:      s.RejectReason,
			ExpiresAtMs:       s.ExpiresAtMs,
			OCOGroupID:        s.OCOGroupID,
			TrailingDeltaBps:  s.TrailingDeltaBps,
			ActivationPrice:   activation,
			TrailingWatermark: watermark,
			TrailingActive:    s.TrailingActive,
		})
	}
	return out, nil
}
