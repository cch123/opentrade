// Package snapshot serializes the trigger engine's pending /
// terminal records and per-partition offsets to a BlobStore, so a restart
// resumes where the last primary stopped instead of losing every in-
// flight stop order (ADR-0040 §Persistence).
//
// ADR-0049: default on-disk format is protobuf (.pb); JSON (.json) stays
// available as a debug fallback via --snapshot-format=json. Load probes
// .pb first, then .json.
//
// ADR-0058: I/O goes through pkg/snapshot.BlobStore so trigger can later
// share storage with Counter / trade-dump (FS today, S3 backlog) without
// touching this package.
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/pkg/dec"
	pkgsnapshot "github.com/xargin/opentrade/pkg/snapshot"
	"github.com/xargin/opentrade/trigger/internal/engine"
)

// Version of the on-disk format.
const Version = 1

// Snapshot is the root on-disk document.
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

// Capture builds a Snapshot from the engine. Caller stamps TakenAtMs.
func Capture(eng *engine.Engine) *Snapshot {
	pending, terminals, offsets := eng.Snapshot()
	return &Snapshot{
		Version:     Version,
		Offsets:     offsets,
		Pending:     toSnapSlice(pending),
		Terminals:   toSnapSlice(terminals),
		OCOByClient: eng.OCOByClient(),
	}
}

// Restore hydrates engine from snap. Caller must supply a fresh Engine.
func Restore(eng *engine.Engine, snap *Snapshot) error {
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

// -----------------------------------------------------------------------------
// BlobStore I/O
// -----------------------------------------------------------------------------

// Save encodes snap and writes it under `baseKey + format.Ext()`. The
// BlobStore is responsible for atomicity (FSBlobStore stages through
// .tmp + fsync + rename).
func Save(ctx context.Context, store pkgsnapshot.BlobStore, baseKey string, snap *Snapshot, format pkgsnapshot.Format) error {
	data, err := encode(snap, format)
	if err != nil {
		return fmt.Errorf("encode %s: %w", format, err)
	}
	key := baseKey + format.Ext()
	if err := store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// Load fetches a snapshot by probing proto then json under baseKey
// (ADR-0049). Missing both → (nil, nil) so callers can treat absence as
// cold start.
func Load(ctx context.Context, store pkgsnapshot.BlobStore, baseKey string) (*Snapshot, error) {
	for _, format := range []pkgsnapshot.Format{pkgsnapshot.FormatProto, pkgsnapshot.FormatJSON} {
		key := baseKey + format.Ext()
		data, err := store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("get %s: %w", key, err)
		}
		snap, err := decode(data, format)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		if snap.Version != Version {
			return nil, fmt.Errorf("trigger snapshot version mismatch: got %d want %d", snap.Version, Version)
		}
		return snap, nil
	}
	return nil, nil
}

func encode(snap *Snapshot, format pkgsnapshot.Format) ([]byte, error) {
	switch format {
	case pkgsnapshot.FormatProto:
		return proto.Marshal(toProto(snap))
	case pkgsnapshot.FormatJSON:
		return json.MarshalIndent(snap, "", "  ")
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

func decode(data []byte, format pkgsnapshot.Format) (*Snapshot, error) {
	switch format {
	case pkgsnapshot.FormatProto:
		var pb snapshotpb.TriggerSnapshot
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return fromProto(&pb), nil
	case pkgsnapshot.FormatJSON:
		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, err
		}
		return &snap, nil
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

// -----------------------------------------------------------------------------
// Proto <-> Snapshot mapping (ADR-0049)
// -----------------------------------------------------------------------------

func toProto(s *Snapshot) *snapshotpb.TriggerSnapshot {
	if s == nil {
		return nil
	}
	pb := &snapshotpb.TriggerSnapshot{
		Version:     uint32(s.Version),
		TakenAtMs:   s.TakenAtMs,
		Offsets:     s.Offsets,
		OcoByClient: s.OCOByClient,
	}
	if n := len(s.Pending); n > 0 {
		pb.Pending = make([]*snapshotpb.TriggerRecord, 0, n)
		for i := range s.Pending {
			pb.Pending = append(pb.Pending, recordToProto(&s.Pending[i]))
		}
	}
	if n := len(s.Terminals); n > 0 {
		pb.Terminals = make([]*snapshotpb.TriggerRecord, 0, n)
		for i := range s.Terminals {
			pb.Terminals = append(pb.Terminals, recordToProto(&s.Terminals[i]))
		}
	}
	return pb
}

func recordToProto(c *TriggerSnap) *snapshotpb.TriggerRecord {
	return &snapshotpb.TriggerRecord{
		Id:                c.ID,
		ClientTriggerId:   c.ClientTriggerID,
		UserId:            c.UserID,
		Symbol:            c.Symbol,
		Side:              uint32(c.Side),
		Type:              uint32(c.Type),
		StopPrice:         c.StopPrice,
		LimitPrice:        c.LimitPrice,
		Qty:               c.Qty,
		QuoteQty:          c.QuoteQty,
		Tif:               uint32(c.TIF),
		Status:            uint32(c.Status),
		CreatedAtMs:       c.CreatedAtMs,
		TriggeredAtMs:     c.TriggeredAtMs,
		PlacedOrderId:     c.PlacedOrderID,
		RejectReason:      c.RejectReason,
		ExpiresAtMs:       c.ExpiresAtMs,
		OcoGroupId:        c.OCOGroupID,
		TrailingDeltaBps:  c.TrailingDeltaBps,
		ActivationPrice:   c.ActivationPrice,
		TrailingWatermark: c.TrailingWatermark,
		TrailingActive:    c.TrailingActive,
	}
}

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

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func toSnapSlice(in []*engine.Trigger) []TriggerSnap {
	if len(in) == 0 {
		return nil
	}
	out := make([]TriggerSnap, len(in))
	for i, c := range in {
		out[i] = TriggerSnap{
			ID:                c.ID,
			ClientTriggerID:   c.ClientTriggerID,
			UserID:            c.UserID,
			Symbol:            c.Symbol,
			Side:              uint8(c.Side),
			Type:              uint8(c.Type),
			StopPrice:         c.StopPrice.String(),
			LimitPrice:        decOrEmpty(c.LimitPrice),
			Qty:               decOrEmpty(c.Qty),
			QuoteQty:          decOrEmpty(c.QuoteQty),
			TIF:               uint8(c.TIF),
			Status:            uint8(c.Status),
			CreatedAtMs:       c.CreatedAtMs,
			TriggeredAtMs:     c.TriggeredAtMs,
			PlacedOrderID:     c.PlacedOrderID,
			RejectReason:      c.RejectReason,
			ExpiresAtMs:       c.ExpiresAtMs,
			OCOGroupID:        c.OCOGroupID,
			TrailingDeltaBps:  c.TrailingDeltaBps,
			ActivationPrice:   decOrEmpty(c.ActivationPrice),
			TrailingWatermark: decOrEmpty(c.TrailingWatermark),
			TrailingActive:    c.TrailingActive,
		}
	}
	return out
}

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

func decOrEmpty(d dec.Decimal) string {
	if dec.IsZero(d) {
		return ""
	}
	return d.String()
}
