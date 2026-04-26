// Package snapshot is trigger's consumer-side adapter over the trigger
// snapshot wire format (api/gen/snapshot.TriggerSnapshot). Per ADR-0067
// trade-dump's shadow pipeline produces these snapshots
// (pkg/snapshot/trigger.Save); trigger startup reads them via Load on
// boot. Mirrors counter/internal/snapshot — single Load entry point, no
// Save / Capture / Encode surface; capture lives in trade-dump now.
package snapshot

import (
	"context"
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/pkg/dec"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	"github.com/xargin/opentrade/trigger/engine"
)

// Version is the on-disk wire-format version. Bumped only when the
// proto shape changes incompatibly.
const Version = 1

// Restored summarises one Load — caller-visible market-data offsets
// (used to seed the consumer) plus counts / TakenAtMs for the
// "trigger restored" log line. Engine state has already been installed
// onto eng by the time Load returns.
type Restored struct {
	Offsets   map[int32]int64
	TakenAtMs int64
	Pending   int
	Terminals int
}

// Load fetches the trigger snapshot at key from store and installs it
// onto eng (Restore + SetOCOByClient). Returns os.ErrNotExist (via
// triggersnap.Load) when neither .pb nor .json variant exists; callers
// branch on it for cold start.
func Load(
	ctx context.Context,
	store snapshotpkg.BlobStore,
	key string,
	eng *engine.Engine,
) (*Restored, error) {
	pb, err := triggersnap.Load(ctx, store, key)
	if err != nil {
		return nil, err
	}
	if pb.Version != uint32(Version) {
		return nil, fmt.Errorf("trigger snapshot version mismatch: got %d want %d", pb.Version, Version)
	}
	pending, err := triggersFromProto(pb.Pending)
	if err != nil {
		return nil, fmt.Errorf("pending: %w", err)
	}
	terminals, err := triggersFromProto(pb.Terminals)
	if err != nil {
		return nil, fmt.Errorf("terminals: %w", err)
	}
	eng.Restore(pending, terminals, pb.Offsets)
	eng.SetOCOByClient(pb.OcoByClient)
	return &Restored{
		Offsets:   pb.Offsets,
		TakenAtMs: pb.TakenAtMs,
		Pending:   len(pending),
		Terminals: len(terminals),
	}, nil
}

func triggersFromProto(in []*snapshotpb.TriggerRecord) ([]*engine.Trigger, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*engine.Trigger, 0, len(in))
	for _, r := range in {
		stop, err := dec.Parse(r.StopPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d stop_price: %w", r.Id, err)
		}
		limit, err := dec.Parse(r.LimitPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d limit_price: %w", r.Id, err)
		}
		qty, err := dec.Parse(r.Qty)
		if err != nil {
			return nil, fmt.Errorf("id %d qty: %w", r.Id, err)
		}
		qq, err := dec.Parse(r.QuoteQty)
		if err != nil {
			return nil, fmt.Errorf("id %d quote_qty: %w", r.Id, err)
		}
		activation, err := dec.Parse(r.ActivationPrice)
		if err != nil {
			return nil, fmt.Errorf("id %d activation_price: %w", r.Id, err)
		}
		watermark, err := dec.Parse(r.TrailingWatermark)
		if err != nil {
			return nil, fmt.Errorf("id %d trailing_watermark: %w", r.Id, err)
		}
		out = append(out, &engine.Trigger{
			ID:                r.Id,
			ClientTriggerID:   r.ClientTriggerId,
			UserID:            r.UserId,
			Symbol:            r.Symbol,
			Side:              eventpb.Side(r.Side),
			Type:              condrpc.TriggerType(r.Type),
			StopPrice:         stop,
			LimitPrice:        limit,
			Qty:               qty,
			QuoteQty:          qq,
			TIF:               eventpb.TimeInForce(r.Tif),
			Status:            condrpc.TriggerStatus(r.Status),
			CreatedAtMs:       r.CreatedAtMs,
			TriggeredAtMs:     r.TriggeredAtMs,
			PlacedOrderID:     r.PlacedOrderId,
			RejectReason:      r.RejectReason,
			ExpiresAtMs:       r.ExpiresAtMs,
			OCOGroupID:        r.OcoGroupId,
			TrailingDeltaBps:  r.TrailingDeltaBps,
			ActivationPrice:   activation,
			TrailingWatermark: watermark,
			TrailingActive:    r.TrailingActive,
		})
	}
	return out, nil
}
