package snapshot

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	"github.com/xargin/opentrade/trigger/engine"
)

type counterSeq struct{ i uint64 }

func (c *counterSeq) Next() uint64 { c.i++; return c.i }

type nopPlacer struct{}

func (nopPlacer) PlaceOrder(context.Context, *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
	return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
}

func newEngine() *engine.Engine {
	return engine.New(engine.Config{
		TerminalHistoryLimit: 50,
		Clock:                func() time.Time { return time.Unix(1_700_000_000, 0) },
	}, &counterSeq{}, nopPlacer{}, nil, zap.NewNop())
}

func newStore(t *testing.T) snapshotpkg.BlobStore {
	t.Helper()
	return snapshotpkg.NewFSBlobStore(t.TempDir())
}

// TestLoad_HappyPath covers the ADR-0067 cold path: trade-dump's
// shadow pipeline writes a snapshotpb.TriggerSnapshot via
// pkg/snapshot/trigger.Save; trigger startup picks it up via Load and
// the engine + offsets come back hydrated.
func TestLoad_HappyPath(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	pb := &snapshotpb.TriggerSnapshot{
		Version:   uint32(Version),
		TakenAtMs: 99,
		Offsets:   map[int32]int64{0: 555, 2: 200},
		Pending: []*snapshotpb.TriggerRecord{
			{
				Id:         7,
				UserId:     "u2",
				Symbol:     "ETH-USDT",
				Side:       uint32(eventpb.Side_SIDE_BUY),
				Type:       uint32(condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT),
				StopPrice:  "200",
				LimitPrice: "205",
				Qty:        "1",
				Status:     uint32(condrpc.TriggerStatus_TRIGGER_STATUS_PENDING),
			},
		},
		Terminals: []*snapshotpb.TriggerRecord{
			{
				Id:        9,
				UserId:    "u1",
				Symbol:    "BTC-USDT",
				Side:      uint32(eventpb.Side_SIDE_SELL),
				Type:      uint32(condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS),
				StopPrice: "100",
				Qty:       "0.5",
				Status:    uint32(condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED),
			},
		},
	}
	if err := triggersnap.Save(ctx, store, "trigger", pb, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("Save: %v", err)
	}

	eng := newEngine()
	got, err := Load(ctx, store, "trigger", eng)
	if err != nil {
		t.Fatal(err)
	}
	if got.TakenAtMs != 99 {
		t.Errorf("TakenAtMs = %d, want 99", got.TakenAtMs)
	}
	if got.Offsets[0] != 555 || got.Offsets[2] != 200 {
		t.Errorf("Offsets = %+v", got.Offsets)
	}
	if got.Pending != 1 || got.Terminals != 1 {
		t.Errorf("counts = (%d, %d), want (1, 1)", got.Pending, got.Terminals)
	}
	// Engine state hydrated — pending u2 lookup + canceled u1 in
	// terminal ring.
	pending := eng.List("u2", false)
	if len(pending) != 1 || pending[0].Type != condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT {
		t.Fatalf("u2 pending = %+v", pending)
	}
	inactive := eng.List("u1", true)
	if len(inactive) != 1 {
		t.Fatalf("u1 inactive = %+v", inactive)
	}
	if inactive[0].Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("u1 status = %v", inactive[0].Status)
	}
}

// TestLoad_NotExistColdStart: empty store → Load surfaces
// os.ErrNotExist so callers can branch into cold start.
func TestLoad_NotExistColdStart(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	if _, err := Load(ctx, store, "trigger", newEngine()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want os.ErrNotExist", err)
	}
}

func TestLoad_VersionMismatch(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	pb := &snapshotpb.TriggerSnapshot{Version: uint32(Version) + 1}
	if err := triggersnap.Save(ctx, store, "trigger", pb, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := Load(ctx, store, "trigger", newEngine()); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestLoad_BadDecimal(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	pb := &snapshotpb.TriggerSnapshot{
		Version: uint32(Version),
		Pending: []*snapshotpb.TriggerRecord{
			{
				Id:        1,
				UserId:    "u",
				Symbol:    "BTC-USDT",
				StopPrice: "not-a-number",
				Status:    uint32(condrpc.TriggerStatus_TRIGGER_STATUS_PENDING),
			},
		},
	}
	if err := triggersnap.Save(ctx, store, "trigger", pb, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := Load(ctx, store, "trigger", newEngine()); err == nil {
		t.Fatal("expected decimal parse error")
	}
}
