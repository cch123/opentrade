package snapshot

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
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

// TestRestoreFromProto covers the ADR-0067 cold path: trade-dump's
// shadow pipeline produces a snapshotpb.TriggerSnapshot via
// pkg/snapshot/trigger.Save; trigger startup hands the proto to
// RestoreFromProto so the engine + offsets are hydrated in one call.
func TestRestoreFromProto(t *testing.T) {
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

	dst := newEngine()
	got, err := RestoreFromProto(dst, pb)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("RestoreFromProto returned nil snapshot")
	}
	if got.TakenAtMs != 99 {
		t.Errorf("TakenAtMs = %d, want 99", got.TakenAtMs)
	}
	if got.Offsets[0] != 555 || got.Offsets[2] != 200 {
		t.Errorf("Offsets = %+v", got.Offsets)
	}
	// Engine state hydrated — pending u2 lookup + canceled u1 in
	// terminal ring.
	pending := dst.List("u2", false)
	if len(pending) != 1 || pending[0].Type != condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT {
		t.Fatalf("u2 pending = %+v", pending)
	}
	inactive := dst.List("u1", true)
	if len(inactive) != 1 {
		t.Fatalf("u1 inactive = %+v", inactive)
	}
	if inactive[0].Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("u1 status = %v", inactive[0].Status)
	}
}

func TestRestoreFromProto_NilTolerated(t *testing.T) {
	got, err := RestoreFromProto(newEngine(), nil)
	if err != nil {
		t.Fatalf("RestoreFromProto(nil) = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("got = %+v, want nil", got)
	}
}

func TestRestoreFromProto_VersionMismatch(t *testing.T) {
	pb := &snapshotpb.TriggerSnapshot{Version: uint32(Version) + 1}
	if _, err := RestoreFromProto(newEngine(), pb); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestRestoreFromProto_BadDecimal(t *testing.T) {
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
	if _, err := RestoreFromProto(newEngine(), pb); err == nil {
		t.Fatal("expected decimal parse error")
	}
}
