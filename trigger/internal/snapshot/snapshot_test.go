package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	pkgsnapshot "github.com/xargin/opentrade/pkg/snapshot"
	"github.com/xargin/opentrade/trigger/internal/engine"
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

func TestSaveLoad_RoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, pkgsnapshot.FormatProto)
}

func TestSaveLoad_RoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, pkgsnapshot.FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format pkgsnapshot.Format) {
	src := newEngine()
	id1, _, _, _ := src.Place(context.Background(), &condrpc.PlaceTriggerRequest{
		UserId:    "u1",
		Symbol:    "BTC-USDT",
		Side:      eventpb.Side_SIDE_SELL,
		Type:      condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS,
		StopPrice: "100",
		Qty:       "0.5",
	})
	_, _, _ = src.Cancel(context.Background(), "u1", id1)
	_, _, _, _ = src.Place(context.Background(), &condrpc.PlaceTriggerRequest{
		UserId:     "u2",
		Symbol:     "ETH-USDT",
		Side:       eventpb.Side_SIDE_BUY,
		Type:       condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT,
		StopPrice:  "200",
		LimitPrice: "205",
		Qty:        "1",
		Tif:        eventpb.TimeInForce_TIME_IN_FORCE_IOC,
	})
	captured := Capture(src)
	captured.TakenAtMs = 42

	dir := t.TempDir()
	base := filepath.Join(dir, "trigger")
	if err := Save(base, captured, format); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}

	dst := newEngine()
	if err := Restore(dst, loaded); err != nil {
		t.Fatal(err)
	}
	pending := dst.List("u2", false)
	if len(pending) != 1 {
		t.Fatalf("u2 pending = %d, want 1", len(pending))
	}
	if pending[0].Type != condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT {
		t.Errorf("u2 type: %v", pending[0].Type)
	}
	// Terminal (canceled) record preserved for u1.
	inactive := dst.List("u1", true)
	if len(inactive) != 1 {
		t.Fatalf("u1 inactive = %d, want 1", len(inactive))
	}
	if inactive[0].Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("u1 status: %v", inactive[0].Status)
	}
}

func TestLoad_MissingFileReturnsNil(t *testing.T) {
	snap, err := Load(filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("missing file should yield nil,nil: %v", err)
	}
	if snap != nil {
		t.Errorf("snap = %+v", snap)
	}
}

func TestLoad_VersionMismatch(t *testing.T) {
	base := filepath.Join(t.TempDir(), "trigger")
	if err := Save(base, &Snapshot{Version: Version + 1}, pkgsnapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(base); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestSave_AtomicRename(t *testing.T) {
	base := filepath.Join(t.TempDir(), "trigger")
	if err := Save(base, &Snapshot{Version: Version}, pkgsnapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + ".pb.tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp lingered: %v", err)
	}
}

// TestLoad_JSONOnlyMigration covers the upgrade window: only .json on
// disk, Load still returns it (ADR-0049 probe order .pb → .json).
func TestLoad_JSONOnlyMigration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "trigger")
	if err := Save(base, &Snapshot{Version: Version, TakenAtMs: 7}, pkgsnapshot.FormatJSON); err != nil {
		t.Fatal(err)
	}
	snap, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.TakenAtMs != 7 {
		t.Fatalf("json-only load: got %+v", snap)
	}
}
