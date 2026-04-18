package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/conditional/internal/engine"
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
	}, &counterSeq{}, nopPlacer{}, zap.NewNop())
}

func TestSaveLoad_RoundTrip(t *testing.T) {
	src := newEngine()
	id1, _, _, _ := src.Place(&condrpc.PlaceConditionalRequest{
		UserId:    "u1",
		Symbol:    "BTC-USDT",
		Side:      eventpb.Side_SIDE_SELL,
		Type:      condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS,
		StopPrice: "100",
		Qty:       "0.5",
	})
	_, _, _ = src.Cancel("u1", id1)
	_, _, _, _ = src.Place(&condrpc.PlaceConditionalRequest{
		UserId:     "u2",
		Symbol:     "ETH-USDT",
		Side:       eventpb.Side_SIDE_BUY,
		Type:       condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT,
		StopPrice:  "200",
		LimitPrice: "205",
		Qty:        "1",
		Tif:        eventpb.TimeInForce_TIME_IN_FORCE_IOC,
	})
	captured := Capture(src)
	captured.TakenAtMs = 42

	dir := t.TempDir()
	path := filepath.Join(dir, "cond.json")
	if err := Save(path, captured); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
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
	if pending[0].Type != condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT {
		t.Errorf("u2 type: %v", pending[0].Type)
	}
	// Terminal (canceled) record preserved for u1.
	inactive := dst.List("u1", true)
	if len(inactive) != 1 {
		t.Fatalf("u1 inactive = %d, want 1", len(inactive))
	}
	if inactive[0].Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED {
		t.Errorf("u1 status: %v", inactive[0].Status)
	}
}

func TestLoad_MissingFileReturnsNil(t *testing.T) {
	snap, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("missing file should yield nil,nil: %v", err)
	}
	if snap != nil {
		t.Errorf("snap = %+v", snap)
	}
}

func TestLoad_VersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cond.json")
	if err := Save(path, &Snapshot{Version: Version + 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestSave_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cond.json")
	if err := Save(path, &Snapshot{Version: Version}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp lingered: %v", err)
	}
}
