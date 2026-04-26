package snapshotrpc

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	triggershadow "github.com/xargin/opentrade/trade-dump/internal/snapshot/trigger/shadow"
)

// triggerHandlerEnv wires a Server with a populated TriggerBackend
// against a stub Kafka admin and an FSBlobStore in t.TempDir.
type triggerHandlerEnv struct {
	server *Server
	shadow *triggershadow.Engine
	admin  *stubAdmin
	store  snapshotpkg.BlobStore
}

func newTriggerHandlerEnv(t *testing.T, partitionCount int) *triggerHandlerEnv {
	t.Helper()
	admin := newStubAdmin()
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	shadow := triggershadow.New(0)
	srv := New(Config{
		Logger:           zap.NewNop(),
		WaitApplyTimeout: 200 * time.Millisecond,
		WorkTimeout:      1 * time.Second,
		Trigger: &TriggerBackend{
			Shadow:         shadow,
			Admin:          admin,
			BlobStore:      store,
			Topic:          "trigger-event",
			PartitionCount: partitionCount,
			SnapshotFormat: snapshotpkg.FormatProto,
		},
	})
	return &triggerHandlerEnv{server: srv, shadow: shadow, admin: admin, store: store}
}

// TakeTriggerSnapshot returns Unimplemented when the trigger backend
// is missing — production wiring fallback contract.
func TestTakeTriggerSnapshot_NoBackendUnimplemented(t *testing.T) {
	srv := New(Config{Logger: zap.NewNop()})
	_, err := srv.TakeTriggerSnapshot(context.Background(),
		&tradedumprpc.TakeTriggerSnapshotRequest{RequesterNodeId: "trigger-0"})
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("code = %s, want Unimplemented", got)
	}
}

func TestTakeTriggerSnapshot_NilRequestInvalidArgument(t *testing.T) {
	env := newTriggerHandlerEnv(t, 1)
	_, err := env.server.TakeTriggerSnapshot(context.Background(), nil)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument", got)
	}
}

// Happy path: shadow has applied past LEO; handler returns key + offsets
// and the snapshot is loadable from the store.
func TestTakeTriggerSnapshot_HappyPath(t *testing.T) {
	env := newTriggerHandlerEnv(t, 2)

	// Drive the shadow past the LEO for both partitions.
	apply(t, env.shadow, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 0)
	apply(t, env.shadow, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED, 1, 4)

	// LEO matches the partition cursor.
	env.admin.set(0, 1) // shadow next = 1
	env.admin.set(1, 5) // shadow next = 5

	resp, err := env.server.TakeTriggerSnapshot(context.Background(),
		&tradedumprpc.TakeTriggerSnapshotRequest{RequesterNodeId: "trigger-0"})
	if err != nil {
		t.Fatalf("TakeTriggerSnapshot: %v", err)
	}
	if resp.SnapshotKey == "" {
		t.Fatal("empty snapshot_key")
	}
	if got := resp.TriggerEventOffsets[0]; got != 1 {
		t.Errorf("trigger_event_offsets[0] = %d, want 1", got)
	}
	if got := resp.TriggerEventOffsets[1]; got != 5 {
		t.Errorf("trigger_event_offsets[1] = %d, want 5", got)
	}

	loaded, err := triggersnap.Load(context.Background(), env.store, resp.SnapshotKey)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Pending) != 1 {
		t.Errorf("pending after load = %+v", loaded.Pending)
	}
	if len(loaded.Terminals) != 1 {
		t.Errorf("terminals after load = %+v", loaded.Terminals)
	}
}

// Shadow lagging behind LEO → DeadlineExceeded after WaitApplyTimeout.
func TestTakeTriggerSnapshot_DeadlineExceededWhenShadowLags(t *testing.T) {
	env := newTriggerHandlerEnv(t, 1)
	env.admin.set(0, 100) // LEO = 100
	// Shadow has consumed nothing; waitAppliedTo will time out.

	_, err := env.server.TakeTriggerSnapshot(context.Background(),
		&tradedumprpc.TakeTriggerSnapshotRequest{RequesterNodeId: "trigger-0"})
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("code = %s, want DeadlineExceeded", got)
	}
}

// Admin LEO query failure → Unavailable.
func TestTakeTriggerSnapshot_LEOErrorUnavailable(t *testing.T) {
	env := newTriggerHandlerEnv(t, 1)
	env.admin.setErr(0, errFromMsg("kafka down"))

	_, err := env.server.TakeTriggerSnapshot(context.Background(),
		&tradedumprpc.TakeTriggerSnapshotRequest{RequesterNodeId: "trigger-0"})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable", got)
	}
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func apply(t *testing.T, eng *triggershadow.Engine, id uint64, status eventpb.TriggerEventStatus, partition int32, offset int64) {
	t.Helper()
	u := &eventpb.TriggerUpdate{
		Id:           id,
		UserId:       "u1",
		Symbol:       "BTC-USDT",
		Status:       status,
		StopPrice:    "100",
		Qty:          "1",
		TriggerSeqId: id,
	}
	if err := eng.ApplyTriggerUpdate(u, partition, offset); err != nil {
		t.Fatalf("ApplyTriggerUpdate: %v", err)
	}
}

type errMsg string

func (e errMsg) Error() string  { return string(e) }
func errFromMsg(s string) error { return errMsg(s) }
