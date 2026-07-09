package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	"github.com/xargin/opentrade/trigger/engine"
	loadsnapshot "github.com/xargin/opentrade/trigger/internal/snapshot"
)

type testSeq struct{ next uint64 }

func (s *testSeq) Next() uint64 { s.next++; return s.next }

type testPlacer struct{}

func (testPlacer) PlaceOrder(context.Context, *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
	return &counterrpc.PlaceOrderResponse{Accepted: true, OrderId: 1}, nil
}

func testEngine() *engine.Engine {
	return engine.New(engine.Config{
		TerminalHistoryLimit: 10,
		Clock:                func() time.Time { return time.Unix(1_700_000_000, 0) },
	}, &testSeq{}, testPlacer{}, nil, zap.NewNop())
}

func triggerRecord(t *testing.T, partition int32, offset int64, id uint64, status eventpb.TriggerEventStatus) *kgo.Record {
	return triggerRecordWithGroup(t, partition, offset, id, status, "")
}

func triggerRecordWithGroup(t *testing.T, partition int32, offset int64, id uint64, status eventpb.TriggerEventStatus, groupID string) *kgo.Record {
	t.Helper()
	envelope := &eventpb.TriggerEvent{
		Payload: &eventpb.TriggerEvent_Update{
			Update: &eventpb.TriggerUpdate{
				Id:              id,
				ClientTriggerId: "client-" + string(rune('0'+id)),
				UserId:          1001,
				Symbol:          "BTC-USDT",
				Side:            eventpb.Side_SIDE_SELL,
				Type:            eventpb.TriggerEventType_TRIGGER_EVENT_TYPE_STOP_LOSS,
				StopPrice:       "100",
				Qty:             "1",
				Status:          status,
				TriggerSeqId:    uint64(offset + 1),
				OcoGroupId:      groupID,
			},
		},
	}
	value, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal trigger event: %v", err)
	}
	return &kgo.Record{Topic: "trigger-event", Partition: partition, Offset: offset, Value: value}
}

func checkpointRecord(t *testing.T, partition int32, offset int64, marketOffsets, perpOffsets map[int32]int64) *kgo.Record {
	t.Helper()
	envelope := &eventpb.TriggerEvent{
		Payload: &eventpb.TriggerEvent_MarketCheckpoint{
			MarketCheckpoint: &eventpb.TriggerMarketCheckpointEvent{
				MarketOffsets:    marketOffsets,
				PerpPriceOffsets: perpOffsets,
			},
		},
	}
	value, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal checkpoint event: %v", err)
	}
	return &kgo.Record{Topic: "trigger-event", Partition: partition, Offset: offset, Value: value}
}

// A periodic or on-demand snapshot can be stale as soon as it is written.
// This regression test pins the two dangerous windows: a newly placed order
// after the snapshot must appear, and a post-snapshot cancel must not leave the
// snapshot's pending copy alive.
func TestCatchUp_SnapshotThenPlacedAndCancelled(t *testing.T) {
	ctx := context.Background()
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	seed := &snapshotpb.TriggerSnapshot{
		Version:             uint32(loadsnapshot.Version),
		TriggerEventOffsets: map[int32]int64{0: 10},
		Offsets:             map[int32]int64{0: 50},
		PerpPriceOffsets:    map[int32]int64{0: 60},
		Pending: []*snapshotpb.TriggerRecord{
			{
				Id:              1,
				ClientTriggerId: "client-1",
				UserId:          1001,
				Symbol:          "BTC-USDT",
				Side:            uint32(eventpb.Side_SIDE_SELL),
				Type:            uint32(condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS),
				StopPrice:       "100",
				Qty:             "1",
				Status:          uint32(condrpc.TriggerStatus_TRIGGER_STATUS_PENDING),
			},
		},
	}
	if err := triggersnap.Save(ctx, store, "trigger", seed, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("save seed snapshot: %v", err)
	}

	eng := testEngine()
	restored, err := loadsnapshot.Load(ctx, store, "trigger", eng)
	if err != nil {
		t.Fatalf("load seed snapshot: %v", err)
	}
	source := &fakeSource{
		bounds: []map[int32]partitionBounds{
			{0: {start: 0, end: 13}},
			{0: {start: 0, end: 13}}, // unchanged after replay => stable LEO
		},
		polls: [][]*kgo.Record{{
			triggerRecord(t, 0, 10, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING),
			triggerRecord(t, 0, 11, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_CANCELED),
			checkpointRecord(t, 0, 12, map[int32]int64{0: 75}, map[int32]int64{0: 85}),
		}},
	}
	if err := catchUp(ctx, source, restored.TriggerEventOffsets, eng, zap.NewNop()); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if len(source.assignments) != 1 || source.assignments[0][0] != 10 {
		t.Fatalf("assignments = %+v, want partition 0 at snapshot offset 10", source.assignments)
	}

	pending := eng.List(1001, false)
	if len(pending) != 1 || pending[0].ID != 2 {
		t.Fatalf("pending after replay = %+v, want only post-snapshot id=2", pending)
	}
	canceled, err := eng.Get(1001, 1)
	if err != nil {
		t.Fatalf("get canceled id=1: %v", err)
	}
	if canceled.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Fatalf("id=1 status = %s, want CANCELED", canceled.Status)
	}
	if got := eng.Offsets()[0]; got != 75 {
		t.Fatalf("market offset after replay = %d, want 75", got)
	}
	if got := eng.PerpPriceOffsets()[0]; got != 85 {
		t.Fatalf("perp-price offset after replay = %d, want 85", got)
	}

	// Applying the terminal record twice does not duplicate terminal history,
	// and even a stale PENDING update cannot resurrect it.
	if err := ApplyRecord(eng, triggerRecord(t, 0, 11, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_CANCELED)); err != nil {
		t.Fatal(err)
	}
	if err := ApplyRecord(eng, triggerRecord(t, 0, 12, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING)); err != nil {
		t.Fatal(err)
	}
	all := eng.List(1001, true)
	terminalCount := 0
	for _, got := range all {
		if got.ID == 1 {
			terminalCount++
			if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
				t.Fatalf("stale replay resurrected id=1: %+v", got)
			}
		}
	}
	if terminalCount != 1 {
		t.Fatalf("id=1 occurrences = %d, want exactly one terminal", terminalCount)
	}
}

func TestCatchUp_FollowsMovingLEOUntilStable(t *testing.T) {
	eng := testEngine()
	source := &fakeSource{
		bounds: []map[int32]partitionBounds{
			{0: {start: 0, end: 2}},
			{0: {start: 0, end: 3}}, // old primary appended while we replayed
			{0: {start: 0, end: 3}}, // now stable
		},
		polls: [][]*kgo.Record{
			{
				triggerRecord(t, 0, 0, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING),
				triggerRecord(t, 0, 1, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING),
			},
			{triggerRecord(t, 0, 2, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_CANCELED)},
		},
	}
	if err := catchUp(context.Background(), source, nil, eng, zap.NewNop()); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if source.boundsCalls != 3 {
		t.Fatalf("Bounds calls = %d, want 3 (initial, advanced, stable)", source.boundsCalls)
	}
	if pending := eng.List(1001, false); len(pending) != 1 || pending[0].ID != 2 {
		t.Fatalf("pending = %+v, want only id=2", pending)
	}
}

func TestCatchUp_TerminalUpdateReconstructsOCOCascade(t *testing.T) {
	eng := testEngine()
	source := &fakeSource{
		bounds: []map[int32]partitionBounds{
			{0: {start: 0, end: 3}},
			{0: {start: 0, end: 3}},
		},
		polls: [][]*kgo.Record{{
			triggerRecordWithGroup(t, 0, 0, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, "oco-recovery"),
			triggerRecordWithGroup(t, 0, 1, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, "oco-recovery"),
			// Model a crash after the primary terminal update was durable but
			// before the queued sibling-CANCELED update reached Kafka.
			triggerRecordWithGroup(t, 0, 2, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED, "oco-recovery"),
		}},
	}
	if err := catchUp(context.Background(), source, nil, eng, zap.NewNop()); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if pending := eng.List(1001, false); len(pending) != 0 {
		t.Fatalf("OCO sibling remained pending: %+v", pending)
	}
	sibling, err := eng.Get(1001, 2)
	if err != nil {
		t.Fatalf("Get sibling: %v", err)
	}
	if sibling.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Fatalf("sibling status = %s, want CANCELED", sibling.Status)
	}
}

func TestCatchUp_RejectsRetentionGap(t *testing.T) {
	source := &fakeSource{bounds: []map[int32]partitionBounds{{0: {start: 5, end: 10}}}}
	err := catchUp(context.Background(), source, map[int32]int64{0: 2}, testEngine(), zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "retention gap") {
		t.Fatalf("err = %v, want retention gap", err)
	}
}

type fakeSource struct {
	bounds      []map[int32]partitionBounds
	polls       [][]*kgo.Record
	boundsCalls int
	pollCalls   int
	assignments []map[int32]int64
}

func (f *fakeSource) Bounds(context.Context) (map[int32]partitionBounds, error) {
	if len(f.bounds) == 0 {
		return nil, errors.New("fakeSource: no bounds")
	}
	index := f.boundsCalls
	if index >= len(f.bounds) {
		index = len(f.bounds) - 1
	}
	f.boundsCalls++
	return f.bounds[index], nil
}

func (f *fakeSource) AddPartitions(starts map[int32]int64) {
	copyOfStarts := make(map[int32]int64, len(starts))
	for partition, offset := range starts {
		copyOfStarts[partition] = offset
	}
	f.assignments = append(f.assignments, copyOfStarts)
}

func (f *fakeSource) Poll(context.Context) ([]*kgo.Record, error) {
	if f.pollCalls >= len(f.polls) {
		return nil, errors.New("fakeSource: unexpected poll")
	}
	records := f.polls[f.pollCalls]
	f.pollCalls++
	return records, nil
}

func (*fakeSource) Close() {}
