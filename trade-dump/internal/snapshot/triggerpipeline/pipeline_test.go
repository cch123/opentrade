package triggerpipeline

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
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/trade-dump/snapshot/trigger"
)

// mustPipeline builds a pipeline backed by an FS BlobStore in t.TempDir
// with tight timing knobs suitable for synchronous tests. SnapshotInterval
// is large by default so the count-based trigger drives Capture; tests
// override SnapshotEventCount to force.
func mustPipeline(t *testing.T, override Config) *Pipeline {
	t.Helper()
	cfg := Config{
		Brokers:            []string{"localhost:9092"}, // unused — tests drive handleRecord directly
		PartitionCount:     1,
		Store:              snapshotpkg.NewFSBlobStore(t.TempDir()),
		SnapshotInterval:   time.Hour, // count-based default in tests
		SnapshotEventCount: 1_000_000,
		SaveTimeout:        2 * time.Second,
		Logger:             zap.NewNop(),
	}
	if override.Brokers != nil {
		cfg.Brokers = override.Brokers
	}
	if override.Store != nil {
		cfg.Store = override.Store
	}
	if override.PartitionCount != 0 {
		cfg.PartitionCount = override.PartitionCount
	}
	if override.SnapshotInterval != 0 {
		cfg.SnapshotInterval = override.SnapshotInterval
	}
	if override.SnapshotEventCount != 0 {
		cfg.SnapshotEventCount = override.SnapshotEventCount
	}
	if override.SnapshotKey != "" {
		cfg.SnapshotKey = override.SnapshotKey
	}
	if override.TerminalLimit != 0 {
		cfg.TerminalLimit = override.TerminalLimit
	}
	if override.SaveTimeout != 0 {
		cfg.SaveTimeout = override.SaveTimeout
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func updateRecord(t *testing.T, id, seq uint64, status eventpb.TriggerEventStatus, partition int32, offset int64) *kgo.Record {
	t.Helper()
	u := &eventpb.TriggerUpdate{
		Id:           id,
		UserId:       "u1",
		Symbol:       "BTC-USDT",
		Status:       status,
		StopPrice:    "100",
		Qty:          "1",
		TriggerSeqId: seq,
	}
	payload, err := proto.Marshal(u)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Topic: "trigger-event", Partition: partition, Offset: offset, Value: payload}
}

func TestNew_Validates(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "no_brokers",
			cfg:  Config{Store: snapshotpkg.NewFSBlobStore(t.TempDir()), PartitionCount: 1},
			want: "Brokers",
		},
		{
			name: "no_store",
			cfg:  Config{Brokers: []string{"x"}, PartitionCount: 1},
			want: "Store",
		},
		{
			name: "no_partitions",
			cfg:  Config{Brokers: []string{"x"}, Store: snapshotpkg.NewFSBlobStore(t.TempDir())},
			want: "PartitionCount",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestNew_DefaultsApplied(t *testing.T) {
	p, err := New(Config{
		Brokers:        []string{"x"},
		Store:          snapshotpkg.NewFSBlobStore(t.TempDir()),
		PartitionCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.SnapshotKey != DefaultSnapshotKey {
		t.Errorf("SnapshotKey = %q, want %q", p.cfg.SnapshotKey, DefaultSnapshotKey)
	}
	if p.cfg.TriggerEventTopic != "trigger-event" {
		t.Errorf("TriggerEventTopic = %q", p.cfg.TriggerEventTopic)
	}
	if p.cfg.SnapshotInterval != 60*time.Second {
		t.Errorf("SnapshotInterval = %v", p.cfg.SnapshotInterval)
	}
	if p.cfg.SnapshotEventCount != 60000 {
		t.Errorf("SnapshotEventCount = %d", p.cfg.SnapshotEventCount)
	}
}

func TestHandleRecord_AppliesTriggerUpdate(t *testing.T) {
	p := mustPipeline(t, Config{})
	rec := updateRecord(t, 1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 100)
	p.handleRecord(context.Background(), rec)
	snap := p.engine.Capture(0, false)
	if len(snap.Pending) != 1 || snap.Pending[0].Id != 1 {
		t.Fatalf("pending = %+v", snap.Pending)
	}
	if got := p.engine.NextTriggerEventOffset(0); got != 101 {
		t.Errorf("cursor = %d, want 101", got)
	}
}

func TestHandleRecord_TerminalMovesToRing(t *testing.T) {
	p := mustPipeline(t, Config{})
	p.handleRecord(context.Background(), updateRecord(t, 1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 0))
	p.handleRecord(context.Background(), updateRecord(t, 1, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED, 0, 1))
	snap := p.engine.Capture(0, false)
	if len(snap.Pending) != 0 {
		t.Errorf("pending after terminal = %+v, want empty", snap.Pending)
	}
	if len(snap.Terminals) != 1 {
		t.Fatalf("terminals = %+v, want 1", snap.Terminals)
	}
}

func TestHandleRecord_DecodeErrorDropped(t *testing.T) {
	p := mustPipeline(t, Config{})
	bad := &kgo.Record{Topic: "trigger-event", Partition: 0, Offset: 0, Value: []byte("garbage")}
	p.handleRecord(context.Background(), bad) // must not panic
	snap := p.engine.Capture(0, false)
	if len(snap.Pending) != 0 || len(snap.Terminals) != 0 {
		t.Errorf("garbage record mutated state: pending=%+v terminals=%+v", snap.Pending, snap.Terminals)
	}
}

// Count-based snapshot trigger fires after SnapshotEventCount records,
// async-saves the result to the BlobStore.
func TestMaybeCapture_CountThresholdSavesSnapshot(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	p := mustPipeline(t, Config{
		Store:              store,
		SnapshotEventCount: 1, // every record triggers
	})
	p.handleRecord(context.Background(), updateRecord(t, 1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 0))
	p.Close() // wait for async save

	snap, err := triggersnap.Load(context.Background(), store, DefaultSnapshotKey)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(snap.Pending) != 1 || snap.Pending[0].Id != 1 {
		t.Fatalf("loaded snapshot pending = %+v", snap.Pending)
	}
	if got := snap.TriggerEventOffsets[0]; got != 1 {
		t.Errorf("loaded cursor = %d, want 1", got)
	}
}

// Time-based snapshot trigger fires after SnapshotInterval.
func TestMaybeCapture_TimeThresholdSavesSnapshot(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	p := mustPipeline(t, Config{
		Store:              store,
		SnapshotInterval:   1 * time.Millisecond,
		SnapshotEventCount: 1_000_000, // disabled
	})
	// Sleep so the next handleRecord crosses the time threshold.
	time.Sleep(5 * time.Millisecond)
	p.handleRecord(context.Background(), updateRecord(t, 7, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 0))
	p.Close()

	snap, err := triggersnap.Load(context.Background(), store, DefaultSnapshotKey)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(snap.Pending) != 1 || snap.Pending[0].Id != 7 {
		t.Fatalf("loaded snapshot pending = %+v", snap.Pending)
	}
}

// loadSnapshot returns nil on a fresh store (cold start).
func TestLoadSnapshot_ColdStart(t *testing.T) {
	p := mustPipeline(t, Config{})
	snap, err := p.loadSnapshot(context.Background())
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if snap != nil {
		t.Errorf("cold-start snap = %+v, want nil", snap)
	}
}

// loadSnapshot returns the seeded snapshot when one exists.
func TestLoadSnapshot_RestoresExisting(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	seed := &snapshotpb.TriggerSnapshot{
		Version:             1,
		TakenAtMs:           42,
		TriggerEventOffsets: map[int32]int64{0: 555},
		Pending: []*snapshotpb.TriggerRecord{
			{Id: 9, UserId: "u1", Symbol: "BTC-USDT"},
		},
	}
	if err := triggersnap.Save(context.Background(), store, DefaultSnapshotKey, seed, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("seed: %v", err)
	}
	p := mustPipeline(t, Config{Store: store})
	got, err := p.loadSnapshot(context.Background())
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if got == nil {
		t.Fatal("loadSnapshot returned nil for seeded store")
	}
	if got.TakenAtMs != 42 || got.TriggerEventOffsets[0] != 555 || len(got.Pending) != 1 {
		t.Errorf("loaded = %+v", got)
	}
}

// Round-trip across pipeline lifecycles: pipeline A applies events
// and saves; pipeline B starts in same store and primes from it.
func TestRestart_PrimesEngineFromSnapshot(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())

	a := mustPipeline(t, Config{
		Store:              store,
		SnapshotEventCount: 1,
	})
	a.handleRecord(context.Background(), updateRecord(t, 1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 100))
	a.Close()

	// Pipeline B reads the snapshot via loadSnapshot + RestoreFromSnapshot
	// (mirrors what Start does without opening a real Kafka client).
	b := mustPipeline(t, Config{Store: store})
	snap, err := b.loadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil {
		t.Fatal("pipeline B saw no snapshot")
	}
	if err := b.engine.RestoreFromSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if got := b.engine.NextTriggerEventOffset(0); got != 101 {
		t.Errorf("restored cursor = %d, want 101", got)
	}
}

// SaveTimeout exceeded — pipeline still functions on next tick.
func TestSaveSnapshot_TimeoutDoesNotPanic(t *testing.T) {
	store := &slowStore{delay: 50 * time.Millisecond}
	p := mustPipeline(t, Config{
		Store:              store,
		SnapshotEventCount: 1,
		SaveTimeout:        5 * time.Millisecond, // shorter than store delay
	})
	p.handleRecord(context.Background(), updateRecord(t, 1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING, 0, 0))
	p.Close()

	if got := store.calls.Load(); got == 0 {
		t.Errorf("store.Put never called: %d", got)
	}
}

// slowStore returns ctx.Err() after `delay` to simulate a save taking
// longer than SaveTimeout.
type slowStore struct {
	delay time.Duration
	calls atomicInt64
}

func (s *slowStore) Put(ctx context.Context, key string, data []byte) error {
	s.calls.Add(1)
	select {
	case <-time.After(s.delay):
		return errors.New("slowStore: simulated success path not reached")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *slowStore) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, snapshotErrNotExist
}

// minimal stand-in for testing — we don't actually need atomic but
// stub a tiny one to avoid pulling sync/atomic into the test fixture.
type atomicInt64 struct{ v int64 }

func (a *atomicInt64) Add(n int64) { a.v += n }
func (a *atomicInt64) Load() int64 { return a.v }

// snapshotErrNotExist mirrors os.ErrNotExist for the slowStore stub.
// Real BlobStore implementations return os.ErrNotExist directly; this
// is just a placeholder for tests that don't exercise Get.
var snapshotErrNotExist = errors.New("not exist")
