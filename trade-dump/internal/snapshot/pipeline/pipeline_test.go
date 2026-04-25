package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/shadow"
)

// mustPipeline builds a pipeline over a filesystem blob store in a
// tempdir, with tight timing knobs suitable for synchronous tests.
func mustPipeline(t *testing.T, cfg Config) *Pipeline {
	t.Helper()
	if cfg.Brokers == nil {
		cfg.Brokers = []string{"localhost:9092"} // unused — tests drive handleRecord directly
	}
	if cfg.Store == nil {
		cfg.Store = snapshotpkg.NewFSBlobStore(t.TempDir())
	}
	if cfg.VShardCount == 0 {
		cfg.VShardCount = 4
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = time.Hour // large: count-trigger exercised separately
	}
	if cfg.SnapshotEventCount == 0 {
		cfg.SnapshotEventCount = 1_000_000 // large: time-trigger exercised separately
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// depositEvt builds a Transfer journal event that drives a
// deterministic balance mutation for tests.
func depositEvt(user, tx, amount string, seq uint64) *eventpb.CounterJournalEvent {
	return &eventpb.CounterJournalEvent{
		CounterSeqId: seq,
		Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{
				UserId: user, TransferId: tx, Asset: "USDT", Amount: amount,
				Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: user, Asset: "USDT",
					Available: amount, Frozen: "0", Version: seq,
				},
			},
		},
	}
}

// mustMarshalRecord wraps a CounterJournalEvent into a kgo.Record
// with (partition, offset) so handleRecord can round-trip it.
func mustMarshalRecord(t *testing.T, evt *eventpb.CounterJournalEvent, part int32, offset int64) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Topic: "counter-journal", Partition: part, Offset: offset, Value: payload}
}

// TestNew_Validates covers the config validation surface. Missing
// required fields must error before any Kafka / blob I/O.
func TestNew_Validates(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "no_brokers",
			cfg:  Config{Store: snapshotpkg.NewFSBlobStore(t.TempDir()), VShardCount: 1},
			want: "Brokers",
		},
		{
			name: "no_store",
			cfg:  Config{Brokers: []string{"x"}, VShardCount: 1},
			want: "Store",
		},
		{
			name: "zero_vshards",
			cfg:  Config{Brokers: []string{"x"}, Store: snapshotpkg.NewFSBlobStore(t.TempDir())},
			want: "VShardCount",
		},
		{
			name: "owned_out_of_range",
			cfg:  Config{Brokers: []string{"x"}, Store: snapshotpkg.NewFSBlobStore(t.TempDir()), VShardCount: 2, OwnedVShards: []int{2}},
			want: "outside",
		},
		{
			name: "owned_duplicate",
			cfg:  Config{Brokers: []string{"x"}, Store: snapshotpkg.NewFSBlobStore(t.TempDir()), VShardCount: 2, OwnedVShards: []int{1, 1}},
			want: "duplicate",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil || !contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// TestPrimeFromStore_ColdStart leaves the store empty and asserts
// every vshard boots with a fresh ShadowEngine + zero offset.
func TestPrimeFromStore_ColdStart(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 3})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if len(p.engines) != 3 {
		t.Fatalf("engines = %d, want 3", len(p.engines))
	}
	for part, eng := range p.engines {
		if eng.NextJournalOffset() != 0 {
			t.Fatalf("vshard %d cold-start offset = %d, want 0", part, eng.NextJournalOffset())
		}
		if eng.CounterSeq() != 0 {
			t.Fatalf("vshard %d cold-start counterSeq = %d, want 0", part, eng.CounterSeq())
		}
	}
}

// TestPrimeFromStore_ExistingSnapshot seeds a snapshot for vshard
// 1 and verifies prime restores the engine + leaves vshard 0
// fresh.
func TestPrimeFromStore_ExistingSnapshot(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	snap := &snapshotpkg.ShardSnapshot{
		Version:       2,
		ShardID:       1,
		CounterSeq:    100,
		JournalOffset: 555,
		TimestampMS:   1,
	}
	key := fmt.Sprintf("vshard-%03d", 1)
	if err := snapshotpkg.Save(context.Background(), store, key, snap, snapshotpkg.FormatProto); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	p := mustPipeline(t, Config{VShardCount: 2, Store: store})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	eng1 := p.engines[1]
	if eng1.CounterSeq() != 100 {
		t.Fatalf("eng1 counterSeq = %d, want 100", eng1.CounterSeq())
	}
	if eng1.NextJournalOffset() != 555 {
		t.Fatalf("eng1 NextJournalOffset = %d, want 555", eng1.NextJournalOffset())
	}
	if p.engines[0].NextJournalOffset() != 0 {
		t.Fatalf("eng0 cold offset = %d, want 0", p.engines[0].NextJournalOffset())
	}
}

func TestPrimeFromStore_OwnedSubset(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 4, OwnedVShards: []int{1, 3}})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if len(p.engines) != 2 {
		t.Fatalf("engines = %d, want 2", len(p.engines))
	}
	if _, ok := p.ShadowEngine(1); !ok {
		t.Fatal("vshard 1 should be owned")
	}
	if _, ok := p.ShadowEngine(3); !ok {
		t.Fatal("vshard 3 should be owned")
	}
	if _, ok := p.ShadowEngine(0); ok {
		t.Fatal("vshard 0 should not be owned")
	}
}

// TestHandleRecord_AppliesAndAdvances drives one Transfer event
// through handleRecord; the shadow engine should reflect the
// balance + advance counterSeq + NextJournalOffset.
func TestHandleRecord_AppliesAndAdvances(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 4})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := mustMarshalRecord(t, depositEvt("u1", "tx-1", "100", 7), 2, 500)
	p.handleRecord(context.Background(), rec)

	eng := p.engines[2]
	if eng.CounterSeq() != 7 {
		t.Fatalf("counterSeq = %d, want 7", eng.CounterSeq())
	}
	if eng.NextJournalOffset() != 501 {
		t.Fatalf("NextJournalOffset = %d, want 501", eng.NextJournalOffset())
	}
	bal := eng.State().Balance("u1", "USDT")
	if bal.Available.String() != "100" {
		t.Fatalf("balance = %+v, want 100", bal)
	}
}

// TestHandleRecord_UnknownPartitionDropped ensures a record from
// a partition outside VShardCount logs + drops (doesn't panic).
func TestHandleRecord_UnknownPartitionDropped(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 2})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := mustMarshalRecord(t, depositEvt("u", "tx", "1", 1), 99, 0)
	p.handleRecord(context.Background(), rec) // must not panic
}

// TestHandleRecord_DecodeErrorDropped feeds garbage bytes at a
// known partition; shouldn't mutate state or panic.
func TestHandleRecord_DecodeErrorDropped(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 1})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := &kgo.Record{Topic: "counter-journal", Partition: 0, Offset: 1, Value: []byte{0xff, 0x00}}
	p.handleRecord(context.Background(), rec)
	if p.engines[0].NextJournalOffset() != 0 {
		t.Fatal("decode-error record advanced offset")
	}
}

// TestMaybeCapture_EventCountTrigger exercises the count-based
// trigger. Set SnapshotEventCount=3, apply 3 events, assert
// Capture + save fired.
func TestMaybeCapture_EventCountTrigger(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	p := mustPipeline(t, Config{
		VShardCount:        1,
		Store:              store,
		SnapshotEventCount: 3,
		SnapshotInterval:   time.Hour,
	})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 3; i++ {
		rec := mustMarshalRecord(t, depositEvt("u1", fmt.Sprintf("tx-%d", i), "1", uint64(i+1)), 0, i)
		p.handleRecord(context.Background(), rec)
	}
	p.saveWG.Wait()

	snap, err := snapshotpkg.Load(context.Background(), store, "vshard-000")
	if err != nil {
		t.Fatalf("load saved snapshot: %v", err)
	}
	if snap.CounterSeq != 3 {
		t.Fatalf("snap counter_seq = %d, want 3", snap.CounterSeq)
	}
	if snap.JournalOffset != 3 {
		t.Fatalf("snap journal_offset = %d, want 3 (offset 2 + 1)", snap.JournalOffset)
	}
	if p.engines[0].EventsSinceLastSnapshot() != 0 {
		t.Fatalf("events counter not reset: %d", p.engines[0].EventsSinceLastSnapshot())
	}
}

// TestMaybeCapture_TimeTrigger exercises the interval-based
// trigger: events within the interval don't fire, events past
// the interval do.
func TestMaybeCapture_TimeTrigger(t *testing.T) {
	store := snapshotpkg.NewFSBlobStore(t.TempDir())
	p := mustPipeline(t, Config{
		VShardCount:        1,
		Store:              store,
		SnapshotInterval:   30 * time.Millisecond,
		SnapshotEventCount: 1_000_000,
	})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}

	// First two events arrive within interval — no save fires.
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-1", "1", 1), 0, 0))
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-2", "1", 2), 0, 1))
	p.saveWG.Wait()

	if _, err := snapshotpkg.Load(context.Background(), store, "vshard-000"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no snapshot yet, got err=%v", err)
	}

	// Past interval, next event triggers a time-window save.
	time.Sleep(40 * time.Millisecond)
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-3", "1", 3), 0, 2))
	p.saveWG.Wait()

	snap, err := snapshotpkg.Load(context.Background(), store, "vshard-000")
	if err != nil {
		t.Fatalf("load after time trigger: %v", err)
	}
	if snap.CounterSeq != 3 {
		t.Fatalf("snap counter_seq = %d, want 3 (time-triggered)", snap.CounterSeq)
	}
	if snap.JournalOffset != 3 {
		t.Fatalf("snap journal_offset = %d, want 3", snap.JournalOffset)
	}
}

// TestMaybeCapture_InFlightSkipped stalls the first save by using
// a slow store, fires a second trigger while the first is
// running, and asserts only one save landed. Proves the atomic
// in-flight guard.
func TestMaybeCapture_InFlightSkipped(t *testing.T) {
	slow := &slowStore{
		inner:      snapshotpkg.NewFSBlobStore(t.TempDir()),
		putRelease: make(chan struct{}),
	}
	p := mustPipeline(t, Config{
		VShardCount:        1,
		Store:              slow,
		SnapshotEventCount: 1,
		SnapshotInterval:   time.Hour,
	})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}

	// First event: triggers save, which blocks inside slowStore.Put.
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-1", "1", 1), 0, 0))
	waitFor(t, func() bool { return slow.putsInFlight.Load() == 1 }, time.Second)

	// Second event: trigger condition met but save in flight →
	// maybeCapture must skip (no new Put attempt).
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-2", "1", 2), 0, 1))

	// Give any erroneous second save time to attempt Put.
	time.Sleep(20 * time.Millisecond)
	if got := slow.putsAttempted.Load(); got != 1 {
		t.Fatalf("puts attempted = %d, want 1 (second trigger should have been skipped)", got)
	}

	close(slow.putRelease)
	p.saveWG.Wait()

	if slow.putsAttempted.Load() != 1 {
		t.Fatalf("puts attempted after release = %d, want 1", slow.putsAttempted.Load())
	}
}

// TestClose_WaitsForInFlightSaves ensures Close doesn't return
// until pending async saves have completed.
func TestClose_WaitsForInFlightSaves(t *testing.T) {
	slow := &slowStore{
		inner:      snapshotpkg.NewFSBlobStore(t.TempDir()),
		putRelease: make(chan struct{}),
	}
	p := mustPipeline(t, Config{
		VShardCount:        1,
		Store:              slow,
		SnapshotEventCount: 1,
	})
	if err := p.primeEnginesFromStore(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.handleRecord(context.Background(), mustMarshalRecord(t, depositEvt("u1", "tx-1", "1", 1), 0, 0))
	waitFor(t, func() bool { return slow.putsInFlight.Load() == 1 }, time.Second)

	closeDone := make(chan struct{})
	go func() {
		p.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned while save still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	close(slow.putRelease)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close never returned after save released")
	}
}

// TestClose_Idempotent — second call to Close must not panic.
func TestClose_Idempotent(t *testing.T) {
	p := mustPipeline(t, Config{VShardCount: 1})
	_ = p.primeEnginesFromStore(context.Background())
	p.Close()
	p.Close()
}

// ------------------------------------------------------------------
// Test-only helpers (package-private; live in _test.go so they
// don't leak into production API).
// ------------------------------------------------------------------

// primeEnginesFromStore is the Kafka-free half of Start. Exposed
// on the struct here so tests can exercise the engine map +
// snapshot-restore wiring without having to stand up a broker.
func (p *Pipeline) primeEnginesFromStore(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("pipeline: already started")
	}
	startedAt := time.Now()
	for _, v := range p.owned {
		part := int32(v)
		eng := shadow.New(v)
		p.engines[part] = eng
		key := fmt.Sprintf(p.cfg.SnapshotKeyFormat, v)
		snap, err := snapshotpkg.Load(ctx, p.cfg.Store, key)
		switch {
		case err == nil:
			if err := eng.RestoreFromSnapshot(snap); err != nil {
				return err
			}
		case errors.Is(err, os.ErrNotExist):
			// cold start
		default:
			return err
		}
		p.lastSnapshotAt[part] = startedAt
	}
	return nil
}

// slowStore is a BlobStore wrapper that blocks Put until
// putRelease is closed — used to exercise in-flight save guard
// and Close drain.
type slowStore struct {
	inner         snapshotpkg.BlobStore
	putRelease    chan struct{}
	putsAttempted atomic.Int32
	putsInFlight  atomic.Int32
}

func (s *slowStore) Put(ctx context.Context, key string, data []byte) error {
	s.putsAttempted.Add(1)
	s.putsInFlight.Add(1)
	defer s.putsInFlight.Add(-1)
	select {
	case <-s.putRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.Put(ctx, key, data)
}

func (s *slowStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.inner.Get(ctx, key)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never satisfied")
}

func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
