package snapshotrpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	"github.com/xargin/opentrade/api/gen/rpc/tradedump/tradedumprpcconnect"
	countersnap "github.com/xargin/opentrade/pkg/snapshot/counter"
	"github.com/xargin/opentrade/pkg/connectx"
	countershadow "github.com/xargin/opentrade/trade-dump/internal/snapshot/counter/shadow"
)

// -----------------------------------------------------------------------------
// Test doubles
// -----------------------------------------------------------------------------

// stubShadow implements ShadowAccessor against an in-memory map of
// countershadow.Engine instances. Populated via addEngine per test.
type stubShadow struct {
	engines map[int32]*countershadow.Engine
}

func newStubShadow() *stubShadow { return &stubShadow{engines: map[int32]*countershadow.Engine{}} }

func (s *stubShadow) ShadowEngine(v int32) (*countershadow.Engine, bool) {
	e, ok := s.engines[v]
	return e, ok
}

func (s *stubShadow) addEngine(v int32) *countershadow.Engine {
	e := countershadow.New(int(v))
	s.engines[v] = e
	return e
}

// stubAdmin returns canned LEOs, optionally blocking or erroring.
type stubAdmin struct {
	mu     sync.Mutex
	offs   map[int32]int64
	errOn  map[int32]error
	calls  atomic.Int64
	block  atomic.Bool
	blockC chan struct{}
}

func newStubAdmin() *stubAdmin {
	return &stubAdmin{
		offs:   map[int32]int64{},
		errOn:  map[int32]error{},
		blockC: make(chan struct{}),
	}
}

func (s *stubAdmin) set(p int32, leo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offs[p] = leo
}

func (s *stubAdmin) setErr(p int32, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[p] = err
}

func (s *stubAdmin) ListEndOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	s.calls.Add(1)
	if s.block.Load() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.blockC:
			// unblocked by test
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.errOn[partition]; ok && err != nil {
		return 0, err
	}
	off, ok := s.offs[partition]
	if !ok {
		return 0, fmt.Errorf("partition %d not set", partition)
	}
	return off, nil
}

// stubBlob is an in-memory BlobStore that captures Save calls so
// tests can assert keys and payload presence.
type stubBlob struct {
	mu      sync.Mutex
	objects map[string][]byte
	saveErr error
	saves   atomic.Int64
}

func newStubBlob() *stubBlob {
	return &stubBlob{objects: map[string][]byte{}}
}

func (b *stubBlob) Put(ctx context.Context, key string, body []byte) error {
	b.saves.Add(1)
	if b.saveErr != nil {
		return b.saveErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	clone := make([]byte, len(body))
	copy(clone, body)
	b.objects[key] = clone
	return nil
}

func (b *stubBlob) Get(ctx context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if body, ok := b.objects[key]; ok {
		clone := make([]byte, len(body))
		copy(clone, body)
		return clone, nil
	}
	return nil, os.ErrNotExist
}

func (b *stubBlob) hasKey(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.objects[key]
	return ok
}

// helper: drive a single Apply into an engine at kafkaOffset so
// PublishedOffset() jumps to kafkaOffset+1. Cheapest no-state-impact
// event: TECheckpoint.
func seedApply(t *testing.T, eng *countershadow.Engine, seq uint64, kafkaOffset int64) {
	t.Helper()
	evt := &eventpb.CounterJournalEvent{
		CounterSeqId: seq,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: int32(eng.VShardID()), TeOffset: int64(seq)},
		},
	}
	if err := eng.Apply(evt, kafkaOffset); err != nil {
		t.Fatalf("seedApply: %v", err)
	}
}

// fixedNow returns a time that doesn't drift — keeps on-demand key
// generation deterministic for test assertions.
func fixedNow(ms int64) func() time.Time {
	ts := time.UnixMilli(ms)
	return func() time.Time { return ts }
}

// connectErrMsg returns the message of a *connect.Error or err.Error()
// for other types. Used in t.Fatalf format strings so test failure logs
// stay informative under the Connect wire.
func connectErrMsg(err error) string {
	if err == nil {
		return ""
	}
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return cerr.Message()
	}
	return err.Error()
}

// -----------------------------------------------------------------------------
// EpochTracker unit tests
// -----------------------------------------------------------------------------

func TestEpochTracker_FirstRequestAccepted(t *testing.T) {
	tr := NewEpochTracker()
	ok, last := tr.CheckAndAdvance(5, 10)
	if !ok || last != 0 {
		t.Fatalf("CheckAndAdvance first call: ok=%v last=%d", ok, last)
	}
	if got := tr.Get(5); got != 10 {
		t.Fatalf("Get(5) = %d, want 10", got)
	}
}

func TestEpochTracker_EqualEpochAccepted(t *testing.T) {
	tr := NewEpochTracker()
	tr.CheckAndAdvance(5, 10)
	ok, _ := tr.CheckAndAdvance(5, 10)
	if !ok {
		t.Fatal("equal-epoch request rejected; should be allowed")
	}
}

func TestEpochTracker_StaleEpochRejected(t *testing.T) {
	tr := NewEpochTracker()
	tr.CheckAndAdvance(5, 10)
	ok, last := tr.CheckAndAdvance(5, 9)
	if ok {
		t.Fatal("stale epoch accepted")
	}
	if last != 10 {
		t.Fatalf("last = %d, want 10", last)
	}
	// Stale request must NOT change stored epoch.
	if got := tr.Get(5); got != 10 {
		t.Fatalf("Get(5) after stale reject = %d, want 10", got)
	}
}

func TestEpochTracker_AdvancesOnNewer(t *testing.T) {
	tr := NewEpochTracker()
	tr.CheckAndAdvance(5, 10)
	ok, _ := tr.CheckAndAdvance(5, 11)
	if !ok {
		t.Fatal("newer epoch rejected")
	}
	if got := tr.Get(5); got != 11 {
		t.Fatalf("Get(5) = %d, want 11", got)
	}
}

func TestEpochTracker_PerVshardIsolation(t *testing.T) {
	tr := NewEpochTracker()
	tr.CheckAndAdvance(5, 10)
	tr.CheckAndAdvance(6, 3)
	// Stale for vshard 5, fresh for 6.
	if ok, _ := tr.CheckAndAdvance(5, 5); ok {
		t.Fatal("vshard 5 epoch=5 should be stale")
	}
	if ok, _ := tr.CheckAndAdvance(6, 3); !ok {
		t.Fatal("vshard 6 epoch=3 should be accepted (equal)")
	}
}

// -----------------------------------------------------------------------------
// Handler skeleton fallback (M1b compatibility)
// -----------------------------------------------------------------------------

// TestServer_TakeSnapshot_NilRequestInvalidArgument pins the most
// basic guard — handler must not nil-deref. Survives both skeleton
// and full-backend modes.
func TestServer_TakeSnapshot_NilRequestInvalidArgument(t *testing.T) {
	srv := New(Config{Logger: zap.NewNop()})
	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest((*tradedumprpc.TakeSnapshotRequest)(nil)))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", got)
	}
}

// TestServer_TakeSnapshot_SkeletonReturnsUnimplemented pins M1b
// backward compat: if any backend dep (Shadow/Admin/BlobStore) is
// missing, handler must return Unimplemented so Counter falls back
// cleanly.
func TestServer_TakeSnapshot_SkeletonReturnsUnimplemented(t *testing.T) {
	srv := New(Config{Logger: zap.NewNop()}) // no backend deps
	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 5, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeUnimplemented {
		t.Fatalf("want Unimplemented, got %v", got)
	}
}

// TestServer_TakeSnapshot_TransportRegistration is a transport-level
// smoke test that survives M1b → M1c wiring: a real h2c listener, a
// real Connect client dial, and a call that must not panic. Backend
// intentionally left missing so the reply is Unimplemented.
func TestServer_TakeSnapshot_TransportRegistration(t *testing.T) {
	mux := http.NewServeMux()
	path, handler := tradedumprpcconnect.NewTradeDumpSnapshotHandler(New(Config{Logger: zap.NewNop()}))
	mux.Handle(path, handler)
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.EnableHTTP2 = true
	srv.Start()
	t.Cleanup(srv.Close)

	endpoint := strings.TrimPrefix(srv.URL, "http://")
	httpClient := connectx.NewH2CClient()
	t.Cleanup(httpClient.CloseIdleConnections)
	client := tradedumprpcconnect.NewTradeDumpSnapshotClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)

	callCtx, cancelCall := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCall()
	_, err := client.TakeSnapshot(callCtx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 1, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeUnimplemented {
		t.Fatalf("want Unimplemented over transport, got %v", got)
	}
}

// -----------------------------------------------------------------------------
// Full-backend handler tests
// -----------------------------------------------------------------------------

// newTestServer wires a Server with stub deps backing vshards 0..n-1.
func newTestServer(t *testing.T, nVshards int32) (*Server, *stubShadow, *stubAdmin, *stubBlob) {
	t.Helper()
	sh := newStubShadow()
	for i := int32(0); i < nVshards; i++ {
		sh.addEngine(i)
	}
	admin := newStubAdmin()
	blob := newStubBlob()
	srv := New(Config{
		Logger:    zap.NewNop(),
		Shadow:    sh,
		Admin:     admin,
		BlobStore: blob,
		KeyPrefix: "test/",
		// Short WaitApplyTimeout so timeout path tests don't drag.
		WaitApplyTimeout: 50 * time.Millisecond,
		nowFn:            fixedNow(1_700_000_000_000),
	})
	return srv, sh, admin, blob
}

// TestServer_TakeSnapshot_HappyPath walks the full §2 flow:
// epoch accepted → singleflight → sem acquired → LEO query →
// WaitAppliedTo succeeds (seeded via Apply) → Capture → upload →
// return (key, leo, counter_seq).
func TestServer_TakeSnapshot_HappyPath(t *testing.T) {
	srv, sh, admin, blob := newTestServer(t, 4)

	// Seed vshard 2 with one Apply at offset 99 so PublishedOffset
	// = 100. Admin reports LEO = 100 → WaitAppliedTo succeeds
	// immediately on fast path.
	eng := sh.engines[2]
	seedApply(t, eng, 42, 99)
	admin.set(2, 100)

	resp, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId:        2,
		RequesterNodeId: "counter-node-A",
		RequesterEpoch:  7,
	}))
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	if resp.Msg.Leo != 100 {
		t.Errorf("resp.Msg.Leo = %d, want 100", resp.Msg.Leo)
	}
	if resp.Msg.CounterSeq != 42 {
		t.Errorf("resp.Msg.CounterSeq = %d, want 42", resp.Msg.CounterSeq)
	}
	wantKey := "test/vshard-002-ondemand-1700000000000"
	if resp.Msg.SnapshotKey != wantKey {
		t.Errorf("resp.Msg.SnapshotKey = %q, want %q", resp.Msg.SnapshotKey, wantKey)
	}
	// Blob store should have the expected key written. snapshotpkg
	// appends extension — proto format → ".pb".
	if !blob.hasKey(wantKey + ".pb") {
		t.Errorf("blob store missing expected key %s.pb", wantKey)
	}
}

// TestServer_TakeSnapshot_UnknownVshard fails fast when this
// trade-dump instance does not own the requested vshard. Counter's
// fallback picks this up and retries via cluster routing.
func TestServer_TakeSnapshot_UnknownVshard(t *testing.T) {
	srv, _, _, _ := newTestServer(t, 2)
	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 99, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", got)
	}
}

// TestServer_TakeSnapshot_StaleEpochRejected drives Counter-zombie
// scenario: first accept epoch 5, then reject a request at epoch 3.
func TestServer_TakeSnapshot_StaleEpochRejected(t *testing.T) {
	srv, sh, admin, _ := newTestServer(t, 2)
	seedApply(t, sh.engines[0], 1, 0)
	admin.set(0, 1)

	// First request at epoch 5 succeeds.
	if _, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 5,
	})); err != nil {
		t.Fatalf("first request: %v", err)
	}
	// Stale request at epoch 3 rejected.
	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 3,
	}))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition for stale epoch, got %v (msg=%v)", got, connectErrMsg(err))
	}
}

// TestServer_TakeSnapshot_LEOQueryFailure maps Admin errors to
// codes.Unavailable so Counter falls back to legacy startup.
func TestServer_TakeSnapshot_LEOQueryFailure(t *testing.T) {
	srv, sh, admin, _ := newTestServer(t, 1)
	seedApply(t, sh.engines[0], 1, 0)
	admin.setErr(0, errors.New("kafka broker unreachable"))

	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("want Unavailable, got %v (msg=%v)", got, connectErrMsg(err))
	}
}

func TestServer_TakeSnapshot_WorkTimeoutBoundsDetachedLEO(t *testing.T) {
	sh := newStubShadow()
	eng := sh.addEngine(0)
	seedApply(t, eng, 1, 0)
	admin := newStubAdmin()
	admin.set(0, 1)
	admin.block.Store(true)
	blob := newStubBlob()
	srv := New(Config{
		Logger:           zap.NewNop(),
		Shadow:           sh,
		Admin:            admin,
		BlobStore:        blob,
		WaitApplyTimeout: time.Second,
		WorkTimeout:      30 * time.Millisecond,
		nowFn:            fixedNow(1_700_000_000_000),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := srv.TakeSnapshot(ctx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("want DeadlineExceeded from detached work timeout, got %v (msg=%v)", got, connectErrMsg(err))
	}
	admin.block.Store(false)
	close(admin.blockC)
}

// TestServer_TakeSnapshot_WaitApplyTimeout fires when shadow does
// not catch up to LEO within WaitApplyTimeout. Maps to
// DeadlineExceeded.
func TestServer_TakeSnapshot_WaitApplyTimeout(t *testing.T) {
	srv, sh, admin, _ := newTestServer(t, 1)
	// Engine at offset 50 but LEO=200 — WaitApplyTimeout (50ms in
	// newTestServer) will fire before any Apply bumps the cursor.
	seedApply(t, sh.engines[0], 1, 49)
	admin.set(0, 200)

	start := time.Now()
	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	elapsed := time.Since(start)
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("want DeadlineExceeded, got %v (msg=%v)", got, connectErrMsg(err))
	}
	// Must not drag beyond the configured budget + a small buffer.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("WaitApply timeout path took too long: %v", elapsed)
	}
}

// TestServer_TakeSnapshot_BlobStoreFailure maps upload errors to
// codes.Unavailable.
func TestServer_TakeSnapshot_BlobStoreFailure(t *testing.T) {
	srv, sh, admin, blob := newTestServer(t, 1)
	seedApply(t, sh.engines[0], 1, 99)
	admin.set(0, 100)
	blob.saveErr = errors.New("s3 503")

	_, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("want Unavailable for blob failure, got %v (msg=%v)", got, connectErrMsg(err))
	}
}

// TestServer_TakeSnapshot_SingleflightCoalescesSameVshard asserts
// concurrent requests for the same vshard share the single Capture
// + upload. 10 concurrent callers → Admin.ListEndOffsets called
// once, blob.Save called once.
func TestServer_TakeSnapshot_SingleflightCoalescesSameVshard(t *testing.T) {
	srv, sh, admin, blob := newTestServer(t, 1)
	seedApply(t, sh.engines[0], 1, 99)
	admin.set(0, 100)

	// Block Admin briefly so all 10 callers converge inside
	// singleflight before the first finishes.
	admin.block.Store(true)

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
				VshardId: 0, RequesterEpoch: 1,
			}))
		}()
	}
	// Let the goroutines pile up inside singleflight.
	time.Sleep(20 * time.Millisecond)
	admin.block.Store(false)
	close(admin.blockC)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d error: %v", i, err)
		}
	}
	if c := admin.calls.Load(); c != 1 {
		t.Errorf("Admin.ListEndOffset calls = %d, want 1 (singleflight)", c)
	}
	if c := blob.saves.Load(); c != 1 {
		t.Errorf("Blob.Save calls = %d, want 1 (singleflight)", c)
	}
}

// TestServer_TakeSnapshot_SemaphoreLimitsInflight drives genuine
// saturation of the global in-flight semaphore and asserts the
// (Concurrency+1)-th request receives ResourceExhausted after the
// configured SemAcquireTimeout.
//
// Setup: concurrency=2, 4 vshards; block Admin so 2 winners hold
// sem permits indefinitely in the singleflight inner work. A 3rd
// request on a distinct vshard hits the inner sem.Acquire with a
// 30ms SemAcquireTimeout and must return ResourceExhausted — not
// ctx.Canceled from the caller (caller ctx stays live for 1s).
//
// Post codex review P2: caller ctx no longer flows into sem
// acquire directly; the inner worker uses a detached context with
// its own bounded timeout (Config.SemAcquireTimeout). This test
// pins that contract end-to-end.
func TestServer_TakeSnapshot_SemaphoreLimitsInflight(t *testing.T) {
	sh := newStubShadow()
	for i := int32(0); i < 4; i++ {
		eng := sh.addEngine(i)
		seedApply(t, eng, 1, 0)
	}
	admin := newStubAdmin()
	for i := int32(0); i < 4; i++ {
		admin.set(i, 1)
	}
	blob := newStubBlob()
	srv := New(Config{
		Logger:            zap.NewNop(),
		Shadow:            sh,
		Admin:             admin,
		BlobStore:         blob,
		KeyPrefix:         "",
		Concurrency:       2,
		WaitApplyTimeout:  500 * time.Millisecond,
		SemAcquireTimeout: 30 * time.Millisecond, // fail-fast for the test
		nowFn:             fixedNow(1_700_000_000_000),
	})

	// Block all Admin calls so the 2 winners hang inside
	// takeSnapshotOnce holding sem permits; a 3rd different-vshard
	// caller queues on sem until our 30ms SemAcquireTimeout fires.
	admin.block.Store(true)

	winnersCtx, cancelWinners := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWinners()

	var wg sync.WaitGroup
	for i := int32(0); i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = srv.TakeSnapshot(winnersCtx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
				VshardId: uint32(i), RequesterEpoch: 1,
			}))
		}()
	}
	// Let winners acquire the 2 permits.
	time.Sleep(20 * time.Millisecond)

	// Loser: live 1s ctx (plenty of budget from the caller's
	// perspective). The failure MUST come from the inner sem
	// acquire timing out at 30ms, mapped to ResourceExhausted —
	// not from the caller ctx.
	loserCtx, cancelLoser := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelLoser()
	_, loserErr := srv.TakeSnapshot(loserCtx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 2, RequesterEpoch: 1,
	}))
	if got := connect.CodeOf(loserErr); got != connect.CodeResourceExhausted {
		t.Errorf("want ResourceExhausted for saturated-sem loser, got %v (msg=%v)",
			got, connectErrMsg(loserErr))
	}

	// Unblock winners so the test finishes cleanly.
	admin.block.Store(false)
	close(admin.blockC)
	wg.Wait()
}

// autoPrefixBlob wraps a stubBlob to simulate S3BlobStore's internal
// prefix behaviour: every Put/Get call has the configured prefix
// prepended to the key before it reaches the backing map.
//
// Used by TestServer_TakeSnapshot_NoDoubleKeyPrefix to verify the
// handler does NOT apply its own KeyPrefix on top of a store that
// already self-prefixes (codex review M1d catch — double-prefixed
// keys fall outside the housekeeper's scan prefix).
type autoPrefixBlob struct {
	inner  *stubBlob
	prefix string
}

func (b *autoPrefixBlob) Put(ctx context.Context, key string, body []byte) error {
	return b.inner.Put(ctx, b.prefix+key, body)
}

func (b *autoPrefixBlob) Get(ctx context.Context, key string) ([]byte, error) {
	return b.inner.Get(ctx, b.prefix+key)
}

// TestServer_TakeSnapshot_NoDoubleKeyPrefix regression-tests the
// M1d codex P2 finding: when the BlobStore self-prefixes (as
// S3BlobStore does), the handler MUST NOT apply its own KeyPrefix
// in addition — otherwise the final key would be
// "<storeprefix><handlerprefix>vshard-NNN-ondemand-*", which the
// housekeeper's List("vshard-") scan misses and stale snapshots
// accumulate forever.
//
// We assert: with a store that prepends "s3prefix/" internally and
// a handler Config.KeyPrefix="" (production default), the final
// stored key is "s3prefix/vshard-NNN-ondemand-<ms>.pb" — a single
// prefix, matching where List("vshard-") would look.
func TestServer_TakeSnapshot_NoDoubleKeyPrefix(t *testing.T) {
	sh := newStubShadow()
	sh.addEngine(0)
	seedApply(t, sh.engines[0], 1, 99)
	admin := newStubAdmin()
	admin.set(0, 100)
	innerBlob := newStubBlob()
	store := &autoPrefixBlob{inner: innerBlob, prefix: "s3prefix/"}

	srv := New(Config{
		Logger:           zap.NewNop(),
		Shadow:           sh,
		Admin:            admin,
		BlobStore:        store,
		KeyPrefix:        "", // production default — rely on store's own prefix
		WaitApplyTimeout: 200 * time.Millisecond,
		nowFn:            fixedNow(1_700_000_000_000),
	})

	resp, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Handler-reported key is in the caller's namespace (pre-store-
	// prefix). The backing map has the store-prefixed full key.
	wantStoredKey := "s3prefix/vshard-000-ondemand-1700000000000.pb"
	if !innerBlob.hasKey(wantStoredKey) {
		t.Errorf("expected stored key %q not found; inner map = %+v",
			wantStoredKey, innerBlob.objects)
	}
	// Regression: the double-prefix pathological key MUST NOT exist.
	doubleKey := "s3prefix/s3prefix/vshard-000-ondemand-1700000000000.pb"
	if innerBlob.hasKey(doubleKey) {
		t.Errorf("double-prefixed key %q was written; handler applied prefix twice", doubleKey)
	}
	// Handler-returned SnapshotKey is in the caller-facing namespace
	// (no store prefix), which Counter's Load will pass back to the
	// same store's Get for automatic re-prefixing.
	if resp.Msg.SnapshotKey != "vshard-000-ondemand-1700000000000" {
		t.Errorf("resp.Msg.SnapshotKey = %q, want %q",
			resp.Msg.SnapshotKey, "vshard-000-ondemand-1700000000000")
	}
}

// TestServer_TakeSnapshot_LeoReportsCaptureCursor pins codex review
// P1: when the pipeline's Apply cursor advances past the queried
// LEO between the LEO query and Capture, the response MUST report
// snap.JournalOffset (the cursor the snapshot is actually aligned
// to), not the stale queried LEO. A Counter caller using the stale
// LEO for resume semantics would read-miss state already baked in
// and potentially duplicate-apply future journal records.
func TestServer_TakeSnapshot_LeoReportsCaptureCursor(t *testing.T) {
	srv, sh, admin, _ := newTestServer(t, 1)

	eng := sh.engines[0]
	// Seed publishedOffset = 100. Admin will report LEO=100.
	seedApply(t, eng, 1, 99)
	admin.set(0, 100)

	// Between WaitAppliedTo passing and Capture running, drive a
	// concurrent Apply so nextJournalOffset / publishedOffset
	// advance to 150. The shadow mutex serialises this vs Capture,
	// and the ADR contract is that whichever wins the mutex, the
	// resp.leo matches what Capture actually stamped.
	//
	// We simulate the "Apply raced ahead" case deterministically
	// by Apply'ing the extra records FIRST (so Capture sees cursor
	// = 150 when it runs), then verifying resp.Msg.Leo reflects 150,
	// NOT the Admin's queried 100.
	for i := int64(100); i < 150; i++ {
		evt := &eventpb.CounterJournalEvent{
			CounterSeqId: uint64(i + 1),
			Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
				TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 0, TeOffset: i},
			},
		}
		if err := eng.Apply(evt, i); err != nil {
			t.Fatalf("advance apply %d: %v", i, err)
		}
	}

	resp, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	// Queried LEO was 100, but Capture saw cursor at 150.
	// resp.Msg.Leo must be 150 (the real alignment), not 100.
	if resp.Msg.Leo != 150 {
		t.Errorf("resp.Msg.Leo = %d, want 150 (snap.JournalOffset after Apply race, not queried LEO 100)", resp.Msg.Leo)
	}
}

// TestServer_TakeSnapshot_SingleflightLeaderCancelDoesNotKillFollower
// pins codex review P2: when 2 same-vshard callers coalesce via
// singleflight and the leader's ctx expires before the follower's,
// the follower must still observe a successful response rather
// than inheriting the leader's ctx error. The detached inner
// context lets work complete under the follower's longer budget.
func TestServer_TakeSnapshot_SingleflightLeaderCancelDoesNotKillFollower(t *testing.T) {
	srv, sh, admin, _ := newTestServer(t, 1)
	seedApply(t, sh.engines[0], 1, 99)
	admin.set(0, 100)

	// Block Admin so both callers queue inside singleflight. The
	// leader's ctx will expire while it's blocked.
	admin.block.Store(true)

	// Leader: tight 20ms deadline. Will return ctx.DeadlineExceeded.
	leaderErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := srv.TakeSnapshot(ctx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
			VshardId: 0, RequesterEpoch: 1,
		}))
		leaderErr <- err
	}()
	// Let leader enter singleflight.
	time.Sleep(5 * time.Millisecond)

	// Follower: generous 2s deadline. Must succeed after leader
	// gives up and work eventually completes.
	followerResult := make(chan struct {
		resp *connect.Response[tradedumprpc.TakeSnapshotResponse]
		err  error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := srv.TakeSnapshot(ctx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
			VshardId: 0, RequesterEpoch: 1,
		}))
		followerResult <- struct {
			resp *connect.Response[tradedumprpc.TakeSnapshotResponse]
			err  error
		}{resp, err}
	}()
	// Give both goroutines time to enter singleflight.
	time.Sleep(10 * time.Millisecond)

	// Verify leader times out first.
	select {
	case err := <-leaderErr:
		if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
			t.Fatalf("leader expected DeadlineExceeded, got %v (msg=%v)", got, connectErrMsg(err))
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("leader did not time out in 200ms")
	}

	// Unblock Admin so the detached inner work can progress.
	admin.block.Store(false)
	close(admin.blockC)

	// Follower must succeed despite leader's earlier cancel.
	select {
	case r := <-followerResult:
		if r.err != nil {
			t.Fatalf("follower err (leader cancel leaked into shared work): %v", r.err)
		}
		if r.resp == nil || r.resp.Msg.Leo != 100 {
			t.Fatalf("follower resp = %+v, want non-nil with Leo=100", r.resp)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("follower did not complete")
	}
}

// TestServer_TakeSnapshot_SnapshotContentValid decodes the uploaded
// blob and verifies it reflects shadow state at Capture time —
// counter_seq, journal_offset, te_watermark all match engine
// values. Protects against an accidental empty-state bug during
// Capture wiring.
func TestServer_TakeSnapshot_SnapshotContentValid(t *testing.T) {
	srv, sh, admin, blob := newTestServer(t, 1)

	eng := sh.engines[0]
	// Apply two checkpoints so counterSeq advances to 10 and
	// teWatermark to the last offset value (=10). Using
	// TeCheckpointEvent so Apply bumps counterSeq + teWatermark
	// together (it pulls cp.TeOffset).
	for i := int64(1); i <= 10; i++ {
		evt := &eventpb.CounterJournalEvent{
			CounterSeqId: uint64(i),
			Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
				TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 0, TeOffset: i},
			},
		}
		if err := eng.Apply(evt, i-1); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	// PublishedOffset is now 10 (last kafkaOffset + 1).
	admin.set(0, 10)

	resp, err := srv.TakeSnapshot(context.Background(), connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId: 0, RequesterEpoch: 1,
	}))
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	if resp.Msg.CounterSeq != 10 {
		t.Errorf("resp.Msg.CounterSeq = %d, want 10", resp.Msg.CounterSeq)
	}
	if resp.Msg.Leo != 10 {
		t.Errorf("resp.Msg.Leo = %d, want 10", resp.Msg.Leo)
	}

	// Decode uploaded blob and assert matching content via the
	// countersnap.LoadPath path (same code Counter uses to read
	// an on-demand snapshot, so we exercise the real round-trip).
	snap, err := countersnap.LoadPath(context.Background(), blob, resp.Msg.SnapshotKey+".pb")
	if err != nil {
		t.Fatalf("LoadPath uploaded blob: %v", err)
	}
	if snap.CounterSeq != 10 {
		t.Errorf("decoded snap.CounterSeq = %d, want 10", snap.CounterSeq)
	}
	if snap.JournalOffset != 10 {
		t.Errorf("decoded snap.JournalOffset = %d, want 10", snap.JournalOffset)
	}
}
