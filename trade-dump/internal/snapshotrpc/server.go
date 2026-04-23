// Package snapshotrpc implements trade-dump's on-demand snapshot
// gRPC surface (ADR-0064 §2). It is the server counterpart to
// Counter's Phase 1 startup flow (ADR-0064 §3).
//
// M1c wiring (this file): the TakeSnapshot handler is fully
// implemented — epoch validation, concurrency control
// (semaphore + singleflight), Kafka LEO query, shadow WaitAppliedTo,
// Capture, serialize, and blob-store upload. Callers who do not
// wire the full dependency set via Config get the conservative
// defaults (shadow/admin/blobstore=nil → Unimplemented), preserving
// the M1b contract of "server can be registered without a pipeline
// backing it, Counter falls through to legacy".
//
// Counter side (M2, follow-up) treats UNIMPLEMENTED, UNAVAILABLE,
// FAILED_PRECONDITION, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED as
// fallback triggers per ADR-0064 §4.
package snapshotrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	snapshotpkg "github.com/xargin/opentrade/counter/snapshot"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/shadow"
)

// defaultSemAcquireTimeout bounds the wait for the global in-flight
// semaphore inside the detached singleflight worker. If more than
// Concurrency requests are active, the (Concurrency+1)-th caller
// fails fast with ResourceExhausted rather than queueing
// indefinitely (the worker context is detached from caller ctx by
// design, so without this bound a pile-up could stall forever on
// permit starvation). Counter interprets ResourceExhausted as a
// fallback trigger per ADR-0064 §4.
//
// Override per-Server via Config.SemAcquireTimeout (primarily for
// tests that drive the saturation path without waiting 1s).
const defaultSemAcquireTimeout = 1 * time.Second

// ShadowAccessor is the minimal read surface snapshotrpc needs from
// the pipeline layer. Returning (nil, false) means "this instance
// does not own vshard" — handler answers FAILED_PRECONDITION so
// Counter falls back (ADR-0064 §4) and retries via its cluster
// routing layer.
//
// Interface so tests can stub without importing the pipeline.
type ShadowAccessor interface {
	ShadowEngine(vshard int32) (*shadow.Engine, bool)
}

// KafkaAdmin abstracts the one Kafka admin call the handler makes —
// counter-journal LEO for a specific vshard partition. Interface
// keeps the implementation (kadm.Client) swappable in tests with a
// canned stub that returns known offsets on cue.
type KafkaAdmin interface {
	// ListEndOffset returns the Log End Offset for (topic,
	// partition) using READ_UNCOMMITTED semantics (ADR-0064 §2.3 —
	// we want the physical cursor, not ReadCommitted LSO, because
	// the sentinel commit marker may still be in the aborted-txn
	// shadow when LSO lags).
	ListEndOffset(ctx context.Context, topic string, partition int32) (int64, error)
}

// EpochTracker records the highest requester_epoch observed per
// vshard and rejects strictly-older requests. Defense in depth
// over ADR-0058 vshard lock: even if an old Counter owner leaks an
// RPC past fencing, trade-dump rejects it — preserving the "snapshot
// you got is for the current generation" contract.
//
// Lost on trade-dump restart (in-memory). First request post-restart
// populates the tracker, subsequent stale requests are rejected
// until something stronger (persisted epoch) becomes necessary.
type EpochTracker struct {
	mu   sync.RWMutex
	seen map[int32]uint64
}

// NewEpochTracker constructs an empty tracker.
func NewEpochTracker() *EpochTracker {
	return &EpochTracker{seen: make(map[int32]uint64)}
}

// CheckAndAdvance returns (true, 0) if epoch is >= the last observed
// epoch for vshard (updating the tracker to the new max), or
// (false, lastSeen) if the request is strictly older. Equal-epoch
// requests pass — a Counter may call TakeSnapshot multiple times
// with the same epoch in failure-recovery scenarios (our design
// says no retry, but defense in depth).
func (t *EpochTracker) CheckAndAdvance(vshard int32, epoch uint64) (ok bool, lastSeen uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	last, present := t.seen[vshard]
	if present && epoch < last {
		return false, last
	}
	t.seen[vshard] = epoch
	return true, 0
}

// Get returns the last observed epoch for vshard, or 0 if unseen.
// Primarily for test assertions and error messages.
func (t *EpochTracker) Get(vshard int32) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.seen[vshard]
}

// Config bundles the full dependency set. Each field has a default
// behaviour noted; zero-value fields that drop the handler to
// Unimplemented are documented per field.
type Config struct {
	// Logger. Nil → zap.NewNop.
	Logger *zap.Logger

	// Shadow exposes per-vshard shadow engines (for LEO wait,
	// Capture). Nil → TakeSnapshot returns Unimplemented.
	Shadow ShadowAccessor

	// Admin queries counter-journal LEO. Nil → TakeSnapshot
	// returns Unimplemented.
	Admin KafkaAdmin

	// BlobStore receives the serialized on-demand snapshot. Nil →
	// TakeSnapshot returns Unimplemented.
	BlobStore snapshotpkg.BlobStore

	// Epoch is the per-vshard last-seen epoch tracker. Nil →
	// NewEpochTracker() (fresh tracker, safe to share).
	Epoch *EpochTracker

	// JournalTopic is passed to Admin.ListEndOffset. Default
	// "counter-journal".
	JournalTopic string

	// KeyPrefix is prepended to the on-demand snapshot blob-store
	// key. Default "" (root). Pass the same prefix the pipeline
	// uses so housekeeping and periodic snapshots live in the
	// same key-space.
	KeyPrefix string

	// Concurrency is the global in-flight limit on concurrent
	// TakeSnapshot requests. Protects against whole-host restart
	// saturating the Capture + upload machinery. Default 16.
	Concurrency int

	// WaitApplyTimeout bounds the per-request wait for the shadow
	// engine to catch up to the Kafka LEO. Default 2s. Callers
	// (Counter) typically pass a 3s overall RPC deadline, so this
	// leaves ~1s for LEO query + Capture + upload.
	WaitApplyTimeout time.Duration

	// SnapshotFormat controls the encoding passed to
	// snapshotpkg.Save. Default snapshotpkg.FormatProto (same as
	// pipeline).
	SnapshotFormat snapshotpkg.Format

	// SemAcquireTimeout overrides the default global-semaphore
	// wait budget. Zero → defaultSemAcquireTimeout (1s). Tests
	// set this to a low value so saturation paths don't slow the
	// suite down.
	SemAcquireTimeout time.Duration

	// nowFn is a seam so tests can freeze wall-clock timestamps
	// in generated blob-store keys. Default time.Now.
	nowFn func() time.Time
}

// Server implements tradedumprpc.TradeDumpSnapshotServer.
type Server struct {
	tradedumprpc.UnimplementedTradeDumpSnapshotServer

	cfg               Config
	sem               *semaphore.Weighted
	sf                singleflight.Group
	epoch             *EpochTracker
	nowFn             func() time.Time
	semAcquireTimeout time.Duration
}

// New constructs a Server with the given Config. Nil Config fields
// receive sensible defaults (see Config doc). If Shadow / Admin /
// BlobStore are all nil, the handler stays in M1b Unimplemented
// mode — useful for deployments where snapshot pipeline is not yet
// enabled or for integration smoke tests.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.JournalTopic == "" {
		cfg.JournalTopic = "counter-journal"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.WaitApplyTimeout <= 0 {
		cfg.WaitApplyTimeout = 2 * time.Second
	}
	// snapshotpkg.FormatProto == 0 is the zero value already, so
	// unset SnapshotFormat is FormatProto by default. Kept as a
	// no-op branch here to flag intent — future additions to the
	// Format enum (beyond proto/json) may break this assumption.
	_ = cfg.SnapshotFormat
	if cfg.nowFn == nil {
		cfg.nowFn = time.Now
	}
	epoch := cfg.Epoch
	if epoch == nil {
		epoch = NewEpochTracker()
	}
	semTimeout := cfg.SemAcquireTimeout
	if semTimeout <= 0 {
		semTimeout = defaultSemAcquireTimeout
	}
	return &Server{
		cfg:               cfg,
		sem:               semaphore.NewWeighted(int64(cfg.Concurrency)),
		epoch:             epoch,
		nowFn:             cfg.nowFn,
		semAcquireTimeout: semTimeout,
	}
}

// hasFullBackend reports whether the server is wired with the full
// dep set required to actually produce an on-demand snapshot. If
// any piece is missing the handler short-circuits to Unimplemented
// — this matches the M1b skeleton's contract that Counter's
// fallback path can run unchanged against any trade-dump build.
func (s *Server) hasFullBackend() bool {
	return s.cfg.Shadow != nil && s.cfg.Admin != nil && s.cfg.BlobStore != nil
}

// TakeSnapshot implements the ADR-0064 §2 on-demand snapshot flow.
// Error codes map directly to Counter's fallback decision table
// (ADR-0064 §4) — every non-OK code instructs Counter to fall
// through to the legacy "load periodic snapshot + catchUpJournal"
// path.
func (s *Server) TakeSnapshot(
	ctx context.Context,
	req *tradedumprpc.TakeSnapshotRequest,
) (*tradedumprpc.TakeSnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}

	// M1b compatibility: if any backend dep is missing, stay in
	// Unimplemented so Counter takes fallback. Tests can cover the
	// full path by supplying stub Shadow / Admin / BlobStore.
	if !s.hasFullBackend() {
		s.cfg.Logger.Info("TakeSnapshot hit without full backend (skeleton mode)",
			zap.Uint32("vshard_id", req.VshardId),
			zap.String("requester_node_id", req.RequesterNodeId),
			zap.Uint64("requester_epoch", req.RequesterEpoch))
		return nil, status.Error(codes.Unimplemented,
			"TakeSnapshot backend not wired (Shadow/Admin/BlobStore missing)")
	}

	partition := int32(req.VshardId)

	// Ownership check first — if this instance doesn't own the
	// vshard, fail fast with FAILED_PRECONDITION. Counter then
	// retries via cluster routing or falls back.
	eng, ok := s.cfg.Shadow.ShadowEngine(partition)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition,
			"vshard %d not owned by this trade-dump instance", partition)
	}

	// Epoch validation. Defense in depth over ADR-0058 lock.
	if ok, lastSeen := s.epoch.CheckAndAdvance(partition, req.RequesterEpoch); !ok {
		return nil, status.Errorf(codes.FailedPrecondition,
			"stale epoch %d for vshard %d (last_seen=%d)",
			req.RequesterEpoch, partition, lastSeen)
	}

	// Singleflight by vshard: same-vshard concurrent requests
	// share the Capture + upload work rather than each burning a
	// Sem permit. Counter retry is disabled by design (ADR-0064
	// §4) but defence in depth.
	//
	// Use DoChan + a detached inner context so:
	//   (a) a caller's ctx cancellation (leader or follower) does
	//       NOT poison the shared work (codex review P2: a leader
	//       with a tight deadline would otherwise kill the capture
	//       out from under followers that still had budget);
	//   (b) each caller's own ctx still bounds its wait via the
	//       select against result ch.
	// Internal timeouts (sem acquire bound below + WaitApplyTimeout
	// + franz-go / kadm request deadlines + BlobStore client
	// timeouts) bound the detached work duration. Server shutdown
	// is handled by gRPC GracefulStop draining handler goroutines.
	sfKey := fmt.Sprintf("vshard-%d", partition)
	ch := s.sf.DoChan(sfKey, func() (any, error) {
		return s.takeSnapshotOnce(context.Background(), partition, eng)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*tradedumprpc.TakeSnapshotResponse), nil
	case <-ctx.Done():
		// Caller gave up. Inner work continues so other waiters
		// (or future same-epoch calls hitting the singleflight
		// cache window) still observe a result.
		if cerr := ctx.Err(); errors.Is(cerr, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, cerr.Error())
		}
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
}

// takeSnapshotOnce does the actual Capture+upload, guarded by the
// global concurrency semaphore. Separated from TakeSnapshot so
// singleflight wraps it cleanly.
//
// ctx here is a DETACHED context (context.Background-derived) so
// leader cancellation never kills shared work; a dedicated bounded
// context is derived inside for the sem acquire so the method does
// not hang forever if permits are saturated.
func (s *Server) takeSnapshotOnce(
	ctx context.Context,
	partition int32,
	eng *shadow.Engine,
) (*tradedumprpc.TakeSnapshotResponse, error) {
	// Concurrency cap — protects the 256-vshard whole-host restart
	// scenario from saturating Capture+S3 in parallel. The
	// Acquire's own ctx is bounded to semAcquireTimeout so a fully
	// saturated host returns ResourceExhausted fast rather than
	// queueing indefinitely.
	semCtx, cancelSem := context.WithTimeout(ctx, s.semAcquireTimeout)
	if err := s.sem.Acquire(semCtx, 1); err != nil {
		cancelSem()
		return nil, status.Error(codes.ResourceExhausted,
			"in-flight TakeSnapshot limit reached")
	}
	cancelSem()
	defer s.sem.Release(1)

	// 1. Query partition LEO. ReadUncommitted semantics — we want
	//    the physical terminus (ADR-0064 §2.3). kadm.ListEndOffsets
	//    default isolation is ReadUncommitted.
	leo, err := s.cfg.Admin.ListEndOffset(ctx, s.cfg.JournalTopic, partition)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable,
			"leo_query_fail: %v", err)
	}

	// 2. Wait for shadow to apply up to LEO. Bounded by configured
	//    WaitApplyTimeout (default 2s). Failure maps to
	//    DeadlineExceeded — Counter's fallback trigger.
	waitCtx, cancel := context.WithTimeout(ctx, s.cfg.WaitApplyTimeout)
	defer cancel()
	if err := eng.WaitAppliedTo(waitCtx, leo); err != nil {
		// Distinguish: parent ctx canceled (Counter gave up) vs.
		// our WaitApplyTimeout fired. Both return as
		// DeadlineExceeded with different reasons for logs.
		reason := "wait_apply_timeout"
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			reason = "caller_canceled"
		}
		return nil, status.Errorf(codes.DeadlineExceeded,
			"%s: target=%d published=%d err=%v",
			reason, leo, eng.PublishedOffset(), err)
	}

	// 3. Capture + serialize + upload.
	//
	// Capture takes the shadow mutex; between WaitAppliedTo
	// returning and Capture acquiring the mutex, pipeline Apply
	// may advance the cursor past the queried LEO. That's
	// benign — snap.JournalOffset will be >= leo, so the snapshot
	// reflects more journal history, not less, than Counter
	// expects. The response MUST report snap.JournalOffset (not
	// the queried leo) so Counter's recovery sees the cursor the
	// snapshot is actually aligned to (codex review P1: returning
	// the stale leo could let a caller restore from an older
	// offset than the state already baked in, risking duplicate
	// apply on any future resume path).
	now := s.nowFn()
	snap := eng.Capture(now.UnixMilli())

	// On-demand key namespace: separate from periodic snapshot
	// key to avoid churn on the standard "vshard-NNN" name.
	// Housekeeper (M1d) scans this prefix for cleanup.
	key := fmt.Sprintf("%svshard-%03d-ondemand-%d",
		s.cfg.KeyPrefix, partition, now.UnixMilli())

	if err := snapshotpkg.Save(ctx, s.cfg.BlobStore, key, snap, s.cfg.SnapshotFormat); err != nil {
		return nil, status.Errorf(codes.Unavailable,
			"s3_upload_error: %v", err)
	}

	s.cfg.Logger.Info("on-demand snapshot produced",
		zap.Int32("vshard", partition),
		zap.Int64("queried_leo", leo),
		zap.Int64("capture_journal_offset", snap.JournalOffset),
		zap.Uint64("counter_seq", snap.CounterSeq),
		zap.String("key", key))

	return &tradedumprpc.TakeSnapshotResponse{
		SnapshotKey: key,
		Leo:         snap.JournalOffset,
		CounterSeq:  snap.CounterSeq,
	}, nil
}
