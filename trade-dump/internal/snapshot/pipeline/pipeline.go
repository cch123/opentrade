// Package pipeline is trade-dump's ADR-0061 snapshot pipeline.
//
// One Pipeline instance consumes every counter-journal partition
// (assign mode, no consumer group) and maintains one ShadowEngine
// per vshard. For each record:
//
//  1. Decode the CounterJournalEvent.
//  2. engine.Apply (advances shadow state and engine-local
//     counterSeq / teWatermark / journalOffset).
//  3. maybeCapture — if the time window or event-count window has
//     been exceeded, Capture the snapshot and ship it to the blob
//     store asynchronously.
//
// Offset authority is the snapshot, not Kafka consumer groups
// (same pattern as Counter, ADR-0048 / ADR-0060 §4.1): on restart
// the pipeline loads the last snapshot per vshard, seeds
// ShadowEngine via RestoreFromSnapshot, and seeks the Kafka
// consumer to snap.JournalOffset. If no snapshot exists the
// partition resumes AtStart (cold rebuild, one-time per
// deployment).
//
// Threading model (ADR-0061 §4.2): a single Run goroutine owns all
// per-vshard ShadowEngine state, so Apply and Capture are
// trivially serialised. Save runs on a background goroutine per
// vshard — at most one save in flight per vshard (new triggers
// skip if busy) so we never race the blob store key overwrite.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	snapshotpkg "github.com/xargin/opentrade/counter/snapshot"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/shadow"
)

// Config wires every knob the pipeline needs. Zero values fall
// back to sensible defaults noted per-field.
type Config struct {
	// Brokers is the Kafka seed set.
	Brokers []string

	// ClientID is stamped on every Kafka request for audit.
	// Default "trade-dump-snapshot".
	ClientID string

	// JournalTopic is the counter-journal topic name. Default
	// "counter-journal".
	JournalTopic string

	// VShardCount is how many vshards the cluster is running. ONE
	// ShadowEngine is constructed per vshard; record.Partition is
	// the vshard id (ADR-0058 §2a).
	VShardCount int

	// Store is the blob store snapshots land in. Required; usually
	// an S3 or FS implementation shared with Counter's reader.
	Store snapshotpkg.BlobStore

	// SnapshotFormat controls the on-disk encoding. Default
	// snapshot.FormatProto.
	SnapshotFormat snapshotpkg.Format

	// SnapshotKeyFormat is the blob-store key stem per vshard.
	// Default "vshard-%03d" (same as Counter's SnapshotKeyFormat
	// so the stub works for both readers during cut-over).
	SnapshotKeyFormat string

	// SnapshotInterval is the max wall-clock gap between two
	// snapshots of the same vshard. Default 60s (ADR-0064 §5 — the
	// on-demand path took over "recovery main cursor" duty, so the
	// periodic role collapsed to "fallback starting point when
	// trade-dump is down". Earlier default was 10s from ADR-0061).
	SnapshotInterval time.Duration

	// SnapshotEventCount is the max number of applied events
	// between snapshots (triggers first between interval and
	// count). Default 60000 (ADR-0064 §5; was 10000 under
	// ADR-0061).
	SnapshotEventCount uint64

	// SaveTimeout bounds each snapshot.Save call. Default 30s;
	// exceeded saves log + skip the commit.
	SaveTimeout time.Duration

	// Logger defaults to zap.NewNop on nil.
	Logger *zap.Logger
}

// Pipeline owns the Kafka consumer + per-vshard ShadowEngine set +
// per-vshard save coordination. Construct via New; drive via Run.
type Pipeline struct {
	cfg    Config
	cli    *kgo.Client
	logger *zap.Logger

	// engines is keyed by vshard id / partition. Populated in
	// Start before the consumer opens and never mutated after, so
	// reads from the main loop don't need locking.
	engines map[int32]*shadow.Engine

	// lastSnapshotAt tracks the wall-clock timestamp of the last
	// completed Capture per vshard. Read by maybeCapture in the
	// main goroutine — no locking needed.
	lastSnapshotAt map[int32]time.Time

	// savingInFlight[vshard] is a lightweight atomic guard so two
	// maybeCapture ticks in quick succession (before the previous
	// save finished) don't race the blob-store key. One dropped
	// snapshot is fine — the trigger fires every
	// SnapshotInterval / SnapshotEventCount anyway. Atomic + no
	// map mutation at runtime keeps the hot path lock-free.
	savingInFlight sync.Map // int32 vshard → *atomic.Bool

	// saveWG waits on all in-flight async saves during shutdown
	// so the caller can drain before exiting the process.
	saveWG sync.WaitGroup

	// started / closed gate Start and Close so they're idempotent.
	started atomic.Bool
	closed  atomic.Bool
}

// New validates cfg and returns a Pipeline ready for Start.
func New(cfg Config) (*Pipeline, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("pipeline: Brokers required")
	}
	if cfg.Store == nil {
		return nil, errors.New("pipeline: Store required")
	}
	if cfg.VShardCount <= 0 {
		return nil, errors.New("pipeline: VShardCount must be > 0")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "trade-dump-snapshot"
	}
	if cfg.JournalTopic == "" {
		cfg.JournalTopic = "counter-journal"
	}
	if cfg.SnapshotKeyFormat == "" {
		cfg.SnapshotKeyFormat = "vshard-%03d"
	}
	if cfg.SnapshotInterval <= 0 {
		// ADR-0064 §5: periodic snapshot is the fallback starting
		// point, not the hot-path recovery cursor. 60s RPO is the
		// ceiling the ADR commits to; tighten per deployment via
		// --snapshot-interval if the operator wants shorter
		// fallback windows.
		cfg.SnapshotInterval = 60 * time.Second
	}
	if cfg.SnapshotEventCount == 0 {
		cfg.SnapshotEventCount = 60000
	}
	if cfg.SaveTimeout <= 0 {
		cfg.SaveTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Pipeline{
		cfg:            cfg,
		logger:         cfg.Logger.With(zap.String("component", "snapshot-pipeline")),
		engines:        make(map[int32]*shadow.Engine, cfg.VShardCount),
		lastSnapshotAt: make(map[int32]time.Time, cfg.VShardCount),
	}, nil
}

// Start primes every vshard's ShadowEngine from the blob store (if
// a snapshot exists) and opens the Kafka consumer with per-
// partition seek offsets derived from those snapshots.
//
// Returns when the consumer is attached; caller then invokes Run
// to drive the apply loop.
//
// ctx is used only for blob-store reads during priming; the
// consumer uses a separate context in Run.
func (p *Pipeline) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("pipeline: Start already called")
	}

	partitionOffsets := make(map[int32]kgo.Offset, p.cfg.VShardCount)
	startedAt := time.Now()
	for v := 0; v < p.cfg.VShardCount; v++ {
		part := int32(v)
		eng := shadow.New(v)
		p.engines[part] = eng

		key := fmt.Sprintf(p.cfg.SnapshotKeyFormat, v)
		snap, err := snapshotpkg.Load(ctx, p.cfg.Store, key)
		switch {
		case err == nil:
			if err := eng.RestoreFromSnapshot(snap); err != nil {
				return fmt.Errorf("pipeline: restore vshard %d: %w", v, err)
			}
			partitionOffsets[part] = kgo.NewOffset().At(eng.NextJournalOffset())
			p.logger.Info("vshard primed from snapshot",
				zap.Int("vshard", v),
				zap.Uint64("counter_seq", eng.CounterSeq()),
				zap.Int64("next_offset", eng.NextJournalOffset()))
		case errors.Is(err, os.ErrNotExist):
			partitionOffsets[part] = kgo.NewOffset().AtStart()
			p.logger.Info("vshard cold start (no snapshot)", zap.Int("vshard", v))
		default:
			return fmt.Errorf("pipeline: load vshard %d: %w", v, err)
		}
		// Seed lastSnapshotAt at Start so the time-based trigger
		// fires SnapshotInterval later — without this seeding the
		// first event would always pass the "never captured" test
		// and we'd snapshot prematurely.
		p.lastSnapshotAt[part] = startedAt
	}

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(p.cfg.Brokers...),
		kgo.ClientID(p.cfg.ClientID),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.JournalTopic: partitionOffsets,
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return fmt.Errorf("pipeline: kgo.NewClient: %w", err)
	}
	p.cli = cli
	return nil
}

// Run drives the apply + snapshot loop until ctx is cancelled or
// the consumer client is closed. After Run returns, callers MUST
// call Close to drain any in-flight async saves.
//
// Single-threaded on the apply side by design (ADR-0061 §4.2).
func (p *Pipeline) Run(ctx context.Context) error {
	if !p.started.Load() {
		return errors.New("pipeline: Run before Start")
	}
	if p.closed.Load() {
		return errors.New("pipeline: Run after Close")
	}
	for {
		fetches := p.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, part int32, err error) {
			p.logger.Warn("fetch error",
				zap.String("topic", t), zap.Int32("partition", part), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			p.handleRecord(ctx, rec)
		})
	}
}

// Close shuts the Kafka client down and waits for every in-flight
// async save to finish. Idempotent.
func (p *Pipeline) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	if p.cli != nil {
		p.cli.Close()
	}
	p.saveWG.Wait()
}

// handleRecord decodes one journal record, Applies it, and
// triggers a snapshot if the window is exceeded. All on the
// caller's goroutine (the Run loop).
func (p *Pipeline) handleRecord(ctx context.Context, rec *kgo.Record) {
	eng, ok := p.engines[rec.Partition]
	if !ok {
		// Unknown partition. Likely a VShardCount misconfig — log
		// once per partition and drop.
		p.logger.Warn("unknown partition", zap.Int32("partition", rec.Partition))
		return
	}
	var evt eventpb.CounterJournalEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		p.logger.Error("decode counter-journal record",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err))
		return
	}
	if err := eng.Apply(&evt, rec.Offset); err != nil {
		p.logger.Error("apply counter-journal record",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err))
		return
	}
	p.maybeCapture(ctx, rec.Partition, eng)
}

// maybeCapture checks the trigger conditions for vshard part and,
// if exceeded, Captures a snapshot and ships it to the background
// save goroutine. Called on the main loop goroutine, synchronous
// with Apply — Capture is inexpensive (engine deep-copies) and
// the expensive Save is deferred.
func (p *Pipeline) maybeCapture(ctx context.Context, part int32, eng *shadow.Engine) {
	now := time.Now()
	lastAt := p.lastSnapshotAt[part] // seeded in Start so always valid
	byTime := now.Sub(lastAt) >= p.cfg.SnapshotInterval
	byCount := eng.EventsSinceLastSnapshot() >= p.cfg.SnapshotEventCount
	if !byTime && !byCount {
		return
	}

	// Bail early if a save for this vshard is already running.
	// We'd rather miss a trigger than blow two overlapping saves
	// at the blob store and race the key.
	inFlight := p.getSavingFlag(part)
	if !inFlight.CompareAndSwap(false, true) {
		return
	}

	snap := eng.Capture(now.UnixMilli())
	eng.ClearEventsSinceLastSnapshot()
	p.lastSnapshotAt[part] = now

	p.saveWG.Add(1)
	go func() {
		defer p.saveWG.Done()
		defer inFlight.Store(false)
		p.saveSnapshot(ctx, part, snap)
	}()
}

// saveSnapshot runs on a background goroutine. Failures log +
// drop; the next trigger re-captures and retries (idempotent —
// snapshots are key-overwrite).
func (p *Pipeline) saveSnapshot(parent context.Context, part int32, snap *snapshotpkg.ShardSnapshot) {
	key := fmt.Sprintf(p.cfg.SnapshotKeyFormat, part)
	ctx, cancel := context.WithTimeout(parent, p.cfg.SaveTimeout)
	defer cancel()
	start := time.Now()
	if err := snapshotpkg.Save(ctx, p.cfg.Store, key, snap, p.cfg.SnapshotFormat); err != nil {
		p.logger.Error("snapshot save failed",
			zap.Int32("vshard", part),
			zap.Int64("journal_offset", snap.JournalOffset),
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(err))
		return
	}
	p.logger.Debug("snapshot saved",
		zap.Int32("vshard", part),
		zap.Int64("journal_offset", snap.JournalOffset),
		zap.Uint64("counter_seq", snap.CounterSeq),
		zap.Duration("elapsed", time.Since(start)))
}

// getSavingFlag returns (creating if needed) the atomic flag used
// to serialise per-vshard saves.
func (p *Pipeline) getSavingFlag(part int32) *atomic.Bool {
	if v, ok := p.savingInFlight.Load(part); ok {
		return v.(*atomic.Bool)
	}
	flag := &atomic.Bool{}
	actual, _ := p.savingInFlight.LoadOrStore(part, flag)
	return actual.(*atomic.Bool)
}

// Engines is a read-only view for tests / diagnostics. The returned
// map itself is populated in Start and not mutated afterward, so
// concurrent access is safe; the *shadow.Engine values carry their
// own internal synchronisation (ADR-0064 M1c-α mutex + atomic
// cursor).
func (p *Pipeline) Engines() map[int32]*shadow.Engine { return p.engines }

// ShadowEngine returns the shadow engine for vshard (== Kafka
// partition id) and ok=true if this pipeline owns it, else (nil,
// false). ADR-0064 on-demand snapshot handler
// (snapshotrpc.TakeSnapshot) uses this to reach the per-vshard
// atomic cursor and Capture path without holding a reference to
// the full engines map. Safe to call from any goroutine.
func (p *Pipeline) ShadowEngine(vshard int32) (*shadow.Engine, bool) {
	eng, ok := p.engines[vshard]
	return eng, ok
}
