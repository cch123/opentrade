// Package triggerpipeline is trade-dump's ADR-0067 trigger snapshot
// pipeline. One Pipeline instance owns the trigger-event Kafka
// consumer + a single triggershadow.Engine + a single fixed-key
// snapshot in the BlobStore.
//
// Mirror of the counter pipeline (ADR-0061), simplified:
//
//   - One shadow engine total. Trigger doesn't shard, so there is a
//     single apply cursor map (per Kafka partition) and a single
//     snapshot key in the BlobStore.
//   - trigger-event has no Kafka EOS. Default fetch isolation is
//     ReadUncommitted; LEO is the natural cursor.
//   - Single instance per cluster — assign mode on the configured
//     partition set so Kafka offset commits never override the
//     snapshot's cursor (ADR-0048).
//
// M2 wires only ApplyTriggerUpdate. ApplyMarketCheckpoint dispatch
// arrives in M3 once trigger primary starts producing the checkpoint
// event.
//
// Threading: a single Run goroutine drives Apply + Capture serially,
// matching ADR-0061 §4.2. Save runs on a background goroutine; the
// in-flight flag prevents two Saves from racing the BlobStore key.
package triggerpipeline

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
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/triggershadow"
)

// DefaultSnapshotKey is the BlobStore key stem the pipeline writes to.
// Fixed (no instance suffix) so a trigger HA standby reads the same
// snapshot the primary's events produced — closes the ADR-0042
// instance-id-keyed sharing gap.
const DefaultSnapshotKey = "trigger"

// Config wires every knob the pipeline needs. Zero values fall back
// to documented defaults.
type Config struct {
	// Brokers is the Kafka seed set.
	Brokers []string

	// ClientID is stamped on every Kafka request. Default
	// "trade-dump-trigger-snapshot".
	ClientID string

	// TriggerEventTopic is the topic name the trigger primary writes
	// post-change records to. Default "trigger-event".
	TriggerEventTopic string

	// PartitionCount is how many partitions the trigger-event topic
	// has. The pipeline assigns to all of [0, PartitionCount). At
	// least 1; mismatching the actual topic partition count means
	// some partitions are silently ignored, so operator config must
	// match topic provisioning.
	PartitionCount int

	// Store receives the produced snapshot. Required.
	Store snapshotpkg.BlobStore

	// SnapshotFormat controls the on-disk encoding. Default
	// snapshotpkg.FormatProto.
	SnapshotFormat snapshotpkg.Format

	// SnapshotKey is the BlobStore key stem (no extension). Default
	// DefaultSnapshotKey.
	SnapshotKey string

	// SnapshotInterval is the max wall-clock gap between two
	// snapshots. Default 60s (mirrors ADR-0064 §5).
	SnapshotInterval time.Duration

	// SnapshotEventCount is the max applied events between
	// snapshots. Default 60000.
	SnapshotEventCount uint64

	// SaveTimeout bounds each snapshot.Save call. Default 30s.
	SaveTimeout time.Duration

	// TerminalLimit caps the FIFO terminal ring on the shadow.
	// Default 10000 (~10× trigger primary's typical ADR-0040 cap so
	// a startup catch-up has plenty of headroom while keeping the
	// snapshot small).
	TerminalLimit int

	// Logger defaults to zap.NewNop on nil.
	Logger *zap.Logger
}

// Pipeline owns the consumer + shadow engine + save coordination.
// Construct via New, drive via Start + Run; Close on shutdown.
type Pipeline struct {
	cfg    Config
	cli    *kgo.Client
	logger *zap.Logger

	engine *triggershadow.Engine

	// lastSnapshotAt is read/written only on the Run goroutine —
	// no locking needed.
	lastSnapshotAt time.Time

	// savingInFlight guards the BlobStore key against two
	// overlapping Saves. CAS lets a slow Save skip the next tick
	// rather than queue.
	savingInFlight atomic.Bool

	// saveWG waits for any in-flight async save during Close.
	saveWG sync.WaitGroup

	started atomic.Bool
	closed  atomic.Bool
}

// New validates cfg and returns a Pipeline ready for Start.
func New(cfg Config) (*Pipeline, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("triggerpipeline: Brokers required")
	}
	if cfg.Store == nil {
		return nil, errors.New("triggerpipeline: Store required")
	}
	if cfg.PartitionCount <= 0 {
		return nil, errors.New("triggerpipeline: PartitionCount must be > 0")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "trade-dump-trigger-snapshot"
	}
	if cfg.TriggerEventTopic == "" {
		cfg.TriggerEventTopic = "trigger-event"
	}
	if cfg.SnapshotKey == "" {
		cfg.SnapshotKey = DefaultSnapshotKey
	}
	if cfg.SnapshotInterval <= 0 {
		cfg.SnapshotInterval = 60 * time.Second
	}
	if cfg.SnapshotEventCount == 0 {
		cfg.SnapshotEventCount = 60000
	}
	if cfg.SaveTimeout <= 0 {
		cfg.SaveTimeout = 30 * time.Second
	}
	if cfg.TerminalLimit <= 0 {
		cfg.TerminalLimit = 10000
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Pipeline{
		cfg:            cfg,
		logger:         cfg.Logger.With(zap.String("component", "trigger-snapshot-pipeline")),
		engine:         triggershadow.New(cfg.TerminalLimit),
		lastSnapshotAt: time.Now(),
	}, nil
}

// Engine returns the underlying shadow engine. Primarily for tests
// and the on-demand RPC handler (M4).
func (p *Pipeline) Engine() *triggershadow.Engine { return p.engine }

// Start primes the shadow engine from the BlobStore (if a snapshot
// exists) and opens the Kafka consumer with per-partition seek
// offsets derived from that snapshot. Cold start (no snapshot)
// resumes every partition AtStart.
func (p *Pipeline) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("triggerpipeline: Start already called")
	}

	snap, err := p.loadSnapshot(ctx)
	if err != nil {
		return err
	}

	partitionOffsets := make(map[int32]kgo.Offset, p.cfg.PartitionCount)
	if snap == nil {
		p.logger.Info("cold start (no snapshot)",
			zap.String("key", p.cfg.SnapshotKey))
		for part := 0; part < p.cfg.PartitionCount; part++ {
			partitionOffsets[int32(part)] = kgo.NewOffset().AtStart()
		}
	} else {
		if err := p.engine.RestoreFromSnapshot(snap); err != nil {
			return fmt.Errorf("triggerpipeline: restore: %w", err)
		}
		p.logger.Info("restored from snapshot",
			zap.String("key", p.cfg.SnapshotKey),
			zap.Int("pending", len(snap.Pending)),
			zap.Int("terminals", len(snap.Terminals)),
			zap.Int("partitions", len(snap.TriggerEventOffsets)))
		for part := 0; part < p.cfg.PartitionCount; part++ {
			off := p.engine.NextTriggerEventOffset(int32(part))
			if off > 0 {
				partitionOffsets[int32(part)] = kgo.NewOffset().At(off)
			} else {
				partitionOffsets[int32(part)] = kgo.NewOffset().AtStart()
			}
		}
	}
	p.lastSnapshotAt = time.Now()

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(p.cfg.Brokers...),
		kgo.ClientID(p.cfg.ClientID),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.TriggerEventTopic: partitionOffsets,
		}),
	)
	if err != nil {
		return fmt.Errorf("triggerpipeline: kgo.NewClient: %w", err)
	}
	p.cli = cli
	return nil
}

func (p *Pipeline) loadSnapshot(ctx context.Context) (*snapshotpb.TriggerSnapshot, error) {
	snap, err := triggersnap.Load(ctx, p.cfg.Store, p.cfg.SnapshotKey)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("triggerpipeline: load: %w", err)
	}
	return snap, nil
}

// Run drives the apply + snapshot loop until ctx is cancelled or the
// consumer client closes. After Run returns the caller MUST Close to
// drain any in-flight save.
func (p *Pipeline) Run(ctx context.Context) error {
	if !p.started.Load() {
		return errors.New("triggerpipeline: Run before Start")
	}
	if p.closed.Load() {
		return errors.New("triggerpipeline: Run after Close")
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

// Close shuts down the consumer and waits for any in-flight save.
// Idempotent.
func (p *Pipeline) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	if p.cli != nil {
		p.cli.Close()
	}
	p.saveWG.Wait()
}

// handleRecord decodes one trigger-event record, dispatches to the
// shadow apply path, and triggers a snapshot when the window is
// exceeded.
//
// Wire format note (M2): trigger-event currently carries raw
// TriggerUpdate proto with no envelope. M3 may switch to an envelope
// or add a sibling topic for TriggerMarketCheckpointEvent — until
// then this handler treats every record as a TriggerUpdate.
func (p *Pipeline) handleRecord(ctx context.Context, rec *kgo.Record) {
	var u eventpb.TriggerUpdate
	if err := proto.Unmarshal(rec.Value, &u); err != nil {
		p.logger.Error("decode trigger-event record",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err))
		return
	}
	if err := p.engine.ApplyTriggerUpdate(&u, rec.Partition, rec.Offset); err != nil {
		p.logger.Error("apply trigger-event record",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Uint64("trigger_id", u.Id),
			zap.Error(err))
		return
	}
	p.maybeCapture(ctx)
}

// maybeCapture runs on the Run goroutine. If either the time window
// or event-count window has been exceeded, Capture and dispatch an
// async Save (one save in flight at a time).
func (p *Pipeline) maybeCapture(ctx context.Context) {
	now := time.Now()
	byTime := now.Sub(p.lastSnapshotAt) >= p.cfg.SnapshotInterval
	byCount := p.engine.EventsSinceLastSnapshot() >= p.cfg.SnapshotEventCount
	if !byTime && !byCount {
		return
	}
	if !p.savingInFlight.CompareAndSwap(false, true) {
		// Save still in flight from previous tick — skip. Next
		// tick re-evaluates; one dropped Capture is fine because
		// the interval / count fires again.
		return
	}
	snap := p.engine.Capture(now.UnixMilli(), true)
	p.lastSnapshotAt = now
	p.saveWG.Add(1)
	go func() {
		defer p.saveWG.Done()
		defer p.savingInFlight.Store(false)
		p.saveSnapshot(ctx, snap)
	}()
}

func (p *Pipeline) saveSnapshot(parent context.Context, snap *snapshotpb.TriggerSnapshot) {
	ctx, cancel := context.WithTimeout(parent, p.cfg.SaveTimeout)
	defer cancel()
	start := time.Now()
	if err := triggersnap.Save(ctx, p.cfg.Store, p.cfg.SnapshotKey, snap, p.cfg.SnapshotFormat); err != nil {
		p.logger.Error("snapshot save failed",
			zap.String("key", p.cfg.SnapshotKey),
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(err))
		return
	}
	p.logger.Debug("snapshot saved",
		zap.String("key", p.cfg.SnapshotKey),
		zap.Int("pending", len(snap.Pending)),
		zap.Int("terminals", len(snap.Terminals)),
		zap.Duration("elapsed", time.Since(start)))
}
