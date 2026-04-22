// Package shadow is trade-dump's in-memory mirror of one Counter
// vshard (ADR-0061 M2). It consumes counter-journal records in
// offset order and drives engine.ShardState through the same
// ApplyCounterJournalEvent surface Counter uses for recovery; at
// configured triggers (time or event-count) it captures a
// snapshot.ShardSnapshot and hands it to the pipeline's async save
// path.
//
// Concurrency model: single-threaded per vshard. The pipeline owns
// one ShadowEngine per counter-journal partition (== one vshard)
// and drives Apply + Capture serially from a dedicated goroutine
// (ADR-0061 §4.2). This is explicitly simpler than Counter's
// sequencer + snapshotMu dance — no apply can race a capture.
//
// Post-ADR-0061 cutover the snapshots produced here become the
// authoritative source Counter reads on restart. Until then they
// run in parallel with Counter's own snapshots (Phase A: ADR-0061
// §5).
package shadow

import (
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	snapshotpkg "github.com/xargin/opentrade/counter/snapshot"
)

// Engine is the per-vshard shadow state. Exported only for type
// hints; construct via New.
type Engine struct {
	vshardID int
	state    *engine.ShardState

	// counterSeq tracks the highest CounterSeqId seen in any applied
	// event. Sample is exact (no missing gaps): the shadow engine
	// applies journal records sequentially in offset order and
	// counter-journal's per-vshard partition preserves the
	// producer's emission order (ADR-0017). Exposed so Capture can
	// stamp it onto ShardSnapshot.
	counterSeq uint64

	// teWatermark is the highest fully-processed trade-event offset
	// known for this vshard, extracted from TECheckpointEvent
	// payloads (ADR-0060). Monotonic — older checkpoints are
	// ignored. Exposed so Capture can stamp it onto
	// ShardSnapshot.Offsets (the ADR-0048 slice).
	teWatermark int64

	// teWatermarkPartition records which te_partition the
	// checkpoint applies to. In the steady-state 1-vshard-
	// ↔-1-te-partition world (ADR-0058) this equals vshardID; we
	// persist it anyway so the snapshot stays self-describing and
	// future partition re-sharding won't silently corrupt.
	teWatermarkPartition int32

	// nextJournalOffset records where the next journal record
	// should be read from. Mirrors snapshot.ShardSnapshot.JournalOffset
	// (ADR-0060 §4.1) so a restarted ShadowEngine (from its own
	// snapshot) can resume at the correct Kafka offset. Updated
	// after every successful Apply: value == record.Offset + 1.
	nextJournalOffset int64

	// eventsSinceLastSnapshot is a lightweight counter the pipeline
	// reads to decide when to trigger a Capture (ADR-0061 §4.1).
	// Reset to 0 by the pipeline immediately before Capture.
	eventsSinceLastSnapshot uint64
}

// New constructs a fresh ShadowEngine for vshardID. state is always
// freshly allocated; callers that need to resume from a prior
// snapshot call RestoreFromSnapshot after New.
func New(vshardID int) *Engine {
	return &Engine{
		vshardID:             vshardID,
		state:                engine.NewShardState(vshardID),
		teWatermarkPartition: int32(vshardID),
	}
}

// RestoreFromSnapshot seeds the engine from a previously captured
// snapshot so pipeline restarts resume at the last durable point
// without replaying journal from offset 0. MUST be called before
// any Apply — RestoreState requires empty state and the
// pipeline-managed watermark fields are overwritten wholesale.
//
// On return the engine's invariants line up with Engine.Capture's
// contract: NextJournalOffset is snap.JournalOffset, teWatermark is
// the largest offset in snap.Offsets (falls back to zero if
// absent), counterSeq is snap.CounterSeq. The pipeline seeks its
// Kafka consumer to NextJournalOffset to resume.
func (e *Engine) RestoreFromSnapshot(snap *snapshotpkg.ShardSnapshot) error {
	if snap == nil {
		return fmt.Errorf("shadow: RestoreFromSnapshot: nil snap")
	}
	if err := snapshotpkg.RestoreState(e.vshardID, e.state, snap); err != nil {
		return fmt.Errorf("shadow: RestoreState: %w", err)
	}
	e.counterSeq = snap.CounterSeq
	e.nextJournalOffset = snap.JournalOffset
	// Pick the te_watermark from snap.Offsets. Post-ADR-0058 there
	// is exactly one entry (vshard ↔ partition 1:1), but we defend
	// against future re-sharding by taking the max offset across
	// any entries that match the vshard's partition; otherwise the
	// first matching entry wins.
	e.teWatermark = 0
	e.teWatermarkPartition = int32(e.vshardID)
	for _, off := range snap.Offsets {
		if off.Partition == int32(e.vshardID) && off.Offset > e.teWatermark {
			e.teWatermark = off.Offset
			e.teWatermarkPartition = off.Partition
		}
	}
	e.eventsSinceLastSnapshot = 0
	return nil
}

// VShardID returns the vshard this engine mirrors.
func (e *Engine) VShardID() int { return e.vshardID }

// State exposes the underlying ShardState, primarily for tests and
// the snapshot-diff tool (ADR-0061 M5) that compares shadow state
// against Counter-produced snapshots.
func (e *Engine) State() *engine.ShardState { return e.state }

// CounterSeq returns the highest counter_seq_id seen.
func (e *Engine) CounterSeq() uint64 { return e.counterSeq }

// TeWatermark returns the (partition, offset) pair from the most
// recent TECheckpointEvent applied. Zero for a fresh engine with no
// checkpoints yet seen.
func (e *Engine) TeWatermark() (partition int32, offset int64) {
	return e.teWatermarkPartition, e.teWatermark
}

// NextJournalOffset returns the next-to-consume counter-journal
// offset (== last applied offset + 1). Zero for a fresh engine.
func (e *Engine) NextJournalOffset() int64 { return e.nextJournalOffset }

// EventsSinceLastSnapshot is the count of Apply calls since the
// pipeline last cleared the counter. Pipeline reads this + the
// wall-clock ticker to decide when to Capture.
func (e *Engine) EventsSinceLastSnapshot() uint64 {
	return e.eventsSinceLastSnapshot
}

// ClearEventsSinceLastSnapshot is the pipeline's hook to reset the
// counter immediately before triggering a Capture. Called from the
// same goroutine that drives Apply, so no concurrency considered.
func (e *Engine) ClearEventsSinceLastSnapshot() {
	e.eventsSinceLastSnapshot = 0
}

// Apply drives the shadow state forward by one journal record.
// kafkaOffset is the record's offset on counter-journal; used to
// seed nextJournalOffset so a restart can resume correctly.
//
// Errors from the engine apply pass are returned; the pipeline
// logs and continues (matching Counter's catch-up semantics —
// corrupt events don't brick the vshard).
//
// Apply is NOT safe for concurrent use. The pipeline guarantees
// single-threaded drive per vshard.
func (e *Engine) Apply(evt *eventpb.CounterJournalEvent, kafkaOffset int64) error {
	if evt == nil {
		return nil
	}

	// Snapshot cursor first — so a partial apply still leaves the
	// cursor pointing past this record (subsequent restart will
	// resume at kafkaOffset+1, and idempotent apply handles any
	// re-delivered record downstream). This mirrors Counter's
	// journalHighOffset atomic-max behaviour.
	e.nextJournalOffset = kafkaOffset + 1

	if evt.CounterSeqId > e.counterSeq {
		e.counterSeq = evt.CounterSeqId
	}

	// TECheckpointEvent is handled here (not in
	// ApplyCounterJournalEvent) because it advances engine-local
	// watermark bookkeeping, not ShardState. The engine apply
	// function is a no-op for this variant by design.
	if cp := evt.GetTeCheckpoint(); cp != nil {
		if cp.TeOffset > e.teWatermark {
			e.teWatermark = cp.TeOffset
			e.teWatermarkPartition = cp.TePartition
		}
		e.eventsSinceLastSnapshot++
		return nil
	}

	if err := engine.ApplyCounterJournalEvent(e.state, evt); err != nil {
		return fmt.Errorf("shadow apply: %w", err)
	}
	e.eventsSinceLastSnapshot++
	return nil
}

// Capture freezes the current shadow state into a
// snapshot.ShardSnapshot. Callers (the pipeline) invoke from the
// same goroutine that drives Apply, so there is no concurrent
// mutation to worry about — unlike Counter's Capture path which
// has to hold SnapshotMu as a stop-the-world barrier (ADR-0060
// §5). That single-threaded property is the whole point of
// ADR-0061's shadow approach.
//
// The returned snapshot carries:
//
//   - snapshot.ShardSnapshot.CounterSeq      = e.counterSeq
//   - snapshot.ShardSnapshot.Offsets         = [{Topic: "trade-event",
//     Partition: e.teWatermarkPartition, Offset: e.teWatermark}]
//   - snapshot.ShardSnapshot.JournalOffset   = e.nextJournalOffset
//   - snapshot.ShardSnapshot.TimestampMS     = tsMS
//
// …plus the full Accounts / Orders / Reservations projection
// produced by snapshot.CaptureFromState.
//
// Dedup slice stays empty — shadow engine never populates the
// legacy dedup.Table (ADR-0048 backlog #4 migrated Transfer
// idempotency to per-account rings, which are in AccountSnapshot
// already).
func (e *Engine) Capture(tsMS int64) *snapshotpkg.ShardSnapshot {
	var offsets map[int32]int64
	if e.teWatermark > 0 {
		offsets = map[int32]int64{e.teWatermarkPartition: e.teWatermark}
	}
	return snapshotpkg.CaptureFromState(
		e.vshardID,
		e.state,
		e.counterSeq,
		offsets,
		e.nextJournalOffset,
		tsMS,
	)
}
