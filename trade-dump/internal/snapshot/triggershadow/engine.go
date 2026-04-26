// Package triggershadow is trade-dump's in-memory mirror of the
// trigger service (ADR-0067 M1). It applies trigger-event records into
// a small state set (pending + terminal trigger records, market-data
// offset checkpoint) and produces snapshots that trigger startup
// restores from — mirror of counter shadow under ADR-0061.
//
// Differences from counter shadow:
//
//   - One engine total, not per-vshard. Trigger doesn't shard.
//   - trigger-event has no Kafka EOS; LEO is naturally stable, so no
//     ADR-0064 sentinel produce / fence dance is needed.
//   - Apply is byte-trivial: upsert pending-or-terminal record by
//     trigger_id. No business logic (matches ADR-0066 §准入).
//
// Ordering / replay: trigger-event is consumed in Kafka assign mode by
// a single pipeline goroutine, so each offset is delivered exactly
// once per session. After restart the pipeline restores from snapshot
// and seeks the consumer to nextTriggerEventOffset, so pre-snapshot
// offsets are never re-delivered. Apply therefore does not need a
// per-trigger LWW guard — same simplification counter shadow uses.
//
// Concurrency: single-threaded driver per ADR-0061 §4.2 model. The
// pipeline goroutine (added in M2) owns Apply + Capture serially. The
// mutex below exists so on-demand Capture (TakeTriggerSnapshot RPC,
// added in M4) can run from the gRPC handler without racing.
package triggershadow

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
)

// Engine is the trigger-shadow state. Construct via New.
type Engine struct {
	mu sync.Mutex

	// terminalLimit caps the FIFO terminal ring (ADR-0040
	// terminal_history_limit equivalent on the shadow side). Zero
	// disables the limit (keep everything).
	terminalLimit int

	// pending is the active trigger set, keyed by trigger_id.
	pending map[uint64]*snapshotpb.TriggerRecord

	// terminals is a FIFO of triggered/canceled/rejected/expired
	// records, capped at terminalLimit.
	terminals []*snapshotpb.TriggerRecord

	// marketOffsets is partition → next-to-consume on the market-data
	// topic, advanced by TriggerMarketCheckpointEvent. Stamped onto
	// the snapshot so a restored trigger primary resumes its consumer
	// at the right position (ADR-0048).
	marketOffsets map[int32]int64

	// nextTriggerEventOffsets is the per-partition apply cursor on the
	// trigger-event topic. Mirror of counter shadow's NextJournalOffset
	// but partition-aware because trigger-event is partitioned by
	// user_id. After a successful Apply at (partition, offset), the
	// map entry equals offset+1.
	nextTriggerEventOffsets map[int32]int64

	// eventsSinceLastSnapshot is the count-based snapshot trigger
	// pipeline (M2) reads. Reset to 0 by the pipeline immediately
	// before Capture.
	eventsSinceLastSnapshot uint64
}

// New constructs a fresh Engine with the given terminal-ring cap.
// Pass terminalLimit <= 0 to disable the cap (keep all terminals).
func New(terminalLimit int) *Engine {
	if terminalLimit < 0 {
		terminalLimit = 0
	}
	return &Engine{
		terminalLimit:           terminalLimit,
		pending:                 make(map[uint64]*snapshotpb.TriggerRecord),
		marketOffsets:           make(map[int32]int64),
		nextTriggerEventOffsets: make(map[int32]int64),
	}
}

// NextTriggerEventOffset returns the next-to-consume offset for the
// given partition on the trigger-event topic. Returns 0 for partitions
// the shadow has never seen (cold start; consumer should AtStart).
func (e *Engine) NextTriggerEventOffset(partition int32) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nextTriggerEventOffsets[partition]
}

// NextTriggerEventOffsets returns a copy of the per-partition cursor
// map. Caller-safe — mutations to the result don't affect engine state.
func (e *Engine) NextTriggerEventOffsets() map[int32]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[int32]int64, len(e.nextTriggerEventOffsets))
	for p, o := range e.nextTriggerEventOffsets {
		out[p] = o
	}
	return out
}

// EventsSinceLastSnapshot returns the count of Apply calls since the
// last Capture (or since New). Used by the pipeline tick decision.
func (e *Engine) EventsSinceLastSnapshot() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.eventsSinceLastSnapshot
}

// ApplyTriggerUpdate ingests one TriggerUpdate from (partition,
// kafkaOffset) on the trigger-event topic. Terminal status (TRIGGERED
// / CANCELED / REJECTED / EXPIRED) moves the record from pending into
// the terminal ring, trimming oldest entries when the cap is exceeded;
// non-terminal status overwrites the pending entry.
func (e *Engine) ApplyTriggerUpdate(u *eventpb.TriggerUpdate, partition int32, kafkaOffset int64) error {
	if u == nil {
		return fmt.Errorf("triggershadow: nil TriggerUpdate")
	}
	if u.Id == 0 {
		return fmt.Errorf("triggershadow: TriggerUpdate.id is zero")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	rec := triggerUpdateToRecord(u)
	if isTerminal(u.Status) {
		// Drop from pending if present, push to terminal ring.
		delete(e.pending, u.Id)
		e.terminals = append(e.terminals, rec)
		if e.terminalLimit > 0 && len(e.terminals) > e.terminalLimit {
			drop := len(e.terminals) - e.terminalLimit
			e.terminals = e.terminals[drop:]
		}
	} else {
		// Active state — overwrite pending entry.
		e.pending[u.Id] = rec
	}
	e.advanceCursorLocked(partition, kafkaOffset)
	return nil
}

// ApplyMarketCheckpoint advances the shadow's market-data offset map
// from one TriggerMarketCheckpointEvent. Trigger state is untouched
// (the checkpoint is ADR-0067 metadata).
//
// Per-partition LWW on offsets: an entry is overwritten only when the
// incoming offset is strictly higher, so out-of-order checkpoints
// don't roll the cursor backwards.
func (e *Engine) ApplyMarketCheckpoint(c *eventpb.TriggerMarketCheckpointEvent, partition int32, kafkaOffset int64) error {
	if c == nil {
		return fmt.Errorf("triggershadow: nil TriggerMarketCheckpointEvent")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for part, off := range c.MarketOffsets {
		if cur, ok := e.marketOffsets[part]; !ok || off > cur {
			e.marketOffsets[part] = off
		}
	}
	e.advanceCursorLocked(partition, kafkaOffset)
	return nil
}

// AdvanceCursor moves the per-partition apply cursor past kafkaOffset
// without touching shadow state. Called by the pipeline for unknown
// envelope payload variants (forward-compat with newer producer
// versions): no Apply happens, but the consumer must move past the
// record or it'll replay it forever after a snapshot+restart.
func (e *Engine) AdvanceCursor(partition int32, kafkaOffset int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.advanceCursorLocked(partition, kafkaOffset)
	return nil
}

func (e *Engine) advanceCursorLocked(partition int32, kafkaOffset int64) {
	next := kafkaOffset + 1
	if next > e.nextTriggerEventOffsets[partition] {
		e.nextTriggerEventOffsets[partition] = next
	}
	e.eventsSinceLastSnapshot++
}

// Capture serializes the current state into a TriggerSnapshot. The
// returned snapshot is a fresh proto — caller can hand it off to a
// background save without holding the engine mutex.
//
// ResetSnapshotCounter controls whether eventsSinceLastSnapshot is
// reset to zero. The pipeline calls Capture(true) immediately before
// dispatching the save; tests may pass false to inspect counter state.
func (e *Engine) Capture(takenAtMs int64, resetCounter bool) *snapshotpb.TriggerSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := &snapshotpb.TriggerSnapshot{
		Version:   1,
		TakenAtMs: takenAtMs,
	}
	if len(e.nextTriggerEventOffsets) > 0 {
		snap.TriggerEventOffsets = make(map[int32]int64, len(e.nextTriggerEventOffsets))
		for p, o := range e.nextTriggerEventOffsets {
			snap.TriggerEventOffsets[p] = o
		}
	}
	if len(e.pending) > 0 {
		snap.Pending = make([]*snapshotpb.TriggerRecord, 0, len(e.pending))
		for _, r := range e.pending {
			snap.Pending = append(snap.Pending, cloneRecord(r))
		}
	}
	if len(e.terminals) > 0 {
		snap.Terminals = make([]*snapshotpb.TriggerRecord, 0, len(e.terminals))
		for _, r := range e.terminals {
			snap.Terminals = append(snap.Terminals, cloneRecord(r))
		}
	}
	if len(e.marketOffsets) > 0 {
		snap.Offsets = make(map[int32]int64, len(e.marketOffsets))
		for p, o := range e.marketOffsets {
			snap.Offsets[p] = o
		}
	}
	// OcoByClient is intentionally empty — shadow can't reconstruct
	// the client_oco_id → group_id map from trigger-event alone (the
	// mapping is set on PlaceOCO and never appears in TriggerUpdate
	// payloads). M2/M3 will revisit; until then trigger restart with
	// outstanding client OCO ids in flight loses the dedup history.
	if resetCounter {
		e.eventsSinceLastSnapshot = 0
	}
	return snap
}

// RestoreFromSnapshot seeds the engine from a previously captured
// snapshot. MUST be called before any Apply — the engine state is
// overwritten wholesale. After return the per-partition cursor map
// matches snap.TriggerEventOffsets; the pipeline seeks each partition
// in the Kafka consumer to its respective offset to resume.
func (e *Engine) RestoreFromSnapshot(snap *snapshotpb.TriggerSnapshot) error {
	if snap == nil {
		return fmt.Errorf("triggershadow: RestoreFromSnapshot: nil snap")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pending = make(map[uint64]*snapshotpb.TriggerRecord, len(snap.Pending))
	e.terminals = e.terminals[:0]
	e.marketOffsets = make(map[int32]int64, len(snap.Offsets))
	e.nextTriggerEventOffsets = make(map[int32]int64, len(snap.TriggerEventOffsets))

	for _, r := range snap.Pending {
		e.pending[r.Id] = cloneRecord(r)
	}
	for _, r := range snap.Terminals {
		e.terminals = append(e.terminals, cloneRecord(r))
	}
	for p, o := range snap.Offsets {
		e.marketOffsets[p] = o
	}
	for p, o := range snap.TriggerEventOffsets {
		e.nextTriggerEventOffsets[p] = o
	}
	e.eventsSinceLastSnapshot = 0
	return nil
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func isTerminal(s eventpb.TriggerEventStatus) bool {
	switch s {
	case eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED,
		eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_CANCELED,
		eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_REJECTED,
		eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_EXPIRED:
		return true
	}
	return false
}

func triggerUpdateToRecord(u *eventpb.TriggerUpdate) *snapshotpb.TriggerRecord {
	return &snapshotpb.TriggerRecord{
		Id:                u.Id,
		ClientTriggerId:   u.ClientTriggerId,
		UserId:            u.UserId,
		Symbol:            u.Symbol,
		Side:              uint32(u.Side),
		Type:              uint32(u.Type),
		StopPrice:         u.StopPrice,
		LimitPrice:        u.LimitPrice,
		Qty:               u.Qty,
		QuoteQty:          u.QuoteQty,
		Tif:               uint32(u.Tif),
		Status:            uint32(u.Status),
		CreatedAtMs:       u.CreatedAtUnixMs,
		TriggeredAtMs:     u.TriggeredAtUnixMs,
		PlacedOrderId:     u.PlacedOrderId,
		RejectReason:      u.RejectReason,
		ExpiresAtMs:       u.ExpiresAtUnixMs,
		OcoGroupId:        u.OcoGroupId,
		TrailingDeltaBps:  u.TrailingDeltaBps,
		ActivationPrice:   u.ActivationPrice,
		TrailingWatermark: u.TrailingWatermark,
		TrailingActive:    u.TrailingActive,
	}
}

func cloneRecord(r *snapshotpb.TriggerRecord) *snapshotpb.TriggerRecord {
	if r == nil {
		return nil
	}
	return proto.Clone(r).(*snapshotpb.TriggerRecord)
}
