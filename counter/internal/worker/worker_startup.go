package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/metrics"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/tradedumpclient"
	"github.com/xargin/opentrade/counter/snapshot"
)

// loadOnDemand executes the ADR-0064 §3 Phase 1 on-demand recovery
// path and returns fresh state/seq/dt populated from the captured
// snapshot, plus the extracted offsets + journal cursor.
//
// Return contract:
//
//   - nil error → caller installs the returned state atomically at
//     Phase 2 and SKIPS catch-up.
//   - errors.Is(err, tradedumpclient.ErrFallback) → caller takes
//     the legacy path (Run decides, based on StartupMode).
//   - any other error → fatal, caller returns it from Run.
//
// Every intermediate error before Restore is wrapped with
// ErrFallback so a network blip / trade-dump hiccup degrades to
// legacy gracefully (ADR-0064 §4). Restore failure is also
// wrapped as ErrFallback because we populated a LOCAL fresh
// ShardState that will be discarded — the worker's installed
// state is still pristine.
//
// Phase 1 step-by-step:
//
//  ③ ProduceFenceSentinel — forces franz-go InitProducerID,
//    fencing the prior owner and writing abort markers for their
//    pending txns so trade-dump's shadow consumer sees a stable
//    LEO.
//  ④ TakeSnapshot RPC — gets (key, LEO, counter_seq) from
//    trade-dump.
//  ⑤ Download + decode + Restore into fresh state.
func (w *VShardWorker) loadOnDemand(
	ctx context.Context,
	producer *journal.TxnProducer,
	logger *zap.Logger,
) (*engine.ShardState, *sequencer.UserSequencer, *dedup.Table, map[int32]int64, int64, error) {
	// Single budget for the entire on-demand attempt (sentinel
	// produce + RPC + snapshot download). Without this, a slow
	// sentinel or blob-store read would block startup indefinitely
	// because the outer worker ctx has no deadline — manager.go
	// just passes the root ctx. We also rely on this ctx to
	// bound TxnProducer.ProduceFenceSentinel's runTxnWithRetry:
	// that helper returns ctx.Err() on cancel BEFORE checking the
	// retry budget, so deadline expiry here converts the retry
	// path into a clean fallback error instead of the budget-
	// exhaustion panic it would trip on the RPC path.
	//
	// Default 3s if unset — matches the Counter-side budget in the
	// ADR-0064 §3 Phase 1 delay table and leaves ~1s each for
	// fence, RPC, and download.
	timeout := w.cfg.OnDemandTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	onDemandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Step ③: sentinel produce. Wrapped in onDemandCtx so a stuck
	// Kafka cluster times out cleanly instead of panicking past
	// the retry budget (codex review: the usual retry-then-panic
	// semantics are a steady-state RPC concern, not the startup
	// fallback path). Any error becomes ErrFallback — legacy path
	// doesn't need the fence (its catchUpJournal reads whatever
	// is in the partition up to HWM).
	sentinelStart := time.Now()
	sentinelErr := producer.ProduceFenceSentinel(onDemandCtx, int32(w.cfg.VShardID))
	w.cfg.Metrics.RecordSentinelProduceDuration(
		int32(w.cfg.VShardID),
		sentinelErr == nil,
		time.Since(sentinelStart).Seconds(),
	)
	if sentinelErr != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("%w: sentinel produce: %v",
			tradedumpclient.ErrFallback, sentinelErr)
	}

	// Step ④: RPC under the shared budget. The client already maps
	// every ADR-0064 §4 fallback-class error to ErrFallback, so we
	// can pass err through verbatim.
	rpcStart := time.Now()
	resp, err := w.cfg.OnDemandClient.TakeSnapshot(
		onDemandCtx,
		uint32(w.cfg.VShardID),
		w.cfg.NodeID,
		w.cfg.Epoch,
	)
	w.cfg.Metrics.RecordOnDemandRPCDuration(
		int32(w.cfg.VShardID),
		metrics.OnDemandRPCResultLabel(err),
		time.Since(rpcStart).Seconds(),
	)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	// Step ⑤: download. snapshotpkg.LoadPath expects the full
	// object path including extension. Our server writes .pb so
	// LoadPath(key+".pb") matches the trade-dump upload convention
	// (snapshotpkg.Save appends the extension). Uses onDemandCtx
	// so a slow blob backend can't outrun the budget.
	snap, err := snapshot.LoadPath(onDemandCtx, w.cfg.Store, resp.SnapshotKey+".pb")
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("%w: download %s: %v",
			tradedumpclient.ErrFallback, resp.SnapshotKey, err)
	}

	// Step ⑤ (continued): Restore into fresh state. Fresh is
	// LOCAL to this call — on failure we discard and let the
	// fallback path build its own state from scratch, so
	// Restore failure remains recoverable at the worker level.
	freshState := engine.NewShardState(int(w.cfg.VShardID))
	freshSeq := sequencer.New()
	freshDt := dedup.New(w.cfg.DedupTTL)
	if err := snapshot.Restore(int(w.cfg.VShardID), freshState, freshSeq, freshDt, snap); err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("%w: restore: %v",
			tradedumpclient.ErrFallback, err)
	}

	offsets := snapshot.OffsetsSliceToMap(snap.Offsets)
	journalOffset := snap.JournalOffset
	logger.Info("on-demand snapshot restored",
		zap.String("snapshot_key", resp.SnapshotKey),
		zap.Int64("leo", resp.LEO),
		zap.Uint64("counter_seq", resp.CounterSeq),
		zap.Int("accounts", len(snap.Accounts)),
		zap.Int("orders", len(snap.Orders)),
		zap.Int64("journal_offset", journalOffset),
		zap.Int("partitions", len(offsets)))
	return freshState, freshSeq, freshDt, offsets, journalOffset, nil
}

// loadLegacy is the ADR-0060 §4.2 classic recovery path: Load the
// latest periodic snapshot (may be absent — treated as cold start),
// Restore, and return the extracted offsets + journal cursor. The
// caller runs catchUpJournal afterwards so the shard reflects every
// journal event produced since snapshot capture.
//
// Any snapshot-load/restore failure is treated as fatal here — if
// we got this far, on-demand has already fallen through and legacy
// is our only option. Returning ErrFallback again would loop.
func (w *VShardWorker) loadLegacy(
	ctx context.Context,
	logger *zap.Logger,
) (*engine.ShardState, *sequencer.UserSequencer, *dedup.Table, map[int32]int64, int64, error) {
	state := engine.NewShardState(int(w.cfg.VShardID))
	seq := sequencer.New()
	dt := dedup.New(w.cfg.DedupTTL)

	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	snap, err := snapshot.Load(ctx, w.cfg.Store, key)
	switch {
	case err == nil:
		if err := snapshot.Restore(int(w.cfg.VShardID), state, seq, dt, snap); err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("snapshot restore: %w", err)
		}
		offsets := snapshot.OffsetsSliceToMap(snap.Offsets)
		journalOffset := snap.JournalOffset
		logger.Info("legacy path: restored from periodic snapshot",
			zap.Int("version", snap.Version),
			zap.Uint64("counter_seq", snap.CounterSeq),
			zap.Int("accounts", len(snap.Accounts)),
			zap.Int("orders", len(snap.Orders)),
			zap.Int("partitions", len(offsets)),
			zap.Int64("journal_offset", journalOffset))
		return state, seq, dt, offsets, journalOffset, nil
	case errors.Is(err, os.ErrNotExist):
		logger.Info("legacy path: no snapshot found, starting fresh")
		return state, seq, dt, nil, 0, nil
	default:
		return nil, nil, nil, nil, 0, fmt.Errorf("snapshot load: %w", err)
	}
}
