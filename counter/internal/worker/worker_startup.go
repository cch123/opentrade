package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/metrics"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/counter/internal/tradedumpclient"
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
//	③ ProduceFenceSentinel — forces franz-go InitProducerID,
//	  fencing the prior owner and writing abort markers for their
//	  pending txns so trade-dump's shadow consumer sees a stable
//	  LEO.
//	④ TakeSnapshot RPC — gets (key, LEO, counter_seq) from
//	  trade-dump.
//	⑤ Download + decode + Restore into fresh state via
//	  counter/internal/snapshot.Load.
func (w *VShardWorker) loadOnDemand(
	ctx context.Context,
	producer *journal.TxnProducer,
	logger *zap.Logger,
) (*counterstate.ShardState, *sequencer.UserSequencer, *dedup.Table, map[int32]int64, int64, error) {
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

	// Step ⑤: download + decode + Restore via the consumer-side
	// adapter. trade-dump writes the on-demand snapshot under
	// resp.SnapshotKey with format-extension appended; pass the full
	// path so probe is skipped. Failure becomes ErrFallback because
	// the worker's installed state is still pristine.
	r, err := snapshot.Load(onDemandCtx, w.cfg.Store, resp.SnapshotKey+".pb", int(w.cfg.VShardID))
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("%w: download %s: %v",
			tradedumpclient.ErrFallback, resp.SnapshotKey, err)
	}
	freshDt := dedup.New(w.cfg.DedupTTL)

	logger.Info("on-demand snapshot restored",
		zap.String("snapshot_key", resp.SnapshotKey),
		zap.Int64("leo", resp.LEO),
		zap.Uint64("counter_seq", resp.CounterSeq),
		zap.Int("accounts", r.AccountCount),
		zap.Int("orders", r.OrderCount),
		zap.Int64("journal_offset", r.JournalOffset),
		zap.Int("partitions", len(r.Offsets)))
	return r.State, r.Seq, freshDt, r.Offsets, r.JournalOffset, nil
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
) (*counterstate.ShardState, *sequencer.UserSequencer, *dedup.Table, map[int32]int64, int64, error) {
	dt := dedup.New(w.cfg.DedupTTL)
	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	r, err := snapshot.Load(ctx, w.cfg.Store, key, int(w.cfg.VShardID))
	switch {
	case err == nil:
		logger.Info("legacy path: restored from periodic snapshot",
			zap.Int("version", r.Version),
			zap.Uint64("counter_seq", r.CounterSeq),
			zap.Int("accounts", r.AccountCount),
			zap.Int("orders", r.OrderCount),
			zap.Int("partitions", len(r.Offsets)),
			zap.Int64("journal_offset", r.JournalOffset))
		return r.State, r.Seq, dt, r.Offsets, r.JournalOffset, nil
	case errors.Is(err, os.ErrNotExist):
		logger.Info("legacy path: no snapshot found, starting fresh")
		return counterstate.NewShardState(int(w.cfg.VShardID)), sequencer.New(), dt, nil, 0, nil
	default:
		return nil, nil, nil, nil, 0, fmt.Errorf("snapshot load: %w", err)
	}
}
