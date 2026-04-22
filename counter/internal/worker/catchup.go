// ADR-0060 §4: catch-up journal replay after snapshot restore.
//
// The advancer advances te_watermark + journal_offset independently and
// in different goroutines; either can race past the other under crash.
// Catch-up bridges the gap: seek the vshard's counter-journal partition
// to snapshot.JournalOffset and re-apply every event up to HWM
// idempotently, so the state + guards the trade-event consumer will
// then rely on are already converged.
//
// Consumer termination: idle-timeout. We don't query HWM upfront — it
// races anyway against ongoing producers in other vshards (not ours,
// which is quiesced until we open Run's Service) — and instead treat
// "no records for IdleTimeout consecutive polls" as "caught up". The
// recovery path is not latency-sensitive and the extra ~300ms beats
// bringing in a kadm dep for a slight guarantee improvement.
//
// Apply logic lives in `counter/engine.ApplyCounterJournalEvent`
// (ADR-0061 M1); this file only owns the consumer + counterSeq
// advance glue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
)

const (
	// defaultCatchUpIdleTimeout is how long we wait in an empty poll
	// before declaring catch-up complete. 300ms is plenty for a
	// healthy Kafka — partition EOF + empty fetch returns in single
	// digit ms; the buffer absorbs broker scheduling hiccups.
	defaultCatchUpIdleTimeout = 300 * time.Millisecond

	// defaultCatchUpMaxDuration caps the total wall time catch-up
	// will burn. Sized for ~1M journal events at ~3Mb/s fetch rate
	// (rough napkin: 64MB batch / ~200 byte event ≈ 320k events per
	// batch, a few batches a second). For pathological cases an
	// operator can hand-roll a larger value.
	defaultCatchUpMaxDuration = 2 * time.Minute
)

// catchUpJournal seeks the vshard's counter-journal partition to
// startOffset and applies every record up to HWM via
// engine.ApplyCounterJournalEvent. On return the local state has
// absorbed every event the last running owner committed, including
// events published after the most recent snapshot was captured.
//
// startOffset == 0 is treated specially: no prior journal record has
// ever been written for this vshard (cold start / freshly-initialised
// topic), so there is nothing to replay. Early return.
//
// Errors from the apply pass are logged but do NOT abort — the
// catch-up loop continues on the next record so a corrupt event
// cannot brick recovery. This matches how fn errors are handled in
// the live path (logged + state left as-is).
func (w *VShardWorker) catchUpJournal(ctx context.Context, startOffset int64) error {
	if startOffset <= 0 {
		return nil
	}
	logger := w.cfg.Logger.With(
		zap.Int("vshard", int(w.cfg.VShardID)),
		zap.Int64("start_offset", startOffset))

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(w.cfg.Brokers...),
		kgo.ClientID(fmt.Sprintf("%s-vshard-%03d-catchup", w.cfg.NodeID, w.cfg.VShardID)),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			w.cfg.JournalTopic: {
				int32(w.cfg.VShardID): kgo.NewOffset().At(startOffset),
			},
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return fmt.Errorf("catchup: kgo.NewClient: %w", err)
	}
	defer cli.Close()

	deadline := time.Now().Add(defaultCatchUpMaxDuration)
	applied := 0
	lastOffset := int64(-1)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			logger.Warn("catchup exceeded max duration",
				zap.Duration("budget", defaultCatchUpMaxDuration),
				zap.Int("applied", applied),
				zap.Int64("last_offset", lastOffset))
			return fmt.Errorf("catchup: budget exhausted after %d events", applied)
		}

		pollCtx, cancel := context.WithTimeout(ctx, defaultCatchUpIdleTimeout)
		fetches := cli.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("catchup: client closed unexpectedly")
		}

		fetches.EachError(func(t string, p int32, err error) {
			logger.Warn("catchup fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		n := 0
		fetches.EachRecord(func(rec *kgo.Record) {
			n++
			lastOffset = rec.Offset
			if applyErr := applyJournalRecord(w.state, w.seq, rec); applyErr != nil {
				logger.Warn("catchup apply",
					zap.Int64("offset", rec.Offset), zap.Error(applyErr))
			}
			applied++
		})
		if n == 0 {
			break
		}
	}

	// Prime TxnProducer.journalHighOffset so the next writeSnapshot
	// records a journal_offset >= what we just observed. Without this,
	// a snapshot taken before any new publish would regress
	// journal_offset back to startOffset and the next recovery would
	// replay events we already applied (harmless via idempotency, but
	// wasted wall time).
	if lastOffset >= 0 {
		w.producer.NoteObservedJournalOffset(lastOffset)
	}

	w.cfg.Metrics.RecordCatchUpApplied(int32(w.cfg.VShardID), applied)

	logger.Info("catchup complete",
		zap.Int("applied", applied),
		zap.Int64("last_offset", lastOffset))
	return nil
}

// applyJournalRecord decodes rec into a CounterJournalEvent, advances
// the sequencer's counterSeq to cover the event, and dispatches the
// payload-specific state mutation to engine.ApplyCounterJournalEvent.
//
// Errors are per-record — returned to the caller which typically
// logs + continues.
func applyJournalRecord(
	state *engine.ShardState,
	seq *sequencer.UserSequencer,
	rec *kgo.Record,
) error {
	var evt eventpb.CounterJournalEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if evt.CounterSeqId > 0 {
		seq.AdvanceCounterSeqTo(evt.CounterSeqId)
	}
	return engine.ApplyCounterJournalEvent(state, &evt)
}
