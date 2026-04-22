package worker

import (
	"context"
	"sync"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// asyncTradeService is the narrow Service subset the async consumer
// path calls. Defined as an interface so tests can substitute a
// stub instead of spinning up a full Service + Kafka producer.
type asyncTradeService interface {
	HandleTradeRecordAsync(
		ctx context.Context,
		evt *eventpb.TradeEvent,
		onCount func(count int32),
		cb func(err error),
	)
}

// asyncTradeHandler is the ADR-0060 replacement for the synchronous
// TradeHandler. It dispatches trade-event records into the user
// sequencer via Service.HandleTradeRecordAsync, registers each event
// in pendingList before any fn runs, and signals the advancer
// whenever a watermark-eligible transition happens (fn count hits 0
// or new entry with count==0).
//
// The whole path is non-blocking from the consumer loop's perspective:
// HandleTradeRecord returns immediately after SubmitAsync calls have
// been queued. The advancer goroutine (runAdvancer) consumes the
// signal channel, pops consecutive-done TEs from pendingList, and
// both publishes TECheckpointEvent AND advances the local offset map
// (M3 will wire the TECheckpointEvent publish; in M2 we only advance
// the offset map so existing snapshot offset semantics are preserved).
type asyncTradeHandler struct {
	svc      asyncTradeService
	pending  *pendingList
	advance  chan<- struct{}
	logger   *zap.Logger
}

func newAsyncTradeHandler(svc asyncTradeService, pending *pendingList, advance chan<- struct{}, logger *zap.Logger) *asyncTradeHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &asyncTradeHandler{
		svc:     svc,
		pending: pending,
		advance: advance,
		logger:  logger,
	}
}

// HandleTradeRecord implements the journal.TradeHandler interface from
// the trade-event consumer. Returns nil unconditionally — fn-level
// failures surface via the async cb (logged; the Publish-5s-panic
// upstream tears down the process on systemic failures). Returning
// nil here tells the consumer to continue polling; the advancer is
// responsible for offset management.
//
// Race-safety contract with Service.HandleTradeRecordAsync:
//   - The onCount callback fires synchronously BEFORE any SubmitAsync.
//   - Enqueue happens inside onCount, so the inFlightTE pointer is
//     visible to cb before any drain goroutine can invoke cb.
//   - If onCount reports 0, no cb is invoked and we signal the
//     advancer directly (the 0-count entry is immediately eligible).
func (h *asyncTradeHandler) HandleTradeRecord(ctx context.Context, evt *eventpb.TradeEvent, partition int32, offset int64) error {
	// infl is mutated inside onCount and read inside cb; the onCount
	// callback is guaranteed to return before any cb fires (see
	// Service.HandleTradeEventAsync contract), so the assignment
	// happens-before every cb read. No mu needed.
	var infl *inFlightTE
	var submitted sync.Once
	onCount := func(count int32) {
		infl = h.pending.Enqueue(partition, offset, count)
		if count == 0 {
			submitted.Do(func() {
				select {
				case h.advance <- struct{}{}:
				default:
				}
			})
		}
	}
	cb := func(err error) {
		if err != nil {
			// Publish-level retries are handled inside TxnProducer;
			// anything that reaches here is a non-retryable fn error
			// (state inconsistency, invalid payload). Log and let
			// pendingList advance — the match_seq / ring guards keep
			// the system idempotent across restart.
			h.logger.Error("async trade fn",
				zap.Int32("partition", partition),
				zap.Int64("offset", offset),
				zap.Error(err))
		}
		if h.pending.MarkFnDone(infl) {
			select {
			case h.advance <- struct{}{}:
			default:
			}
		}
	}
	h.svc.HandleTradeRecordAsync(ctx, evt, onCount, cb)
	return nil
}
