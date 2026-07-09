package worker

import (
	"context"
	"fmt"
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
// tradeevent.Handler. It dispatches trade-event records into the user
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
	svc     asyncTradeService
	pending *pendingList
	advance chan<- struct{}
	logger  *zap.Logger
	// fatal is invoked for any per-record fn error. Production panics so the
	// process cannot checkpoint past a partially applied financial event;
	// tests replace it to assert the fail-stop boundary without crashing the
	// test process.
	fatal func(error)
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
		fatal: func(err error) {
			panic(fmt.Errorf("counter: async trade processing failed: %w", err))
		},
	}
}

// HandleTradeRecord implements the tradeevent.Handler interface from the
// trade-event consumer. Submission itself is non-blocking; fn-level failures
// arrive through cb and fail-stop the process. Returning nil here only means
// dispatch succeeded, not that the record is checkpointable.
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
			// Never decrement pending on failure. Doing so would let the
			// advancer publish a checkpoint for state that was not applied.
			// The default fatal hook panics; if a test hook returns, keeping
			// this entry pending still preserves the production invariant.
			h.logger.Error("async trade fn",
				zap.Int32("partition", partition),
				zap.Int64("offset", offset),
				zap.Error(err))
			h.fatal(err)
			return
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
