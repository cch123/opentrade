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
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
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
// startOffset and applies every record up to HWM via applyJournalEvent.
// On return the local state has absorbed every event the last running
// owner committed, including events published after the most recent
// snapshot was captured.
//
// startOffset == 0 is treated specially: no prior journal record has
// ever been written for this vshard (cold start / freshly-initialised
// topic), so there is nothing to replay. Early return.
//
// Errors from applyJournalEvent are logged but do NOT abort — the
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

		// Any broker-side error is logged but not fatal — transient
		// leader failovers recover on the next poll. Fatal-looking
		// errors (topic auth) should surface as no records + deadline.
		fetches.EachError(func(t string, p int32, err error) {
			logger.Warn("catchup fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		n := 0
		fetches.EachRecord(func(rec *kgo.Record) {
			n++
			lastOffset = rec.Offset
			if applyErr := applyJournalRecord(w.state, w.seq, rec, logger); applyErr != nil {
				logger.Warn("catchup apply",
					zap.Int64("offset", rec.Offset), zap.Error(applyErr))
			}
			applied++
		})
		if n == 0 {
			// Idle round — nothing new to replay, we're caught up.
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

	logger.Info("catchup complete",
		zap.Int("applied", applied),
		zap.Int64("last_offset", lastOffset))
	return nil
}

// applyJournalRecord decodes rec into a CounterJournalEvent and
// dispatches to the per-payload apply helpers. Errors are per-record
// and bubble up so the caller can log but not abort.
func applyJournalRecord(
	state *engine.ShardState,
	seq *sequencer.UserSequencer,
	rec *kgo.Record,
	logger *zap.Logger,
) error {
	var evt eventpb.CounterJournalEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if evt.CounterSeqId > 0 {
		seq.AdvanceCounterSeqTo(evt.CounterSeqId)
	}
	return applyJournalEvent(state, &evt, logger)
}

// applyJournalEvent mutates state per the event's payload. Balances
// are written verbatim from the event's `*balance_after` fields
// (engine.Account.PutForRestore), so a sequential replay of every
// event in [startOffset, HWM) converges regardless of ordering
// between snapshot-state and snapshot-journal_offset reads. Order
// status / filled_qty are likewise set verbatim.
//
// Idempotent sinks:
//   - Balance set-by-value: repeat with identical value = no-op
//     semantically; intermediate reverts during replay are caught up
//     by subsequent events.
//   - OrderStatus / FilledQty set-by-value: same reasoning.
//   - RememberTransfer: map-based, insert-if-missing.
//   - RememberTerminated: map-based, insert-if-missing.
//   - Orders.Delete: ErrOrderNotFound-safe.
//   - TECheckpoint / CancelRequested: true no-ops.
//
// Returns error on payloads that cannot be applied (missing required
// field, invalid decimal string, etc.). Caller logs + continues —
// corrupt event shouldn't brick the whole vshard.
func applyJournalEvent(state *engine.ShardState, evt *eventpb.CounterJournalEvent, logger *zap.Logger) error {
	switch p := evt.Payload.(type) {
	case *eventpb.CounterJournalEvent_Freeze:
		return applyFreezeEvent(state, p.Freeze)
	case *eventpb.CounterJournalEvent_Unfreeze:
		return applyUnfreezeEvent(state, p.Unfreeze)
	case *eventpb.CounterJournalEvent_Settlement:
		return applySettlementEvent(state, p.Settlement)
	case *eventpb.CounterJournalEvent_Transfer:
		return applyTransferEvent(state, p.Transfer)
	case *eventpb.CounterJournalEvent_OrderStatus:
		return applyOrderStatusEvent(state, p.OrderStatus)
	case *eventpb.CounterJournalEvent_CancelReq:
		// Informational signal to downstream consumers; Counter state
		// already reflects whatever terminal transition the matching
		// CancelRequested produced. No-op.
		return nil
	case *eventpb.CounterJournalEvent_TeCheckpoint:
		// ADR-0060 watermark broadcast — no state effect.
		return nil
	case *eventpb.CounterJournalEvent_OrderEvicted:
		return applyOrderEvictedEvent(state, p.OrderEvicted)
	case nil:
		return nil
	default:
		if logger != nil {
			logger.Debug("catchup: unknown payload",
				zap.String("type", fmt.Sprintf("%T", p)))
		}
		return nil
	}
}

// applyBalanceSnapshot writes bs to the target (user, asset) verbatim
// via PutForRestore — bypasses the version-bump path so replay
// preserves the on-disk version counter exactly as the event recorded.
func applyBalanceSnapshot(state *engine.ShardState, bs *eventpb.BalanceSnapshot) error {
	if bs == nil || bs.UserId == "" || bs.Asset == "" {
		return nil
	}
	available, err := dec.Parse(bs.Available)
	if err != nil {
		return fmt.Errorf("parse available: %w", err)
	}
	frozen, err := dec.Parse(bs.Frozen)
	if err != nil {
		return fmt.Errorf("parse frozen: %w", err)
	}
	acc := state.Account(bs.UserId)
	acc.PutForRestore(bs.Asset, engine.Balance{
		Available: available,
		Frozen:    frozen,
		Version:   bs.Version,
	})
	return nil
}

// applyFreezeEvent: PlaceOrder was observed. Reconstruct the order in
// byID if absent (the original Insert happened before snapshot but
// Status/FilledQty may have moved forward; if the order is already
// present, don't overwrite — later OrderStatus / Settlement events
// will bring it up to date). Set balance_after.
func applyFreezeEvent(state *engine.ShardState, evt *eventpb.FreezeEvent) error {
	if evt == nil {
		return nil
	}
	if err := applyBalanceSnapshot(state, evt.BalanceAfter); err != nil {
		return fmt.Errorf("freeze balance: %w", err)
	}
	if state.Orders().Get(evt.OrderId) != nil {
		return nil
	}
	price := dec.Zero
	if evt.Price != "" {
		v, err := dec.Parse(evt.Price)
		if err != nil {
			return fmt.Errorf("freeze price: %w", err)
		}
		price = v
	}
	qty := dec.Zero
	if evt.Qty != "" {
		v, err := dec.Parse(evt.Qty)
		if err != nil {
			return fmt.Errorf("freeze qty: %w", err)
		}
		qty = v
	}
	frozenAmount := dec.Zero
	if evt.FreezeAmount != "" {
		v, err := dec.Parse(evt.FreezeAmount)
		if err != nil {
			return fmt.Errorf("freeze amount: %w", err)
		}
		frozenAmount = v
	}
	state.Orders().RestoreInsert(&engine.Order{
		ID:            evt.OrderId,
		ClientOrderID: evt.ClientOrderId,
		UserID:        evt.UserId,
		Symbol:        evt.Symbol,
		Side:          sideFromProto(evt.Side),
		Type:          orderTypeFromProto(evt.OrderType),
		TIF:           tifFromProto(evt.Tif),
		Price:         price,
		Qty:           qty,
		FrozenAsset:   evt.FreezeAsset,
		FrozenAmount:  frozenAmount,
		Status:        engine.OrderStatusPendingNew,
	})
	return nil
}

// applyUnfreezeEvent: reject / cancel / expire path reversed the
// freeze. Just set balance_after; the associated OrderStatusEvent in
// the same partition advances order status.
func applyUnfreezeEvent(state *engine.ShardState, evt *eventpb.UnfreezeEvent) error {
	if evt == nil {
		return nil
	}
	return applyBalanceSnapshot(state, evt.BalanceAfter)
}

// applySettlementEvent: per-party settlement. base_balance_after +
// quote_balance_after + order.FilledQty (via SetFilledQty). Ignored
// if the order is unknown (foreign / already evicted).
func applySettlementEvent(state *engine.ShardState, evt *eventpb.SettlementEvent) error {
	if evt == nil {
		return nil
	}
	if err := applyBalanceSnapshot(state, evt.BaseBalanceAfter); err != nil {
		return fmt.Errorf("settle base balance: %w", err)
	}
	if err := applyBalanceSnapshot(state, evt.QuoteBalanceAfter); err != nil {
		return fmt.Errorf("settle quote balance: %w", err)
	}
	if evt.OrderId == 0 {
		return nil
	}
	// FilledQty monotonically advances via this event's qty field
	// (cumulative filled_qty_after is not carried by SettlementEvent —
	// we only know the trade's qty). Reconstruct FilledQty by adding
	// evt.Qty to the order's current value. If the order is absent
	// (evicted or foreign), skip.
	o := state.Orders().Get(evt.OrderId)
	if o == nil {
		return nil
	}
	if evt.Qty == "" {
		return nil
	}
	qty, err := dec.Parse(evt.Qty)
	if err != nil {
		return fmt.Errorf("settle qty: %w", err)
	}
	// Monotonic guard: if the event would reduce FilledQty (replay of
	// an older event after a newer one already landed), skip.
	newFilled := o.FilledQty.Add(qty)
	if newFilled.Cmp(o.FilledQty) <= 0 {
		return nil
	}
	if _, err := state.Orders().SetFilledQty(evt.OrderId, newFilled, 0); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
		return fmt.Errorf("settle filled_qty: %w", err)
	}
	// Also accumulate FrozenSpent if this is the owner's side — needed
	// for unfreezeResidual to arithmetic correctly if the order later
	// transitions terminal during live traffic after catch-up.
	// SettlementEvent doesn't carry frozen_spent delta directly; derive
	// from delta_base / delta_quote based on order side.
	return accumulateFrozenSpent(state, o, evt)
}

// accumulateFrozenSpent mirrors the settlement-side FrozenSpent
// bookkeeping so a terminal transition after catch-up correctly
// refunds residual. Sell-side orders consume FrozenAmount in base
// units (== Qty); buy-side LIMIT orders consume in quote units
// (== price * qty); buy-side market-by-quote orders consume in quote
// units accumulating from delta_quote.
func accumulateFrozenSpent(state *engine.ShardState, o *engine.Order, evt *eventpb.SettlementEvent) error {
	if o == nil || evt == nil {
		return nil
	}
	if o.FrozenAsset == "" {
		return nil
	}
	qty, _ := dec.Parse(evt.Qty)
	price, _ := dec.Parse(evt.Price)
	var delta dec.Decimal
	switch o.Side {
	case engine.SideAsk:
		delta = qty
	case engine.SideBid:
		if o.IsMarketBuyByQuote() {
			// Quote consumed = abs(delta_quote) since delta_quote is
			// signed negative for buy-side outflow. Catch-up is
			// best-effort; if event's delta_quote is empty, use
			// price * qty fallback.
			d, err := dec.Parse(evt.DeltaQuote)
			if err == nil {
				if d.Sign() < 0 {
					delta = d.Neg()
				} else {
					delta = d
				}
			} else {
				delta = price.Mul(qty)
			}
		} else {
			delta = price.Mul(qty)
		}
	default:
		return nil
	}
	if !dec.IsPositive(delta) {
		return nil
	}
	if _, err := state.Orders().AddFrozenSpent(evt.OrderId, delta); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
		return fmt.Errorf("accumulate frozen_spent: %w", err)
	}
	return nil
}

// applyTransferEvent: writes balance_after and re-seats transfer_id
// in the user's ring so RPC replay dedups.
func applyTransferEvent(state *engine.ShardState, evt *eventpb.TransferEvent) error {
	if evt == nil {
		return nil
	}
	if err := applyBalanceSnapshot(state, evt.BalanceAfter); err != nil {
		return fmt.Errorf("transfer balance: %w", err)
	}
	if evt.UserId != "" && evt.TransferId != "" {
		state.Account(evt.UserId).RememberTransfer(evt.TransferId)
	}
	return nil
}

// applyOrderStatusEvent: sets Status, FilledQty (parsed). No-op if
// the order isn't in byID (foreign / already evicted). New status
// writes through UpdateStatus so derived indices (activeByCOID /
// activeLimits / TerminatedAt) stay consistent.
func applyOrderStatusEvent(state *engine.ShardState, evt *eventpb.OrderStatusEvent) error {
	if evt == nil || evt.OrderId == 0 {
		return nil
	}
	o := state.Orders().Get(evt.OrderId)
	if o == nil {
		return nil
	}
	newStatus := journal.OrderStatusFromProto(evt.NewStatus)
	if newStatus == engine.OrderStatusUnspecified {
		return nil
	}
	// Monotonicity: never walk back from a terminal state. Replay of
	// an older event over a newer state is a no-op.
	if o.Status.IsTerminal() && !newStatus.IsTerminal() {
		return nil
	}
	if evt.FilledQty != "" {
		filled, err := dec.Parse(evt.FilledQty)
		if err == nil && filled.Cmp(o.FilledQty) > 0 {
			if _, err := state.Orders().SetFilledQty(evt.OrderId, filled, 0); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
				return fmt.Errorf("order status filled_qty: %w", err)
			}
		}
	}
	if newStatus != o.Status {
		// UpdatedAt 0 → UpdateStatus uses time.Now() for TerminatedAt
		// fallback (ADR-0062 guard), which is acceptable for catch-up:
		// the event may be minutes old but we're racing to get state
		// consistent, not to backfill exact timestamps. Downstream
		// projections (MySQL) can use the OrderStatusEvent's own
		// event-time fields for accurate audit.
		if _, err := state.Orders().UpdateStatus(evt.OrderId, newStatus, 0); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
			return fmt.Errorf("order status: %w", err)
		}
	}
	return nil
}

// applyOrderEvictedEvent: mirror the evictor's ring→delete dance.
// Remember + Delete are both idempotent so duplicate events are safe.
func applyOrderEvictedEvent(state *engine.ShardState, evt *eventpb.OrderEvictedEvent) error {
	if evt == nil || evt.UserId == "" || evt.OrderId == 0 {
		return nil
	}
	acc := state.Account(evt.UserId)
	acc.RememberTerminated(engine.TerminatedOrderEntry{
		OrderID:       evt.OrderId,
		FinalStatus:   journal.OrderStatusFromProto(evt.FinalStatus),
		TerminatedAt:  evt.TerminatedAt,
		ClientOrderID: evt.ClientOrderId,
		Symbol:        evt.Symbol,
	})
	if err := state.Orders().Delete(evt.OrderId); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
		return fmt.Errorf("evict delete: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Proto enum conversion helpers — internal, module-private.
// -----------------------------------------------------------------------------

func sideFromProto(s eventpb.Side) engine.Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return engine.SideBid
	case eventpb.Side_SIDE_SELL:
		return engine.SideAsk
	}
	return engine.SideBid
}

func orderTypeFromProto(t eventpb.OrderType) engine.OrderType {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return engine.OrderTypeLimit
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return engine.OrderTypeMarket
	}
	return engine.OrderTypeLimit
}

func tifFromProto(t eventpb.TimeInForce) engine.TIF {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC:
		return engine.TIFGTC
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return engine.TIFIOC
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return engine.TIFFOK
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return engine.TIFPostOnly
	}
	return engine.TIFGTC
}
