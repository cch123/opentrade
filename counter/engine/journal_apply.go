// Apply logic for replaying CounterJournalEvent records back onto
// ShardState (ADR-0060 M6 recovery + ADR-0061 shadow engine).
//
// This file is the pure-function "apply" surface the counter service,
// counter recovery, and trade-dump shadow engine share. Every apply
// function is:
//
//   - Stateless (no goroutines, no Kafka, no logger writes that
//     affect behaviour).
//   - Idempotent when called in sequential journal order — balances
//     are set verbatim from *balance_after snapshots; order status /
//     filled_qty are set-by-value with monotonic guards; rings are
//     insert-if-missing; Delete is ErrOrderNotFound-safe.
//   - Caller-locked: callers own concurrency (Counter holds sequencer
//     barrier; shadow engine is single-threaded).
//
// ADR-0061 M1: consolidating the apply logic here (previously
// lived in worker/catchup.go) makes trade-dump's shadow engine a
// thin wrapper — it needs only this package + api/gen/event.
package engine

import (
	"errors"
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// ApplyCounterJournalEvent dispatches evt by payload variant and
// applies the corresponding state mutation. Returns an error only
// when the event's payload is structurally invalid (bad decimal
// string, etc.); malformed events should be logged but not abort the
// caller's replay loop.
//
// Callers MUST invoke in strict journal-order. Out-of-order replay
// can violate the set-by-value monotonicity guard in
// applyOrderStatusEvent and the cumulative FrozenSpent arithmetic
// in applySettlementEvent. Catch-up (worker.catchUpJournal) and
// shadow engine (trade-dump) both guarantee this by consuming the
// counter-journal partition in its native Kafka offset order.
func ApplyCounterJournalEvent(state *ShardState, evt *eventpb.CounterJournalEvent) error {
	if evt == nil {
		return nil
	}
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
		// Informational — Counter state already reflects whatever
		// terminal transition the matching user flow produced.
		return nil
	case *eventpb.CounterJournalEvent_TeCheckpoint:
		// ADR-0060 watermark broadcast — no state effect on the
		// ShardState itself. Callers that track te_watermark (shadow
		// engine) read the payload separately; the apply here is a
		// deliberate no-op.
		return nil
	case *eventpb.CounterJournalEvent_OrderEvicted:
		return applyOrderEvictedEvent(state, p.OrderEvicted)
	case nil:
		return nil
	default:
		// Forward-compat: unknown oneof variant is a no-op. Shadow
		// engine / catch-up may log it at Debug level; this package
		// stays silent so it can be linked into binaries without a
		// logger dependency.
		_ = p
		return nil
	}
}

// applyBalanceSnapshot writes bs to (user, asset) verbatim via
// PutForRestore. Bypasses the version-bump path so a replayed event
// preserves the event's exact Version value (critical for
// per-(user, asset) Balance.Version consumers that key off it).
func applyBalanceSnapshot(state *ShardState, bs *eventpb.BalanceSnapshot) error {
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
	acc.PutForRestore(bs.Asset, Balance{
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
func applyFreezeEvent(state *ShardState, evt *eventpb.FreezeEvent) error {
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
	state.Orders().RestoreInsert(&Order{
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
		Status:        OrderStatusPendingNew,
	})
	return nil
}

// applyUnfreezeEvent: reject / cancel / expire path reversed the
// freeze. Just set balance_after; the associated OrderStatusEvent in
// the same partition advances order status.
func applyUnfreezeEvent(state *ShardState, evt *eventpb.UnfreezeEvent) error {
	if evt == nil {
		return nil
	}
	return applyBalanceSnapshot(state, evt.BalanceAfter)
}

// applySettlementEvent: per-party settlement. base_balance_after +
// quote_balance_after + order.FilledQty advanced by evt.Qty.
// Ignored if the order is unknown (foreign / already evicted).
func applySettlementEvent(state *ShardState, evt *eventpb.SettlementEvent) error {
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
	newFilled := o.FilledQty.Add(qty)
	if newFilled.Cmp(o.FilledQty) <= 0 {
		return nil
	}
	if _, err := state.Orders().SetFilledQty(evt.OrderId, newFilled, 0); err != nil && !errors.Is(err, ErrOrderNotFound) {
		return fmt.Errorf("settle filled_qty: %w", err)
	}
	return accumulateFrozenSpent(state, o, evt)
}

// accumulateFrozenSpent mirrors the settlement-side FrozenSpent
// bookkeeping so a terminal transition after replay correctly
// refunds residual. Sell-side orders consume FrozenAmount in base
// units (== qty); buy-side LIMIT orders consume in quote units
// (== price × qty); buy-side market-by-quote orders consume in quote
// units derived from delta_quote.
func accumulateFrozenSpent(state *ShardState, o *Order, evt *eventpb.SettlementEvent) error {
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
	case SideAsk:
		delta = qty
	case SideBid:
		if o.IsMarketBuyByQuote() {
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
	if _, err := state.Orders().AddFrozenSpent(evt.OrderId, delta); err != nil && !errors.Is(err, ErrOrderNotFound) {
		return fmt.Errorf("accumulate frozen_spent: %w", err)
	}
	return nil
}

// applyTransferEvent: writes balance_after and re-seats transfer_id
// in the user's ring so RPC replay dedups.
func applyTransferEvent(state *ShardState, evt *eventpb.TransferEvent) error {
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

// applyOrderStatusEvent: sets Status and FilledQty. No-op if the
// order isn't in byID (foreign / already evicted). Status updates
// flow through OrderStore.UpdateStatus so derived indices
// (activeByCOID / activeLimits / TerminatedAt) stay consistent.
func applyOrderStatusEvent(state *ShardState, evt *eventpb.OrderStatusEvent) error {
	if evt == nil || evt.OrderId == 0 {
		return nil
	}
	o := state.Orders().Get(evt.OrderId)
	if o == nil {
		return nil
	}
	newStatus := OrderStatusFromProto(evt.NewStatus)
	if newStatus == OrderStatusUnspecified {
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
			if _, err := state.Orders().SetFilledQty(evt.OrderId, filled, 0); err != nil && !errors.Is(err, ErrOrderNotFound) {
				return fmt.Errorf("order status filled_qty: %w", err)
			}
		}
	}
	if newStatus != o.Status {
		if _, err := state.Orders().UpdateStatus(evt.OrderId, newStatus, 0); err != nil && !errors.Is(err, ErrOrderNotFound) {
			return fmt.Errorf("order status: %w", err)
		}
	}
	return nil
}

// applyOrderEvictedEvent: mirror the evictor's ring→delete dance.
// Remember + Delete are both idempotent so duplicate events are safe.
func applyOrderEvictedEvent(state *ShardState, evt *eventpb.OrderEvictedEvent) error {
	if evt == nil || evt.UserId == "" || evt.OrderId == 0 {
		return nil
	}
	acc := state.Account(evt.UserId)
	acc.RememberTerminated(TerminatedOrderEntry{
		OrderID:       evt.OrderId,
		FinalStatus:   OrderStatusFromProto(evt.FinalStatus),
		TerminatedAt:  evt.TerminatedAt,
		ClientOrderID: evt.ClientOrderId,
		Symbol:        evt.Symbol,
	})
	if err := state.Orders().Delete(evt.OrderId); err != nil && !errors.Is(err, ErrOrderNotFound) {
		return fmt.Errorf("evict delete: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Proto enum conversion helpers. Exported so trade-dump shadow engine
// can reuse them without redefining a duplicate mapping table.
// -----------------------------------------------------------------------------

// OrderStatusFromProto is the inverse of the internal-status mapping
// emitted by journal.BuildOrderStatusEvent. Unknown / UNSPECIFIED
// values map to OrderStatusUnspecified; callers treat that as
// "skip — replay nothing".
func OrderStatusFromProto(s eventpb.InternalOrderStatus) OrderStatus {
	switch s {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW:
		return OrderStatusPendingNew
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW:
		return OrderStatusNew
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED:
		return OrderStatusPartiallyFilled
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED:
		return OrderStatusFilled
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL:
		return OrderStatusPendingCancel
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED:
		return OrderStatusCanceled
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED:
		return OrderStatusRejected
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return OrderStatusExpired
	}
	return OrderStatusUnspecified
}

func sideFromProto(s eventpb.Side) Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return SideBid
	case eventpb.Side_SIDE_SELL:
		return SideAsk
	}
	return SideBid
}

func orderTypeFromProto(t eventpb.OrderType) OrderType {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return OrderTypeLimit
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return OrderTypeMarket
	}
	return OrderTypeLimit
}

func tifFromProto(t eventpb.TimeInForce) TIF {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC:
		return TIFGTC
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return TIFIOC
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return TIFFOK
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return TIFPostOnly
	}
	return TIFGTC
}
