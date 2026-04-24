package mysqlstore

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
)

// Conversion helpers between the int8 columns stored in MySQL (written by
// trade-dump in projection.go) and the typed enums on the history.proto
// wire. Kept as pure functions so they're trivially unit-testable and so
// the store file stays focused on SQL.

func sideFromInt(n int8) eventpb.Side {
	switch n {
	case int8(eventpb.Side_SIDE_BUY):
		return eventpb.Side_SIDE_BUY
	case int8(eventpb.Side_SIDE_SELL):
		return eventpb.Side_SIDE_SELL
	}
	return eventpb.Side_SIDE_UNSPECIFIED
}

func orderTypeFromInt(n int8) eventpb.OrderType {
	switch n {
	case int8(eventpb.OrderType_ORDER_TYPE_LIMIT):
		return eventpb.OrderType_ORDER_TYPE_LIMIT
	case int8(eventpb.OrderType_ORDER_TYPE_MARKET):
		return eventpb.OrderType_ORDER_TYPE_MARKET
	case int8(eventpb.OrderType_ORDER_TYPE_STOP):
		return eventpb.OrderType_ORDER_TYPE_STOP
	case int8(eventpb.OrderType_ORDER_TYPE_STOP_LIMIT):
		return eventpb.OrderType_ORDER_TYPE_STOP_LIMIT
	}
	return eventpb.OrderType_ORDER_TYPE_UNSPECIFIED
}

func tifFromInt(n int8) eventpb.TimeInForce {
	switch n {
	case int8(eventpb.TimeInForce_TIME_IN_FORCE_GTC):
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC
	case int8(eventpb.TimeInForce_TIME_IN_FORCE_IOC):
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC
	case int8(eventpb.TimeInForce_TIME_IN_FORCE_FOK):
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK
	case int8(eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY):
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY
	}
	return eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
}

func tradeRoleFromInt(n int8) historypb.TradeRole {
	switch n {
	case int8(historypb.TradeRole_TRADE_ROLE_MAKER):
		return historypb.TradeRole_TRADE_ROLE_MAKER
	case int8(historypb.TradeRole_TRADE_ROLE_TAKER):
		return historypb.TradeRole_TRADE_ROLE_TAKER
	}
	return historypb.TradeRole_TRADE_ROLE_UNSPECIFIED
}

// externalStatusFromInternal folds the 8-state internal machine
// (event.InternalOrderStatus) into the 6-state external view defined by
// history.proto. Mirrors bff/internal/rest/orders.go's
// externalStatusFromInternal, but returns the proto enum instead of a
// string — BFF continues to do the final string translation at the REST
// boundary.
func externalStatusFromInternal(n int8) historypb.OrderStatus {
	switch eventpb.InternalOrderStatus(n) {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW:
		return historypb.OrderStatus_ORDER_STATUS_NEW
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL:
		// PENDING_CANCEL preserves the pre-cancel status for the client;
		// without a filled_qty check here we conservatively return
		// PARTIALLY_FILLED when the internal state indicated it, else NEW.
		if eventpb.InternalOrderStatus(n) == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED {
			return historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
		}
		return historypb.OrderStatus_ORDER_STATUS_NEW
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED:
		return historypb.OrderStatus_ORDER_STATUS_FILLED
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED:
		return historypb.OrderStatus_ORDER_STATUS_CANCELED
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED:
		return historypb.OrderStatus_ORDER_STATUS_REJECTED
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return historypb.OrderStatus_ORDER_STATUS_EXPIRED
	}
	return historypb.OrderStatus_ORDER_STATUS_UNSPECIFIED
}

// internalStatusesForExternal is the inverse fan-out: ListOrders
// receives an external OrderStatus filter and must translate it to the
// (one or more) int8 codes the `orders.status` column may carry for
// rows in that visible state.
func internalStatusesForExternal(s historypb.OrderStatus) []int8 {
	switch s {
	case historypb.OrderStatus_ORDER_STATUS_NEW:
		return []int8{
			int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW),
			int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW),
		}
	case historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED:
		return []int8{
			int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED),
			int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL),
		}
	case historypb.OrderStatus_ORDER_STATUS_FILLED:
		return []int8{int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED)}
	case historypb.OrderStatus_ORDER_STATUS_CANCELED:
		return []int8{int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED)}
	case historypb.OrderStatus_ORDER_STATUS_REJECTED:
		return []int8{int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED)}
	case historypb.OrderStatus_ORDER_STATUS_EXPIRED:
		return []int8{int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED)}
	}
	return nil
}

// StatusesForScope expands an OrderScope to the set of external statuses
// it covers. Used by the server layer to fold scope → statuses before
// passing to the store.
func StatusesForScope(s historypb.OrderScope) []historypb.OrderStatus {
	switch s {
	case historypb.OrderScope_ORDER_SCOPE_OPEN:
		return []historypb.OrderStatus{
			historypb.OrderStatus_ORDER_STATUS_NEW,
			historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		}
	case historypb.OrderScope_ORDER_SCOPE_TERMINAL:
		return []historypb.OrderStatus{
			historypb.OrderStatus_ORDER_STATUS_FILLED,
			historypb.OrderStatus_ORDER_STATUS_CANCELED,
			historypb.OrderStatus_ORDER_STATUS_REJECTED,
			historypb.OrderStatus_ORDER_STATUS_EXPIRED,
		}
	}
	// UNSPECIFIED and ALL both mean "no filter".
	return nil
}

// InternalStatusesForFilters flattens a (possibly empty) slice of external
// statuses into the deduplicated internal-code slice the store needs.
// Empty input → nil (no filter).
func InternalStatusesForFilters(external []historypb.OrderStatus) []int8 {
	if len(external) == 0 {
		return nil
	}
	seen := map[int8]struct{}{}
	var out []int8
	for _, s := range external {
		for _, n := range internalStatusesForExternal(s) {
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			out = append(out, n)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Trigger enum helpers (ADR-0047)
// ---------------------------------------------------------------------------

func triggerTypeFromInt(n int8) condrpc.TriggerType {
	switch n {
	case int8(condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS):
		return condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS
	case int8(condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT):
		return condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT
	case int8(condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT):
		return condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT
	case int8(condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT):
		return condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT
	case int8(condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS):
		return condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS
	}
	return condrpc.TriggerType_TRIGGER_TYPE_UNSPECIFIED
}

func triggerStatusFromInt(n int8) condrpc.TriggerStatus {
	switch n {
	case int8(condrpc.TriggerStatus_TRIGGER_STATUS_PENDING):
		return condrpc.TriggerStatus_TRIGGER_STATUS_PENDING
	case int8(condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED):
		return condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED
	case int8(condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED):
		return condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED
	case int8(condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED):
		return condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED
	case int8(condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED):
		return condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED
	}
	return condrpc.TriggerStatus_TRIGGER_STATUS_UNSPECIFIED
}

// TriggerStatusesForScope expands a TriggerScope to the set of
// condrpc statuses it covers. Used by the server layer to fold
// scope → statuses before calling the store.
func TriggerStatusesForScope(s historypb.TriggerScope) []condrpc.TriggerStatus {
	switch s {
	case historypb.TriggerScope_TRIGGER_SCOPE_ACTIVE:
		return []condrpc.TriggerStatus{
			condrpc.TriggerStatus_TRIGGER_STATUS_PENDING,
		}
	case historypb.TriggerScope_TRIGGER_SCOPE_TERMINAL:
		return []condrpc.TriggerStatus{
			condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED,
			condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED,
			condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED,
			condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED,
		}
	}
	return nil
}

// InternalTriggerStatuses flattens a (possibly empty) slice of
// condrpc statuses into the deduped int8 slice the store needs.
func InternalTriggerStatuses(external []condrpc.TriggerStatus) []int8 {
	if len(external) == 0 {
		return nil
	}
	seen := map[int8]struct{}{}
	var out []int8
	for _, s := range external {
		n := int8(s)
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

// StatesForTransferScope expands a TransferScope to the set of
// transferledger.State strings it covers. Used by the server layer to
// fold scope → states before calling the store.
func StatesForTransferScope(s historypb.TransferScope) []string {
	switch s {
	case historypb.TransferScope_TRANSFER_SCOPE_IN_FLIGHT:
		return []string{"INIT", "DEBITED", "COMPENSATING"}
	case historypb.TransferScope_TRANSFER_SCOPE_TERMINAL:
		return []string{"COMPLETED", "FAILED", "COMPENSATED", "COMPENSATE_STUCK"}
	}
	// UNSPECIFIED and ALL both mean "no filter".
	return nil
}

func rejectReasonToString(n int8) string {
	switch eventpb.RejectReason(n) {
	case eventpb.RejectReason_REJECT_REASON_UNSPECIFIED:
		return ""
	case eventpb.RejectReason_REJECT_REASON_INVALID_PRICE_TICK:
		return "invalid_price_tick"
	case eventpb.RejectReason_REJECT_REASON_INVALID_LOT_SIZE:
		return "invalid_lot_size"
	case eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE:
		return "post_only_would_take"
	case eventpb.RejectReason_REJECT_REASON_SELF_TRADE_PREVENTED:
		return "self_trade_prevented"
	case eventpb.RejectReason_REJECT_REASON_SYMBOL_NOT_TRADING:
		return "symbol_not_trading"
	case eventpb.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID:
		return "duplicate_order_id"
	case eventpb.RejectReason_REJECT_REASON_FOK_NOT_FILLED:
		return "fok_not_filled"
	case eventpb.RejectReason_REJECT_REASON_INTERNAL:
		return "internal"
	}
	return ""
}
