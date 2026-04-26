// Package journal serializes Counter state-machine events to the
// counter-journal Kafka topic (ADR-0004).
//
// This file contains pure protobuf conversion helpers — no Kafka dependency,
// fully unit-testable.
package journal

import (
	"fmt"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/counterstate"
)

// TransferEventInput is the internal input used to build a journal envelope.
//
// AccountVersion is the user's post-mutation Account.version (ADR-0048
// backlog: 双层 version 方案 B). Callers read it AFTER committing the
// balance so the emitted event reflects the new authoritative state.
// BalanceAfter.Version likewise carries the per-asset counter.
type TransferEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	Req          counterstate.TransferRequest
	BalanceAfter counterstate.Balance
}

// BuildTransferEvent builds a CounterJournalEvent with a TransferEvent payload.
func BuildTransferEvent(in TransferEventInput) (*eventpb.CounterJournalEvent, error) {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	pbType, err := transferTypeToProto(in.Req.Type)
	if err != nil {
		return nil, err
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs:   ts,
			TraceId:    in.TraceID,
			ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{
				UserId:         in.Req.UserID,
				TransferId:     in.Req.TransferID,
				Asset:          in.Req.Asset,
				Amount:         in.Req.Amount.String(),
				Type:           pbType,
				BizRefId:       in.Req.BizRefID,
				Memo:           in.Req.Memo,
				SagaTransferId: in.Req.SagaTransferID,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    in.Req.UserID,
					Asset:     in.Req.Asset,
					Available: in.BalanceAfter.Available.String(),
					Frozen:    in.BalanceAfter.Frozen.String(),
					Version:   in.BalanceAfter.Version,
				},
			},
		},
	}, nil
}

// transferTypeToProto maps the internal TransferType to the proto enum inside
// TransferEvent.
func transferTypeToProto(t counterstate.TransferType) (eventpb.TransferEvent_TransferType, error) {
	switch t {
	case counterstate.TransferDeposit:
		return eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT, nil
	case counterstate.TransferWithdraw:
		return eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW, nil
	case counterstate.TransferFreeze:
		return eventpb.TransferEvent_TRANSFER_TYPE_FREEZE, nil
	case counterstate.TransferUnfreeze:
		return eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE, nil
	default:
		return 0, fmt.Errorf("unknown transfer type: %d", t)
	}
}

// -----------------------------------------------------------------------------
// PlaceOrder / Cancel events
// -----------------------------------------------------------------------------

// PlaceOrderEventInput is the input to BuildPlaceOrderEvents.
type PlaceOrderEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	Order        *counterstate.Order
	BalanceAfter counterstate.Balance
}

// BuildPlaceOrderEvents constructs the (counter-journal, order-event) pair
// that must be atomically published when Counter accepts a new order.
func BuildPlaceOrderEvents(in PlaceOrderEventInput) (*eventpb.CounterJournalEvent, *eventpb.OrderEvent, error) {
	o := in.Order
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	side, err := sideToProto(o.Side)
	if err != nil {
		return nil, nil, err
	}
	ot, err := orderTypeToProto(o.Type)
	if err != nil {
		return nil, nil, err
	}
	tif, err := tifToProto(o.TIF)
	if err != nil {
		return nil, nil, err
	}

	journal := &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{
				UserId:        o.UserID,
				OrderId:       o.ID,
				ClientOrderId: o.ClientOrderID,
				Symbol:        o.Symbol,
				Side:          side,
				OrderType:     ot,
				Tif:           tif,
				Price:         o.Price.String(),
				Qty:           o.Qty.String(),
				FreezeAsset:   o.FrozenAsset,
				FreezeAmount:  o.FrozenAmount.String(),
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    o.UserID,
					Asset:     o.FrozenAsset,
					Available: in.BalanceAfter.Available.String(),
					Frozen:    in.BalanceAfter.Frozen.String(),
					Version:   in.BalanceAfter.Version,
				},
			},
		},
	}

	orderEvt := &eventpb.OrderEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		CounterSeqId: in.CounterSeqID,
		Payload: &eventpb.OrderEvent_Placed{
			Placed: &eventpb.OrderPlaced{
				UserId:        o.UserID,
				OrderId:       o.ID,
				ClientOrderId: o.ClientOrderID,
				Symbol:        o.Symbol,
				Side:          side,
				OrderType:     ot,
				Tif:           tif,
				Price:         o.Price.String(),
				Qty:           o.Qty.String(),
				QuoteQty:      o.QuoteQty.String(),
				FreezeCap:     o.FrozenAmount.String(),
			},
		},
	}
	return journal, orderEvt, nil
}

// CancelOrderEventInput is the input to BuildCancelEvents.
type CancelOrderEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	Order *counterstate.Order
}

// BuildCancelEvents constructs the (counter-journal, order-event) pair that
// must be atomically published when Counter receives a cancel request.
func BuildCancelEvents(in CancelOrderEventInput) (*eventpb.CounterJournalEvent, *eventpb.OrderEvent) {
	o := in.Order
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	journal := &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_CancelReq{
			CancelReq: &eventpb.CancelRequested{
				UserId:  o.UserID,
				OrderId: o.ID,
				Symbol:  o.Symbol,
			},
		},
	}
	orderEvt := &eventpb.OrderEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		CounterSeqId: in.CounterSeqID,
		Payload: &eventpb.OrderEvent_Cancel{
			Cancel: &eventpb.OrderCancel{
				UserId:  o.UserID,
				OrderId: o.ID,
				Symbol:  o.Symbol,
			},
		},
	}
	return journal, orderEvt
}

// SettlementEventInput is the input to BuildSettlementEvent.
type SettlementEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	Symbol  string
	Party   counterstate.PartySettlement
	TradeID string
	Side    counterstate.Side
	Price   string
	Qty     string

	// Snapshot of the (base, quote) balances for the party AFTER applying the
	// settlement. Balance.Version is propagated to BalanceSnapshot.Version.
	BaseAfter  counterstate.Balance
	QuoteAfter counterstate.Balance
}

// BuildSettlementEvent builds a CounterJournalEvent/SettlementEvent for one
// side of a Trade.
func BuildSettlementEvent(in SettlementEventInput) (*eventpb.CounterJournalEvent, error) {
	base, quote, err := counterstate.SymbolAssets(in.Symbol)
	if err != nil {
		return nil, err
	}
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	side, err := sideToProto(in.Side)
	if err != nil {
		return nil, err
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{
				UserId:        in.Party.UserID,
				OrderId:       in.Party.OrderID,
				TradeId:       in.TradeID,
				Symbol:        in.Symbol,
				Side:          side,
				DeltaBase:     in.Party.BaseDelta.String(),
				DeltaQuote:    in.Party.QuoteDelta.String(),
				UnfreezeBase:  in.Party.FrozenBaseDelta.Neg().String(),
				UnfreezeQuote: in.Party.FrozenQuoteDelta.Neg().String(),
				FeeAmount:     "0",
				Qty:           in.Qty,
				Price:         in.Price,
				BaseBalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    in.Party.UserID,
					Asset:     base,
					Available: in.BaseAfter.Available.String(),
					Frozen:    in.BaseAfter.Frozen.String(),
					Version:   in.BaseAfter.Version,
				},
				QuoteBalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    in.Party.UserID,
					Asset:     quote,
					Available: in.QuoteAfter.Available.String(),
					Frozen:    in.QuoteAfter.Frozen.String(),
					Version:   in.QuoteAfter.Version,
				},
			},
		},
	}, nil
}

// UnfreezeEventInput is the input to BuildUnfreezeEvent.
type UnfreezeEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	UserID       string
	OrderID      uint64
	Asset        string
	Amount       string
	BalanceAfter counterstate.Balance
}

// BuildUnfreezeEvent builds a CounterJournalEvent/UnfreezeEvent for terminal
// residual releases (reject / cancel / expire).
func BuildUnfreezeEvent(in UnfreezeEventInput) *eventpb.CounterJournalEvent {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_Unfreeze{
			Unfreeze: &eventpb.UnfreezeEvent{
				UserId:  in.UserID,
				OrderId: in.OrderID,
				Asset:   in.Asset,
				Amount:  in.Amount,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    in.UserID,
					Asset:     in.Asset,
					Available: in.BalanceAfter.Available.String(),
					Frozen:    in.BalanceAfter.Frozen.String(),
					Version:   in.BalanceAfter.Version,
				},
			},
		},
	}
}

// OrderStatusEventInput is the input to BuildOrderStatusEvent.
//
// AccountVersion is the CURRENT version at emit time (not a post-mutation
// bump): status transitions do not touch balances, so the counter stays
// where it was — consumers using it as a cache handle see the value as a
// stable witness of the account's state at this moment.
type OrderStatusEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	UserID    string
	OrderID   uint64
	OldStatus counterstate.OrderStatus
	NewStatus counterstate.OrderStatus
	FilledQty string
	Reject    eventpb.RejectReason
}

// TECheckpointEventInput is the input to BuildTECheckpointEvent. ADR-0060 §2.
//
// AccountVersion has no meaningful value on a checkpoint (the event
// mutates no balance); callers pass 0 or the vshard's most recent
// account version — neither is material to correctness. CounterSeqID
// is the advancer's allocation at publish time so downstream tracking
// of counterSeq monotonicity stays intact.
type TECheckpointEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	TePartition int32
	TeOffset    int64
}

// BuildTECheckpointEvent wraps a TECheckpointEvent payload into a
// CounterJournalEvent envelope.
func BuildTECheckpointEvent(in TECheckpointEventInput) *eventpb.CounterJournalEvent {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{
				TePartition: in.TePartition,
				TeOffset:    in.TeOffset,
			},
		},
	}
}

// BuildOrderStatusEvent wraps an OrderStatusEvent payload into a
// CounterJournalEvent envelope.
func BuildOrderStatusEvent(in OrderStatusEventInput) *eventpb.CounterJournalEvent {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs: ts, TraceId: in.TraceID, ProducerId: in.ProducerID,
		},
		AccountVersion: in.AccountVersion,
		CounterSeqId:   in.CounterSeqID,
		Payload: &eventpb.CounterJournalEvent_OrderStatus{
			OrderStatus: &eventpb.OrderStatusEvent{
				UserId:       in.UserID,
				OrderId:      in.OrderID,
				OldStatus:    orderStatusToProto(in.OldStatus),
				NewStatus:    orderStatusToProto(in.NewStatus),
				FilledQty:    in.FilledQty,
				RejectReason: in.Reject,
			},
		},
	}
}

// -----------------------------------------------------------------------------
// enum mappings
// -----------------------------------------------------------------------------

func sideToProto(s counterstate.Side) (eventpb.Side, error) {
	switch s {
	case counterstate.SideBid:
		return eventpb.Side_SIDE_BUY, nil
	case counterstate.SideAsk:
		return eventpb.Side_SIDE_SELL, nil
	default:
		return 0, fmt.Errorf("unknown side: %d", s)
	}
}

func orderTypeToProto(t counterstate.OrderType) (eventpb.OrderType, error) {
	switch t {
	case counterstate.OrderTypeLimit:
		return eventpb.OrderType_ORDER_TYPE_LIMIT, nil
	case counterstate.OrderTypeMarket:
		return eventpb.OrderType_ORDER_TYPE_MARKET, nil
	default:
		return 0, fmt.Errorf("unknown order type: %d", t)
	}
}

func tifToProto(t counterstate.TIF) (eventpb.TimeInForce, error) {
	switch t {
	case counterstate.TIFGTC:
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC, nil
	case counterstate.TIFIOC:
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC, nil
	case counterstate.TIFFOK:
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK, nil
	case counterstate.TIFPostOnly:
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, nil
	default:
		return 0, fmt.Errorf("unknown TIF: %d", t)
	}
}

func orderStatusToProto(s counterstate.OrderStatus) eventpb.InternalOrderStatus {
	switch s {
	case counterstate.OrderStatusPendingNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW
	case counterstate.OrderStatusNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW
	case counterstate.OrderStatusPartiallyFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED
	case counterstate.OrderStatusFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED
	case counterstate.OrderStatusPendingCancel:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
	case counterstate.OrderStatusCanceled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED
	case counterstate.OrderStatusRejected:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED
	case counterstate.OrderStatusExpired:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED
	}
	return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED
}

// OrderStatusFromProto moved to counter/counterstate.OrderStatusFromProto
// (ADR-0061 M1). Journal builders still use orderStatusToProto (above)
// for the emit path; the inverse mapping now lives next to the
// shadow / catch-up apply logic so trade-dump can import it without
// pulling journal's Kafka + transactional dependencies.
