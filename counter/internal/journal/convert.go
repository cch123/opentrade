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
	"github.com/xargin/opentrade/counter/internal/engine"
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

	Req          engine.TransferRequest
	BalanceAfter engine.Balance
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
func transferTypeToProto(t engine.TransferType) (eventpb.TransferEvent_TransferType, error) {
	switch t {
	case engine.TransferDeposit:
		return eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT, nil
	case engine.TransferWithdraw:
		return eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW, nil
	case engine.TransferFreeze:
		return eventpb.TransferEvent_TRANSFER_TYPE_FREEZE, nil
	case engine.TransferUnfreeze:
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

	Order        *engine.Order
	BalanceAfter engine.Balance
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

	Order *engine.Order
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
	Party   engine.PartySettlement
	TradeID string

	// Snapshot of the (base, quote) balances for the party AFTER applying the
	// settlement. Balance.Version is propagated to BalanceSnapshot.Version.
	BaseAfter  engine.Balance
	QuoteAfter engine.Balance
}

// BuildSettlementEvent builds a CounterJournalEvent/SettlementEvent for one
// side of a Trade.
func BuildSettlementEvent(in SettlementEventInput) (*eventpb.CounterJournalEvent, error) {
	base, quote, err := engine.SymbolAssets(in.Symbol)
	if err != nil {
		return nil, err
	}
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
		Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{
				UserId:        in.Party.UserID,
				OrderId:       in.Party.OrderID,
				TradeId:       in.TradeID,
				Symbol:        in.Symbol,
				DeltaBase:     in.Party.BaseDelta.String(),
				DeltaQuote:    in.Party.QuoteDelta.String(),
				UnfreezeBase:  in.Party.FrozenBaseDelta.Neg().String(),
				UnfreezeQuote: in.Party.FrozenQuoteDelta.Neg().String(),
				FeeAmount:     "0",
				Qty:           "",
				Price:         "",
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
	OldStatus engine.OrderStatus
	NewStatus engine.OrderStatus
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

// OrderEvictedEventInput is the input to BuildOrderEvictedEvent. ADR-0062.
//
// AccountVersion stays at whatever the account currently carries —
// eviction touches no balance and is purely a shrink-state fan-out
// signal to trade-dump's shadow engine + MySQL projection pipeline.
type OrderEvictedEventInput struct {
	CounterSeqID   uint64
	TsUnixMS       int64
	TraceID        string
	ProducerID     string
	AccountVersion uint64

	UserID        string
	OrderID       uint64
	Symbol        string
	FinalStatus   engine.OrderStatus
	TerminatedAt  int64 // ms, mirrors engine.Order.TerminatedAt
	ClientOrderID string
}

// BuildOrderEvictedEvent wraps an OrderEvictedEvent payload into a
// CounterJournalEvent envelope.
func BuildOrderEvictedEvent(in OrderEvictedEventInput) *eventpb.CounterJournalEvent {
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
		Payload: &eventpb.CounterJournalEvent_OrderEvicted{
			OrderEvicted: &eventpb.OrderEvictedEvent{
				UserId:        in.UserID,
				OrderId:       in.OrderID,
				Symbol:        in.Symbol,
				FinalStatus:   orderStatusToProto(in.FinalStatus),
				TerminatedAt:  in.TerminatedAt,
				ClientOrderId: in.ClientOrderID,
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

func sideToProto(s engine.Side) (eventpb.Side, error) {
	switch s {
	case engine.SideBid:
		return eventpb.Side_SIDE_BUY, nil
	case engine.SideAsk:
		return eventpb.Side_SIDE_SELL, nil
	default:
		return 0, fmt.Errorf("unknown side: %d", s)
	}
}

func orderTypeToProto(t engine.OrderType) (eventpb.OrderType, error) {
	switch t {
	case engine.OrderTypeLimit:
		return eventpb.OrderType_ORDER_TYPE_LIMIT, nil
	case engine.OrderTypeMarket:
		return eventpb.OrderType_ORDER_TYPE_MARKET, nil
	default:
		return 0, fmt.Errorf("unknown order type: %d", t)
	}
}

func tifToProto(t engine.TIF) (eventpb.TimeInForce, error) {
	switch t {
	case engine.TIFGTC:
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC, nil
	case engine.TIFIOC:
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC, nil
	case engine.TIFFOK:
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK, nil
	case engine.TIFPostOnly:
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, nil
	default:
		return 0, fmt.Errorf("unknown TIF: %d", t)
	}
}

func orderStatusToProto(s engine.OrderStatus) eventpb.InternalOrderStatus {
	switch s {
	case engine.OrderStatusPendingNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW
	case engine.OrderStatusNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW
	case engine.OrderStatusPartiallyFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED
	case engine.OrderStatusFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED
	case engine.OrderStatusPendingCancel:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
	case engine.OrderStatusCanceled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED
	case engine.OrderStatusRejected:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED
	case engine.OrderStatusExpired:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED
	}
	return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED
}
