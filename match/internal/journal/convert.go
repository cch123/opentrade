// Package journal wires Kafka (order-event consumer, trade-event producer)
// to the SymbolWorker actors.
//
// This file contains pure conversion helpers between protobuf wire types and
// the internal Match types. It has no Kafka dependency and is fully
// unit-testable on its own.
package journal

import (
	"fmt"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// OrderEventToInternal converts an order-event protobuf message to a
// sequencer.Event. Returns (nil, nil) for payloads that Match does not
// consume. Returns a non-nil error if the message is malformed.
func OrderEventToInternal(pb *eventpb.OrderEvent, src sequencer.SourceMeta) (*sequencer.Event, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil OrderEvent")
	}
	switch p := pb.Payload.(type) {
	case *eventpb.OrderEvent_Placed:
		placed := p.Placed
		if placed == nil {
			return nil, fmt.Errorf("nil OrderPlaced payload")
		}
		side, err := protoSideToInternal(placed.Side)
		if err != nil {
			return nil, err
		}
		typ, err := protoOrderTypeToInternal(placed.OrderType)
		if err != nil {
			return nil, err
		}
		tif, err := protoTIFToInternal(placed.Tif)
		if err != nil {
			return nil, err
		}
		price, err := dec.Parse(placed.Price)
		if err != nil {
			return nil, fmt.Errorf("bad price: %w", err)
		}
		qty, err := dec.Parse(placed.Qty)
		if err != nil {
			return nil, fmt.Errorf("bad qty: %w", err)
		}
		quoteQty, err := dec.Parse(placed.QuoteQty)
		if err != nil {
			return nil, fmt.Errorf("bad quote_qty: %w", err)
		}
		// Market buy with quote_qty (ADR-0035) drives matching by budget, so
		// base qty is allowed to be zero. Every other shape still requires
		// a positive qty.
		isQuoteBuy := typ == orderbook.Market && side == orderbook.Bid && dec.IsPositive(quoteQty)
		if !isQuoteBuy && !dec.IsPositive(qty) {
			return nil, fmt.Errorf("qty must be positive: %s", placed.Qty)
		}
		var createdAt int64
		if pb.Meta != nil {
			createdAt = pb.Meta.TsUnixMs * int64(time.Millisecond)
		}
		o := &orderbook.Order{
			ID:             placed.OrderId,
			UserID:         placed.UserId,
			ClientID:       placed.ClientOrderId,
			Symbol:         placed.Symbol,
			Side:           side,
			Type:           typ,
			TIF:            tif,
			Price:          price,
			Qty:            qty,
			Remaining:      qty,
			QuoteQty:       quoteQty,
			RemainingQuote: quoteQty,
			CreatedAt:      createdAt,
		}
		return &sequencer.Event{
			Kind:   sequencer.EventOrderPlaced,
			Symbol: placed.Symbol,
			Order:  o,
			Source: src,
		}, nil

	case *eventpb.OrderEvent_Cancel:
		c := p.Cancel
		if c == nil {
			return nil, fmt.Errorf("nil OrderCancel payload")
		}
		return &sequencer.Event{
			Kind:    sequencer.EventOrderCancel,
			Symbol:  c.Symbol,
			OrderID: c.OrderId,
			UserID:  c.UserId,
			Source:  src,
		}, nil
	}
	// Unknown payload — caller may ignore.
	return nil, nil
}

// OutputToTradeEvent converts a sequencer.Output to a protobuf TradeEvent.
func OutputToTradeEvent(out *sequencer.Output, producerID string) (*eventpb.TradeEvent, error) {
	te := &eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{
			SeqId:      out.SeqID,
			TsUnixMs:   time.Now().UnixMilli(),
			ProducerId: producerID,
		},
	}
	switch out.Kind {
	case sequencer.OutputOrderAccepted:
		te.Payload = &eventpb.TradeEvent_Accepted{
			Accepted: &eventpb.OrderAccepted{
				UserId:       out.UserID,
				OrderId:      out.OrderID,
				Symbol:       out.Symbol,
				Side:         sideToProto(out.Side),
				Price:        out.Price.String(),
				RemainingQty: out.TakerRemaining.String(),
			},
		}
	case sequencer.OutputOrderRejected:
		te.Payload = &eventpb.TradeEvent_Rejected{
			Rejected: &eventpb.OrderRejected{
				UserId:  out.UserID,
				OrderId: out.OrderID,
				Symbol:  out.Symbol,
				Reason:  rejectReasonToProto(out.RejectReason),
			},
		}
	case sequencer.OutputTrade:
		te.Payload = &eventpb.TradeEvent_Trade{
			Trade: &eventpb.Trade{
				TradeId:             fmt.Sprintf("%s:%d", out.Symbol, out.SeqID),
				Symbol:              out.Symbol,
				Price:               out.Price.String(),
				Qty:                 out.Qty.String(),
				MakerUserId:         out.MakerUserID,
				MakerOrderId:        out.MakerOrderID,
				TakerUserId:         out.UserID,
				TakerOrderId:        out.OrderID,
				TakerSide:           sideToProto(out.Side),
				MakerFilledQtyAfter: out.MakerFilledAfter.String(),
				TakerFilledQtyAfter: out.TakerFilledAfter.String(),
			},
		}
	case sequencer.OutputOrderCancelled:
		te.Payload = &eventpb.TradeEvent_Cancelled{
			Cancelled: &eventpb.OrderCancelled{
				UserId:    out.UserID,
				OrderId:   out.OrderID,
				Symbol:    out.Symbol,
				FilledQty: out.FilledQty.String(),
			},
		}
	case sequencer.OutputOrderExpired:
		te.Payload = &eventpb.TradeEvent_Expired{
			Expired: &eventpb.OrderExpired{
				UserId:    out.UserID,
				OrderId:   out.OrderID,
				Symbol:    out.Symbol,
				FilledQty: out.FilledQty.String(),
				Reason:    rejectReasonToProto(out.RejectReason),
			},
		}
	default:
		return nil, fmt.Errorf("unknown output kind: %d", out.Kind)
	}
	return te, nil
}

// ---------------------------------------------------------------------------
// enum mappings
// ---------------------------------------------------------------------------

func protoSideToInternal(s eventpb.Side) (orderbook.Side, error) {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return orderbook.Bid, nil
	case eventpb.Side_SIDE_SELL:
		return orderbook.Ask, nil
	default:
		return 0, fmt.Errorf("unknown proto Side: %v", s)
	}
}

func sideToProto(s orderbook.Side) eventpb.Side {
	switch s {
	case orderbook.Bid:
		return eventpb.Side_SIDE_BUY
	case orderbook.Ask:
		return eventpb.Side_SIDE_SELL
	default:
		return eventpb.Side_SIDE_UNSPECIFIED
	}
}

func protoOrderTypeToInternal(t eventpb.OrderType) (orderbook.OrderType, error) {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return orderbook.Limit, nil
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return orderbook.Market, nil
	// STOP / STOP_LIMIT: not handled in MVP-1; translate to LIMIT with a TODO.
	default:
		return 0, fmt.Errorf("unsupported proto OrderType: %v", t)
	}
}

func protoTIFToInternal(t eventpb.TimeInForce) (orderbook.TIF, error) {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC:
		return orderbook.GTC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return orderbook.IOC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return orderbook.FOK, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return orderbook.PostOnly, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED:
		// Default to GTC when unset.
		return orderbook.GTC, nil
	default:
		return 0, fmt.Errorf("unknown proto TIF: %v", t)
	}
}

func rejectReasonToProto(r orderbook.RejectReason) eventpb.RejectReason {
	switch r {
	case orderbook.RejectNone:
		return eventpb.RejectReason_REJECT_REASON_UNSPECIFIED
	case orderbook.RejectInvalidPriceTick:
		return eventpb.RejectReason_REJECT_REASON_INVALID_PRICE_TICK
	case orderbook.RejectInvalidLotSize:
		return eventpb.RejectReason_REJECT_REASON_INVALID_LOT_SIZE
	case orderbook.RejectPostOnlyWouldTake:
		return eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE
	case orderbook.RejectSelfTradePrevented:
		return eventpb.RejectReason_REJECT_REASON_SELF_TRADE_PREVENTED
	case orderbook.RejectSymbolNotTrading:
		return eventpb.RejectReason_REJECT_REASON_SYMBOL_NOT_TRADING
	case orderbook.RejectDuplicateOrderID:
		return eventpb.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID
	case orderbook.RejectFOKNotFilled:
		return eventpb.RejectReason_REJECT_REASON_FOK_NOT_FILLED
	default:
		return eventpb.RejectReason_REJECT_REASON_INTERNAL
	}
}
