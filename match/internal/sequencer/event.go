// Package sequencer hosts the per-symbol Actor-model worker that serializes
// all matching for a single symbol (see ADR-0016, 0019).
package sequencer

import (
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// EventKind distinguishes worker inputs.
type EventKind uint8

const (
	// EventOrderPlaced — new order received from order-event topic.
	EventOrderPlaced EventKind = 1
	// EventOrderCancel — cancel request received from order-event topic.
	EventOrderCancel EventKind = 2
)

// Event is a single input dispatched to a SymbolWorker.
type Event struct {
	Kind    EventKind
	Symbol  string           // always set; used by Dispatcher to route
	Order   *orderbook.Order // required for EventOrderPlaced
	OrderID uint64           // required for EventOrderCancel
	UserID  string           // required for EventOrderCancel (authorization)

	// Source is Kafka provenance, carried through so the Kafka layer can
	// commit the offset after the event has been fully processed.
	Source SourceMeta
}

// SourceMeta carries Kafka consumer offsets. For in-process tests Source may
// be left zero.
type SourceMeta struct {
	Topic     string
	Partition int32
	Offset    int64
}

// OutputKind classifies a SymbolWorker emission (maps 1:1 to trade-event
// protobuf payload types).
type OutputKind uint8

const (
	// OutputOrderAccepted — order was placed on the book.
	OutputOrderAccepted OutputKind = 1
	// OutputOrderRejected — order failed a rule check (price tick / Post-Only /
	// STP / FOK pre-check / duplicate id / symbol not trading).
	OutputOrderRejected OutputKind = 2
	// OutputTrade — one fill between a maker and a taker.
	OutputTrade OutputKind = 3
	// OutputOrderCancelled — cancel took effect on an order that was on the book.
	OutputOrderCancelled OutputKind = 4
	// OutputOrderExpired — IOC remainder / market out-of-liquidity / FOK
	// (defensive fallback) — order left the book without further fills.
	OutputOrderExpired OutputKind = 5
)

// Output is the structured emission from a SymbolWorker. Each field is only
// populated for the relevant OutputKind(s); see the per-kind list.
type Output struct {
	Kind   OutputKind
	SeqID  uint64 // per-symbol monotonic sequence id (ADR-0019)
	Symbol string

	// Identity of the order that produced this emission.
	UserID  string
	OrderID uint64
	Side    orderbook.Side

	// Populated for OutputTrade (trade price) and OutputOrderAccepted (the
	// resting limit price of the order that just entered the book).
	Price dec.Decimal
	// OutputTrade only: qty filled by this trade.
	Qty            dec.Decimal
	MakerUserID    string
	MakerOrderID   uint64
	MakerSide      orderbook.Side
	MakerRemaining dec.Decimal
	// OutputTrade: remaining qty of the taker after this fill.
	// OutputOrderAccepted: qty still live on the book once the order rests.
	TakerRemaining dec.Decimal

	// OutputOrderRejected / OutputOrderExpired.
	RejectReason orderbook.RejectReason

	// OutputOrderCancelled / OutputOrderExpired: how much of the taker filled
	// before it left the book.
	FilledQty dec.Decimal

	// SourceOffset carries the Kafka offset from which this emission
	// originated (propagated from Event.Source).
	SourceOffset SourceMeta
}
