// Package journal wires perp-counter to Kafka: it produces order-event records
// to Match (order-event-<symbol>, ADR-0050) and the perp-journal WAL, and
// consumes Match's perp-trade-event stream back into the service (ADR-0068 §1/§2).
//
// The adapters here are deliberately thin and symbol-agnostic — the service
// builds every wire event (it owns the perp domain types), so this package only
// touches protobuf envelopes + Kafka. That keeps the dependency one-way
// (service defines the Dispatcher / Journal / TradeHandler interfaces; this
// package satisfies them structurally without importing the service).
//
// This file holds the pure, Kafka-free helpers so they unit-test on their own.
package journal

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// orderEventTopicFor returns the per-symbol order-event topic Match consumes
// (ADR-0050). Mirrors counter's TxnProducer.orderEventTopicFor: a perp order
// for BTC-USDT-PERP lands on `order-event-BTC-USDT-PERP`, sharing the prefix
// with spot — the `-PERP` symbol suffix is what routes it to the perp Match
// deployment's owned topic set (ADR-0068 §1).
func orderEventTopicFor(prefix, symbol string) string {
	if prefix != "" && symbol != "" {
		return prefix + "-" + symbol
	}
	return prefix
}

// journalPartitionKey extracts the user_id a perp-journal record is keyed by
// (perp_journal.proto: partition key is user_id). Every payload variant carries
// a user_id; an unrecognized/empty payload returns "" (default partitioner).
func journalPartitionKey(evt *eventpb.PerpJournalEvent) string {
	switch p := evt.GetPayload().(type) {
	case *eventpb.PerpJournalEvent_OrderStatus:
		return p.OrderStatus.GetUserId()
	case *eventpb.PerpJournalEvent_Settlement:
		return p.Settlement.GetUserId()
	case *eventpb.PerpJournalEvent_Margin:
		return p.Margin.GetUserId()
	case *eventpb.PerpJournalEvent_Funding:
		return p.Funding.GetUserId()
	case *eventpb.PerpJournalEvent_Liquidation:
		return p.Liquidation.GetUserId()
	default:
		return ""
	}
}

// orderEventKey returns the Kafka record key for an order-event. Match consumes
// per-symbol topics keyed by symbol (ADR-0050) so symbol is the natural key.
func orderEventKey(symbol string) string { return symbol }
