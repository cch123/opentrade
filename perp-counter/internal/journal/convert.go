// Package journal wires perp-counter's outbound Kafka records: order-event
// records to Match (order-event-<symbol>, ADR-0050) and the perp-journal WAL.
//
// The adapters here are deliberately thin and symbol-agnostic — the service
// builds every wire event (it owns the perp domain types), so this package only
// touches protobuf envelopes + Kafka. That keeps the dependency one-way
// (service defines the Dispatcher / Journal interfaces; this package satisfies
// them structurally without importing the service).
//
// This file holds the pure, Kafka-free helpers so they unit-test on their own.
package journal

import (
	"strconv"

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
// (perp_journal.proto: partition key is user_id). System-level risk-pool
// settlement events have no user and return "" (default partitioner).
func journalPartitionKey(evt *eventpb.PerpJournalEvent) string {
	switch p := evt.GetPayload().(type) {
	case *eventpb.PerpJournalEvent_OrderStatus:
		return journalUserKey(p.OrderStatus.GetUserId())
	case *eventpb.PerpJournalEvent_Settlement:
		return journalUserKey(p.Settlement.GetUserId())
	case *eventpb.PerpJournalEvent_Margin:
		return journalUserKey(p.Margin.GetUserId())
	case *eventpb.PerpJournalEvent_Funding:
		return journalUserKey(p.Funding.GetUserId())
	case *eventpb.PerpJournalEvent_Liquidation:
		return journalUserKey(p.Liquidation.GetUserId())
	case *eventpb.PerpJournalEvent_Takeover:
		return journalUserKey(p.Takeover.GetUserId())
	case *eventpb.PerpJournalEvent_Adl:
		return journalUserKey(p.Adl.GetUserId())
	case *eventpb.PerpJournalEvent_CustomerFee:
		return journalUserKey(p.CustomerFee.GetUserId())
	case *eventpb.PerpJournalEvent_RiskPoolSettlement:
		return ""
	default:
		return ""
	}
}

func journalUserKey(userID uint64) string {
	if userID == 0 {
		return ""
	}
	return strconv.FormatUint(userID, 10)
}

// orderEventKey returns the Kafka record key for an order-event. Match consumes
// per-symbol topics keyed by symbol (ADR-0050) so symbol is the natural key.
func orderEventKey(symbol string) string { return symbol }
