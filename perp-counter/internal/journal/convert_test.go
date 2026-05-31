package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestOrderEventTopicFor(t *testing.T) {
	cases := []struct {
		prefix, symbol, want string
	}{
		{"order-event", "BTC-USDT-PERP", "order-event-BTC-USDT-PERP"},
		{"order-event", "ETH-USDT-PERP", "order-event-ETH-USDT-PERP"},
		{"perp-order", "BTC-USDT-PERP", "perp-order-BTC-USDT-PERP"},
		{"order-event", "", "order-event"}, // missing symbol → bare prefix
	}
	for _, c := range cases {
		if got := orderEventTopicFor(c.prefix, c.symbol); got != c.want {
			t.Errorf("orderEventTopicFor(%q,%q) = %q, want %q", c.prefix, c.symbol, got, c.want)
		}
	}
}

func TestOrderEventKey(t *testing.T) {
	if got := orderEventKey("BTC-USDT-PERP"); got != "BTC-USDT-PERP" {
		t.Errorf("orderEventKey = %q, want symbol", got)
	}
}

func TestJournalPartitionKey_AllPayloads(t *testing.T) {
	cases := []struct {
		name string
		evt  *eventpb.PerpJournalEvent
		want string
	}{
		{"order_status", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_OrderStatus{
			OrderStatus: &eventpb.PerpOrderStatusEvent{UserId: 1001}}}, "1001"},
		{"settlement", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Settlement{
			Settlement: &eventpb.PerpSettlementEvent{UserId: 1002}}}, "1002"},
		{"margin", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Margin{
			Margin: &eventpb.PerpMarginEvent{UserId: 1003}}}, "1003"},
		{"funding", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Funding{
			Funding: &eventpb.PerpFundingEvent{UserId: 1004}}}, "1004"},
		{"liquidation", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Liquidation{
			Liquidation: &eventpb.PerpLiquidationEvent{UserId: 1005}}}, "1005"},
		{"takeover", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{
			Takeover: &eventpb.PerpTakeoverEvent{UserId: 1006}}}, "1006"},
		{"adl", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Adl{
			Adl: &eventpb.PerpAdlEvent{UserId: 1007}}}, "1007"},
		{"empty", &eventpb.PerpJournalEvent{}, ""},
	}
	for _, c := range cases {
		if got := journalPartitionKey(c.evt); got != c.want {
			t.Errorf("%s: journalPartitionKey = %q, want %q", c.name, got, c.want)
		}
	}
}
