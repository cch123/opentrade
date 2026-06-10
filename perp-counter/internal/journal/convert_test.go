package journal

import (
	"strconv"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

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
		{"position_config", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_PositionConfig{
			PositionConfig: &eventpb.PerpPositionConfigEvent{UserId: 1008}}}, "1008"},
		{"margin_adjustment", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_MarginAdjustment{
			MarginAdjustment: &eventpb.PerpMarginAdjustmentEvent{UserId: 1009}}}, "1009"},
		{"customer_risk_limit", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_CustomerRiskLimit{
			CustomerRiskLimit: &eventpb.PerpCustomerRiskLimitEvent{UserId: 1010}}}, "1010"},
		{"invariant_breach", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_InvariantBreach{
			InvariantBreach: &eventpb.PerpInvariantBreachEvent{UserId: 1011}}}, "1011"},
		{"customer_fee", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_CustomerFee{
			CustomerFee: &eventpb.PerpCustomerFeeEvent{UserId: 1012}}}, "1012"},
		// System-level: no user attribution → default partitioner.
		{"risk_pool_settlement", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_RiskPoolSettlement{
			RiskPoolSettlement: &eventpb.RiskPoolSettlementEvent{LotId: "lot-1"}}}, ""},
		{"zero_user_id", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_OrderStatus{
			OrderStatus: &eventpb.PerpOrderStatusEvent{}}}, ""},
		{"empty", &eventpb.PerpJournalEvent{}, ""},
	}
	for _, c := range cases {
		if got := journalPartitionKey(c.evt); got != c.want {
			t.Errorf("%s: journalPartitionKey = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestJournalPartitionKey_OneofExhaustive walks the PerpJournalEvent payload
// oneof by protobuf reflection: every payload carrying a user_id field must
// partition by it (perp_journal.proto header: "Partition key: user_id"), and
// payloads without one must return "" (default partitioner). A payload type
// added to the proto without a matching journalPartitionKey case falls into
// the switch default and fails here — this is the guard against repeating the
// omission that left position_config / margin_adjustment / customer_risk_limit
// / invariant_breach unkeyed.
func TestJournalPartitionKey_OneofExhaustive(t *testing.T) {
	const userID = uint64(7777)
	want := strconv.FormatUint(userID, 10)

	oneof := (&eventpb.PerpJournalEvent{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")
	if oneof == nil {
		t.Fatal("PerpJournalEvent: payload oneof not found")
	}
	fields := oneof.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		evt := &eventpb.PerpJournalEvent{}
		payload := evt.ProtoReflect().Mutable(fd).Message()
		userField := payload.Descriptor().Fields().ByName("user_id")
		if userField == nil {
			if got := journalPartitionKey(evt); got != "" {
				t.Errorf("%s: journalPartitionKey = %q, want \"\" (payload has no user_id)", fd.Name(), got)
			}
			continue
		}
		if userField.Kind() != protoreflect.Uint64Kind {
			t.Fatalf("%s: user_id is %v, want uint64", fd.Name(), userField.Kind())
		}
		payload.Set(userField, protoreflect.ValueOfUint64(userID))
		if got := journalPartitionKey(evt); got != want {
			t.Errorf("%s: journalPartitionKey = %q, want %q — user-attributed payloads must partition by user_id", fd.Name(), got, want)
		}
	}
}
