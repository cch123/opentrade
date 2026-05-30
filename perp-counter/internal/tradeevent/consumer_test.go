package tradeevent

import (
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// nopHandler satisfies Handler for constructor tests without pulling in the
// full service. The consumer package owns only stream decoding and routing.
type nopHandler struct{}

func (nopHandler) HandleTradeEvent(*eventpb.TradeEvent, int32, int64) {}

func TestNewConsumer_Validation(t *testing.T) {
	logger := zap.NewNop()
	if _, err := NewConsumer(ConsumerConfig{GroupID: "g", Topic: "t"}, nopHandler{}, logger); err == nil {
		t.Error("expected error for empty brokers")
	}
	if _, err := NewConsumer(ConsumerConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}, nopHandler{}, logger); err == nil {
		t.Error("expected error for empty group")
	}
	if _, err := NewConsumer(ConsumerConfig{Brokers: []string{"localhost:9092"}, GroupID: "g"}, nil, logger); err == nil {
		t.Error("expected error for nil handler")
	}
}

func TestNewConsumer_DefaultsTopic(t *testing.T) {
	c, err := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:9092"}, GroupID: "perp-counter",
	}, nopHandler{}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close()
	if c.topic != "perp-trade-event" {
		t.Errorf("default topic = %q, want perp-trade-event", c.topic)
	}
}
