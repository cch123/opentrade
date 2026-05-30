package journal

import (
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// nopHandler satisfies TradeHandler for constructor tests.
type nopHandler struct{}

func (nopHandler) HandleTradeEvent(*eventpb.TradeEvent, int32, int64) {}

func TestNewProducer_RejectsEmptyBrokers(t *testing.T) {
	if _, err := NewProducer(ProducerConfig{}, zap.NewNop()); err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestNewProducer_DefaultsAndConstructs(t *testing.T) {
	// Constructing a client does not dial — it validates config and is enough
	// to exercise the default-topic wiring without a live broker.
	p, err := NewProducer(ProducerConfig{Brokers: []string{"localhost:9092"}}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if p.cfg.OrderEventTopicPrefix != "order-event" {
		t.Errorf("default order prefix = %q", p.cfg.OrderEventTopicPrefix)
	}
	if p.cfg.JournalTopic != "perp-journal" {
		t.Errorf("default journal topic = %q", p.cfg.JournalTopic)
	}
	if p.transactional {
		t.Error("empty TransactionalID should stay idempotent")
	}
	if got := p.orderTopic("BTC-USDT-PERP"); got != "order-event-BTC-USDT-PERP" {
		t.Errorf("orderTopic = %q", got)
	}
}

func TestNewProducer_TransactionalMode(t *testing.T) {
	p, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:9092"}, TransactionalID: "perp-counter-0",
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if !p.transactional {
		t.Error("non-empty TransactionalID should enable transactional mode")
	}
}

func TestNewTradeConsumer_Validation(t *testing.T) {
	logger := zap.NewNop()
	if _, err := NewTradeConsumer(TradeConsumerConfig{GroupID: "g", Topic: "t"}, nopHandler{}, logger); err == nil {
		t.Error("expected error for empty brokers")
	}
	if _, err := NewTradeConsumer(TradeConsumerConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}, nopHandler{}, logger); err == nil {
		t.Error("expected error for empty group")
	}
	if _, err := NewTradeConsumer(TradeConsumerConfig{Brokers: []string{"localhost:9092"}, GroupID: "g"}, nil, logger); err == nil {
		t.Error("expected error for nil handler")
	}
}

func TestNewTradeConsumer_DefaultsTopic(t *testing.T) {
	c, err := NewTradeConsumer(TradeConsumerConfig{
		Brokers: []string{"localhost:9092"}, GroupID: "perp-counter",
	}, nopHandler{}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTradeConsumer: %v", err)
	}
	defer c.Close()
	if c.topic != "perp-trade-event" {
		t.Errorf("default topic = %q, want perp-trade-event", c.topic)
	}
}
