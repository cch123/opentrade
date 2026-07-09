package journal

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

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

func TestEmitPublishFailureNotifiesOwnerAndPanics(t *testing.T) {
	boom := errors.New("broker unavailable")
	var notified error
	p := &Producer{
		cfg:     ProducerConfig{JournalTopic: "perp-journal"},
		logger:  zap.NewNop(),
		onFatal: func(err error) { notified = err },
		publishHook: func(string, string, proto.Message) error {
			return boom
		},
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Emit did not panic on an undurable WAL record")
		}
		if !errors.Is(notified, boom) {
			t.Fatalf("OnFatal error = %v, want wrapped broker error", notified)
		}
		if err := p.Flush(context.Background()); err == nil {
			t.Fatal("Flush accepted a producer after fatal WAL failure")
		}
	}()
	p.Emit(&eventpb.PerpJournalEvent{})
}
