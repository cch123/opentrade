package journal

import (
	"testing"

	"go.uber.org/zap"
)

func TestNewMarketDataConsumer_Validation(t *testing.T) {
	logger := zap.NewNop()
	book := NewMidBook()
	if _, err := NewMarketDataConsumer(MarketDataConsumerConfig{Topics: []string{"market-data"}}, book, logger); err == nil {
		t.Error("expected error for empty brokers")
	}
	if _, err := NewMarketDataConsumer(MarketDataConsumerConfig{Brokers: []string{"localhost:9092"}}, book, logger); err == nil {
		t.Error("expected error for no topics")
	}
	if _, err := NewMarketDataConsumer(MarketDataConsumerConfig{Brokers: []string{"localhost:9092"}, Topics: []string{"market-data"}}, nil, logger); err == nil {
		t.Error("expected error for nil book")
	}
	c, err := NewMarketDataConsumer(MarketDataConsumerConfig{
		Brokers: []string{"localhost:9092"}, Topics: []string{"market-data", "perp-market-data"},
	}, book, logger)
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	c.Close()
}

func TestNewMarkProducer_Validation(t *testing.T) {
	logger := zap.NewNop()
	if _, err := NewMarkProducer(MarkProducerConfig{ProducerID: "m"}, logger); err == nil {
		t.Error("expected error for empty brokers")
	}
	if _, err := NewMarkProducer(MarkProducerConfig{Brokers: []string{"localhost:9092"}}, logger); err == nil {
		t.Error("expected error for empty ProducerID")
	}
	p, err := NewMarkProducer(MarkProducerConfig{Brokers: []string{"localhost:9092"}, ProducerID: "markprice-0"}, logger)
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	defer p.Close()
	if p.cfg.Topic != "mark-price" {
		t.Errorf("default topic = %q, want mark-price", p.cfg.Topic)
	}
}

func TestFundingRoundID(t *testing.T) {
	if got := FundingRoundID("BTC-USDT-PERP", 1748505600); got != "BTC-USDT-PERP:1748505600" {
		t.Errorf("FundingRoundID = %q", got)
	}
}
