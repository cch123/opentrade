package journal

import "testing"

// TestOrderEventTopicFor covers the ADR-0050 per-symbol topic routing plus
// the legacy single-topic fallback when OrderEventTopicPrefix is empty.
func TestOrderEventTopicFor(t *testing.T) {
	t.Run("per-symbol prefix wins", func(t *testing.T) {
		p := &TxnProducer{cfg: TxnProducerConfig{
			OrderEventTopicPrefix: "order-event",
			OrderEventTopic:       "order-event-legacy",
		}}
		if got := p.orderEventTopicFor("BTC-USDT"); got != "order-event-BTC-USDT" {
			t.Fatalf("prefix path: got %q, want order-event-BTC-USDT", got)
		}
	})

	t.Run("legacy single topic when prefix empty", func(t *testing.T) {
		p := &TxnProducer{cfg: TxnProducerConfig{
			OrderEventTopic: "order-event",
		}}
		if got := p.orderEventTopicFor("BTC-USDT"); got != "order-event" {
			t.Fatalf("legacy path: got %q, want order-event", got)
		}
	})

	t.Run("empty symbol falls back to legacy", func(t *testing.T) {
		// Shouldn't happen in production (symbol is required on the order
		// path), but guard against bad fixtures: empty symbol must not
		// produce "order-event-" with a trailing dash.
		p := &TxnProducer{cfg: TxnProducerConfig{
			OrderEventTopicPrefix: "order-event",
			OrderEventTopic:       "order-event",
		}}
		if got := p.orderEventTopicFor(""); got != "order-event" {
			t.Fatalf("empty symbol: got %q, want order-event", got)
		}
	})

	t.Run("custom prefix", func(t *testing.T) {
		p := &TxnProducer{cfg: TxnProducerConfig{
			OrderEventTopicPrefix: "oe",
		}}
		if got := p.orderEventTopicFor("ETH-USDT"); got != "oe-ETH-USDT" {
			t.Fatalf("custom prefix: got %q, want oe-ETH-USDT", got)
		}
	})
}
