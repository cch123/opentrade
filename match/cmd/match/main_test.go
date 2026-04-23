package main

import "testing"

func TestOrderConsumerTopics_PerSymbolOwnedOnly(t *testing.T) {
	cfg := Config{OrderTopicPrefix: "order-event", OrderTopic: "order-event"}
	got := orderConsumerTopics([]string{"ETH-USDT", "BTC-USDT"}, cfg)
	want := []string{"order-event-BTC-USDT", "order-event-ETH-USDT"}
	if len(got) != len(want) {
		t.Fatalf("topics = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("topics = %v, want %v", got, want)
		}
	}
}

func TestOrderConsumerTopics_RegexOptOut(t *testing.T) {
	cfg := Config{OrderTopicPrefix: "order-event", OrderTopicRegex: "^order-event-.+$"}
	if got := orderConsumerTopics([]string{"BTC-USDT"}, cfg); got != nil {
		t.Fatalf("regex mode topics = %v, want nil", got)
	}
}

func TestOrderConsumerTopics_LegacySingleTopic(t *testing.T) {
	cfg := Config{OrderTopicPrefix: "", OrderTopic: "order-event"}
	got := orderConsumerTopics([]string{"BTC-USDT", "ETH-USDT"}, cfg)
	if len(got) != 1 || got[0] != "order-event" {
		t.Fatalf("legacy topics = %v, want [order-event]", got)
	}
}
