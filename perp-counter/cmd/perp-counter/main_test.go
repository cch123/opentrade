package main

import "testing"

func TestValidateConfig_ADR0071RequiresCoordinatorForMultipleVShards(t *testing.T) {
	if err := validateConfig(Config{VShardCount: 2}); err == nil {
		t.Fatal("multi-vshard perp-counter must refuse to start without perp-risk")
	}
	if err := validateConfig(Config{VShardCount: 2, RiskCoordinator: true}); err != nil {
		t.Fatalf("coordinator-enabled multi-vshard config rejected: %v", err)
	}
	if err := validateConfig(Config{VShardCount: 1}); err != nil {
		t.Fatalf("single-vshard fallback should remain valid: %v", err)
	}
}
