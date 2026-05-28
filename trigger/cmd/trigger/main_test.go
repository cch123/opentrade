package main

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

// startSymbolRegistry must degrade to compat mode (a nil SymbolLookup, so
// only the default cap applies) when the ADR-0054 per-symbol cap lookup is
// unconfigured or misconfigured — never refuse triggers because etcd wiring
// is absent. Mirrors counter's compat-mode fallback. The configured-and-
// reachable path dials a live etcd and is exercised by integration, not here.
func TestStartSymbolRegistry_CompatModeWhenUnconfigured(t *testing.T) {
	logger := zap.NewNop()
	cases := []struct {
		name string
		cfg  Config
	}{
		{"empty prefix disables lookup", Config{EtcdSymbolPrefix: ""}},
		{"prefix set but no etcd endpoints", Config{EtcdSymbolPrefix: "/cex/match/symbols/"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := startSymbolRegistry(context.Background(), tc.cfg, logger); got != nil {
				t.Fatal("expected nil lookup (compat mode), got non-nil")
			}
		})
	}
}
