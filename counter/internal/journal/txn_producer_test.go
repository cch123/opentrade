package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

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

// -----------------------------------------------------------------------------
// ADR-0064 M2a ProduceFenceSentinel tests
// -----------------------------------------------------------------------------

// TestBuildFenceSentinelEvent_ShapeAndContract locks the on-wire
// shape of the startup sentinel envelope:
//   - Payload variant is StartupFenceEvent (oneof tag 52)
//   - CounterSeqId is 0 (sentinels do NOT allocate counter_seq —
//     apply contract in ADR-0064 §1.2 relies on this)
//   - node_id / epoch / ts_ms are carried verbatim from the
//     producer config, not re-computed at apply time
//
// If this shape ever changes, the M1a apply-side tests in
// counter/engine + trade-dump/shadow need to move in lockstep —
// shadow Apply gates the "unknown oneof variant" path on a specific
// tag number, and counterSeq advancement explicitly skips
// CounterSeqId=0 so the sentinel doesn't corrupt counter_seq
// monotonicity.
func TestBuildFenceSentinelEvent_ShapeAndContract(t *testing.T) {
	evt := buildFenceSentinelEvent("counter-node-A", 7, 1_700_000_000_000)
	if evt == nil {
		t.Fatal("nil event")
	}
	if evt.CounterSeqId != 0 {
		t.Errorf("CounterSeqId = %d, want 0 (sentinel contract)", evt.CounterSeqId)
	}
	fence, ok := evt.Payload.(*eventpb.CounterJournalEvent_StartupFence)
	if !ok {
		t.Fatalf("payload = %T, want *CounterJournalEvent_StartupFence", evt.Payload)
	}
	if fence.StartupFence == nil {
		t.Fatal("StartupFence = nil")
	}
	if fence.StartupFence.NodeId != "counter-node-A" {
		t.Errorf("NodeId = %q, want counter-node-A", fence.StartupFence.NodeId)
	}
	if fence.StartupFence.Epoch != 7 {
		t.Errorf("Epoch = %d, want 7", fence.StartupFence.Epoch)
	}
	if fence.StartupFence.TsMs != 1_700_000_000_000 {
		t.Errorf("TsMs = %d, want 1_700_000_000_000", fence.StartupFence.TsMs)
	}
}

// TestBuildFenceSentinelEvent_ZeroValuesAccepted documents that a
// zero node_id / epoch are legal at the proto layer — they are
// informational fields for audit, not correctness fields. Guards
// against a future refactor that starts rejecting zero values and
// breaks tests / dev setups without writer metadata wired.
func TestBuildFenceSentinelEvent_ZeroValuesAccepted(t *testing.T) {
	evt := buildFenceSentinelEvent("", 0, 0)
	if evt.CounterSeqId != 0 {
		t.Errorf("CounterSeqId = %d, want 0", evt.CounterSeqId)
	}
	fence := evt.Payload.(*eventpb.CounterJournalEvent_StartupFence).StartupFence
	if fence.NodeId != "" || fence.Epoch != 0 || fence.TsMs != 0 {
		t.Errorf("zero fields mutated: %+v", fence)
	}
}

// TestFenceSentinelKey locks the audit key format used on every
// sentinel record. Dashboards and `kcat | grep` workflows depend on
// "vshard-%03d-startup-fence" — changing it silently rotates the
// grep pattern out from under operators.
func TestFenceSentinelKey(t *testing.T) {
	cases := []struct {
		vshard int32
		want   string
	}{
		{0, "vshard-000-startup-fence"},
		{5, "vshard-005-startup-fence"},
		{42, "vshard-042-startup-fence"},
		{255, "vshard-255-startup-fence"},
	}
	for _, tc := range cases {
		if got := fenceSentinelKey(tc.vshard); got != tc.want {
			t.Errorf("fenceSentinelKey(%d) = %q, want %q", tc.vshard, got, tc.want)
		}
	}
}
