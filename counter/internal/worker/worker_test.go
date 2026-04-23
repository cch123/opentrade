package worker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xargin/opentrade/counter/internal/clustering"
)

// fakeStore is a BlobStore that always reports "not found" so Run's
// restore path hits the cold-start branch. Real round-trip storage
// tests live in the snapshot package.
type fakeStore struct{}

func (fakeStore) Put(_ context.Context, _ string, _ []byte) error { return nil }
func (fakeStore) Get(_ context.Context, _ string) ([]byte, error) { return nil, os.ErrNotExist }

// baseCfg is a valid Config with all required fields filled so tests
// can override one field at a time to probe validation.
func baseCfg() Config {
	return Config{
		VShardID:    0,
		Epoch:       1,
		NodeID:      "node-A",
		VShardCount: 8,
		Brokers:     []string{"localhost:9092"},
		Store:       fakeStore{},
	}
}

func TestNew_RequiresStore(t *testing.T) {
	c := baseCfg()
	c.Store = nil
	if _, err := New(c); err == nil || !strings.Contains(err.Error(), "Store") {
		t.Fatalf("want Store error, got %v", err)
	}
}

func TestNew_RequiresBrokers(t *testing.T) {
	c := baseCfg()
	c.Brokers = nil
	if _, err := New(c); err == nil || !strings.Contains(err.Error(), "Brokers") {
		t.Fatalf("want Brokers error, got %v", err)
	}
}

func TestNew_RequiresNodeID(t *testing.T) {
	c := baseCfg()
	c.NodeID = ""
	if _, err := New(c); err == nil || !strings.Contains(err.Error(), "NodeID") {
		t.Fatalf("want NodeID error, got %v", err)
	}
}

func TestNew_RequiresVShardCountPositive(t *testing.T) {
	c := baseCfg()
	c.VShardCount = 0
	if _, err := New(c); err == nil || !strings.Contains(err.Error(), "VShardCount") {
		t.Fatalf("want VShardCount error, got %v", err)
	}
}

func TestNew_RejectsVShardIDOutOfRange(t *testing.T) {
	c := baseCfg()
	c.VShardID = clustering.VShardID(8) // VShardCount = 8 → valid range [0, 8)
	_, err := New(c)
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("want out-of-range error, got %v", err)
	}
}

// TestNew_FillsDefaults checks every zero-valued field that the
// constructor is expected to default. If the set of defaults changes
// this test is the reminder to keep the behaviour explicit.
func TestNew_FillsDefaults(t *testing.T) {
	c := baseCfg() // all optional defaults left unset
	w, err := New(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := w.cfg
	if got.JournalTopic != "counter-journal" {
		t.Errorf("JournalTopic = %q", got.JournalTopic)
	}
	if got.TradeEventTopic != "trade-event" {
		t.Errorf("TradeEventTopic = %q", got.TradeEventTopic)
	}
	if got.OrderEventTopic != "order-event" {
		t.Errorf("OrderEventTopic = %q", got.OrderEventTopic)
	}
	if got.DedupTTL != 24*time.Hour {
		t.Errorf("DedupTTL = %v", got.DedupTTL)
	}
	if got.Logger == nil {
		t.Error("Logger should default to zap.NewNop(), got nil")
	}
}

func TestVShardIDAndEpochAccessors(t *testing.T) {
	c := baseCfg()
	c.VShardCount = 256
	c.VShardID = 42
	c.Epoch = 7
	w, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if w.VShardID() != 42 {
		t.Errorf("VShardID = %d, want 42", w.VShardID())
	}
	if w.Epoch() != 7 {
		t.Errorf("Epoch = %d, want 7", w.Epoch())
	}
}

// TestReadyNotClosedBeforeRun: a freshly constructed worker hasn't
// started its lifecycle; Ready() must still be open so dispatchers
// don't accept traffic prematurely.
func TestReadyNotClosedBeforeRun(t *testing.T) {
	w, err := New(baseCfg())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.Ready():
		t.Fatal("Ready() should not be closed before Run() wires the service")
	default:
	}
	if w.Service() != nil {
		t.Error("Service() should be nil before Run() completes restore")
	}
}

// TestFormatConstants pins the strings that land in etcd (snapshot
// key) and Kafka (transactional id). Silent drift here would break
// cross-process recovery or snap fencing, so keep them explicit.
//
// Stability contract (ADR-0058 §4): TransactionalIDFormat is
// vshard-only with no epoch. The Kafka Transaction Coordinator needs
// the same transactional.id across owner changes so a new owner's
// InitProducerID bumps producer_epoch on the same PID and fences the
// previous owner (KIP-98). Epoch lives in etcd assignment + record
// headers, not in this string.
func TestFormatConstants(t *testing.T) {
	if got := fmt.Sprintf(SnapshotKeyFormat, 42); got != "vshard-042" {
		t.Errorf("snapshot key = %q, want vshard-042", got)
	}
	if got := fmt.Sprintf(TransactionalIDFormat, 42); got != "counter-vshard-042" {
		t.Errorf("txn id = %q, want counter-vshard-042", got)
	}
}

// -----------------------------------------------------------------------------
// ADR-0064 M2c StartupMode tests
// -----------------------------------------------------------------------------

// TestStartupMode_StringRoundTrip — every enumerated value surfaces
// a stable wire string AND ParseStartupMode accepts it. Operators
// wire --startup-mode via CLI; drift between ParseStartupMode and
// String would silently mis-route Counter boot.
func TestStartupMode_StringRoundTrip(t *testing.T) {
	cases := []struct {
		mode StartupMode
		name string
	}{
		{StartupModeAuto, "auto"},
		{StartupModeOnDemand, "on-demand"},
	}
	for _, tc := range cases {
		if got := tc.mode.String(); got != tc.name {
			t.Errorf("%d.String() = %q, want %q", tc.mode, got, tc.name)
		}
		parsed, err := ParseStartupMode(tc.name)
		if err != nil {
			t.Errorf("ParseStartupMode(%q) err: %v", tc.name, err)
		}
		if parsed != tc.mode {
			t.Errorf("ParseStartupMode(%q) = %d, want %d", tc.name, parsed, tc.mode)
		}
	}
}

// TestParseStartupMode_EmptyDefaultsAuto pins the CLI affordance:
// unset --startup-mode (or blank env var) should yield Auto rather
// than force operators to type "auto" explicitly.
func TestParseStartupMode_EmptyDefaultsAuto(t *testing.T) {
	got, err := ParseStartupMode("")
	if err != nil {
		t.Fatalf("ParseStartupMode(\"\") err: %v", err)
	}
	if got != StartupModeAuto {
		t.Fatalf("ParseStartupMode(\"\") = %d, want StartupModeAuto(%d)",
			got, StartupModeAuto)
	}
}

// TestParseStartupMode_UnknownErrors — unknown values must be an
// error, not silently downgraded to Auto. Silent downgrade would
// mask typos in deployment configs.
func TestParseStartupMode_UnknownErrors(t *testing.T) {
	cases := []string{"AUTO", "Legacy", "ondemand", "disabled", "xxx"}
	for _, s := range cases {
		_, err := ParseStartupMode(s)
		if err == nil {
			t.Errorf("ParseStartupMode(%q) err = nil, want error", s)
		}
	}
}

// TestParseStartupMode_LegacyRetired pins ADR-0064 M4: the
// previously-supported "legacy" value now returns a directed
// error that tells operators how to reach the legacy recovery
// path through the --trade-dump-endpoint flag instead. Silent
// downgrade to Auto would hide stale deployment configs that
// explicitly asked for legacy.
func TestParseStartupMode_LegacyRetired(t *testing.T) {
	_, err := ParseStartupMode("legacy")
	if err == nil {
		t.Fatal("ParseStartupMode(\"legacy\") must error after M4")
	}
	// Error message should steer operators to the replacement
	// so upgrading isn't a mystery — they just unset
	// --trade-dump-endpoint.
	if !strings.Contains(err.Error(), "trade-dump-endpoint") {
		t.Errorf("error should reference --trade-dump-endpoint: %v", err)
	}
}

// TestStartupMode_StringFallback: future values that haven't been
// added to the String() switch still produce something readable
// instead of Go's empty-string default.
func TestStartupMode_StringFallback(t *testing.T) {
	future := StartupMode(99)
	got := future.String()
	if got == "" {
		t.Fatal("String() returned empty; expected a placeholder")
	}
	if !strings.Contains(got, "99") {
		t.Errorf("String() = %q, want to contain raw number", got)
	}
}
