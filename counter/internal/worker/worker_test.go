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
	if got.SnapshotFlushTimeout != defaultSnapshotFlushTimeout {
		t.Errorf("SnapshotFlushTimeout = %v", got.SnapshotFlushTimeout)
	}
	if got.ShutdownFlushTimeout != defaultShutdownFlushTimeout {
		t.Errorf("ShutdownFlushTimeout = %v", got.ShutdownFlushTimeout)
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
