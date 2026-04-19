package snapshot

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

func newLimitOrder(id uint64, user string, side orderbook.Side, price, qty string) *orderbook.Order {
	p := dec.Zero
	if price != "" {
		p = dec.New(price)
	}
	q := dec.New(qty)
	return &orderbook.Order{
		ID:        id,
		UserID:    user,
		Symbol:    "BTC-USDT",
		Side:      side,
		Type:      orderbook.Limit,
		TIF:       orderbook.GTC,
		Price:     p,
		Qty:       q,
		Remaining: q,
		CreatedAt: int64(id),
	}
}

func TestSaveLoadRoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatProto)
}

func TestSaveLoadRoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format Format) {
	base := filepath.Join(t.TempDir(), "snap")
	snap := &SymbolSnapshot{
		Version:     Version,
		Symbol:      "BTC-USDT",
		MatchSeqID:  42,
		Offsets:     []KafkaOffset{{Topic: "order-event", Partition: 0, Offset: 1000}},
		TimestampMS: 1700000000000,
		Orders: []OrderSnapshot{
			{ID: 1, UserID: "u1", Side: 1, Type: 1, TIF: 1, Price: "100", Qty: "1", Remaining: "1", CreatedAt: 1},
		},
	}
	if err := Save(base, snap, format); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(base)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Symbol != snap.Symbol || got.MatchSeqID != snap.MatchSeqID || len(got.Orders) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if got.Orders[0].Price != "100" {
		t.Fatalf("order price = %s, want 100", got.Orders[0].Price)
	}
}

// TestLoad_JSONOnlyMigration verifies Load still reads legacy .json files
// when only that format is present (ADR-0049 upgrade window).
func TestLoad_JSONOnlyMigration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "snap")
	snap := &SymbolSnapshot{Version: Version, Symbol: "BTC-USDT", MatchSeqID: 7}
	if err := Save(base, snap, FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.MatchSeqID != 7 {
		t.Fatalf("json-only load: got match_seq=%d", got.MatchSeqID)
	}
}

func TestCaptureAndRestoreWorker(t *testing.T) {
	outbox := make(chan *sequencer.Output, 32)
	w := sequencer.NewSymbolWorker(sequencer.Config{Symbol: "BTC-USDT", Inbox: 8}, outbox)

	// Seed with a mixed book.
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	seeds := []*orderbook.Order{
		newLimitOrder(1, "u1", orderbook.Bid, "100", "1"),
		newLimitOrder(2, "u2", orderbook.Bid, "101", "2"),
		newLimitOrder(3, "u3", orderbook.Ask, "200", "1"),
		newLimitOrder(4, "u4", orderbook.Bid, "100", "3"),
	}
	for _, o := range seeds {
		w.Submit(&sequencer.Event{Kind: sequencer.EventOrderPlaced, Order: o})
	}
	// Wait for inbox to drain. Read book size under the worker lock so we
	// don't race with in-flight handle() goroutines (ADR-0048).
	for i := 0; i < 100; i++ {
		var n int
		w.WithStateLocked(func(book *orderbook.Book, _ uint64, _ map[int32]int64) {
			n = book.Len()
		})
		if n == len(seeds) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-w.Done()

	// Seed a partition offset so round-trip verifies persistence.
	w.SetOffsets(map[int32]int64{0: 42})

	// Capture state.
	snap := Capture(w, 0)

	// Persist and reload to file to exercise serialization (default proto).
	base := filepath.Join(t.TempDir(), "btc")
	if err := Save(base, snap, FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}

	// Restore into a fresh worker.
	outbox2 := make(chan *sequencer.Output, 32)
	w2 := sequencer.NewSymbolWorker(sequencer.Config{Symbol: "BTC-USDT", Inbox: 8}, outbox2)
	if err := Restore(w2, loaded); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify state equivalence.
	if w2.Book().Len() != len(seeds) {
		t.Fatalf("restored book size = %d, want %d", w2.Book().Len(), len(seeds))
	}
	if w2.MatchSeq() != w.MatchSeq() {
		t.Fatalf("match seq mismatch: %d vs %d", w2.MatchSeq(), w.MatchSeq())
	}
	// Time-priority must survive: best bid should be the earliest order at 101.
	best, _ := w2.Book().Best(orderbook.Bid)
	if best.ID != 2 {
		t.Fatalf("best bid id = %d, want 2 (preserved time priority)", best.ID)
	}
	// Offsets must survive round-trip.
	if off := w2.Offsets(); len(off) != 1 || off[0] != 42 {
		t.Fatalf("restored offsets = %v, want {0:42}", off)
	}
	// At 100, time priority: 1 before 4.
	var atLevel100 []uint64
	w2.Book().Walk(orderbook.Bid, func(o *orderbook.Order) bool {
		if o.Price.Cmp(dec.New("100")) == 0 {
			atLevel100 = append(atLevel100, o.ID)
		}
		return true
	})
	if len(atLevel100) != 2 || atLevel100[0] != 1 || atLevel100[1] != 4 {
		t.Fatalf("time priority at 100: %v, want [1 4]", atLevel100)
	}
}

func TestRestoreRejectsNonEmptyWorker(t *testing.T) {
	outbox := make(chan *sequencer.Output, 8)
	w := sequencer.NewSymbolWorker(sequencer.Config{Symbol: "BTC-USDT", Inbox: 4}, outbox)
	// Pre-populate.
	if err := w.Book().Insert(newLimitOrder(1, "u1", orderbook.Bid, "100", "1")); err != nil {
		t.Fatal(err)
	}
	snap := &SymbolSnapshot{Version: Version, Symbol: "BTC-USDT"}
	if err := Restore(w, snap); err == nil {
		t.Fatal("expected error restoring into non-empty worker")
	}
}
