package orderbook

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func mkOrder(id uint64, user string, side Side, price, qty string) *Order {
	p := dec.Zero
	if price != "" {
		p = dec.New(price)
	}
	q := dec.New(qty)
	return &Order{
		ID:        id,
		UserID:    user,
		Symbol:    "BTC-USDT",
		Side:      side,
		Type:      Limit,
		TIF:       GTC,
		Price:     p,
		Qty:       q,
		Remaining: q,
		CreatedAt: int64(id),
	}
}

func TestInsertAndBest(t *testing.T) {
	b := NewBook("BTC-USDT")

	if _, ok := b.Best(Bid); ok {
		t.Fatal("expected empty bid side")
	}
	if _, ok := b.Best(Ask); ok {
		t.Fatal("expected empty ask side")
	}

	o1 := mkOrder(1, "u1", Bid, "100", "1")
	o2 := mkOrder(2, "u2", Bid, "101", "1")
	o3 := mkOrder(3, "u3", Bid, "99", "1")
	for _, o := range []*Order{o1, o2, o3} {
		if err := b.Insert(o); err != nil {
			t.Fatalf("Insert %d: %v", o.ID, err)
		}
	}
	best, ok := b.Best(Bid)
	if !ok || best.ID != 2 {
		t.Fatalf("best bid id=%d ok=%v, want 2", best.ID, ok)
	}

	o4 := mkOrder(4, "u4", Ask, "200", "1")
	o5 := mkOrder(5, "u5", Ask, "199", "1")
	for _, o := range []*Order{o4, o5} {
		if err := b.Insert(o); err != nil {
			t.Fatalf("Insert %d: %v", o.ID, err)
		}
	}
	best, ok = b.Best(Ask)
	if !ok || best.ID != 5 {
		t.Fatalf("best ask id=%d ok=%v, want 5", best.ID, ok)
	}
}

func TestTimePriorityWithinLevel(t *testing.T) {
	b := NewBook("BTC-USDT")
	a := mkOrder(1, "u1", Bid, "100", "1")
	c := mkOrder(2, "u2", Bid, "100", "1")
	if err := b.Insert(a); err != nil {
		t.Fatal(err)
	}
	if err := b.Insert(c); err != nil {
		t.Fatal(err)
	}
	best, _ := b.Best(Bid)
	if best.ID != 1 {
		t.Fatalf("time priority broken: head id=%d want 1", best.ID)
	}
	if _, err := b.Cancel(1); err != nil {
		t.Fatal(err)
	}
	best, _ = b.Best(Bid)
	if best.ID != 2 {
		t.Fatalf("after cancel, head id=%d want 2", best.ID)
	}
}

func TestCancelRemovesLevelWhenEmpty(t *testing.T) {
	b := NewBook("BTC-USDT")
	o := mkOrder(1, "u1", Bid, "100", "1")
	if err := b.Insert(o); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Cancel(1); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Best(Bid); ok {
		t.Fatal("expected empty side after canceling only order")
	}
	if !b.bids.empty() {
		t.Fatal("bids sideBook should be empty")
	}
}

func TestDuplicateInsert(t *testing.T) {
	b := NewBook("BTC-USDT")
	o := mkOrder(1, "u1", Bid, "100", "1")
	if err := b.Insert(o); err != nil {
		t.Fatal(err)
	}
	if err := b.Insert(o); err != ErrDuplicateOrderID {
		t.Fatalf("expected ErrDuplicateOrderID, got %v", err)
	}
}

func TestDepth(t *testing.T) {
	b := NewBook("BTC-USDT")
	orders := []*Order{
		mkOrder(1, "u1", Bid, "100", "1"),
		mkOrder(2, "u2", Bid, "100", "2"),
		mkOrder(3, "u3", Bid, "99", "3"),
	}
	for _, o := range orders {
		if err := b.Insert(o); err != nil {
			t.Fatal(err)
		}
	}
	d := b.Depth(Bid)
	if d.String() != "6" {
		t.Fatalf("Depth = %s, want 6", d.String())
	}
}

func TestFillPartial(t *testing.T) {
	b := NewBook("BTC-USDT")
	o := mkOrder(1, "u1", Bid, "100", "5")
	if err := b.Insert(o); err != nil {
		t.Fatal(err)
	}
	maker, full, err := b.Fill(1, dec.New("2"))
	if err != nil {
		t.Fatal(err)
	}
	if full {
		t.Fatal("expected partial fill")
	}
	if maker.Remaining.String() != "3" {
		t.Fatalf("remaining = %s, want 3", maker.Remaining.String())
	}
	if b.LevelQty(Bid, dec.New("100")).String() != "3" {
		t.Fatalf("level qty = %s, want 3", b.LevelQty(Bid, dec.New("100")).String())
	}
}

func TestFillFull(t *testing.T) {
	b := NewBook("BTC-USDT")
	o := mkOrder(1, "u1", Bid, "100", "5")
	if err := b.Insert(o); err != nil {
		t.Fatal(err)
	}
	_, full, err := b.Fill(1, dec.New("5"))
	if err != nil {
		t.Fatal(err)
	}
	if !full {
		t.Fatal("expected full fill")
	}
	if b.Len() != 0 {
		t.Fatalf("book not empty: %d orders", b.Len())
	}
	if _, ok := b.Best(Bid); ok {
		t.Fatal("bid side should be empty")
	}
}

func TestFillOverflow(t *testing.T) {
	b := NewBook("BTC-USDT")
	o := mkOrder(1, "u1", Bid, "100", "5")
	_ = b.Insert(o)
	if _, _, err := b.Fill(1, dec.New("6")); err != ErrFillExceedsQty {
		t.Fatalf("expected ErrFillExceedsQty, got %v", err)
	}
}

func TestWalkPriority(t *testing.T) {
	b := NewBook("BTC-USDT")
	// Two levels, with multiple orders per level.
	orders := []*Order{
		mkOrder(1, "u1", Bid, "100", "1"),
		mkOrder(2, "u2", Bid, "101", "1"),
		mkOrder(3, "u3", Bid, "100", "1"),
		mkOrder(4, "u4", Bid, "101", "1"),
	}
	for _, o := range orders {
		_ = b.Insert(o)
	}
	// Priority: 2, 4 (price 101 FIFO), then 1, 3 (price 100 FIFO).
	want := []uint64{2, 4, 1, 3}
	var got []uint64
	b.Walk(Bid, func(o *Order) bool {
		got = append(got, o.ID)
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("walk produced %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk order[%d] = %d, want %d (full=%v)", i, got[i], want[i], got)
		}
	}
}
