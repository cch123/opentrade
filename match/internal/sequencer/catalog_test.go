package sequencer

// catalog_test.go covers the ADR-0075 Match-side handshake: the four-way
// config_version branch, the orderbook-scope status gates, the same-version
// precision/price-bound re-checks, and fail-closed behavior on a missing or
// stale catalog.

import (
	"sync"
	"testing"
	"time"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
)

const perpSym = "BTC-USDT-PERP"

// fakeCatalog implements ConfigLookup with a settable view. Mutex-guarded
// like the real perpcfg.Cache — tests mutate it while the worker goroutine
// reads.
type fakeCatalog struct {
	mu    sync.Mutex
	view  perpcfg.View
	has   bool
	stale bool
}

func (f *fakeCatalog) Active(string) (perpcfg.View, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view, f.has
}

func (f *fakeCatalog) Stale() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stale
}

func (f *fakeCatalog) setCfg(cfg *perpcfg.PerpSymbolConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.view.Cfg = cfg
}

func catalogAt(version uint64, status perpcfg.Status) *fakeCatalog {
	return &fakeCatalog{
		has: true,
		view: perpcfg.View{
			Spec: perpcfg.PerpSymbol{Symbol: perpSym},
			Cfg: &perpcfg.PerpSymbolConfig{
				Symbol: perpSym, ConfigVersion: version, Status: status,
				Precision: perpcfg.Precision{TickSize: dec.New("0.5"), QtyStep: dec.New("0.001")},
				OrderLimits: perpcfg.OrderLimits{
					MinPrice: dec.New("1"), MaxPrice: dec.New("1000000"),
				},
			},
		},
	}
}

func newPerpOrder(id, user uint64, side orderbook.Side, price, qty string, tif orderbook.TIF) *orderbook.Order {
	q := dec.New(qty)
	return &orderbook.Order{
		ID: id, UserID: user, Symbol: perpSym, Side: side,
		Type: orderbook.Limit, TIF: tif,
		Price: dec.New(price), Qty: q, Remaining: q, CreatedAt: int64(id),
	}
}

// submitAndCollect drives one stamped order through a worker wired to cat
// and returns the emissions.
func submitAndCollect(t *testing.T, cat ConfigLookup, evt *Event) []*Output {
	t.Helper()
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: perpSym, Inbox: 8, Catalog: cat}, outbox, nil)
	w.Submit(evt)
	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	return collect()
}

func placedEvt(o *orderbook.Order, version uint64) *Event {
	return &Event{Kind: EventOrderPlaced, Symbol: perpSym, Order: o, ConfigVersion: version}
}

func TestHandshakeFourWay(t *testing.T) {
	cases := []struct {
		name         string
		localVersion uint64
		hasSymbol    bool
		orderVersion uint64
		want         orderbook.RejectReason
	}{
		{"equal versions match", 3, true, 3, orderbook.RejectNone},
		{"local behind order", 2, true, 3, orderbook.RejectConfigVersionTooNew},
		{"local ahead of order", 4, true, 3, orderbook.RejectStaleOrderConfig},
		{"symbol unknown", 0, false, 3, orderbook.RejectUnknownSymbolConfig},
	}
	for _, tc := range cases {
		cat := catalogAt(tc.localVersion, perpcfg.StatusTrading)
		cat.has = tc.hasSymbol
		got := submitAndCollect(t, cat, placedEvt(newPerpOrder(1, 7, orderbook.Bid, "100", "1", orderbook.GTC), tc.orderVersion))
		if len(got) != 1 {
			t.Fatalf("%s: emissions = %d", tc.name, len(got))
		}
		if tc.want == orderbook.RejectNone {
			if got[0].Kind != OutputOrderAccepted {
				t.Errorf("%s: kind = %v reason=%v, want accepted", tc.name, got[0].Kind, got[0].RejectReason)
			}
			continue
		}
		if got[0].Kind != OutputOrderRejected || got[0].RejectReason != tc.want {
			t.Errorf("%s: kind=%v reason=%v, want reject %v", tc.name, got[0].Kind, got[0].RejectReason, tc.want)
		}
	}
}

func TestHandshakeSkippedForSpotOrders(t *testing.T) {
	// ConfigVersion 0 = spot order: no catalog at all must still accept.
	o := newPerpOrder(1, 7, orderbook.Bid, "100.3", "1", orderbook.GTC) // off-grid price — proves checks skipped
	got := submitAndCollect(t, nil, placedEvt(o, 0))
	if len(got) != 1 || got[0].Kind != OutputOrderAccepted {
		t.Fatalf("spot order: %+v", got)
	}
}

func TestHandshakeFailClosedWithoutCatalog(t *testing.T) {
	// A stamped order on a worker with no catalog wired is a deployment
	// error — reject, never match blind.
	got := submitAndCollect(t, nil, placedEvt(newPerpOrder(1, 7, orderbook.Bid, "100", "1", orderbook.GTC), 1))
	if len(got) != 1 || got[0].RejectReason != orderbook.RejectUnknownSymbolConfig {
		t.Fatalf("no catalog: %+v", got)
	}
	// Same when the cache is stale beyond its budget.
	cat := catalogAt(1, perpcfg.StatusTrading)
	cat.stale = true
	got = submitAndCollect(t, cat, placedEvt(newPerpOrder(2, 7, orderbook.Bid, "100", "1", orderbook.GTC), 1))
	if len(got) != 1 || got[0].RejectReason != orderbook.RejectUnknownSymbolConfig {
		t.Fatalf("stale catalog: %+v", got)
	}
}

func TestStatusGatesAtBook(t *testing.T) {
	cases := []struct {
		status perpcfg.Status
		tif    orderbook.TIF
		want   orderbook.RejectReason
	}{
		{perpcfg.StatusTrading, orderbook.GTC, orderbook.RejectNone},
		{perpcfg.StatusPostOnly, orderbook.GTC, orderbook.RejectSymbolStatusForbids},
		{perpcfg.StatusPostOnly, orderbook.PostOnly, orderbook.RejectNone},
		{perpcfg.StatusCancelOnly, orderbook.GTC, orderbook.RejectSymbolStatusForbids},
		// PRE_DELIVERY: reduce-only is the counter's gate; the book accepts.
		{perpcfg.StatusPreDelivery, orderbook.GTC, orderbook.RejectNone},
		{perpcfg.StatusSettling, orderbook.GTC, orderbook.RejectSymbolStatusForbids},
	}
	for _, tc := range cases {
		got := submitAndCollect(t, catalogAt(1, tc.status),
			placedEvt(newPerpOrder(1, 7, orderbook.Bid, "100", "1", tc.tif), 1))
		if len(got) != 1 {
			t.Fatalf("%s: emissions = %d", tc.status, len(got))
		}
		if tc.want == orderbook.RejectNone {
			if got[0].Kind != OutputOrderAccepted {
				t.Errorf("%s/%v: got %v reason=%v, want accept", tc.status, tc.tif, got[0].Kind, got[0].RejectReason)
			}
		} else if got[0].RejectReason != tc.want {
			t.Errorf("%s/%v: reason=%v, want %v", tc.status, tc.tif, got[0].RejectReason, tc.want)
		}
	}
}

func TestSameVersionPrecisionRecheck(t *testing.T) {
	cat := catalogAt(1, perpcfg.StatusTrading)
	cases := []struct {
		price, qty string
		want       orderbook.RejectReason
	}{
		{"100.3", "1", orderbook.RejectInvalidPriceTick},
		{"100", "1.0005", orderbook.RejectInvalidLotSize},
		{"0.5", "1", orderbook.RejectPriceOutOfRange},     // below min_price (on grid)
		{"2000000", "1", orderbook.RejectPriceOutOfRange}, // above max_price
		{"100", "1", orderbook.RejectNone},
	}
	for i, tc := range cases {
		got := submitAndCollect(t, cat,
			placedEvt(newPerpOrder(uint64(i+1), 7, orderbook.Bid, tc.price, tc.qty, orderbook.GTC), 1))
		if tc.want == orderbook.RejectNone {
			if got[0].Kind != OutputOrderAccepted {
				t.Errorf("price=%s qty=%s: %v reason=%v", tc.price, tc.qty, got[0].Kind, got[0].RejectReason)
			}
		} else if got[0].RejectReason != tc.want {
			t.Errorf("price=%s qty=%s: reason=%v want %v", tc.price, tc.qty, got[0].RejectReason, tc.want)
		}
	}
}

func TestCancelsBypassStatusGate(t *testing.T) {
	// A resting order placed in TRADING must remain cancellable after the
	// symbol flips to SETTLING: book cleanup (liquidation / delivery flows)
	// depends on cancels never being status-blocked at Match.
	outbox := make(chan *Output, 16)
	cat := catalogAt(1, perpcfg.StatusTrading)
	w := NewSymbolWorker(Config{Symbol: perpSym, Inbox: 8, Catalog: cat}, outbox, nil)
	w.Submit(placedEvt(newPerpOrder(1, 7, orderbook.Bid, "100", "1", orderbook.GTC), 1))
	collect := runWorker(t, w, outbox)
	time.Sleep(10 * time.Millisecond)

	next := *cat.view.Cfg
	next.Status = perpcfg.StatusSettling
	next.ConfigVersion = 2
	cat.setCfg(&next)

	w.Submit(&Event{Kind: EventOrderCancel, Symbol: perpSym, OrderID: 1, UserID: 7})
	time.Sleep(10 * time.Millisecond)
	got := collect()
	if len(got) != 2 || got[1].Kind != OutputOrderCancelled || got[1].OrderID != 1 {
		t.Fatalf("cancel in SETTLING: %+v", got)
	}
}
