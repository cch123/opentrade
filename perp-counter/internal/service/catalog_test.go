package service

// catalog_test.go covers the ADR-0075 admission + stamping integration:
// status-machine gates, precision/limit checks against the captured view,
// fail-closed on unknown symbol / stale cache, config_version stamping on
// OrderEvent + settlement/funding/liquidation journal records, and the
// per-symbol risk model replacing the global flags.

import (
	"context"
	"testing"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
)

const catSym = "BTC-USDT-PERP"

type catalogFixture struct {
	store *perpcfg.MemoryStore
	cache *perpcfg.Cache
	clock *fakeCatClock
}

type fakeCatClock struct{ now time.Time }

func (f *fakeCatClock) Now() time.Time { return f.now }

func newCatalogFixture(t *testing.T, status perpcfg.Status) *catalogFixture {
	t.Helper()
	store := perpcfg.NewMemoryStore()
	clock := &fakeCatClock{now: time.UnixMilli(1_700_000_000_000)}
	spec := perpcfg.PerpSymbol{
		Symbol: catSym, ContractType: perpcfg.ContractLinearPerp,
		BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: "USDT",
		ContractSize: dec.FromInt(1), PriceScale: 2, QtyScale: 3,
	}
	cfg := perpcfg.PerpSymbolConfig{
		Symbol: catSym, Status: status,
		Precision: perpcfg.Precision{TickSize: dec.New("0.5"), QtyStep: dec.New("0.001")},
		OrderLimits: perpcfg.OrderLimits{
			MinOrderQty: dec.New("0.001"), MaxOrderQty: dec.New("1000"), MinNotional: dec.New("5"),
		},
		RiskTiers: []perpcfg.RiskTier{
			{RiskID: 1, MaxNotional: dec.New("50000"), MaintMarginRatio: dec.New("0.005"),
				MaxLeverage: dec.New("100"), LiqFeeRate: dec.New("0.001")},
			{RiskID: 2, MaxNotional: dec.FromInt(0), MaintMarginRatio: dec.New("0.01"),
				MaxLeverage: dec.New("50"), LiqFeeRate: dec.New("0.002")},
		},
		Funding: perpcfg.FundingParams{IntervalSeconds: 28800, InterestRate: dec.New("0.0003"),
			Cap: dec.New("0.0075"), Floor: dec.New("-0.0075"), Clamp: dec.New("0.0005")},
		Pricing: perpcfg.PricingParams{MarkEmaAlpha: dec.New("0.1"),
			ImpactNotional: dec.New("20000"), IndexDeviationBand: dec.New("0.05")},
		Fees:      perpcfg.FeeParams{MakerFeeRate: dec.New("0.0002"), TakerFeeRate: dec.New("0.00055")},
		RiskApply: perpcfg.RiskApplyStaged,
	}
	if err := store.CreateSymbol(context.Background(), spec, cfg); err != nil {
		t.Fatalf("create symbol: %v", err)
	}
	cache := perpcfg.NewCache(perpcfg.CacheConfig{
		Store: store, MaxStaleness: 10 * time.Second, Clock: clock.Now,
	})
	if err := cache.Load(context.Background()); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	return &catalogFixture{store: store, cache: cache, clock: clock}
}

func (f *catalogFixture) publish(t *testing.T, mutate func(*perpcfg.PerpSymbolConfig)) uint64 {
	t.Helper()
	versions, err := f.store.ListVersions(context.Background(), catSym)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	next := *versions[len(versions)-1]
	mutate(&next)
	ver, err := f.store.PublishConfig(context.Background(), next)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := f.cache.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	return ver
}

func newCatalogSvc(t *testing.T, status perpcfg.Status) (*Service, *engine.Engine, *fakeDispatcher, *fakeJournal, *catalogFixture) {
	t.Helper()
	fix := newCatalogFixture(t, status)
	eng := engine.New()
	disp := &fakeDispatcher{}
	jr := &fakeJournal{}
	var id uint64
	svc := New(eng, disp, jr, func() uint64 { id++; return id },
		Config{ProducerID: "perp-shard-0", Catalog: fix.cache,
			Clock: func() time.Time { return fix.clock.now }})
	return svc, eng, disp, jr, fix
}

func TestCatalogAdmissionStatusGates(t *testing.T) {
	cases := []struct {
		status     perpcfg.Status
		tif        eventpb.TimeInForce
		wantReason string
	}{
		{perpcfg.StatusTrading, eventpb.TimeInForce_TIME_IN_FORCE_GTC, ""},
		{perpcfg.StatusPostOnly, eventpb.TimeInForce_TIME_IN_FORCE_GTC, perpcfg.RejectSymbolStatus},
		{perpcfg.StatusPostOnly, eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, ""},
		{perpcfg.StatusCancelOnly, eventpb.TimeInForce_TIME_IN_FORCE_GTC, perpcfg.RejectSymbolStatus},
		{perpcfg.StatusPreopen, eventpb.TimeInForce_TIME_IN_FORCE_GTC, perpcfg.RejectSymbolStatus},
		{perpcfg.StatusDelisted, eventpb.TimeInForce_TIME_IN_FORCE_GTC, perpcfg.RejectSymbolStatus},
	}
	for _, tc := range cases {
		svc, eng, _, _, _ := newCatalogSvc(t, tc.status)
		eng.Deposit(user1, dec.New("10000"))
		req := placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false)
		req.Tif = tc.tif
		resp, err := svc.PlaceOrder(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.status, err)
		}
		if tc.wantReason == "" && !resp.Accepted {
			t.Errorf("%s/%v: want accept, got reject %s", tc.status, tc.tif, resp.RejectReason)
		}
		if tc.wantReason != "" && (resp.Accepted || resp.RejectReason != tc.wantReason) {
			t.Errorf("%s/%v: want %s, got accepted=%v reason=%s",
				tc.status, tc.tif, tc.wantReason, resp.Accepted, resp.RejectReason)
		}
	}
}

func TestCatalogAdmissionShapeChecks(t *testing.T) {
	svc, eng, _, _, _ := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("100000"))
	cases := []struct {
		price, qty string
		want       string
	}{
		{"100.3", "1", perpcfg.RejectInvalidPriceTick},
		{"100", "1.0005", perpcfg.RejectInvalidLotSize},
		{"100", "0.01", perpcfg.RejectMinNotional},
		{"100", "1001", perpcfg.RejectMaxOrderQty},
	}
	for _, tc := range cases {
		resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, tc.price, tc.qty, "10", false))
		if resp.Accepted || resp.RejectReason != tc.want {
			t.Errorf("price=%s qty=%s: want %s, got accepted=%v reason=%s",
				tc.price, tc.qty, tc.want, resp.Accepted, resp.RejectReason)
		}
	}
}

func TestCatalogFailClosed(t *testing.T) {
	svc, eng, _, _, fix := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("10000"))

	// Unknown symbol → fail closed.
	resp, _ := svc.PlaceOrder(placeReq(user1, "ETH-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if resp.Accepted || resp.RejectReason != "unknown_symbol_config" {
		t.Fatalf("unknown symbol: accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}

	// Stale cache → fail closed (clock jumps past MaxStaleness with no sync).
	fix.clock.now = fix.clock.now.Add(11 * time.Second)
	resp, _ = svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if resp.Accepted || resp.RejectReason != "symbol_config_stale" {
		t.Fatalf("stale cache: accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}
	// A successful sync recovers admission.
	if err := fix.cache.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	resp, _ = svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("after sync: reject %s", resp.RejectReason)
	}
}

func TestCatalogStampsOrderEventVersion(t *testing.T) {
	svc, eng, disp, _, fix := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("10000"))

	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("reject: %s", resp.RejectReason)
	}
	if got := disp.orders[0].GetPlaced().GetSymbolConfigVersion(); got != 1 {
		t.Fatalf("order stamped v%d, want v1", got)
	}

	// Publish v2 (tick change) — the next order carries v2.
	ver := fix.publish(t, func(c *perpcfg.PerpSymbolConfig) { c.Precision.TickSize = dec.New("0.1") })
	if ver != 2 {
		t.Fatalf("published v%d", ver)
	}
	resp, _ = svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100.1", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("v2 order reject: %s", resp.RejectReason)
	}
	if got := disp.orders[1].GetPlaced().GetSymbolConfigVersion(); got != 2 {
		t.Fatalf("order stamped v%d, want v2", got)
	}
}

func TestCatalogStampsSettlementAndFunding(t *testing.T) {
	svc, eng, disp, jr, _ := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("10000"))
	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("reject: %s", resp.RejectReason)
	}
	oid := disp.orders[0].GetPlaced().GetOrderId()

	// Fill the order: settlement journal must stamp the active version.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: catSym, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: oid, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "1",
	}, 1)
	var settle *eventpb.PerpSettlementEvent
	for _, e := range jr.evts {
		if s := e.GetSettlement(); s != nil {
			settle = s
		}
	}
	if settle == nil || settle.GetSymbolConfigVersion() != 1 {
		t.Fatalf("settlement stamp: %+v", settle)
	}

	// Funding settlement carries the tick's config version verbatim.
	eng.SetMark(catSym, dec.New("100"))
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{
		Symbol: catSym,
		Payload: &eventpb.PerpPriceEvent_Funding{Funding: &eventpb.FundingTick{
			FundingRoundId: catSym + ":1700000000", FundingRate: "0.0001",
			MarkPrice: "100", ConfigVersion: 7,
		}},
	})
	var funding *eventpb.PerpFundingEvent
	for _, e := range jr.evts {
		if f := e.GetFunding(); f != nil {
			funding = f
		}
	}
	if funding == nil || funding.GetSymbolConfigVersion() != 7 {
		t.Fatalf("funding stamp: %+v", funding)
	}
}

func TestCatalogCancelGate(t *testing.T) {
	svc, eng, disp, _, fix := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("10000"))
	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("reject: %s", resp.RejectReason)
	}

	// CANCEL_ONLY still allows cancels (TRADING → CANCEL_ONLY is legal).
	fix.publish(t, func(c *perpcfg.PerpSymbolConfig) { c.Status = perpcfg.StatusCancelOnly })
	cresp, _ := svc.CancelOrder(cancelReqFor(user1, resp.OrderId))
	if !cresp.Accepted {
		t.Fatal("cancel must pass in CANCEL_ONLY")
	}
	if disp.cancels != 1 {
		t.Fatalf("cancel dispatches = %d", disp.cancels)
	}

	// SETTLING accepts system ops only — user cancel refused.
	svc2, eng2, disp2, _, fix2 := newCatalogSvc(t, perpcfg.StatusTrading)
	eng2.Deposit(user1, dec.New("10000"))
	resp2, _ := svc2.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp2.Accepted {
		t.Fatalf("reject: %s", resp2.RejectReason)
	}
	fix2.publish(t, func(c *perpcfg.PerpSymbolConfig) { c.Status = perpcfg.StatusCancelOnly })
	fix2.publish(t, func(c *perpcfg.PerpSymbolConfig) { c.Status = perpcfg.StatusSettling })
	cresp2, _ := svc2.CancelOrder(cancelReqFor(user1, resp2.OrderId))
	if cresp2.Accepted || disp2.cancels != 0 {
		t.Fatalf("cancel must be refused in SETTLING (accepted=%v dispatches=%d)",
			cresp2.Accepted, disp2.cancels)
	}
}

func TestCatalogPerSymbolRiskTiers(t *testing.T) {
	svc, eng, _, _, _ := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("1000000"))
	eng.SetMark(catSym, dec.New("100"))

	// Tier 2 (open-ended) caps leverage at 50: a 60k-notional order at 60x
	// must reject through the CATALOG tiers even though no legacy flags are
	// set on this service.
	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "600", "60", false))
	if resp.Accepted || resp.RejectReason != "leverage_exceeds_max" {
		t.Fatalf("tier-2 leverage cap: accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}
	// Same order at 50x passes (within tier 2's cap).
	resp, _ = svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "600", "50", false))
	if !resp.Accepted {
		t.Fatalf("tier-2 ok order rejected: %s", resp.RejectReason)
	}
}

func TestCatalogLiquidationUsesSymbolTiers(t *testing.T) {
	svc, eng, disp, jr, _ := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(catSym, dec.New("100"))
	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("reject: %s", resp.RejectReason)
	}
	oid := disp.orders[0].GetPlaced().GetOrderId()
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: catSym, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: oid, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "1",
	}, 1)

	// Crash the mark far below the bankruptcy price: the catalog's MMR
	// (0.005, tier 1) must arm liquidation with no legacy flags at all.
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{
		Symbol: catSym,
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{
			MarkPrice: "90", TsUnixMs: 1,
		}},
	})
	found := false
	for _, e := range jr.evts {
		if os := e.GetOrderStatus(); os != nil && os.GetReduceOnly() &&
			os.GetNewStatus() == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW &&
			os.GetOrderId() != oid {
			found = true
		}
	}
	if !found {
		t.Fatal("catalog-driven liquidation did not dispatch a bankruptcy order")
	}
	// The bankruptcy order itself carries the active config version.
	last := disp.orders[len(disp.orders)-1].GetPlaced()
	if last.GetSymbolConfigVersion() != 1 {
		t.Fatalf("bankruptcy order stamped v%d, want v1", last.GetSymbolConfigVersion())
	}
}

func cancelReqFor(user, orderID uint64) *perprpc.CancelOrderRequest {
	return &perprpc.CancelOrderRequest{UserId: user, OrderId: orderID}
}

// TestStagedRiskPinning pins the ADR-0075 §3 staged semantics end to end:
// a STAGED tightening leaves existing positions judged at their pinned
// version; new exposure adopts the new version.
func TestStagedRiskPinning(t *testing.T) {
	svc, eng, disp, _, fix := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("100000"))
	eng.SetMark(catSym, dec.New("100"))

	fill := func(orderIdx int, price string) {
		t.Helper()
		resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, price, "1", "10", false))
		if !resp.Accepted {
			t.Fatalf("place @%s: %s", price, resp.RejectReason)
		}
		oid := disp.orders[orderIdx].GetPlaced().GetOrderId()
		svc.HandleTrade(&eventpb.Trade{
			TradeId: "t" + price, Symbol: catSym, Price: price, Qty: "1",
			TakerUserId: user1, TakerOrderId: oid, TakerSide: eventpb.Side_SIDE_BUY,
			TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
			TakerFilledQtyAfter: "1",
		}, uint64(orderIdx+1))
	}

	// Open 1 @100 lev 10 (margin 10) at v1 → position pinned to v1.
	fill(0, "100")
	p, _ := eng.PositionRaw(user1, catSym)
	if p.RiskConfigVersion != 1 {
		t.Fatalf("open pin = v%d, want v1", p.RiskConfigVersion)
	}

	// v2: STAGED tightening — MMR 0.005 → 0.05 (max leverage drops to 16 so
	// MMR×lev stays < 1).
	fix.publish(t, func(c *perpcfg.PerpSymbolConfig) {
		c.RiskTiers[0].MaintMarginRatio = dec.New("0.05")
		c.RiskTiers[0].MaxLeverage = dec.New("16")
		c.RiskTiers[1].MaintMarginRatio = dec.New("0.05")
		c.RiskTiers[1].MaxLeverage = dec.New("16")
		c.RiskApply = perpcfg.RiskApplyStaged
	})
	eng.RebuildRiskIndex()

	// Mark 93: equity 3 sits between v1 maintenance (0.465) and v2
	// maintenance (4.65). Pinned at v1, the position must NOT be a
	// liquidation candidate — that's the staged shield.
	eng.SetMark(catSym, dec.New("93"))
	if cands := eng.LiquidatablePositions(catSym); len(cands) != 0 {
		t.Fatalf("staged tightening leaked onto the pinned position: %+v", cands)
	}

	// Size increase (1 @93, configured lev 10 ≤ new cap 16) → the position
	// adopts v2.
	fill(1, "93")
	p, _ = eng.PositionRaw(user1, catSym)
	if p.RiskConfigVersion != 2 {
		t.Fatalf("post-increase pin = v%d, want v2", p.RiskConfigVersion)
	}
	// Mark 91: equity 19.3 + (91-96.5)×2 = 8.3 < v2 maintenance 9.1 but
	// well above v1 maintenance 0.91 — only the v2 pin makes this a
	// candidate.
	eng.SetMark(catSym, dec.New("91"))
	if cands := eng.LiquidatablePositions(catSym); len(cands) != 1 {
		t.Fatalf("increased position must be judged at v2: %+v", cands)
	}
}

func TestImmediateRiskOverridesPin(t *testing.T) {
	svc, eng, disp, _, fix := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("100000"))
	eng.SetMark(catSym, dec.New("100"))
	resp, _ := svc.PlaceOrder(placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("place: %s", resp.RejectReason)
	}
	oid := disp.orders[0].GetPlaced().GetOrderId()
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: catSym, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: oid, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerFilledQtyAfter: "1",
	}, 1)

	// v2: IMMEDIATE tightening with a reprice policy — the pin at v1 no
	// longer shields the position.
	fix.publish(t, func(c *perpcfg.PerpSymbolConfig) {
		c.RiskTiers[0].MaintMarginRatio = dec.New("0.12")
		c.RiskTiers[1].MaintMarginRatio = dec.New("0.12")
		c.RiskTiers[0].MaxLeverage = dec.New("8")
		c.RiskTiers[1].MaxLeverage = dec.New("8")
		c.RiskApply = perpcfg.RiskApplyImmediate
		c.RepricePolicy = &perpcfg.RiskRepricePolicy{
			PolicyID: "POL-9", MaxAffectedAccounts: 1000, AllowMassLiquidation: true,
		}
	})
	eng.RebuildRiskIndex()
	cands := eng.LiquidatablePositions(catSym)
	if len(cands) != 1 {
		t.Fatalf("IMMEDIATE must re-judge pinned positions: %+v", cands)
	}
}

// TestProjectRiskTiers pins the §3 dry-run math: affected = requirement
// increased, liquidatable = breaches maintenance under the candidate.
func TestProjectRiskTiers(t *testing.T) {
	svc, eng, disp, _, _ := newCatalogSvc(t, perpcfg.StatusTrading)
	eng.Deposit(user1, dec.New("100000"))
	eng.Deposit(user2, dec.New("100000"))
	eng.SetMark(catSym, dec.New("100"))
	open := func(user uint64, orderIdx int, lev string) {
		t.Helper()
		resp, _ := svc.PlaceOrder(placeReq(user, catSym, eventpb.Side_SIDE_BUY, "100", "1", lev, false))
		if !resp.Accepted {
			t.Fatalf("place u%d: %s", user, resp.RejectReason)
		}
		oid := disp.orders[orderIdx].GetPlaced().GetOrderId()
		svc.HandleTrade(&eventpb.Trade{
			TradeId: "t" + lev, Symbol: catSym, Price: "100", Qty: "1",
			TakerUserId: user, TakerOrderId: oid, TakerSide: eventpb.Side_SIDE_BUY,
			TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
			TakerFilledQtyAfter: "1",
		}, 1)
	}
	open(user1, 0, "10") // margin 10 on 100 notional
	open(user2, 1, "50") // margin 2 on 100 notional

	// Candidate: MMR 0.05. Both requirements increase (0.005 → 0.05);
	// user2 (margin 2 < 5) breaches, user1 (margin 10 > 5) does not.
	candidate := (&perpcfg.PerpSymbolConfig{RiskTiers: []perpcfg.RiskTier{
		{RiskID: 1, MaxNotional: dec.FromInt(0), MaintMarginRatio: dec.New("0.05"),
			MaxLeverage: dec.New("20"), LiqFeeRate: dec.New("0.001")},
	}}).RiskModel()
	proj := eng.ProjectRiskTiers(catSym, candidate)
	if proj.Scanned != 2 || proj.Affected != 2 || proj.Liquidatable != 1 {
		t.Fatalf("projection = %+v, want scanned 2 affected 2 liquidatable 1", proj)
	}
}
