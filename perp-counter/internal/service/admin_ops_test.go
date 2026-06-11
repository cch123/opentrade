package service

// admin_ops_test.go covers the ADR-0078 admin plane: §7 force add/sub
// (economics, guards, audit event, idempotency) and §8 block trade
// (all-or-nothing legs, ordered double locking, band check, reduce-only
// capacity, idempotency).

import (
	"sync"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func adjustReq(user uint64, sub bool, side eventpb.Side, qty, price, opID string) *perprpc.ForceAdjustPositionRequest {
	return &perprpc.ForceAdjustPositionRequest{
		UserId: user, Symbol: perpSym, PositionIdx: 0, Sub: sub, Side: side,
		Qty: qty, Price: price,
		Reason: "repair", Ticket: "OPS-1", Operator: "admin@x", ClientOpId: opID,
	}
}

func setLeverage(t *testing.T, svc *Service, user uint64, lev string) {
	t.Helper()
	// client_op_id is a global idempotency key (engine ops cache) — it must
	// be unique per (user, op), not just per op.
	resp, err := svc.SetPositionLeverage(&perprpc.SetPositionLeverageRequest{
		UserId: user, Symbol: perpSym, Leverage: lev, ClientOpId: "lev-" + userIDString(user) + "-" + lev,
	})
	if err != nil || !resp.Accepted {
		t.Fatalf("set leverage: err=%v resp=%+v", err, resp)
	}
}

func TestForceAdjust_AddCreatesPositionWithMargin(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	setLeverage(t, svc, user1, "10")
	resp, err := svc.ForceAdjustPosition(adjustReq(user1, false, eventpb.Side_SIDE_BUY, "2", "100", "fa-1"))
	if err != nil || !resp.Accepted {
		t.Fatalf("force add: err=%v resp=%+v", err, resp)
	}
	pos, ok := eng.PositionOf(user1, perpSym, perpstate.IdxNet)
	if !ok || pos.Size.Cmp(dec.New("2")) != 0 || pos.Side != perpstate.SideBuy {
		t.Fatalf("position after add: %+v", pos)
	}
	eqd(t, pos.Margin, "20", "IM committed at price*qty/lev")
	eqd(t, eng.WalletOf(user1).Available, "980", "wallet debited")
	// Audit event with the mandatory triple + post-op snapshot.
	var evt *eventpb.PerpAdminPositionAdjustmentEvent
	for _, e := range jr.evts {
		if a := e.GetAdminAdjustment(); a != nil {
			evt = a
		}
	}
	if evt == nil || evt.GetReason() != "repair" || evt.GetTicket() != "OPS-1" || evt.GetOperator() != "admin@x" {
		t.Fatalf("audit event: %+v", evt)
	}
	if evt.GetPositionAfter().GetSize() != "2" {
		t.Fatalf("audit snapshot: %+v", evt.GetPositionAfter())
	}
	// No settlement / trade row for an admin adjustment.
	for _, e := range jr.evts {
		if e.GetSettlement() != nil {
			t.Fatal("force adjust must not journal a settlement event")
		}
	}
}

func TestForceAdjust_SubBooksRealizedAndGuards(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10")

	if r, _ := svc.ForceAdjustPosition(adjustReq(user1, true, 0, "3", "110", "fs-over")); r.Accepted || r.RejectReason != "qty_exceeds_position" {
		t.Fatalf("over-sub must reject (no flip): %+v", r)
	}
	r, _ := svc.ForceAdjustPosition(adjustReq(user1, true, 0, "1", "110", "fs-1"))
	if !r.Accepted {
		t.Fatalf("force sub: %+v", r)
	}
	if r.RealizedPnl != "10" { // (110-100)*1
		t.Fatalf("realized: %s", r.RealizedPnl)
	}
	pos, _ := eng.PositionOf(user1, perpSym, perpstate.IdxNet)
	eqd(t, pos.Size, "1", "size after sub")
	// Idempotent replay returns the cached outcome without re-applying.
	again, _ := svc.ForceAdjustPosition(adjustReq(user1, true, 0, "1", "110", "fs-1"))
	if !again.Accepted || again.RealizedPnl != "10" {
		t.Fatalf("idempotent replay: %+v", again)
	}
	pos, _ = eng.PositionOf(user1, perpSym, perpstate.IdxNet)
	eqd(t, pos.Size, "1", "size unchanged on replay")
}

func TestForceAdjust_AddGuards(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("15"))
	if r, _ := svc.ForceAdjustPosition(adjustReq(user1, false, eventpb.Side_SIDE_BUY, "1", "100", "fa-nolev")); r.RejectReason != "leverage_not_configured" {
		t.Fatalf("no leverage config: %+v", r)
	}
	setLeverage(t, svc, user1, "10")
	if r, _ := svc.ForceAdjustPosition(adjustReq(user1, false, eventpb.Side_SIDE_BUY, "2", "100", "fa-poor")); r.RejectReason != "insufficient_margin" {
		t.Fatalf("insufficient free balance: %+v", r)
	}
	if r, _ := svc.ForceAdjustPosition(adjustReq(user1, false, 0, "1", "100", "fa-noside")); r.RejectReason != "side_required_for_add" {
		t.Fatalf("missing side: %+v", r)
	}
	// ADD against an opposite position must be a SUB instead.
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	if r, _ := svc.ForceAdjustPosition(adjustReq(user1, false, eventpb.Side_SIDE_SELL, "1", "100", "fa-net")); r.RejectReason != "would_reduce_use_sub" {
		t.Fatalf("opposite-side add: %+v", r)
	}
}

// --- block trade (§8) -----------------------------------------------------------

func blockReq(id string, buyer, seller uint64, price, qty string) *perprpc.BlockTradeRequest {
	return &perprpc.BlockTradeRequest{
		BlockTradeId: id, Symbol: perpSym, Price: price, Qty: qty,
		Buyer:  &perprpc.BlockTradeLeg{UserId: buyer},
		Seller: &perprpc.BlockTradeLeg{UserId: seller},
		Reason: "otc", Ticket: "OPS-2", Operator: "admin@x",
	}
}

func TestBlockTrade_BothLegsSettle(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.Deposit(user2, dec.New("1000"))
	setLeverage(t, svc, user1, "10")
	setLeverage(t, svc, user2, "10")
	eng.SetMark(perpSym, dec.New("100"))

	resp, err := svc.BlockTrade(blockReq("bt-1", user1, user2, "100", "1"))
	if err != nil || !resp.Accepted || resp.TradeId != "block-bt-1" {
		t.Fatalf("block trade: err=%v resp=%+v", err, resp)
	}
	long, _ := eng.PositionOf(user1, perpSym, perpstate.IdxNet)
	short, _ := eng.PositionOf(user2, perpSym, perpstate.IdxNet)
	if long.Side != perpstate.SideBuy || long.Size.Cmp(dec.New("1")) != 0 ||
		short.Side != perpstate.SideSell || short.Size.Cmp(dec.New("1")) != 0 {
		t.Fatalf("legs: long=%+v short=%+v", long, short)
	}
	eqd(t, long.Margin, "10", "buyer IM")
	eqd(t, short.Margin, "10", "seller IM")
	eqd(t, eng.WalletOf(user1).Reserved, "0", "no lingering reservation")
	// Journal: one block envelope + two settlements with the block trade id.
	settlements := 0
	for _, e := range jr.evts {
		if st := e.GetSettlement(); st != nil {
			if st.GetOrderId() != 0 || st.GetTradeId() != "block-bt-1" {
				t.Fatalf("settlement provenance: %+v", st)
			}
			settlements++
		}
	}
	if settlements != 2 {
		t.Fatalf("want 2 settlements, got %d", settlements)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetBlockTrade() != nil }) != 1 {
		t.Fatal("want one block-trade envelope event")
	}
	// Idempotent replay: same outcome, no double apply.
	again, _ := svc.BlockTrade(blockReq("bt-1", user1, user2, "100", "1"))
	if !again.Accepted || again.TradeId != "block-bt-1" {
		t.Fatalf("idempotent replay: %+v", again)
	}
	long2, _ := eng.PositionOf(user1, perpSym, perpstate.IdxNet)
	eqd(t, long2.Size, "1", "no double apply on replay")
}

func TestBlockTrade_LegFailureRollsBackWhole(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.Deposit(user2, dec.New("5")) // seller cannot fund 10 IM
	setLeverage(t, svc, user1, "10")
	setLeverage(t, svc, user2, "10")
	eng.SetMark(perpSym, dec.New("100"))

	resp, _ := svc.BlockTrade(blockReq("bt-2", user1, user2, "100", "1"))
	if resp.Accepted || resp.RejectReason != "seller: insufficient_margin" {
		t.Fatalf("want seller leg reject: %+v", resp)
	}
	// Buyer's reservation must be released; no position on either side.
	eqd(t, eng.WalletOf(user1).Reserved, "0", "buyer reservation released")
	eqd(t, eng.WalletOf(user1).Available, "1000", "buyer wallet intact")
	if _, ok := eng.PositionOf(user1, perpSym, perpstate.IdxNet); ok {
		t.Fatal("no position may exist after whole-reject")
	}
	// The reject outcome is cached (a retry under the same id cannot
	// half-apply after the seller funds up).
	eng.Deposit(user2, dec.New("100"))
	again, _ := svc.BlockTrade(blockReq("bt-2", user1, user2, "100", "1"))
	if again.Accepted {
		t.Fatalf("same id must replay the recorded reject: %+v", again)
	}
	fresh, _ := svc.BlockTrade(blockReq("bt-2b", user1, user2, "100", "1"))
	if !fresh.Accepted {
		t.Fatalf("fresh id after funding: %+v", fresh)
	}
}

func TestBlockTrade_Guards(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.Deposit(user2, dec.New("1000"))
	setLeverage(t, svc, user1, "10")
	setLeverage(t, svc, user2, "10")

	if r, _ := svc.BlockTrade(blockReq("bt-g1", user1, user1, "100", "1")); r.RejectReason != "same_user" {
		t.Fatalf("same user: %+v", r)
	}
	if r, _ := svc.BlockTrade(blockReq("bt-g2", user1, user2, "100", "1")); r.RejectReason != "no_mark" {
		t.Fatalf("no mark: %+v", r)
	}
	eng.SetMark(perpSym, dec.New("100"))
	// Default band 500 bps: 106 vs mark 100 = 600 bps out.
	if r, _ := svc.BlockTrade(blockReq("bt-g3", user1, user2, "106", "1")); r.RejectReason != "price_out_of_band" {
		t.Fatalf("band: %+v", r)
	}
	// 105 = exactly 500 bps: allowed.
	if r, _ := svc.BlockTrade(blockReq("bt-g4", user1, user2, "105", "1")); !r.Accepted {
		t.Fatalf("at-band price: %+v", r)
	}
}

func TestBlockTrade_ReduceOnlyLeg(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.Deposit(user2, dec.New("1000"))
	setLeverage(t, svc, user1, "10")
	setLeverage(t, svc, user2, "10")
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10") // u1 long 1

	req := blockReq("bt-ro-over", user2, user1, "100", "2")
	req.Seller.ReduceOnly = true // u1 closes the long
	if r, _ := svc.BlockTrade(req); r.RejectReason != "seller: reduce_only_exceeds_position" {
		t.Fatalf("RO over-capacity: %+v", r)
	}
	req = blockReq("bt-ro", user2, user1, "102", "1")
	req.Seller.ReduceOnly = true
	r, _ := svc.BlockTrade(req)
	if !r.Accepted {
		t.Fatalf("RO block: %+v", r)
	}
	if _, ok := eng.PositionOf(user1, perpSym, perpstate.IdxNet); ok {
		t.Fatal("seller leg must be flat after the reduce")
	}
	buyer, _ := eng.PositionOf(user2, perpSym, perpstate.IdxNet)
	eqd(t, buyer.Size, "1", "buyer leg opened")
	// Realized 2 on the close + the 10 margin returned.
	eqd(t, eng.WalletOf(user1).Available, "1002", "seller wallet after close")
}

func TestBlockTrade_ConcurrentReversedPairsNoDeadlock(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("100000"))
	eng.Deposit(user2, dec.New("100000"))
	setLeverage(t, svc, user1, "10")
	setLeverage(t, svc, user2, "10")
	eng.SetMark(perpSym, dec.New("100"))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		idA := "bt-fwd-" + userIDString(uint64(i))
		idB := "bt-rev-" + userIDString(uint64(i))
		go func() { defer wg.Done(); _, _ = svc.BlockTrade(blockReq(idA, user1, user2, "100", "1")) }()
		go func() { defer wg.Done(); _, _ = svc.BlockTrade(blockReq(idB, user2, user1, "100", "1")) }()
	}
	wg.Wait() // do2's ordered locking: reversed pairs must not deadlock
}
