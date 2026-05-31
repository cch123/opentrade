package writer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func perpEvt(seq uint64, payload any) *eventpb.PerpJournalEvent {
	e := &eventpb.PerpJournalEvent{Meta: &eventpb.EventMeta{TsUnixMs: 1000}, PerpSeqId: seq}
	switch p := payload.(type) {
	case *eventpb.PerpOrderStatusEvent:
		e.Payload = &eventpb.PerpJournalEvent_OrderStatus{OrderStatus: p}
	case *eventpb.PerpSettlementEvent:
		e.Payload = &eventpb.PerpJournalEvent_Settlement{Settlement: p}
	case *eventpb.PerpFundingEvent:
		e.Payload = &eventpb.PerpJournalEvent_Funding{Funding: p}
	case *eventpb.PerpLiquidationEvent:
		e.Payload = &eventpb.PerpJournalEvent_Liquidation{Liquidation: p}
	case *eventpb.PerpTakeoverEvent:
		e.Payload = &eventpb.PerpJournalEvent_Takeover{Takeover: p}
	case *eventpb.PerpAdlEvent:
		e.Payload = &eventpb.PerpJournalEvent_Adl{Adl: p}
	case *eventpb.RiskPoolSettlementEvent:
		e.Payload = &eventpb.PerpJournalEvent_RiskPoolSettlement{RiskPoolSettlement: p}
	case *eventpb.PerpMarginEvent:
		e.Payload = &eventpb.PerpJournalEvent_Margin{Margin: p}
	}
	return e
}

func posSnap(user uint64, sym, size, entry string) *eventpb.PerpPositionSnapshot {
	return &eventpb.PerpPositionSnapshot{
		UserId: user, Symbol: sym, Side: eventpb.Side_SIDE_BUY,
		Size: size, EntryPrice: entry, Margin: "10", Leverage: "10", RealizedPnl: "0", Version: 3,
	}
}

func TestBuildPerpBatch_SettlementProjectsRowAndPosition(t *testing.T) {
	b := BuildPerpBatch([]*eventpb.PerpJournalEvent{
		perpEvt(7, &eventpb.PerpSettlementEvent{
			UserId: 1001, OrderId: 100, TradeId: "t1", Symbol: "BTC-USDT-PERP",
			FillSide: eventpb.Side_SIDE_BUY, Price: "100", Qty: "1", RealizedPnl: "0",
			MarginAdded: "10", PositionAfter: posSnap(1001, "BTC-USDT-PERP", "1", "100"),
		}),
	})
	if len(b.Settlements) != 1 || len(b.Positions) != 1 {
		t.Fatalf("want 1 settlement + 1 position, got %d/%d", len(b.Settlements), len(b.Positions))
	}
	s := b.Settlements[0]
	if s.PerpSeqID != 7 || s.UserID != 1001 || s.TradeID != "t1" || s.MarginAdded != "10" {
		t.Fatalf("settlement row wrong: %+v", s)
	}
	if p := b.Positions[0]; p.UserID != 1001 || p.Size != "1" || p.EntryPrice != "100" || p.Version != 3 {
		t.Fatalf("position row wrong: %+v", p)
	}
}

func TestBuildPerpBatch_FundingAndLiquidationAndMargin(t *testing.T) {
	b := BuildPerpBatch([]*eventpb.PerpJournalEvent{
		perpEvt(8, &eventpb.PerpFundingEvent{
			UserId: 1001, Symbol: "BTC-USDT-PERP", FundingRoundId: "BTC-USDT-PERP:1748505600",
			FundingRate: "0.01", MarkPrice: "100", Payment: "-1",
			PositionAfter: posSnap(1001, "BTC-USDT-PERP", "1", "100"),
		}),
		perpEvt(9, &eventpb.PerpLiquidationEvent{
			UserId: 1002, Symbol: "BTC-USDT-PERP", LiqOrderId: 200, BankruptcyPrice: "90",
			MarkPrice: "89", ClosedQty: "1", RealizedPnl: "-10", InsuranceDelta: "-2", AdlQueued: true,
			PositionAfter: posSnap(1002, "BTC-USDT-PERP", "0", "0"),
		}),
		perpEvt(10, &eventpb.PerpMarginEvent{
			UserId: 1001, Kind: eventpb.PerpMarginEvent_KIND_TRANSFER_IN, Asset: "USDT",
			Amount: "500", AvailableAfter: "500", ReservedAfter: "0", RefId: "tx1",
		}),
	})
	if len(b.Funding) != 1 || b.Funding[0].Payment != "-1" || b.Funding[0].FundingRoundID != "BTC-USDT-PERP:1748505600" {
		t.Fatalf("funding row wrong: %+v", b.Funding)
	}
	if len(b.Liquidations) != 1 || !b.Liquidations[0].AdlQueued || b.Liquidations[0].InsuranceDelta != "-2" {
		t.Fatalf("liquidation row wrong: %+v", b.Liquidations)
	}
	if len(b.Margins) != 1 || len(b.Wallets) != 1 {
		t.Fatalf("want 1 margin + 1 wallet, got %d/%d", len(b.Margins), len(b.Wallets))
	}
	if b.Wallets[0].Available != "500" || b.Margins[0].Kind != int8(eventpb.PerpMarginEvent_KIND_TRANSFER_IN) {
		t.Fatalf("margin/wallet wrong: margin=%+v wallet=%+v", b.Margins[0], b.Wallets[0])
	}
	// Funding + liquidation each carried a position snapshot.
	if len(b.Positions) != 2 {
		t.Fatalf("want 2 positions from funding+liquidation, got %d", len(b.Positions))
	}
}

func TestBuildPerpBatch_TakeoverProjectsLiquidationHistory(t *testing.T) {
	b := BuildPerpBatch([]*eventpb.PerpJournalEvent{
		perpEvt(11, &eventpb.PerpTakeoverEvent{
			UserId: 1002, Symbol: "BTC-USDT-PERP", LiqOrderId: 201, LotId: "lot-201",
			BankruptcyPrice: "90", MarkPrice: "89", ClosedQty: "1",
			RealizedPnl: "-10", InsuranceDelta: "-2", TakenOverQty: "1", TakeoverPrice: "90",
			TakenOverBalance: "-2", TakeoverNotional: "90",
			BackstopUserId: 9000, AdlQueued: true, PositionAfter: posSnap(1002, "BTC-USDT-PERP", "0", "0"),
		}),
	})
	if len(b.Liquidations) != 1 || b.Liquidations[0].PerpSeqID != 11 || b.Liquidations[0].InsuranceDelta != "-2" || !b.Liquidations[0].AdlQueued {
		t.Fatalf("takeover liquidation-history row wrong: %+v", b.Liquidations)
	}
	if len(b.Positions) != 1 || b.Positions[0].UserID != 1002 || b.Positions[0].Size != "0" {
		t.Fatalf("takeover position row wrong: %+v", b.Positions)
	}
	if len(b.TakeoverLots) != 1 || b.TakeoverLots[0].LotID != "lot-201" || b.TakeoverLots[0].TakenOverBalance != "-2" ||
		b.TakeoverLots[0].WorkingCapitalRef != "takeover:lot-201" {
		t.Fatalf("takeover lot row wrong: %+v", b.TakeoverLots)
	}
}

func TestBuildPerpBatch_ADLAndRiskPoolSettlement(t *testing.T) {
	b := BuildPerpBatch([]*eventpb.PerpJournalEvent{
		perpEvt(12, &eventpb.PerpAdlEvent{
			UserId: 2001, Symbol: "BTC-USDT-PERP", LotId: "lot-201", AdlRound: 2,
			Price: "90", RequestedQty: "2", FactQty: "1.5", RealizedPnl: "7",
			PositionAfter: posSnap(2001, "BTC-USDT-PERP", "0", "0"),
		}),
		perpEvt(13, &eventpb.RiskPoolSettlementEvent{
			LotId: "lot-201", Symbol: "BTC-USDT-PERP", Coin: "USDT",
			WorkingCapitalRef: "takeover:lot-201", TakenOverBalance: "-2",
			LiqAdlRealisedPnl: "7", CumFee: "0", WorkingCapitalDrawn: "90",
			BorrowedBalance: "85", FinalPoolDelta: "-85", Status: "Done",
		}),
	})
	if len(b.ADL) != 1 || b.ADL[0].LotID != "lot-201" || b.ADL[0].RequestedQty != "2" || b.ADL[0].FactQty != "1.5" {
		t.Fatalf("ADL row wrong: %+v", b.ADL)
	}
	if len(b.RiskPool) != 1 || b.RiskPool[0].LotID != "lot-201" || b.RiskPool[0].FinalPoolDelta != "-85" {
		t.Fatalf("risk-pool row wrong: %+v", b.RiskPool)
	}
	if len(b.Positions) != 1 || b.Positions[0].UserID != 2001 {
		t.Fatalf("ADL should still advance affected position: %+v", b.Positions)
	}
}

func TestBuildPerpBatch_OrderStatusAndSkips(t *testing.T) {
	b := BuildPerpBatch([]*eventpb.PerpJournalEvent{
		perpEvt(1, &eventpb.PerpOrderStatusEvent{
			UserId: 1001, OrderId: 5, Symbol: "BTC-USDT-PERP",
			NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW, FilledQty: "0", ReduceOnly: true,
		}),
		nil,                                     // skipped
		{Meta: &eventpb.EventMeta{TsUnixMs: 1}}, // no payload → skipped
	})
	if len(b.Orders) != 1 {
		t.Fatalf("want 1 order row, got %d", len(b.Orders))
	}
	o := b.Orders[0]
	if o.OrderID != 5 || !o.ReduceOnly || o.Status != int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW) {
		t.Fatalf("order row wrong: %+v", o)
	}
	// Order-status events touch only the orders table.
	if len(b.Positions)+len(b.Wallets)+len(b.Settlements)+len(b.Funding)+len(b.Liquidations)+len(b.TakeoverLots)+len(b.ADL)+len(b.RiskPool)+len(b.Margins) != 0 {
		t.Fatal("order-status event should produce only an order row")
	}
}

func TestBuildPerpBatch_EmptyAndIsEmpty(t *testing.T) {
	b := BuildPerpBatch(nil)
	if !b.IsEmpty() {
		t.Fatal("nil events should produce an empty batch")
	}
}
