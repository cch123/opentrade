package writer

import (
	"context"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// -----------------------------------------------------------------------------
// perp-journal → MySQL projection (ADR-0068 M7, mirrors the counter-journal
// BuildJournalBatch). perp-journal is a state log (every settlement / funding /
// liquidation carries a PerpPositionSnapshot), so it qualifies for trade-dump
// admission (ADR-0066). Rows map 1:1 onto 03-perp-schema.sql.
//
// Idempotency: append-only rows (settlements / funding / liquidations / margin)
// are keyed by perp_seq_id (shard-monotonic) with INSERT IGNORE; state rows
// (positions / wallets / orders) upsert latest-wins guarded by perp_seq_id /
// version so a replay never regresses them.
// -----------------------------------------------------------------------------

// PerpPositionRow mirrors `perp_positions` (one per user+symbol; upserted from
// the PerpPositionSnapshot embedded in settlement / funding / liquidation).
type PerpPositionRow struct {
	UserID      string
	Symbol      string
	Side        int8
	Size        string
	EntryPrice  string
	Margin      string
	Leverage    string
	RealizedPnl string
	Version     uint64
	PerpSeqID   uint64
	UpdatedAtMs int64
}

// PerpWalletRow mirrors `perp_wallets` (per user USDT margin balance; upserted
// from PerpMarginEvent's available/reserved after-values).
type PerpWalletRow struct {
	UserID      string
	Asset       string
	Available   string
	Reserved    string
	PerpSeqID   uint64
	UpdatedAtMs int64
}

// PerpOrderRow mirrors `perp_orders` (lifecycle status from PerpOrderStatusEvent).
type PerpOrderRow struct {
	OrderID      uint64
	UserID       string
	Symbol       string
	Status       int8
	FilledQty    string
	ReduceOnly   bool
	RejectReason int8
	UpdatedAtMs  int64
}

// PerpSettlementRow mirrors `perp_settlements` (append-only fill ledger).
type PerpSettlementRow struct {
	PerpSeqID      uint64
	UserID         string
	OrderID        uint64
	TradeID        string
	Symbol         string
	FillSide       int8
	Price          string
	Qty            string
	RealizedPnl    string
	Fee            string
	MarginAdded    string
	MarginReleased string
	TsUnixMs       int64
}

// PerpFundingRow mirrors `perp_funding` (append-only funding payments).
type PerpFundingRow struct {
	PerpSeqID      uint64
	UserID         string
	Symbol         string
	FundingRoundID string
	FundingRate    string
	MarkPrice      string
	Payment        string
	TsUnixMs       int64
}

// PerpLiquidationRow mirrors `perp_liquidations` (append-only liquidation ledger).
type PerpLiquidationRow struct {
	PerpSeqID       uint64
	UserID          string
	Symbol          string
	LiqOrderID      uint64
	BankruptcyPrice string
	MarkPrice       string
	ClosedQty       string
	RealizedPnl     string
	InsuranceDelta  string
	AdlQueued       bool
	TsUnixMs        int64
}

// PerpMarginRow mirrors `perp_margin_logs` (append-only wallet balance changes).
type PerpMarginRow struct {
	PerpSeqID      uint64
	UserID         string
	Kind           int8
	Asset          string
	Amount         string
	AvailableAfter string
	ReservedAfter  string
	RefID          string
	TsUnixMs       int64
}

// PerpBatch is the aggregate projection of a slice of perp-journal events,
// applied in one MySQL transaction.
type PerpBatch struct {
	Positions    []PerpPositionRow
	Wallets      []PerpWalletRow
	Orders       []PerpOrderRow
	Settlements  []PerpSettlementRow
	Funding      []PerpFundingRow
	Liquidations []PerpLiquidationRow
	Margins      []PerpMarginRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *PerpBatch) IsEmpty() bool {
	return len(b.Positions) == 0 && len(b.Wallets) == 0 && len(b.Orders) == 0 &&
		len(b.Settlements) == 0 && len(b.Funding) == 0 && len(b.Liquidations) == 0 &&
		len(b.Margins) == 0
}

// PerpJournalWriter is the contract the consumer uses against MySQL. Real impl
// in mysql_perp.go; tests can fake it.
type PerpJournalWriter interface {
	ApplyPerpBatch(ctx context.Context, batch PerpBatch) error
}

// BuildPerpBatch projects perp-journal events into a MySQL write batch. Pure,
// no I/O. nil / unknown payloads are skipped.
func BuildPerpBatch(events []*eventpb.PerpJournalEvent) PerpBatch {
	var b PerpBatch
	for _, evt := range events {
		if evt == nil {
			continue
		}
		ts := tsFromMeta(evt.GetMeta())
		seq := evt.GetPerpSeqId()
		switch p := evt.Payload.(type) {
		case *eventpb.PerpJournalEvent_OrderStatus:
			appendPerpOrder(&b, p.OrderStatus, ts)
		case *eventpb.PerpJournalEvent_Settlement:
			appendPerpSettlement(&b, p.Settlement, seq, ts)
		case *eventpb.PerpJournalEvent_Margin:
			appendPerpMargin(&b, p.Margin, seq, ts)
		case *eventpb.PerpJournalEvent_Funding:
			appendPerpFunding(&b, p.Funding, seq, ts)
		case *eventpb.PerpJournalEvent_Liquidation:
			appendPerpLiquidation(&b, p.Liquidation, seq, ts)
		case *eventpb.PerpJournalEvent_Adl:
			appendPerpADL(&b, p.Adl, seq, ts)
		}
	}
	return b
}

func appendPerpOrder(b *PerpBatch, e *eventpb.PerpOrderStatusEvent, ts int64) {
	if e == nil {
		return
	}
	b.Orders = append(b.Orders, PerpOrderRow{
		OrderID: e.GetOrderId(), UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		Status: int8(e.GetNewStatus()), FilledQty: defaultZero(e.GetFilledQty()),
		ReduceOnly: e.GetReduceOnly(), RejectReason: int8(e.GetRejectReason()), UpdatedAtMs: ts,
	})
}

func appendPerpSettlement(b *PerpBatch, e *eventpb.PerpSettlementEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Settlements = append(b.Settlements, PerpSettlementRow{
		PerpSeqID: seq, UserID: e.GetUserId(), OrderID: e.GetOrderId(), TradeID: e.GetTradeId(),
		Symbol: e.GetSymbol(), FillSide: int8(e.GetFillSide()), Price: e.GetPrice(), Qty: e.GetQty(),
		RealizedPnl: defaultZero(e.GetRealizedPnl()), Fee: defaultZero(e.GetFee()),
		MarginAdded: defaultZero(e.GetMarginAdded()), MarginReleased: defaultZero(e.GetMarginReleased()),
		TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpFunding(b *PerpBatch, e *eventpb.PerpFundingEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Funding = append(b.Funding, PerpFundingRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		FundingRoundID: e.GetFundingRoundId(), FundingRate: e.GetFundingRate(),
		MarkPrice: e.GetMarkPrice(), Payment: defaultZero(e.GetPayment()), TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpLiquidation(b *PerpBatch, e *eventpb.PerpLiquidationEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Liquidations = append(b.Liquidations, PerpLiquidationRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(), LiqOrderID: e.GetLiqOrderId(),
		BankruptcyPrice: e.GetBankruptcyPrice(), MarkPrice: e.GetMarkPrice(), ClosedQty: e.GetClosedQty(),
		RealizedPnl: defaultZero(e.GetRealizedPnl()), InsuranceDelta: defaultZero(e.GetInsuranceDelta()),
		AdlQueued: e.GetAdlQueued(), TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpADL(b *PerpBatch, e *eventpb.PerpAdlEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	// ADR-0070 introduces ADL primarily as a user-position mutation and
	// notification. Until the MySQL schema grows a dedicated ADL ledger, the
	// projection at least advances the affected position so history queries do
	// not show a stale profitable size after an ADL event.
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpMargin(b *PerpBatch, e *eventpb.PerpMarginEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Margins = append(b.Margins, PerpMarginRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Kind: int8(e.GetKind()), Asset: e.GetAsset(),
		Amount: defaultZero(e.GetAmount()), AvailableAfter: defaultZero(e.GetAvailableAfter()),
		ReservedAfter: defaultZero(e.GetReservedAfter()), RefID: e.GetRefId(), TsUnixMs: ts,
	})
	b.Wallets = append(b.Wallets, PerpWalletRow{
		UserID: e.GetUserId(), Asset: e.GetAsset(), Available: defaultZero(e.GetAvailableAfter()),
		Reserved: defaultZero(e.GetReservedAfter()), PerpSeqID: seq, UpdatedAtMs: ts,
	})
}

// appendPerpPosition upserts the post-change position snapshot embedded in
// settlement / funding / liquidation events.
func appendPerpPosition(b *PerpBatch, snap *eventpb.PerpPositionSnapshot, seq uint64, ts int64) {
	if snap == nil {
		return
	}
	b.Positions = append(b.Positions, PerpPositionRow{
		UserID: snap.GetUserId(), Symbol: snap.GetSymbol(), Side: int8(snap.GetSide()),
		Size: defaultZero(snap.GetSize()), EntryPrice: defaultZero(snap.GetEntryPrice()),
		Margin: defaultZero(snap.GetMargin()), Leverage: defaultZero(snap.GetLeverage()),
		RealizedPnl: defaultZero(snap.GetRealizedPnl()), Version: snap.GetVersion(),
		PerpSeqID: seq, UpdatedAtMs: ts,
	})
}
