package writer

import (
	"context"
	"strconv"

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
// the PerpPositionSnapshot embedded in settlement / funding / liquidation /
// takeover).
type PerpPositionRow struct {
	UserID      uint64
	Symbol      string
	PositionIdx uint8 // ADR-0077 leg key segment
	Side        int8
	Size        string
	EntryPrice  string
	Margin      string
	Leverage    string
	RealizedPnl string
	MarginMode  int8
	RiskID      uint32
	Version     uint64
	PerpSeqID   uint64
	UpdatedAtMs int64
}

// PerpWalletRow mirrors `perp_wallets` (per user USDT margin balance; upserted
// from PerpMarginEvent's available/reserved after-values).
type PerpWalletRow struct {
	UserID      uint64
	Asset       string
	Available   string
	Reserved    string
	PerpSeqID   uint64
	UpdatedAtMs int64
}

// PerpOrderRow mirrors `perp_orders` (lifecycle status from PerpOrderStatusEvent).
type PerpOrderRow struct {
	OrderID      uint64
	UserID       uint64
	Symbol       string
	Status       int8
	FilledQty    string
	ReduceOnly   bool
	PositionIdx  uint8 // ADR-0077 order intent
	RejectReason int8
	UpdatedAtMs  int64
}

// PerpSettlementRow mirrors `perp_settlements` (append-only fill ledger).
type PerpSettlementRow struct {
	PerpSeqID      uint64
	UserID         uint64
	OrderID        uint64
	TradeID        string
	Symbol         string
	PositionIdx    uint8 // ADR-0077
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
	UserID         uint64
	Symbol         string
	PositionIdx    uint8 // ADR-0077 per-leg funding
	FundingRoundID string
	FundingRate    string
	MarkPrice      string
	Payment        string
	TsUnixMs       int64
}

// PerpLiquidationRow mirrors `perp_liquidations` (append-only liquidation ledger).
type PerpLiquidationRow struct {
	PerpSeqID       uint64
	UserID          uint64
	Symbol          string
	PositionIdx     uint8 // ADR-0077 liquidated leg
	LiqOrderID      uint64
	BankruptcyPrice string
	MarkPrice       string
	ClosedQty       string
	RealizedPnl     string
	InsuranceDelta  string
	AdlQueued       bool
	TsUnixMs        int64
}

// PerpTakeoverLotRow mirrors `perp_takeover_lots`, the initial audit row for a
// coordinator-owned TakenOverLot.
type PerpTakeoverLotRow struct {
	LotID             string
	PerpSeqID         uint64
	UserID            uint64
	Symbol            string
	PositionIdx       uint8 // ADR-0077 taken-over leg
	Side              int8
	TotalQty          string
	LeavesQty         string
	TakeoverPrice     string
	TriggerMarkPrice  string
	TakenOverBalance  string
	WorkingCapitalRef string
	Status            string
	TsUnixMs          int64
}

// PerpADLRow mirrors `perp_adl_events`, an append-only user-visible forced
// close ledger keyed by the affected user and perp_seq_id.
type PerpADLRow struct {
	PerpSeqID    uint64
	UserID       uint64
	Symbol       string
	PositionIdx  uint8 // ADR-0077 reduced leg
	LotID        string
	AdlRound     uint64
	Price        string
	RequestedQty string
	FactQty      string
	RealizedPnl  string
	TsUnixMs     int64
}

// RiskPoolSettlementRow mirrors `perp_risk_pool_settlements`.
type RiskPoolSettlementRow struct {
	LotID               string
	PerpSeqID           uint64
	Symbol              string
	Coin                string
	WorkingCapitalRef   string
	TakenOverBalance    string
	LiqAdlRealisedPnl   string
	CumFee              string
	WorkingCapitalDrawn string
	BorrowedBalance     string
	FinalPoolDelta      string
	Status              string
	TsUnixMs            int64
}

// PerpPositionConfigLogRow mirrors `perp_position_config_logs` (ADR-0074:
// append-only mode / leverage / risk_id / auto-add change history; the
// current config is the row with the highest perp_seq_id, and the live view
// is queryable from perp-counter directly).
type PerpPositionConfigLogRow struct {
	PerpSeqID       uint64
	UserID          uint64
	Symbol          string
	MarginMode      int8
	PositionMode    int8  // ADR-0077: 1 one-way / 2 hedge
	PositionIdx     uint8 // echo-provenance leg
	Leverage        string
	RiskID          uint32
	AutoAddMargin   bool
	AutoAddMax      string
	PositionVersion uint64
	Reason          string
	ClientOpID      string
	TsUnixMs        int64
}

// PerpMarginAdjustmentRow mirrors `perp_margin_adjustments` (ADR-0074 §6/§7:
// append-only isolated-margin movements — manual add/remove, mode-switch
// cash leg, auto-add, leverage resize).
type PerpMarginAdjustmentRow struct {
	PerpSeqID       uint64
	UserID          uint64
	Symbol          string
	PositionIdx     uint8 // ADR-0077: the leg whose margin moved
	Kind            int8
	Amount          string
	MarginBefore    string
	MarginAfter     string
	WalletAfter     string
	PositionVersion uint64
	ClientOpID      string
	MarkPrice       string
	TsUnixMs        int64
}

// PerpCustomerRiskLimitRow mirrors `perp_customer_risk_limits` (ADR-0074 §10:
// append-only admin leverage-cap audit; max_leverage 0 records a removal).
type PerpCustomerRiskLimitRow struct {
	PerpSeqID   uint64
	UserID      uint64
	Symbol      string
	MaxLeverage string
	Reason      string
	UpdatedBy   string
	TsUnixMs    int64
}

// PerpInvariantBreachRow mirrors `perp_invariant_breaches` (ADR-0077 §2 /
// ADR-0081 §2: clamped-off reduce-only excess — the manual-repair queue).
type PerpInvariantBreachRow struct {
	PerpSeqID   uint64
	UserID      uint64
	Symbol      string
	PositionIdx uint8
	OrderID     uint64
	TradeID     string
	Kind        string
	ExcessQty   string
	FillPrice   string
	TsUnixMs    int64
}

// PerpMarginRow mirrors `perp_margin_logs` (append-only wallet balance changes).
type PerpMarginRow struct {
	PerpSeqID      uint64
	UserID         uint64
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
	TakeoverLots []PerpTakeoverLotRow
	ADL          []PerpADLRow
	RiskPool     []RiskPoolSettlementRow
	Margins      []PerpMarginRow
	ConfigLogs   []PerpPositionConfigLogRow
	MarginAdjust []PerpMarginAdjustmentRow
	RiskLimits   []PerpCustomerRiskLimitRow
	Breaches     []PerpInvariantBreachRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *PerpBatch) IsEmpty() bool {
	return len(b.Positions) == 0 && len(b.Wallets) == 0 && len(b.Orders) == 0 &&
		len(b.Settlements) == 0 && len(b.Funding) == 0 && len(b.Liquidations) == 0 &&
		len(b.TakeoverLots) == 0 && len(b.ADL) == 0 && len(b.RiskPool) == 0 && len(b.Margins) == 0 &&
		len(b.ConfigLogs) == 0 && len(b.MarginAdjust) == 0 && len(b.RiskLimits) == 0 &&
		len(b.Breaches) == 0
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
		case *eventpb.PerpJournalEvent_Takeover:
			appendPerpTakeover(&b, p.Takeover, seq, ts)
		case *eventpb.PerpJournalEvent_Adl:
			appendPerpADL(&b, p.Adl, seq, ts)
		case *eventpb.PerpJournalEvent_RiskPoolSettlement:
			appendRiskPoolSettlement(&b, p.RiskPoolSettlement, seq, ts)
		case *eventpb.PerpJournalEvent_PositionConfig:
			appendPerpConfigLog(&b, p.PositionConfig, seq, ts)
		case *eventpb.PerpJournalEvent_MarginAdjustment:
			appendPerpMarginAdjustment(&b, p.MarginAdjustment, seq, ts)
		case *eventpb.PerpJournalEvent_CustomerRiskLimit:
			appendPerpRiskLimit(&b, p.CustomerRiskLimit, seq, ts)
		case *eventpb.PerpJournalEvent_InvariantBreach:
			appendPerpBreach(&b, p.InvariantBreach, seq, ts)
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
		ReduceOnly: e.GetReduceOnly(), PositionIdx: uint8(e.GetPositionIdx()),
		RejectReason: int8(e.GetRejectReason()), UpdatedAtMs: ts,
	})
}

func appendPerpSettlement(b *PerpBatch, e *eventpb.PerpSettlementEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Settlements = append(b.Settlements, PerpSettlementRow{
		PerpSeqID: seq, UserID: e.GetUserId(), OrderID: e.GetOrderId(), TradeID: e.GetTradeId(),
		Symbol: e.GetSymbol(), PositionIdx: uint8(e.GetPositionAfter().GetPositionIdx()),
		FillSide: int8(e.GetFillSide()), Price: e.GetPrice(), Qty: e.GetQty(),
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
		PositionIdx:    uint8(e.GetPositionAfter().GetPositionIdx()),
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
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionAfter().GetPositionIdx()), LiqOrderID: e.GetLiqOrderId(),
		BankruptcyPrice: e.GetBankruptcyPrice(), MarkPrice: e.GetMarkPrice(), ClosedQty: e.GetClosedQty(),
		RealizedPnl: defaultZero(e.GetRealizedPnl()), InsuranceDelta: defaultZero(e.GetInsuranceDelta()),
		AdlQueued: e.GetAdlQueued(), TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpTakeover(b *PerpBatch, e *eventpb.PerpTakeoverEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	// MySQL has one liquidation ledger today. A takeover is still the user's
	// forced close, while the extra inventory/loan fields are coordinator
	// accounting, so preserving the user-facing row here keeps history queries
	// contiguous until a dedicated takeover ledger is introduced.
	b.Liquidations = append(b.Liquidations, PerpLiquidationRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionAfter().GetPositionIdx()), LiqOrderID: e.GetLiqOrderId(),
		BankruptcyPrice: e.GetBankruptcyPrice(), MarkPrice: e.GetMarkPrice(), ClosedQty: e.GetClosedQty(),
		RealizedPnl: defaultZero(e.GetRealizedPnl()), InsuranceDelta: defaultZero(e.GetInsuranceDelta()),
		AdlQueued: e.GetAdlQueued(), TsUnixMs: ts,
	})
	lotID := e.GetLotId()
	if lotID == "" {
		lotID = e.GetSymbol() + ":" + strconv.FormatUint(e.GetLiqOrderId(), 10)
	}
	takenQty := firstNonEmpty(e.GetTakenOverQty(), e.GetClosedQty())
	takeoverPrice := firstNonEmpty(e.GetTakeoverPrice(), e.GetBankruptcyPrice())
	b.TakeoverLots = append(b.TakeoverLots, PerpTakeoverLotRow{
		LotID: lotID, PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionAfter().GetPositionIdx()),
		Side:        int8(e.GetInventorySide()), TotalQty: defaultZero(takenQty), LeavesQty: defaultZero(takenQty),
		TakeoverPrice: defaultZero(takeoverPrice), TriggerMarkPrice: defaultZero(e.GetMarkPrice()),
		TakenOverBalance:  defaultZero(firstNonEmpty(e.GetTakenOverBalance(), e.GetInsuranceDelta())),
		WorkingCapitalRef: "takeover:" + lotID, Status: "Init", TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendPerpADL(b *PerpBatch, e *eventpb.PerpAdlEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	factQty := firstNonEmpty(e.GetFactQty(), e.GetClosedQty())
	b.ADL = append(b.ADL, PerpADLRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionAfter().GetPositionIdx()), LotID: e.GetLotId(),
		AdlRound: e.GetAdlRound(), Price: e.GetPrice(),
		RequestedQty: defaultZero(firstNonEmpty(e.GetRequestedQty(), factQty)),
		FactQty:      defaultZero(factQty), RealizedPnl: defaultZero(e.GetRealizedPnl()), TsUnixMs: ts,
	})
	appendPerpPosition(b, e.GetPositionAfter(), seq, ts)
}

func appendRiskPoolSettlement(b *PerpBatch, e *eventpb.RiskPoolSettlementEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.RiskPool = append(b.RiskPool, RiskPoolSettlementRow{
		LotID: e.GetLotId(), PerpSeqID: seq, Symbol: e.GetSymbol(), Coin: e.GetCoin(),
		WorkingCapitalRef:   e.GetWorkingCapitalRef(),
		TakenOverBalance:    defaultZero(e.GetTakenOverBalance()),
		LiqAdlRealisedPnl:   defaultZero(e.GetLiqAdlRealisedPnl()),
		CumFee:              defaultZero(e.GetCumFee()),
		WorkingCapitalDrawn: defaultZero(e.GetWorkingCapitalDrawn()),
		BorrowedBalance:     defaultZero(e.GetBorrowedBalance()),
		FinalPoolDelta:      defaultZero(e.GetFinalPoolDelta()),
		Status:              e.GetStatus(), TsUnixMs: ts,
	})
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
		UserID: snap.GetUserId(), Symbol: snap.GetSymbol(),
		PositionIdx: uint8(snap.GetPositionIdx()), Side: int8(snap.GetSide()),
		Size: defaultZero(snap.GetSize()), EntryPrice: defaultZero(snap.GetEntryPrice()),
		Margin: defaultZero(snap.GetMargin()), Leverage: defaultZero(snap.GetLeverage()),
		RealizedPnl: defaultZero(snap.GetRealizedPnl()),
		MarginMode:  int8(snap.GetMarginMode()), RiskID: snap.GetRiskId(),
		Version:   snap.GetVersion(),
		PerpSeqID: seq, UpdatedAtMs: ts,
	})
}

func appendPerpConfigLog(b *PerpBatch, e *eventpb.PerpPositionConfigEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.ConfigLogs = append(b.ConfigLogs, PerpPositionConfigLogRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		MarginMode:   int8(e.GetMarginMode()),
		PositionMode: int8(e.GetPositionMode()), PositionIdx: uint8(e.GetPositionIdx()),
		Leverage: defaultZero(e.GetLeverage()),
		RiskID:   e.GetRiskId(), AutoAddMargin: e.GetAutoAddMargin(),
		AutoAddMax: defaultZero(e.GetAutoAddMax()), PositionVersion: e.GetPositionVersion(),
		Reason: e.GetReason(), ClientOpID: e.GetClientOpId(), TsUnixMs: ts,
	})
}

func appendPerpMarginAdjustment(b *PerpBatch, e *eventpb.PerpMarginAdjustmentEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.MarginAdjust = append(b.MarginAdjust, PerpMarginAdjustmentRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionIdx()),
		Kind:        int8(e.GetKind()), Amount: defaultZero(e.GetAmount()),
		MarginBefore: defaultZero(e.GetMarginBefore()), MarginAfter: defaultZero(e.GetMarginAfter()),
		WalletAfter: defaultZero(e.GetWalletAfter()), PositionVersion: e.GetPositionVersion(),
		ClientOpID: e.GetClientOpId(), MarkPrice: defaultZero(e.GetMarkPrice()), TsUnixMs: ts,
	})
}

func appendPerpBreach(b *PerpBatch, e *eventpb.PerpInvariantBreachEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.Breaches = append(b.Breaches, PerpInvariantBreachRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		PositionIdx: uint8(e.GetPositionIdx()), OrderID: e.GetOrderId(), TradeID: e.GetTradeId(),
		Kind: e.GetKind(), ExcessQty: defaultZero(e.GetExcessQty()),
		FillPrice: defaultZero(e.GetFillPrice()), TsUnixMs: ts,
	})
}

func appendPerpRiskLimit(b *PerpBatch, e *eventpb.PerpCustomerRiskLimitEvent, seq uint64, ts int64) {
	if e == nil {
		return
	}
	b.RiskLimits = append(b.RiskLimits, PerpCustomerRiskLimitRow{
		PerpSeqID: seq, UserID: e.GetUserId(), Symbol: e.GetSymbol(),
		MaxLeverage: defaultZero(e.GetMaxLeverage()), Reason: e.GetReason(),
		UpdatedBy: e.GetUpdatedBy(), TsUnixMs: ts,
	})
}
