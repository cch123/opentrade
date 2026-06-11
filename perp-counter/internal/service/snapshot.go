package service

// snapshot.go serializes the service-owned recovery state: the live order store,
// the consumed perp-trade-event offsets, the journal sequence counters, and the
// in-flight liquidation registry (ADR-0068 invariant #5 — everything needed to
// resume must be persisted, not just the engine's positions/wallets). Capture
// runs under the snapshotMu barrier so the image is atomic with the engine
// snapshot and no consumer is mid-mutation.

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Snapshot is the serializable image of the service layer. Decimals are strings
// and enums int32 so it round-trips cleanly through JSON / proto (ADR-0049).
type Snapshot struct {
	Orders       []OrderSnap     `json:"orders"`
	Offsets      map[int32]int64 `json:"offsets"` // next-to-consume perp-trade-event offset per partition
	PerpSeq      uint64          `json:"perp_seq"`
	OrderSeq     uint64          `json:"order_seq"`
	AdlRound     uint64          `json:"adl_round"`
	Liquidations []LiqSnap       `json:"liquidations"`

	// ADR-0078 state. The terminal-COID ring keeps the dedup promise across
	// restarts; pending amends / close-all runs carry their pre-allocated
	// order ids (the replay-convergence anchors, 修订 #6); the admin-op
	// caches keep ForceAdjust / BlockTrade idempotent across restarts.
	TerminalCOIDs []COIDSnap            `json:"terminal_coids,omitempty"`
	Amends        []AmendSnap           `json:"amends,omitempty"`
	CloseAlls     []CloseAllSnap        `json:"close_alls,omitempty"`
	AdjustDone    map[string]AdjustSnap `json:"adjust_done,omitempty"`
	BlockDone     map[string]BlockSnap  `json:"block_done,omitempty"`
}

// COIDSnap is one terminal client_order_id ring entry (FIFO order preserved).
type COIDSnap struct {
	UserID  uint64 `json:"user_id"`
	COID    string `json:"coid"`
	OrderID uint64 `json:"order_id"`
}

// AmendSnap is one pending amend (ADR-0078 §2).
type AmendSnap struct {
	UserID     uint64 `json:"user_id"`
	Symbol     string `json:"symbol"`
	OldOrderID uint64 `json:"old_order_id"`
	NewOrderID uint64 `json:"new_order_id"`
	NewPrice   string `json:"new_price"`
	NewQty     string `json:"new_qty"`
}

// CloseAllSnap is one close-all run (ADR-0078 §5), including terminal runs
// (kept for idempotent re-reads).
type CloseAllSnap struct {
	UserID         uint64            `json:"user_id"`
	OpID           string            `json:"op_id"`
	Symbol         string            `json:"symbol,omitempty"`
	Symbols        []string          `json:"symbols"`
	SlippageBps    uint32            `json:"slippage_bps"`
	Phase          int32             `json:"phase"`
	PendingCancels []uint64          `json:"pending_cancels,omitempty"`
	PendingCloses  []uint64          `json:"pending_closes,omitempty"`
	LegIDs         map[string]uint64 `json:"leg_ids,omitempty"`
	Legs           []CloseAllLegSnap `json:"legs,omitempty"`
}

// CloseAllLegSnap is one close-all leg outcome row.
type CloseAllLegSnap struct {
	Symbol  string `json:"symbol"`
	Idx     uint8  `json:"idx"`
	OrderID uint64 `json:"order_id"`
	Qty     string `json:"qty,omitempty"`
	Reject  string `json:"reject,omitempty"`
}

// AdjustSnap caches one ForceAdjustPosition outcome (ADR-0078 §7).
type AdjustSnap struct {
	Accepted         bool   `json:"accepted"`
	RejectReason     string `json:"reject_reason,omitempty"`
	PositionSize     string `json:"position_size,omitempty"`
	EntryPrice       string `json:"entry_price,omitempty"`
	Margin           string `json:"margin,omitempty"`
	RealizedPnl      string `json:"realized_pnl,omitempty"`
	FreeBalanceAfter string `json:"free_balance_after,omitempty"`
}

// BlockSnap caches one BlockTrade outcome (ADR-0078 §8).
type BlockSnap struct {
	Accepted     bool   `json:"accepted"`
	RejectReason string `json:"reject_reason,omitempty"`
	TradeID      string `json:"trade_id,omitempty"`
}

// OrderSnap is one live (non-terminal) order. Terminal orders are evicted, so
// they never appear here.
type OrderSnap struct {
	OrderID     uint64 `json:"order_id"`
	ClientID    string `json:"client_id"`
	UserID      uint64 `json:"user_id"`
	Symbol      string `json:"symbol"`
	Side        uint8  `json:"side"`
	Type        int32  `json:"type"`
	TIF         int32  `json:"tif"`
	Price       string `json:"price"`
	Qty         string `json:"qty"`
	Leverage    string `json:"leverage"`
	Mode        uint8  `json:"mode,omitempty"`         // perpstate.MarginMode (ADR-0074)
	PositionIdx uint8  `json:"position_idx,omitempty"` // ADR-0077 order intent (0 = net)
	ReduceOnly  bool   `json:"reduce_only"`
	SlippageBps uint32 `json:"slippage_bps,omitempty"` // ADR-0083 protected market order
	ReservedIM  string `json:"reserved_im"`
	ReservedFee string `json:"reserved_fee,omitempty"` // ADR-0079 fee buffer remainder
	FilledQty   string `json:"filled_qty"`
	Status      int32  `json:"status"`
	CreatedMs   int64  `json:"created_ms"`
	UpdatedMs   int64  `json:"updated_ms"`

	// ADR-0075: admission config version (re-stamped on recovery dispatches).
	ConfigVersion uint64 `json:"config_version,omitempty"`

	// ADR-0079 §1 fee pin — must survive restart or replayed fills would
	// re-resolve fees against post-crash state.
	FeeRuleID          string `json:"fee_rule_id,omitempty"`
	FeeMakerRate       string `json:"fee_maker_rate,omitempty"`
	FeeTakerRate       string `json:"fee_taker_rate,omitempty"`
	FeeAsset           string `json:"fee_asset,omitempty"`
	FeeMakerSuppressed bool   `json:"fee_maker_suppressed,omitempty"`
}

// LiqSnap is one in-flight liquidation (bankruptcy order placed, not yet
// fully filled) so its fills still route to insurance after recovery.
type LiqSnap struct {
	UserID      uint64 `json:"user_id"`
	Symbol      string `json:"symbol"`
	PositionIdx uint8  `json:"position_idx,omitempty"` // ADR-0077 leg under liquidation
	OrderID     uint64 `json:"order_id"`
	Mode        uint8  `json:"mode"`
	Side        uint8  `json:"side"`
	OrderPrice  string `json:"order_price"`
	Bankruptcy  string `json:"bankruptcy"`
	LiqFeeRate  string `json:"liq_fee_rate"`
	RiskTier    int32  `json:"risk_tier"`
	Ticks       int    `json:"ticks"`

	// ADR-0075 §3 provenance (mirrors the liquidation registry fields).
	ConfigVersion uint64 `json:"config_version,omitempty"`
	PolicyID      string `json:"policy_id,omitempty"`
}

// Capture takes the barrier, flushes the producer (so every emitted journal /
// order-event is durable before the bound offsets are recorded — ADR-0048
// output flush barrier), then returns a consistent engine + service image. A
// nil flush skips the barrier flush (tests). flush errors abort the capture so
// a snapshot is never written with offsets ahead of un-acked output.
func (s *Service) Capture(flush func() error) (engine.Snapshot, Snapshot, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if flush != nil {
		if err := flush(); err != nil {
			return engine.Snapshot{}, Snapshot{}, err
		}
	}
	return s.eng.Snapshot(), s.snapshotLocked(), nil
}

// snapshotLocked builds the service image. Caller holds snapshotMu (write).
func (s *Service) snapshotLocked() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := Snapshot{
		Offsets:  make(map[int32]int64, len(s.offsets)),
		PerpSeq:  s.perpSeq,
		OrderSeq: s.orderSeq,
		AdlRound: s.adlRound,
	}
	for p, o := range s.offsets {
		snap.Offsets[p] = o
	}
	for _, o := range s.orders {
		snap.Orders = append(snap.Orders, OrderSnap{
			OrderID: o.OrderID, ClientID: o.ClientID, UserID: o.UserID, Symbol: o.Symbol,
			Side: uint8(o.Side), Type: int32(o.Type), TIF: int32(o.TIF),
			Price: o.Price.String(), Qty: o.Qty.String(), Leverage: o.Leverage.String(),
			Mode:        uint8(o.Mode),
			PositionIdx: o.PositionIdx,
			SlippageBps: o.SlippageBps,
			ReduceOnly:  o.ReduceOnly, ReservedIM: o.ReservedIM.String(), ReservedFee: o.ReservedFee.String(),
			FilledQty: o.FilledQty.String(),
			Status:    int32(o.Status), CreatedMs: o.CreatedMs, UpdatedMs: o.UpdatedMs,
			ConfigVersion: o.ConfigVersion,
			FeeRuleID:     o.FeeRuleID, FeeMakerRate: o.FeeMakerRate.String(), FeeTakerRate: o.FeeTakerRate.String(),
			FeeAsset: o.FeeAsset, FeeMakerSuppressed: o.FeeMakerSuppressed,
		})
	}
	for _, liq := range s.liqByOrder {
		snap.Liquidations = append(snap.Liquidations, LiqSnap{
			UserID: liq.userID, Symbol: liq.symbol, PositionIdx: liq.positionIdx, OrderID: liq.orderID,
			Mode: uint8(liq.mode), Side: uint8(liq.side), OrderPrice: liq.orderPrice.String(),
			Bankruptcy: liq.bankruptcy.String(), LiqFeeRate: liq.liqFeeRate.String(),
			RiskTier: liq.tier, Ticks: liq.ticks,
			ConfigVersion: liq.configVersion, PolicyID: liq.policyID,
		})
	}
	for _, e := range s.coidRing.fifo {
		snap.TerminalCOIDs = append(snap.TerminalCOIDs, COIDSnap{UserID: e.User, COID: e.COID, OrderID: e.OrderID})
	}
	for _, pa := range s.amends {
		snap.Amends = append(snap.Amends, AmendSnap{
			UserID: pa.UserID, Symbol: pa.Symbol,
			OldOrderID: pa.OldOrderID, NewOrderID: pa.NewOrderID,
			NewPrice: pa.NewPrice.String(), NewQty: pa.NewQty.String(),
		})
	}
	for _, ca := range s.closeAlls {
		cs := CloseAllSnap{
			UserID: ca.UserID, OpID: ca.OpID, Symbol: ca.Symbol, Symbols: ca.Symbols,
			SlippageBps: ca.SlippageBps, Phase: int32(ca.Phase), LegIDs: ca.LegIDs,
		}
		for id := range ca.PendingCancels {
			cs.PendingCancels = append(cs.PendingCancels, id)
		}
		for id := range ca.PendingCloses {
			cs.PendingCloses = append(cs.PendingCloses, id)
		}
		for _, leg := range ca.Legs {
			cs.Legs = append(cs.Legs, CloseAllLegSnap{
				Symbol: leg.Symbol, Idx: leg.Idx, OrderID: leg.OrderID,
				Qty: leg.Qty.String(), Reject: leg.Reject,
			})
		}
		snap.CloseAlls = append(snap.CloseAlls, cs)
	}
	if len(s.adjustDone) > 0 {
		snap.AdjustDone = make(map[string]AdjustSnap, len(s.adjustDone))
		for op, r := range s.adjustDone {
			snap.AdjustDone[op] = AdjustSnap{
				Accepted: r.Accepted, RejectReason: r.RejectReason,
				PositionSize: r.PositionSize, EntryPrice: r.EntryPrice, Margin: r.Margin,
				RealizedPnl: r.RealizedPnl, FreeBalanceAfter: r.FreeBalanceAfter,
			}
		}
	}
	if len(s.blockDone) > 0 {
		snap.BlockDone = make(map[string]BlockSnap, len(s.blockDone))
		for id, r := range s.blockDone {
			snap.BlockDone[id] = BlockSnap{Accepted: r.Accepted, RejectReason: r.RejectReason, TradeID: r.TradeId}
		}
	}
	return snap
}

// Restore rebuilds the service state from a snapshot (startup, before the
// consumers seek to the bound offsets). Replaces any current state.
func (s *Service) Restore(snap Snapshot) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.perpSeq = snap.PerpSeq
	s.orderSeq = snap.OrderSeq
	s.adlRound = snap.AdlRound
	s.offsets = make(map[int32]int64, len(snap.Offsets))
	for p, o := range snap.Offsets {
		s.offsets[p] = o
	}
	s.orders = make(map[uint64]*Order, len(snap.Orders))
	s.activeByCOID = map[uint64]map[string]uint64{}
	for _, os := range snap.Orders {
		o := &Order{
			OrderID: os.OrderID, ClientID: os.ClientID, UserID: os.UserID, Symbol: os.Symbol,
			Side: perpstate.Side(os.Side), Type: eventpb.OrderType(os.Type), TIF: eventpb.TimeInForce(os.TIF),
			Price: dec.New(os.Price), Qty: dec.New(os.Qty), Leverage: dec.New(os.Leverage),
			Mode:        perpstate.MarginMode(os.Mode),
			PositionIdx: os.PositionIdx,
			SlippageBps: os.SlippageBps,
			ReduceOnly:  os.ReduceOnly, ReservedIM: dec.New(os.ReservedIM), ReservedFee: snapDecimal(os.ReservedFee),
			FilledQty: dec.New(os.FilledQty),
			Status:    eventpb.InternalOrderStatus(os.Status), CreatedMs: os.CreatedMs, UpdatedMs: os.UpdatedMs,
			ConfigVersion: os.ConfigVersion,
			FeeRuleID:     os.FeeRuleID, FeeMakerRate: snapDecimal(os.FeeMakerRate), FeeTakerRate: snapDecimal(os.FeeTakerRate),
			FeeAsset: os.FeeAsset, FeeMakerSuppressed: os.FeeMakerSuppressed,
		}
		s.orders[o.OrderID] = o
		s.indexCOIDLocked(o)
	}
	s.coidRing = newCOIDRing(s.cfg.TerminalCOIDCap)
	for _, e := range snap.TerminalCOIDs {
		s.coidRing.add(e.UserID, e.COID, e.OrderID)
	}
	s.amends = make(map[uint64]*pendingAmend, len(snap.Amends))
	for _, a := range snap.Amends {
		s.amends[a.OldOrderID] = &pendingAmend{
			UserID: a.UserID, Symbol: a.Symbol,
			OldOrderID: a.OldOrderID, NewOrderID: a.NewOrderID,
			NewPrice: dec.New(a.NewPrice), NewQty: dec.New(a.NewQty),
		}
	}
	s.closeAlls = make(map[uint64]*closeAllState, len(snap.CloseAlls))
	for _, cs := range snap.CloseAlls {
		ca := &closeAllState{
			UserID: cs.UserID, OpID: cs.OpID, Symbol: cs.Symbol, Symbols: cs.Symbols,
			SlippageBps: cs.SlippageBps, Phase: perprpc.CloseAllPhase(cs.Phase),
			PendingCancels: map[uint64]struct{}{}, PendingCloses: map[uint64]struct{}{},
			LegIDs: cs.LegIDs,
		}
		if ca.LegIDs == nil {
			ca.LegIDs = map[string]uint64{}
		}
		for _, id := range cs.PendingCancels {
			ca.PendingCancels[id] = struct{}{}
		}
		for _, id := range cs.PendingCloses {
			ca.PendingCloses[id] = struct{}{}
		}
		for _, leg := range cs.Legs {
			ca.Legs = append(ca.Legs, closeAllLeg{
				Symbol: leg.Symbol, Idx: leg.Idx, OrderID: leg.OrderID,
				Qty: snapDecimal(leg.Qty), Reject: leg.Reject,
			})
		}
		s.closeAlls[ca.UserID] = ca
	}
	s.adjustDone = make(map[string]*perprpc.ForceAdjustPositionResponse, len(snap.AdjustDone))
	for op, r := range snap.AdjustDone {
		s.adjustDone[op] = &perprpc.ForceAdjustPositionResponse{
			Accepted: r.Accepted, RejectReason: r.RejectReason,
			PositionSize: r.PositionSize, EntryPrice: r.EntryPrice, Margin: r.Margin,
			RealizedPnl: r.RealizedPnl, FreeBalanceAfter: r.FreeBalanceAfter,
		}
	}
	s.blockDone = make(map[string]*perprpc.BlockTradeResponse, len(snap.BlockDone))
	for id, r := range snap.BlockDone {
		s.blockDone[id] = &perprpc.BlockTradeResponse{Accepted: r.Accepted, RejectReason: r.RejectReason, TradeId: r.TradeID}
	}
	s.liqByKey = make(map[string]*liquidation, len(snap.Liquidations))
	s.liqByOrder = make(map[uint64]*liquidation, len(snap.Liquidations))
	for _, ls := range snap.Liquidations {
		liq := &liquidation{
			userID: ls.UserID, symbol: ls.Symbol, positionIdx: ls.PositionIdx, orderID: ls.OrderID,
			mode: liquidationMode(ls.Mode), side: perpstate.Side(ls.Side),
			orderPrice: snapDecimal(ls.OrderPrice), bankruptcy: dec.New(ls.Bankruptcy),
			liqFeeRate: snapDecimal(ls.LiqFeeRate), tier: ls.RiskTier, ticks: ls.Ticks,
			configVersion: ls.ConfigVersion, policyID: ls.PolicyID,
		}
		if liq.mode == 0 {
			liq.mode = liquidationFull
		}
		s.liqByKey[liqKey(ls.UserID, ls.Symbol, ls.PositionIdx)] = liq
		s.liqByOrder[ls.OrderID] = liq
	}
}

func snapDecimal(v string) dec.Decimal {
	if v == "" {
		return zero
	}
	return dec.New(v)
}

// ConsumedOffsetsForResume returns the per-partition next-to-consume offsets a
// restored service should seed the trade-event consumer with.
func (s *Service) ConsumedOffsetsForResume() map[int32]int64 { return s.ConsumedOffsets() }
