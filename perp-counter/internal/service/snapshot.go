package service

// snapshot.go serializes the service-owned recovery state: the live order store,
// the consumed perp-trade-event offsets, the journal sequence counters, and the
// in-flight liquidation registry (ADR-0068 invariant #5 — everything needed to
// resume must be persisted, not just the engine's positions/wallets). Capture
// runs under the snapshotMu barrier so the image is atomic with the engine
// snapshot and no consumer is mid-mutation.

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
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
	FilledQty   string `json:"filled_qty"`
	Status      int32  `json:"status"`
	CreatedMs   int64  `json:"created_ms"`
	UpdatedMs   int64  `json:"updated_ms"`

	// ADR-0075: admission config version (re-stamped on recovery dispatches).
	ConfigVersion uint64 `json:"config_version,omitempty"`
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
			ReduceOnly:  o.ReduceOnly, ReservedIM: o.ReservedIM.String(), FilledQty: o.FilledQty.String(),
			Status: int32(o.Status), CreatedMs: o.CreatedMs, UpdatedMs: o.UpdatedMs,
			ConfigVersion: o.ConfigVersion,
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
	for _, os := range snap.Orders {
		s.orders[os.OrderID] = &Order{
			OrderID: os.OrderID, ClientID: os.ClientID, UserID: os.UserID, Symbol: os.Symbol,
			Side: perpstate.Side(os.Side), Type: eventpb.OrderType(os.Type), TIF: eventpb.TimeInForce(os.TIF),
			Price: dec.New(os.Price), Qty: dec.New(os.Qty), Leverage: dec.New(os.Leverage),
			Mode:        perpstate.MarginMode(os.Mode),
			PositionIdx: os.PositionIdx,
			SlippageBps: os.SlippageBps,
			ReduceOnly:  os.ReduceOnly, ReservedIM: dec.New(os.ReservedIM), FilledQty: dec.New(os.FilledQty),
			Status: eventpb.InternalOrderStatus(os.Status), CreatedMs: os.CreatedMs, UpdatedMs: os.UpdatedMs,
			ConfigVersion: os.ConfigVersion,
		}
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
