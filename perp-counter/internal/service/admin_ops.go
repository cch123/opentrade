package service

// admin_ops.go is the ADR-0078 admin plane: §7 ForceAdjustPosition (audited
// position repair reusing the fill-settlement math, never a trade row) and
// §8 BlockTrade (bilateral off-book execution under both users' sequencers).
// Both are idempotent on their op ids; the outcome caches persist in the
// snapshot so an admin retry after a restart cannot double-apply.

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// --- §7 force add/sub ----------------------------------------------------------

// ForceAdjustPosition force-adds or force-subs one position leg at an
// admin-specified price. Economics (修订 #8): the same fill primitives as a
// real execution (entry averaging, realized PnL, margin routing) — but the
// journal record is a dedicated PerpAdminPositionAdjustmentEvent: no trade
// row, no fee, no market data. Catalog STATUS gates are bypassed (repairs
// may target HALTed symbols); precision checks still run when the catalog
// covers the symbol.
func (s *Service) ForceAdjustPosition(req *perprpc.ForceAdjustPositionRequest) (*perprpc.ForceAdjustPositionResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	if req.GetClientOpId() == "" {
		return nil, errInvalid("client_op_id required")
	}
	if req.GetReason() == "" || req.GetTicket() == "" || req.GetOperator() == "" {
		return nil, errInvalid("reason, ticket and operator required")
	}
	qty, err := dec.Parse(req.GetQty())
	if err != nil || qty.Sign() <= 0 {
		return nil, errInvalid("invalid qty")
	}
	price, err := dec.Parse(req.GetPrice())
	if err != nil || price.Sign() <= 0 {
		return nil, errInvalid("invalid price")
	}
	if req.GetPositionIdx() > uint32(perpstate.IdxShort) {
		return nil, errInvalid("invalid position_idx")
	}
	idx := uint8(req.GetPositionIdx())
	resp := &perprpc.ForceAdjustPositionResponse{}
	s.seq.do(user, func() {
		if prev := s.cachedAdjust(req.GetClientOpId()); prev != nil {
			copyAdjustResp(resp, prev)
			return
		}
		reject := func(reason string) { resp.RejectReason = reason }
		if r := validateLegIdx(s.eng.PositionModeOf(user, symbol), idx); r != "" {
			reject(r)
			return
		}
		if s.hasLiquidation(liqKey(user, symbol, idx)) {
			reject("liquidation_in_flight")
			return
		}
		// Precision-only catalog check — status gates deliberately bypassed
		// (修订 #8). A stale/absent catalog does not block an admin repair.
		if s.catalog != nil && !s.catalog.Stale() {
			if view, ok := s.catalog.Active(symbol); ok {
				if r := view.Cfg.CheckOrder(price, qty, false); r != "" {
					reject(r)
					return
				}
			}
		}
		var res perpstate.FillResult
		var fillSide perpstate.Side
		if req.GetSub() {
			pos, ok := s.eng.PositionRaw(user, symbol, idx)
			if !ok || pos.Size.Sign() == 0 {
				reject("position_not_found")
				return
			}
			if qty.Cmp(pos.Size) > 0 {
				reject("qty_exceeds_position") // no flip through an admin sub
				return
			}
			fillSide = pos.Side.Opposite()
			res, _ = s.eng.ApplyFill(user, symbol, idx, pos.Leverage, perpstate.Fill{Side: fillSide, Price: price, Qty: qty, Fee: zero})
		} else {
			fillSide = fromEventSide(req.GetSide())
			if fillSide == 0 {
				reject("side_required_for_add")
				return
			}
			symCfg := s.eng.SymbolOrderConfigOf(user, symbol)
			if symCfg.PosMode == perpstate.PositionHedge && perpstate.LegSide(idx) != fillSide {
				reject("side_mismatch_leg")
				return
			}
			lev := symCfg.Leverage
			if pos, ok := s.eng.PositionRaw(user, symbol, idx); ok && pos.Size.Sign() > 0 {
				if pos.Side != fillSide {
					reject("would_reduce_use_sub") // an ADD never nets down
					return
				}
				if pos.Leverage.Sign() > 0 {
					lev = pos.Leverage
				}
			}
			if lev.Sign() <= 0 {
				reject("leverage_not_configured")
				return
			}
			im := perpstate.InitMargin(price, qty, lev)
			if symCfg.MarginMode == perpstate.MarginCross {
				if reason, ok := s.eng.CrossOrderCheck(user, symbol, idx, fillSide, price, qty, lev, im, s.cfg.TargetMarginBuffer); !ok {
					reject(reason)
					return
				}
				if !s.eng.ReserveCross(user, im) {
					reject("insufficient_margin")
					return
				}
			} else if !s.eng.Reserve(user, im) {
				reject("insufficient_margin")
				return
			}
			res, _ = s.eng.ApplyFill(user, symbol, idx, lev, perpstate.Fill{Side: fillSide, Price: price, Qty: qty, Fee: zero})
			// Return whatever the fill did not commit: an isolated increase
			// draws MarginAdded from the reservation (leftover is rounding
			// defense); a cross fill commits nothing (the exposure becomes a
			// derived requirement) so the whole hold converts back to free.
			if symCfg.MarginMode == perpstate.MarginCross {
				s.eng.ReleaseCross(user, im)
			} else if leftover := im.Sub(res.MarginAdded); leftover.Sign() > 0 {
				s.eng.Release(user, leftover)
			}
		}
		moved := res.MarginAdded
		if req.GetSub() {
			moved = res.MarginReleased
		}
		s.journal.Emit(&eventpb.PerpJournalEvent{
			Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
			Payload: &eventpb.PerpJournalEvent_AdminAdjustment{AdminAdjustment: &eventpb.PerpAdminPositionAdjustmentEvent{
				UserId: user, Symbol: symbol, PositionIdx: uint32(idx),
				Sub: req.GetSub(), Side: toEventSide(fillSide),
				Qty: qty.String(), Price: price.String(),
				RealizedPnl: res.Realized.String(), MarginMoved: moved.String(),
				Reason: req.GetReason(), Ticket: req.GetTicket(), Operator: req.GetOperator(),
				ClientOpId:    req.GetClientOpId(),
				PositionAfter: s.positionSnap(user, symbol, idx),
			}},
		})
		resp.Accepted = true
		pos, _ := s.eng.PositionRaw(user, symbol, idx)
		resp.PositionSize = pos.Size.String()
		resp.EntryPrice = pos.Entry.String()
		resp.Margin = pos.Margin.String()
		resp.RealizedPnl = res.Realized.String()
		resp.FreeBalanceAfter = s.eng.WalletOf(user).Available.String()
		s.cacheAdjust(req.GetClientOpId(), resp)
	})
	return resp, nil
}

func (s *Service) cachedAdjust(opID string) *perprpc.ForceAdjustPositionResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adjustDone[opID]
}

func (s *Service) cacheAdjust(opID string, resp *perprpc.ForceAdjustPositionResponse) {
	cp := &perprpc.ForceAdjustPositionResponse{}
	copyAdjustResp(cp, resp)
	s.mu.Lock()
	s.adjustDone[opID] = cp
	s.mu.Unlock()
}

// copyAdjustResp copies the payload fields (a proto message must not be
// value-copied — it embeds an impl mutex).
func copyAdjustResp(dst, src *perprpc.ForceAdjustPositionResponse) {
	dst.Accepted = src.Accepted
	dst.RejectReason = src.RejectReason
	dst.PositionSize = src.PositionSize
	dst.EntryPrice = src.EntryPrice
	dst.Margin = src.Margin
	dst.RealizedPnl = src.RealizedPnl
	dst.FreeBalanceAfter = src.FreeBalanceAfter
}

// --- §8 block trade --------------------------------------------------------------

// blockLeg is one side's resolved admission state.
type blockLeg struct {
	spec orderSpec
	adm  admission
	cost dec.Decimal // im + feeBuf reserved for a non-reduce-only leg
}

// BlockTrade applies a bilateral off-book execution: both legs admit and
// settle inside BOTH users' sequencer locks (ordered acquisition, do2), so
// validation and application share one critical section — no TOCTOU window,
// hence no version-stamp re-validation needed in the single-instance form
// (ADR-0078 修订 #9). Any leg failure rejects the whole trade with every
// reservation released.
func (s *Service) BlockTrade(req *perprpc.BlockTradeRequest) (*perprpc.BlockTradeResponse, error) {
	if req.GetBlockTradeId() == "" {
		return nil, errInvalid("block_trade_id required")
	}
	if req.GetSymbol() == "" {
		return nil, errInvalid("symbol required")
	}
	if req.GetReason() == "" || req.GetTicket() == "" || req.GetOperator() == "" {
		return nil, errInvalid("reason, ticket and operator required")
	}
	price, err := dec.Parse(req.GetPrice())
	if err != nil || price.Sign() <= 0 {
		return nil, errInvalid("invalid price")
	}
	qty, err := dec.Parse(req.GetQty())
	if err != nil || qty.Sign() <= 0 {
		return nil, errInvalid("invalid qty")
	}
	buyer, seller := req.GetBuyer(), req.GetSeller()
	if buyer.GetUserId() == 0 || seller.GetUserId() == 0 {
		return nil, errInvalid("buyer and seller user_id required")
	}
	if buyer.GetPositionIdx() > uint32(perpstate.IdxShort) || seller.GetPositionIdx() > uint32(perpstate.IdxShort) {
		return nil, errInvalid("invalid position_idx")
	}
	resp := &perprpc.BlockTradeResponse{}
	if buyer.GetUserId() == seller.GetUserId() {
		resp.RejectReason = "same_user"
		return resp, nil
	}
	s.seq.do2(buyer.GetUserId(), seller.GetUserId(), func() {
		if prev := s.cachedBlock(req.GetBlockTradeId()); prev != nil {
			copyBlockResp(resp, prev)
			return
		}
		finish := func(reason string) {
			resp.RejectReason = reason
			s.cacheBlock(req.GetBlockTradeId(), resp)
		}
		mark := s.eng.MarkOf(req.GetSymbol())
		if mark.Sign() <= 0 {
			finish("no_mark")
			return
		}
		// Price sanity band (§8): |price - mark| / mark <= band_bps.
		diff := price.Sub(mark).Abs()
		if diff.Mul(dec.FromInt(10_000)).Cmp(mark.Mul(dec.FromInt(int64(s.cfg.BlockTradeBandBps)))) > 0 {
			finish("price_out_of_band")
			return
		}
		legs := [2]*blockLeg{}
		wires := [2]*perprpc.BlockTradeLeg{buyer, seller}
		names := [2]string{"buyer", "seller"}
		sides := [2]perpstate.Side{perpstate.SideBuy, perpstate.SideSell}
		for i, wire := range wires {
			leg, reason := s.admitBlockLegLocked(req.GetSymbol(), sides[i], price, qty, wire)
			if reason != "" {
				finish(names[i] + ": " + reason)
				return
			}
			legs[i] = leg
		}
		// All-or-nothing reservations: the second failure releases the first.
		if reason := s.reserveBlockLeg(legs[0]); reason != "" {
			finish(names[0] + ": " + reason)
			return
		}
		if reason := s.reserveBlockLeg(legs[1]); reason != "" {
			s.releaseBlockLeg(legs[0])
			finish(names[1] + ": " + reason)
			return
		}
		// Both legs hold their cost — apply both fills. ApplyFill cannot fail
		// past this point (the margin gate is admission; reduce legs were
		// capacity-checked under this same critical section).
		tradeID := "block-" + req.GetBlockTradeId()
		for i := range legs {
			s.applyBlockLeg(legs[i], tradeID)
		}
		s.journal.Emit(&eventpb.PerpJournalEvent{
			Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
			Payload: &eventpb.PerpJournalEvent_BlockTrade{BlockTrade: &eventpb.PerpBlockTradeEvent{
				BlockTradeId: req.GetBlockTradeId(), Symbol: req.GetSymbol(),
				Price: price.String(), Qty: qty.String(),
				BuyerUserId: buyer.GetUserId(), BuyerPositionIdx: buyer.GetPositionIdx(), BuyerReduceOnly: buyer.GetReduceOnly(),
				SellerUserId: seller.GetUserId(), SellerPositionIdx: seller.GetPositionIdx(), SellerReduceOnly: seller.GetReduceOnly(),
				Reason: req.GetReason(), Ticket: req.GetTicket(), Operator: req.GetOperator(),
				MarkPrice: mark.String(),
			}},
		})
		resp.Accepted = true
		resp.TradeId = tradeID
		s.cacheBlock(req.GetBlockTradeId(), resp)
	})
	return resp, nil
}

// admitBlockLegLocked runs one leg through the standard order admission
// (catalog gates incl. status — a block trade is a trade, not a repair —
// intent matrix, leverage, fee pin, tier cap) plus the block-specific
// guards. Caller holds both users' seq locks.
func (s *Service) admitBlockLegLocked(symbol string, side perpstate.Side, price, qty dec.Decimal, wire *perprpc.BlockTradeLeg) (*blockLeg, string) {
	user := wire.GetUserId()
	idx := uint8(wire.GetPositionIdx())
	if s.closeAllBlocksLocked(user, symbol) {
		return nil, "close_all_in_progress"
	}
	if s.hasLiquidation(liqKey(user, symbol, idx)) {
		return nil, "liquidation_in_flight"
	}
	sp := orderSpec{
		User: user, Symbol: symbol, Side: side,
		Type: eventpb.OrderType_ORDER_TYPE_LIMIT, TIF: eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Price: price, Qty: qty, ReqLev: zero,
		ReduceOnly: wire.GetReduceOnly(), PosIdx: idx,
	}
	adm := s.admitOrderLocked(sp, true)
	if adm.reject != "" {
		return nil, adm.reject
	}
	// A reduce leg settles immediately (no Match round-trip), so the clamp
	// must be provably unreachable: qty bounded by the leg size under this
	// same critical section.
	if sp.ReduceOnly {
		pos, ok := s.eng.PositionRaw(user, symbol, idx)
		if !ok || pos.Size.Cmp(qty) < 0 {
			return nil, "reduce_only_exceeds_position"
		}
	}
	return &blockLeg{spec: sp, adm: adm, cost: adm.im.Add(adm.feeBuf)}, ""
}

// reserveBlockLeg holds a non-reduce-only leg's order cost (IM + fee buffer).
func (s *Service) reserveBlockLeg(leg *blockLeg) string {
	if leg.spec.ReduceOnly {
		return ""
	}
	if leg.adm.mode == perpstate.MarginCross {
		if reason, ok := s.eng.CrossOrderCheck(leg.spec.User, leg.spec.Symbol, leg.spec.PosIdx, leg.spec.Side,
			leg.adm.imPrice, leg.spec.Qty, leg.adm.lev, leg.adm.im, s.cfg.TargetMarginBuffer); !ok {
			return reason
		}
		if !s.eng.ReserveCross(leg.spec.User, leg.cost) {
			return "insufficient_margin"
		}
		return ""
	}
	if !s.eng.Reserve(leg.spec.User, leg.cost) {
		return "insufficient_margin"
	}
	return ""
}

func (s *Service) releaseBlockLeg(leg *blockLeg) {
	if leg.spec.ReduceOnly || leg.cost.Sign() <= 0 {
		return
	}
	if leg.adm.mode == perpstate.MarginCross {
		s.eng.ReleaseCross(leg.spec.User, leg.cost)
	} else {
		s.eng.Release(leg.spec.User, leg.cost)
	}
}

// applyBlockLeg settles one leg like a taker fill at the block price (the
// pinned taker rate is the §8 fee policy for both sides) and journals a
// PerpSettlementEvent with order_id 0 + the block trade_id — the auditable
// mark of a non-matched execution.
func (s *Service) applyBlockLeg(leg *blockLeg, tradeID string) {
	sp, adm := leg.spec, leg.adm
	feeAmt := sp.Price.Mul(sp.Qty).Mul(adm.pin.Taker)
	charge := engine.FeeCharge{
		Amount: feeAmt, Asset: adm.pin.Asset,
		FromReserve: adm.feeBuf, Cross: adm.mode == perpstate.MarginCross,
	}
	fill := perpstate.Fill{Side: sp.Side, Price: sp.Price, Qty: sp.Qty, Fee: zero}
	res, excess, feeOut, _ := s.eng.ApplyFillWithFee(sp.User, sp.Symbol, sp.PosIdx, adm.lev, 0, fill, charge)
	if !sp.ReduceOnly {
		// Release the unconsumed remainder of the hold: an isolated leg
		// committed MarginAdded and drew ReserveUsed of fees; a cross leg
		// committed nothing (exposure becomes a derived requirement).
		var leftover dec.Decimal
		if adm.mode == perpstate.MarginCross {
			leftover = leg.cost.Sub(feeOut.ReserveUsed)
		} else {
			leftover = leg.cost.Sub(res.MarginAdded).Sub(feeOut.ReserveUsed)
		}
		if leftover.Sign() > 0 {
			if adm.mode == perpstate.MarginCross {
				s.eng.ReleaseCross(sp.User, leftover)
			} else {
				s.eng.Release(sp.User, leftover)
			}
		}
	}
	if excess.Sign() > 0 {
		// Admission bounded reduce legs at the live size under this same
		// critical section, so this is unreachable; if it ever fires, it is
		// surfaced loudly, never dropped (ADR-0081 §2 discipline).
		s.journal.Emit(&eventpb.PerpJournalEvent{
			Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
			Payload: &eventpb.PerpJournalEvent_InvariantBreach{InvariantBreach: &eventpb.PerpInvariantBreachEvent{
				UserId: sp.User, Symbol: sp.Symbol, PositionIdx: uint32(sp.PosIdx),
				TradeId: tradeID, Kind: "block_trade_excess",
				ExcessQty: excess.String(), FillPrice: sp.Price.String(),
			}},
		})
	}
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Settlement{Settlement: &eventpb.PerpSettlementEvent{
			UserId: sp.User, OrderId: 0, TradeId: tradeID, Symbol: sp.Symbol,
			FillSide: toEventSide(sp.Side), Price: sp.Price.String(), Qty: sp.Qty.String(),
			RealizedPnl: res.Realized.String(),
			MarginAdded: res.MarginAdded.String(), MarginReleased: res.MarginReleased.String(),
			PositionAfter:       s.positionSnap(sp.User, sp.Symbol, sp.PosIdx),
			SymbolConfigVersion: adm.cfgVersion,
			Fee:                 feeAmt.String(),
			LiquidityRole:       eventpb.LiquidityRole_LIQUIDITY_ROLE_TAKER,
			FeeRuleId:           adm.pin.RuleID,
			FeeRate:             adm.pin.Taker.String(),
			FeeAsset:            adm.pin.Asset,
			FeeDeficit:          feeOut.Deficit.String(),
			WalletAfter:         feeOut.WalletAfter.String(),
		}},
	})
}

func (s *Service) cachedBlock(id string) *perprpc.BlockTradeResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blockDone[id]
}

func (s *Service) cacheBlock(id string, resp *perprpc.BlockTradeResponse) {
	cp := &perprpc.BlockTradeResponse{}
	copyBlockResp(cp, resp)
	s.mu.Lock()
	s.blockDone[id] = cp
	s.mu.Unlock()
}

// copyBlockResp copies the payload fields (a proto message must not be
// value-copied — it embeds an impl mutex).
func copyBlockResp(dst, src *perprpc.BlockTradeResponse) {
	dst.Accepted = src.Accepted
	dst.RejectReason = src.RejectReason
	dst.TradeId = src.TradeId
}
