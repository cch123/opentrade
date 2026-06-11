package service

// admission.go factors PlaceOrder's gate pass into a reusable, dry-run-able
// pipeline (ADR-0078): PlaceOrder, PreCheckOrder (§4), the amend continuation
// (§2), close-all placement (§5), and block-trade legs (§8) all admit through
// the same gates, so a product API can never reach the book on weaker checks
// than a plain order.

import (
	"errors"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// orderSpec is the normalized order shape after request validation — the
// single input every placement path admits with.
type orderSpec struct {
	User        uint64
	Symbol      string
	ClientID    string
	Side        perpstate.Side
	Type        eventpb.OrderType
	TIF         eventpb.TimeInForce
	Price       dec.Decimal
	Qty         dec.Decimal
	ReqLev      dec.Decimal // 0 = use the configured leverage
	ReduceOnly  bool
	PosIdx      uint8
	SlippageBps uint32
}

func (sp orderSpec) isMarket() bool { return sp.Type == eventpb.OrderType_ORDER_TYPE_MARKET }

// parseOrderSpec validates the PlaceOrder request shape (the error paths map
// to CodeInvalidArgument at the server; business rejects come later as
// accepted=false).
func parseOrderSpec(req *perprpc.PlaceOrderRequest) (orderSpec, error) {
	sp := orderSpec{}
	if req.GetUserId() == 0 || req.GetSymbol() == "" {
		return sp, errors.New("user_id and symbol required")
	}
	side := fromEventSide(req.GetSide())
	if side == 0 {
		return sp, errors.New("invalid side")
	}
	qty, err := dec.Parse(req.GetQty())
	if err != nil || qty.Sign() <= 0 {
		return sp, errors.New("invalid qty")
	}
	// ADR-0074 §8: leverage is position config; the order field is only a
	// convenience entry that creates/overwrites the config. Empty = use the
	// configured leverage.
	reqLev := zero
	if v := req.GetLeverage(); v != "" {
		reqLev, err = dec.Parse(v)
		if err != nil || reqLev.Sign() <= 0 {
			return sp, errors.New("invalid leverage")
		}
	}
	var price dec.Decimal
	isMarket := req.GetOrderType() == eventpb.OrderType_ORDER_TYPE_MARKET
	if !isMarket {
		price, err = dec.Parse(req.GetPrice())
		if err != nil || price.Sign() <= 0 {
			return sp, errors.New("invalid price")
		}
	}
	// ADR-0083: slippage protection is a market-order-only attribute.
	if req.GetSlippageBps() > 10_000 || (req.GetSlippageBps() > 0 && !isMarket) {
		return sp, errors.New("invalid slippage_bps")
	}
	if req.GetPositionIdx() > uint32(perpstate.IdxShort) {
		return sp, errors.New("invalid position_idx")
	}
	return orderSpec{
		User: req.GetUserId(), Symbol: req.GetSymbol(), ClientID: req.GetClientOrderId(),
		Side: side, Type: req.GetOrderType(), TIF: req.GetTif(),
		Price: price, Qty: qty, ReqLev: reqLev,
		ReduceOnly: req.GetReduceOnly(), PosIdx: uint8(req.GetPositionIdx()),
		SlippageBps: req.GetSlippageBps(),
	}, nil
}

// admission is the outcome of one gate pass: everything placeOrderLocked
// needs to reserve + record + dispatch, or the first reject reason.
type admission struct {
	reject     string
	cfgVersion uint64
	lev        dec.Decimal
	mode       perpstate.MarginMode
	riskID     uint32
	pin        feePin
	imPrice    dec.Decimal // IM reference price (limit price / adjusted mark)
	im         dec.Decimal // zero for reduce-only
	feeBuf     dec.Decimal // zero for reduce-only
}

// admitOrderLocked runs every PlaceOrder gate in order. dryRun additionally
// suppresses the ADR-0074 §8 leverage write-through (PreCheck must not
// mutate config). Caller holds the user's seq lock.
func (s *Service) admitOrderLocked(sp orderSpec, dryRun bool) admission {
	adm := admission{}
	// ADR-0075 admission gates: status machine + precision + order limits
	// against ONE captured catalog view. Fail-closed on unknown symbol or a
	// stale cache.
	postOnly := sp.TIF == eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY
	cfgVersion, cfgReject := s.admitAgainstCatalog(sp.Symbol, sp.Price, sp.Qty, sp.isMarket(), postOnly, sp.ReduceOnly)
	if cfgReject != "" {
		adm.reject = cfgReject
		return adm
	}
	adm.cfgVersion = cfgVersion
	// ADR-0077 §2 position-intent matrix, fail-closed both ways.
	symCfg := s.eng.SymbolOrderConfigOf(sp.User, sp.Symbol)
	if reason := perpstate.ValidateOrderIntent(symCfg.PosMode, sp.PosIdx, sp.Side, sp.ReduceOnly); reason != "" {
		adm.reject = reason
		return adm
	}
	// reduce_only must close, never increase: requires an existing position
	// on the side opposite this order, on the targeted leg. Admission gate
	// only — the settlement side clamps (ADR-0077 §2).
	if sp.ReduceOnly {
		pos, ok := s.eng.PositionOf(sp.User, sp.Symbol, sp.PosIdx)
		if !ok || pos.Side == sp.Side {
			adm.reject = "reduce_only_requires_opposite_position"
			return adm
		}
	}
	lev, mode, riskID, reason := s.resolveOrderLeverage(sp.User, sp.Symbol, symCfg, sp.ReqLev, dryRun)
	if reason != "" {
		adm.reject = reason
		return adm
	}
	adm.lev, adm.mode, adm.riskID = lev, mode, riskID
	// ADR-0079 §1: pin the fee quadruple at admission, from the SAME catalog
	// view the gates ran against. Every order pins, including reduce-only.
	pin, pinReject := s.feePinFor(sp.User, sp.Symbol, cfgVersion)
	if pinReject != "" {
		adm.reject = pinReject
		return adm
	}
	adm.pin = pin
	if sp.ReduceOnly {
		adm.im, adm.feeBuf = zero, zero
		return adm
	}
	imPrice := sp.Price
	if sp.isMarket() {
		// Market orders have no limit price to bound exposure — reserve IM at
		// the current mark (ADR-0068 §4), adverse-adjusted for a protected
		// buy (ADR-0083 §5).
		imPrice = s.eng.MarkOf(sp.Symbol)
		if imPrice.Sign() <= 0 {
			adm.reject = "no_mark_for_market_order"
			return adm
		}
		if sp.SlippageBps > 0 && sp.Side == perpstate.SideBuy {
			imPrice = imPrice.Mul(dec.FromInt(10_000 + int64(sp.SlippageBps))).Shift(-4)
		}
	}
	adm.imPrice = imPrice
	if maxLev := s.maxLeverageForOrder(sp.User, sp.Symbol, sp.PosIdx, sp.Side, imPrice, sp.Qty); maxLev.Sign() > 0 && lev.Cmp(maxLev) > 0 {
		adm.reject = "leverage_exceeds_max"
		return adm
	}
	// ADR-0074 §9: position + resting orders + this order must fit the
	// selected risk tier's notional cap (hedge legs sum GROSS, ADR-0077 §7).
	if tierCap := s.riskModelForOrder(sp.Symbol).MaxNotionalFor(riskID); tierCap.Sign() > 0 {
		total := s.eng.SymbolNotionalForCap(sp.User, sp.Symbol).
			Add(s.activeOrderNotional(sp.User, sp.Symbol)).
			Add(imPrice.Mul(sp.Qty))
		if total.Cmp(tierCap) > 0 {
			adm.reject = "notional_exceeds_tier_cap"
			return adm
		}
	}
	adm.im = perpstate.InitMargin(imPrice, sp.Qty, lev)
	// ADR-0079 §4: fee buffer at the pinned taker rate on the IM reference
	// notional, reserved with the IM in one call.
	adm.feeBuf = imPrice.Mul(sp.Qty).Mul(pin.Taker)
	return adm
}

// placeOrderLocked admits, reserves, records, and dispatches one order.
// orderID == 0 allocates a fresh id (the RPC path); a pre-allocated id is
// the replay-convergence anchor for event-driven placements — amend
// continuations and close-all legs re-dispatch the SAME id on crash replay
// and converge through Match's DUPLICATE_ORDER_ID reject (ADR-0078 修订 #6).
// Caller holds the user's seq lock. Returns the order id or a reject reason.
func (s *Service) placeOrderLocked(sp orderSpec, orderID uint64) (uint64, string) {
	adm := s.admitOrderLocked(sp, false)
	if adm.reject != "" {
		return 0, adm.reject
	}
	var reservedIM, reservedFee dec.Decimal = zero, zero
	if !sp.ReduceOnly {
		cost := adm.im.Add(adm.feeBuf)
		if adm.mode == perpstate.MarginCross {
			// ADR-0074 §4: candidate-pool admission, then reserve the order
			// IM from free cash (rule #4).
			if reason, ok := s.eng.CrossOrderCheck(sp.User, sp.Symbol, sp.PosIdx, sp.Side, adm.imPrice, sp.Qty, adm.lev, adm.im, s.cfg.TargetMarginBuffer); !ok {
				return 0, reason
			}
			if !s.eng.ReserveCross(sp.User, cost) {
				return 0, "insufficient_margin"
			}
		} else if !s.eng.Reserve(sp.User, cost) {
			return 0, "insufficient_margin"
		}
		reservedIM, reservedFee = adm.im, adm.feeBuf
	}
	if orderID == 0 {
		orderID = s.nextID()
	}
	o := &Order{
		OrderID: orderID, ClientID: sp.ClientID, UserID: sp.User,
		Symbol: sp.Symbol, Side: sp.Side, Type: sp.Type, TIF: sp.TIF,
		Price: sp.Price, Qty: sp.Qty, Leverage: adm.lev, Mode: adm.mode, PositionIdx: sp.PosIdx,
		ReduceOnly:  sp.ReduceOnly,
		SlippageBps: sp.SlippageBps,
		ReservedIM:  reservedIM, ReservedFee: reservedFee, FilledQty: zero,
		Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
		CreatedMs: s.now(), UpdatedMs: s.now(),
		ConfigVersion: adm.cfgVersion,
		FeeRuleID:     adm.pin.RuleID, FeeMakerRate: adm.pin.Maker, FeeTakerRate: adm.pin.Taker,
		FeeAsset: adm.pin.Asset, FeeMakerSuppressed: adm.pin.MakerSuppressed,
	}
	s.putOrder(o)
	if err := s.dispatch.DispatchOrder(o.Symbol, s.placedOrderEvent(o)); err != nil {
		// Dispatch failure means Match never became responsible for the
		// order, so the reservation is undone synchronously. The COID index
		// entry is dropped WITHOUT entering the terminal ring — a client
		// retry with the same client_order_id must be allowed to try again.
		if cost := reservedIM.Add(reservedFee); cost.Sign() > 0 {
			if adm.mode == perpstate.MarginCross {
				s.eng.ReleaseCross(sp.User, cost)
			} else {
				s.eng.Release(sp.User, cost)
			}
		}
		s.dropOrderNoRing(o)
		return 0, "dispatch_failed"
	}
	s.emitOrderStatus(o, eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW)
	return orderID, ""
}

// PreCheckOrder dry-runs the PlaceOrder admission (ADR-0078 §4): same gates,
// no reservation, no leverage write-through. The response is an estimate —
// a real PlaceOrder re-validates everything.
func (s *Service) PreCheckOrder(req *perprpc.PreCheckOrderRequest) (*perprpc.PreCheckOrderResponse, error) {
	sp, err := parseOrderSpec(req.GetOrder())
	if err != nil {
		return nil, err
	}
	resp := &perprpc.PreCheckOrderResponse{}
	s.seq.do(sp.User, func() {
		adm := s.admitOrderLocked(sp, true)
		resp.ConfigVersion = adm.cfgVersion
		if adm.reject == "" && s.closeAllBlocksLocked(sp.User, sp.Symbol) {
			adm.reject = "close_all_in_progress"
		}
		wallet := s.eng.WalletOf(sp.User)
		if adm.reject == "" && !sp.ReduceOnly {
			// Affordability, mirroring the reserve step without reserving.
			cost := adm.im.Add(adm.feeBuf)
			if adm.mode == perpstate.MarginCross {
				if reason, ok := s.eng.CrossOrderCheck(sp.User, sp.Symbol, sp.PosIdx, sp.Side, adm.imPrice, sp.Qty, adm.lev, adm.im, s.cfg.TargetMarginBuffer); !ok {
					adm.reject = reason
				}
			}
			if adm.reject == "" && wallet.Available.Cmp(cost) < 0 {
				adm.reject = "insufficient_margin"
			}
		}
		resp.WouldAccept = adm.reject == ""
		resp.RejectReason = adm.reject
		resp.RequiredInitialMargin = adm.im.String()
		resp.FeeBuffer = adm.feeBuf.String()
		resp.EffectiveLeverage = adm.lev.String()
		resp.MarginMode = toWireMode(adm.mode)
		resp.RiskId = adm.riskID
		resp.MaxOpenQty = s.maxOpenQtyLocked(sp, adm, wallet.Available).String()
	})
	return resp, nil
}

// maxOpenQtyLocked estimates the largest admissible qty at the spec's price
// and resolved leverage — an estimate, never a guarantee (ADR-0078 修订 #10).
// Reduce-only: the bound is the opposite leg's size. Otherwise available
// cash over per-unit order cost (IM + taker fee buffer), additionally capped
// by the risk tier's remaining notional headroom.
func (s *Service) maxOpenQtyLocked(sp orderSpec, adm admission, available dec.Decimal) dec.Decimal {
	if adm.reject != "" && adm.imPrice.Sign() <= 0 && !sp.ReduceOnly {
		return zero
	}
	if sp.ReduceOnly {
		if pos, ok := s.eng.PositionOf(sp.User, sp.Symbol, sp.PosIdx); ok && pos.Side != sp.Side {
			return pos.Size
		}
		return zero
	}
	imPrice := adm.imPrice
	if imPrice.Sign() <= 0 || adm.lev.Sign() <= 0 {
		return zero
	}
	perUnit := imPrice.Div(adm.lev).Add(imPrice.Mul(adm.pin.Taker))
	if perUnit.Sign() <= 0 {
		return zero
	}
	maxQty := dec.Max(available, zero).Div(perUnit)
	if tierCap := s.riskModelForOrder(sp.Symbol).MaxNotionalFor(adm.riskID); tierCap.Sign() > 0 {
		used := s.eng.SymbolNotionalForCap(sp.User, sp.Symbol).Add(s.activeOrderNotional(sp.User, sp.Symbol))
		headroom := tierCap.Sub(used)
		if headroom.Sign() <= 0 {
			return zero
		}
		maxQty = dec.Min(maxQty, headroom.Div(imPrice))
	}
	return maxQty
}
