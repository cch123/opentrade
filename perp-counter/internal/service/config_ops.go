package service

// config_ops.go is the ADR-0074 P0 operation surface: position-level
// leverage / risk_id / auto-add config, manual isolated-margin adjustment,
// and the admin customer-leverage-limit plane. Every user op runs inside the
// owning user's sequencer; the engine primitive then re-validates and
// mutates atomically under its own lock, with client_op_id idempotency (a
// repeat returns the first outcome, service-level checks bypassed via the
// cache short-circuit).

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// SetMarginMode handles the §5 isolated↔cross switch. Both ADR checks that
// need service-owned state run first (active orders, in-flight liquidation);
// the engine primitive then simulates both resulting states and applies the
// cash leg atomically.
func (s *Service) SetMarginMode(req *perprpc.SetMarginModeRequest) (*perprpc.SetMarginModeResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	target := req.GetTargetMode()
	if target != perprpc.MarginMode_MARGIN_MODE_ISOLATED && target != perprpc.MarginMode_MARGIN_MODE_CROSS {
		return nil, errInvalid("invalid target_mode")
	}
	targetMargin := zero
	if v := req.GetTargetMargin(); v != "" {
		parsed, err := dec.Parse(v)
		if err != nil || parsed.Sign() < 0 {
			return nil, errInvalid("invalid target_margin")
		}
		targetMargin = parsed
	}
	resp := &perprpc.SetMarginModeResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			fillModeResp(resp, out)
			return
		}
		// §5: no live orders may straddle the switch (reduce-only included —
		// alternatives C: provability over UX) and no leg may be
		// mid-liquidation/takeover.
		if s.hasActiveOrders(user, symbol) {
			fillModeResp(resp, engine.OpOutcome{Accepted: false, Reason: "active_orders_cancel_first"})
			return
		}
		if s.hasLiquidationAnyLeg(user, symbol) {
			fillModeResp(resp, engine.OpOutcome{Accepted: false, Reason: "liquidation_in_flight"})
			return
		}
		var out engine.OpOutcome
		if target == perprpc.MarginMode_MARGIN_MODE_CROSS {
			out = s.eng.SwitchToCross(user, symbol, req.GetClientOpId(), s.cfg.TargetMarginBuffer)
		} else {
			out = s.eng.SwitchToIsolated(user, symbol, req.GetClientOpId(), targetMargin,
				s.cfg.TargetMarginBuffer, s.cfg.TargetMarginBuffer)
		}
		fillModeResp(resp, out)
		if !out.Accepted {
			return
		}
		s.emitPositionConfig(user, symbol, "set_margin_mode", req.GetClientOpId())
		// One money-movement record per touched leg (ADR-0077 §7: the switch
		// moves both hedge legs atomically; each leg's cash stays attributable).
		for _, lm := range out.LegMoves {
			if lm.Moved.Sign() > 0 {
				s.emitMarginAdjustmentLeg(user, symbol, lm.PositionIdx,
					eventpb.PerpMarginAdjustmentEvent_KIND_MODE_SWITCH,
					lm.Moved, lm.MarginBefore, lm.MarginAfter, out.FreeAfter, lm.Version, req.GetClientOpId())
			}
		}
	})
	return resp, nil
}

func fillModeResp(resp *perprpc.SetMarginModeResponse, out engine.OpOutcome) {
	resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
	resp.MarginMode = toWireMode(out.Mode)
	resp.PositionMargin = out.MarginAfter.String()
	resp.FreeBalanceAfter = out.FreeAfter.String()
}

// SetPositionLeverage handles the §8 leverage config op.
func (s *Service) SetPositionLeverage(req *perprpc.SetPositionLeverageRequest) (*perprpc.SetPositionLeverageResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	lev, err := dec.Parse(req.GetLeverage())
	if err != nil || lev.Sign() <= 0 {
		return nil, errInvalid("invalid leverage")
	}
	resp := &perprpc.SetPositionLeverageResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			fillLeverageResp(resp, out)
			return
		}
		// §8: changing leverage while orders rest would mix old-config
		// reservations with new requirements — cancel first.
		if s.hasActiveOrders(user, symbol) {
			fillLeverageResp(resp, engine.OpOutcome{Accepted: false, Reason: "active_orders_cancel_first"})
			return
		}
		if s.hasLiquidationAnyLeg(user, symbol) {
			fillLeverageResp(resp, engine.OpOutcome{Accepted: false, Reason: "liquidation_in_flight"})
			return
		}
		out := s.eng.SetLeverage(user, symbol, req.GetClientOpId(), lev, s.cfg.TargetMarginBuffer)
		fillLeverageResp(resp, out)
		if !out.Accepted {
			return
		}
		s.emitPositionConfig(user, symbol, "set_leverage", req.GetClientOpId())
		for _, lm := range out.LegMoves {
			if lm.Moved.Sign() > 0 {
				s.emitMarginAdjustmentLeg(user, symbol, lm.PositionIdx,
					eventpb.PerpMarginAdjustmentEvent_KIND_LEVERAGE_RESIZE,
					lm.Moved, lm.MarginBefore, lm.MarginAfter, out.FreeAfter, lm.Version, req.GetClientOpId())
			}
		}
	})
	return resp, nil
}

// SetRiskId handles the §9 risk-limit tier selection.
func (s *Service) SetRiskId(req *perprpc.SetRiskIdRequest) (*perprpc.SetRiskIdResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	resp := &perprpc.SetRiskIdResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			resp.Accepted, resp.RejectReason, resp.RiskId = out.Accepted, out.Reason, out.RiskID
			return
		}
		if s.hasLiquidationAnyLeg(user, symbol) {
			resp.RejectReason = "liquidation_in_flight"
			return
		}
		// §9: the selected tier's notional cap must also cover resting
		// orders' potential exposure.
		out := s.eng.SetRiskID(user, symbol, req.GetClientOpId(), req.GetRiskId(), s.activeOrderNotional(user, symbol))
		resp.Accepted, resp.RejectReason, resp.RiskId = out.Accepted, out.Reason, out.RiskID
		if out.Accepted {
			s.emitPositionConfig(user, symbol, "set_risk_id", req.GetClientOpId())
		}
	})
	return resp, nil
}

// AdjustIsolatedMargin handles the §6 manual margin add/remove — the only
// per-leg config op (ADR-0077 §7): position_idx must be 0 in ONE_WAY mode and
// 1/2 in HEDGE mode, fail-closed both ways.
func (s *Service) AdjustIsolatedMargin(req *perprpc.AdjustIsolatedMarginRequest) (*perprpc.AdjustIsolatedMarginResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	delta, err := dec.Parse(req.GetDelta())
	if err != nil || delta.Sign() == 0 {
		return nil, errInvalid("invalid delta")
	}
	if req.GetPositionIdx() > uint32(perpstate.IdxShort) {
		return nil, errInvalid("invalid position_idx")
	}
	idx := uint8(req.GetPositionIdx())
	resp := &perprpc.AdjustIsolatedMarginResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			fillAdjustResp(resp, out)
			return
		}
		if reason := validateLegIdx(s.eng.PositionModeOf(user, symbol), idx); reason != "" {
			fillAdjustResp(resp, engine.OpOutcome{Accepted: false, Reason: reason})
			return
		}
		if s.hasLiquidation(liqKey(user, symbol, idx)) {
			fillAdjustResp(resp, engine.OpOutcome{Accepted: false, Reason: "liquidation_in_flight"})
			return
		}
		before, _ := s.eng.PositionRaw(user, symbol, idx)
		out := s.eng.AdjustIsolatedMargin(user, symbol, idx, req.GetClientOpId(), delta, s.cfg.TargetMarginBuffer)
		fillAdjustResp(resp, out)
		if !out.Accepted {
			return
		}
		kind := eventpb.PerpMarginAdjustmentEvent_KIND_ADD_ISOLATED
		if delta.Sign() < 0 {
			kind = eventpb.PerpMarginAdjustmentEvent_KIND_REMOVE_ISOLATED
		}
		s.emitMarginAdjustmentLeg(user, symbol, idx, kind,
			out.Moved, before.Margin, out.MarginAfter, out.FreeAfter, out.Version, req.GetClientOpId())
	})
	return resp, nil
}

// validateLegIdx is the config-op variant of the ADR-0077 §2 fail-closed
// rule: a leg-scoped op must name idx 0 in ONE_WAY mode and a hedge leg
// (1/2) in HEDGE mode.
func validateLegIdx(mode perpstate.PositionMode, idx uint8) string {
	if mode == perpstate.PositionHedge {
		if perpstate.LegSide(idx) == 0 {
			return "position_idx_required_in_hedge_mode"
		}
		return ""
	}
	if idx != perpstate.IdxNet {
		return "position_idx_requires_hedge_mode"
	}
	return ""
}

// SetAutoAddMargin handles the §7 auto-add toggle.
func (s *Service) SetAutoAddMargin(req *perprpc.SetAutoAddMarginRequest) (*perprpc.SetAutoAddMarginResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	maxAdd := zero
	if v := req.GetMaxAddPerEvent(); v != "" {
		parsed, err := dec.Parse(v)
		if err != nil || parsed.Sign() < 0 {
			return nil, errInvalid("invalid max_add_per_event")
		}
		maxAdd = parsed
	}
	resp := &perprpc.SetAutoAddMarginResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
			return
		}
		out := s.eng.SetAutoAdd(user, symbol, req.GetClientOpId(), req.GetEnabled(), maxAdd)
		resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
		if out.Accepted {
			s.emitPositionConfig(user, symbol, "set_auto_add", req.GetClientOpId())
		}
	})
	return resp, nil
}

// SetPositionMode handles the ADR-0077 §3 ONE_WAY ↔ HEDGE switch. The four
// service-owned guards (active orders / position-bound triggers / in-flight
// liquidation on any leg) run first inside the user's sequencer; the engine
// primitive then re-validates flat-only over all legs and writes the mode
// atomically. No automatic netting or position splitting (§3).
func (s *Service) SetPositionMode(req *perprpc.SetPositionModeRequest) (*perprpc.SetPositionModeResponse, error) {
	user, symbol := req.GetUserId(), req.GetSymbol()
	if err := requireUserSymbol(user, symbol); err != nil {
		return nil, err
	}
	var target perpstate.PositionMode
	switch req.GetTargetMode() {
	case perprpc.PositionMode_POSITION_MODE_ONE_WAY:
		target = perpstate.PositionOneWay
	case perprpc.PositionMode_POSITION_MODE_HEDGE:
		target = perpstate.PositionHedge
	default:
		return nil, errInvalid("invalid target_mode")
	}
	resp := &perprpc.SetPositionModeResponse{}
	s.seq.do(user, func() {
		if out, ok := s.eng.CachedOp(req.GetClientOpId()); ok && req.GetClientOpId() != "" {
			fillPositionModeResp(resp, out)
			return
		}
		if s.hasActiveOrders(user, symbol) {
			fillPositionModeResp(resp, engine.OpOutcome{Accepted: false, Reason: "active_orders_cancel_first"})
			return
		}
		// ADR-0077 §3 trigger guard via the TriggerChecker seam: a
		// position-bound trigger is not in the Match book but holds
		// position_idx semantics; switching under it would orphan it.
		if s.cfg.Triggers != nil && s.cfg.Triggers.HasActiveTriggers(user, symbol) {
			fillPositionModeResp(resp, engine.OpOutcome{Accepted: false, Reason: "active_triggers_cancel_first"})
			return
		}
		if s.hasLiquidationAnyLeg(user, symbol) {
			fillPositionModeResp(resp, engine.OpOutcome{Accepted: false, Reason: "liquidation_in_flight"})
			return
		}
		out := s.eng.SetPositionMode(user, symbol, req.GetClientOpId(), target)
		fillPositionModeResp(resp, out)
		if !out.Accepted {
			return
		}
		s.emitPositionConfig(user, symbol, "set_position_mode", req.GetClientOpId())
	})
	return resp, nil
}

func fillPositionModeResp(resp *perprpc.SetPositionModeResponse, out engine.OpOutcome) {
	resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
	resp.PositionMode = toWirePositionModeRPC(out.PosMode)
}

// QueryPositionConfig returns per-(user, symbol) config views (§13).
func (s *Service) QueryPositionConfig(req *perprpc.QueryPositionConfigRequest) (*perprpc.QueryPositionConfigResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	views := s.eng.PositionConfigsOf(req.GetUserId(), req.GetSymbol())
	out := make([]*perprpc.PositionConfig, 0, len(views))
	for _, v := range views {
		out = append(out, &perprpc.PositionConfig{
			Symbol: v.Symbol, MarginMode: toWireMode(v.Mode),
			Leverage: v.Leverage.String(), RiskId: v.RiskID,
			AutoAddMargin: v.AutoAddMargin, AutoAddMax: v.AutoAddMax.String(),
			EffectiveMaxLeverage: v.EffectiveMaxLeverage.String(),
			MaxNotional:          v.MaxNotional.String(),
			PositionIdx:          uint32(v.PositionIdx),
			PositionMode:         toWirePositionModeRPC(v.PosMode),
		})
	}
	return &perprpc.QueryPositionConfigResponse{Configs: out}, nil
}

// QueryAccountConfig returns the account-level view (§13). risk_model is the
// reserved §12 plumbing — STANDARD is the only implemented model.
func (s *Service) QueryAccountConfig(req *perprpc.QueryAccountConfigRequest) (*perprpc.QueryAccountConfigResponse, error) {
	user := req.GetUserId()
	if user == 0 {
		return nil, errInvalid("user_id required")
	}
	limits := s.eng.CustomerLeverageLimits(user)
	rows := make([]*perprpc.CustomerLeverageLimit, 0, len(limits))
	for _, l := range limits {
		rows = append(rows, customerLimitWire(l))
	}
	return &perprpc.QueryAccountConfigResponse{
		SettleAsset:    "USDT",
		RiskModel:      "STANDARD",
		CrossPoolId:    "cross:" + userIDString(user) + ":USDT",
		LeverageLimits: rows,
	}, nil
}

// SetCustomerLeverageLimit is the §10 admin op. It does not enter a user
// sequencer: the cap is admission-time advisory state, and ADR-0074 §10
// forbids mutating user positions from a config write.
func (s *Service) SetCustomerLeverageLimit(req *perprpc.SetCustomerLeverageLimitRequest) (*perprpc.SetCustomerLeverageLimitResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	maxLev := zero
	if v := req.GetMaxLeverage(); v != "" {
		parsed, err := dec.Parse(v)
		if err != nil || parsed.Sign() < 0 {
			return nil, errInvalid("invalid max_leverage")
		}
		maxLev = parsed
	}
	s.eng.SetCustomerLeverageLimit(req.GetUserId(), req.GetSymbol(), maxLev,
		req.GetReason(), req.GetUpdatedBy(), s.now())
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_CustomerRiskLimit{CustomerRiskLimit: &eventpb.PerpCustomerRiskLimitEvent{
			UserId: req.GetUserId(), Symbol: req.GetSymbol(),
			MaxLeverage: maxLev.String(), Reason: req.GetReason(), UpdatedBy: req.GetUpdatedBy(),
		}},
	})
	return &perprpc.SetCustomerLeverageLimitResponse{Accepted: true}, nil
}

// ListCustomerLeverageLimits is the §10 admin list/audit query.
func (s *Service) ListCustomerLeverageLimits(req *perprpc.ListCustomerLeverageLimitsRequest) (*perprpc.ListCustomerLeverageLimitsResponse, error) {
	limits := s.eng.CustomerLeverageLimits(req.GetUserId())
	rows := make([]*perprpc.CustomerLeverageLimit, 0, len(limits))
	for _, l := range limits {
		rows = append(rows, customerLimitWire(l))
	}
	return &perprpc.ListCustomerLeverageLimitsResponse{Limits: rows}, nil
}

// SetCustomerFeeRate is the ADR-0079 §1 admin op: install or remove a
// per-user fee override. Like the leverage cap it is admission-time advisory
// state — it never enters a user sequencer and never touches positions or
// in-flight orders (those keep their pinned rates).
func (s *Service) SetCustomerFeeRate(req *perprpc.SetCustomerFeeRateRequest) (*perprpc.SetCustomerFeeRateResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	ov := engine.FeeOverride{
		RuleID: req.GetFeeRuleId(), Reason: req.GetReason(),
		UpdatedBy: req.GetUpdatedBy(), UpdatedMs: s.now(),
	}
	if ov.RuleID != "" { // empty rule id = remove; rates are ignored then
		maker, err := dec.Parse(req.GetMakerFeeRate())
		if err != nil {
			return nil, errInvalid("invalid maker_fee_rate")
		}
		taker, err := dec.Parse(req.GetTakerFeeRate())
		if err != nil {
			return nil, errInvalid("invalid taker_fee_rate")
		}
		one := dec.FromInt(1)
		switch {
		case taker.Sign() < 0 || taker.Cmp(one) >= 0:
			return &perprpc.SetCustomerFeeRateResponse{RejectReason: "taker_fee_rate_out_of_range"}, nil
		case maker.Abs().Cmp(one) >= 0:
			return &perprpc.SetCustomerFeeRateResponse{RejectReason: "maker_fee_rate_out_of_range"}, nil
		case maker.Cmp(taker) > 0:
			// The taker rate is the fee-buffer upper bound (ADR-0079 §1).
			return &perprpc.SetCustomerFeeRateResponse{RejectReason: "maker_fee_rate_above_taker"}, nil
		case maker.Sign() < 0 && !s.cfg.AllowNegativeMakerFee:
			// Catalog-sourced negative rates degrade at pin time (perp-counter
			// cannot reject an already-published config); an admin override is
			// rejected here instead so the operator gets explicit feedback.
			return &perprpc.SetCustomerFeeRateResponse{RejectReason: "negative_maker_fee_disabled"}, nil
		}
		ov.MakerRate, ov.TakerRate = maker, taker
	}
	s.eng.SetCustomerFeeOverride(req.GetUserId(), req.GetSymbol(), ov)
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_CustomerFee{CustomerFee: &eventpb.PerpCustomerFeeEvent{
			UserId: req.GetUserId(), Symbol: req.GetSymbol(),
			FeeRuleId: ov.RuleID, MakerFeeRate: ov.MakerRate.String(), TakerFeeRate: ov.TakerRate.String(),
			Reason: ov.Reason, UpdatedBy: ov.UpdatedBy,
		}},
	})
	return &perprpc.SetCustomerFeeRateResponse{Accepted: true}, nil
}

// ListCustomerFeeRates is the ADR-0079 §1 admin list/audit query.
func (s *Service) ListCustomerFeeRates(req *perprpc.ListCustomerFeeRatesRequest) (*perprpc.ListCustomerFeeRatesResponse, error) {
	rows := s.eng.CustomerFeeOverrides(req.GetUserId())
	out := make([]*perprpc.CustomerFeeRate, 0, len(rows))
	for _, r := range rows {
		out = append(out, &perprpc.CustomerFeeRate{
			UserId: r.UserID, Symbol: r.Symbol, FeeRuleId: r.RuleID,
			MakerFeeRate: r.MakerRate.String(), TakerFeeRate: r.TakerRate.String(),
			Reason: r.Reason, UpdatedBy: r.UpdatedBy, UpdatedAtUnixMs: r.UpdatedMs,
		})
	}
	return &perprpc.ListCustomerFeeRatesResponse{FeeRates: out}, nil
}

// runAutoAdd is the §7 mark-tick pass: for every auto-add-enabled position
// leg in symbol, attempt the top-up inside the owning user's sequencer BEFORE
// the liquidation scan runs (the scan's sequencer re-check then sees the
// topped-up state). A user with any leg already under liquidation is skipped
// — the §7 ladder never refunds an armed takeover.
func (s *Service) runAutoAdd(symbol string) {
	for _, user := range s.eng.AutoAddUsersWith(symbol) {
		s.seq.do(user, func() {
			if s.hasLiquidationAnyLeg(user, symbol) {
				return
			}
			for _, r := range s.eng.AutoAddMargin(user, symbol,
				s.cfg.AutoAddTriggerBuffer, s.cfg.AutoAddTargetBuffer, s.cfg.AutoAddMaxPerEvent) {
				s.emitMarginAdjustmentLeg(user, symbol, r.PositionIdx,
					eventpb.PerpMarginAdjustmentEvent_KIND_AUTO_ADD,
					r.Out.Moved, r.MarginBefore, r.Out.MarginAfter, r.Out.FreeAfter, r.Out.Version, "")
			}
		})
	}
}

// hasLiquidationAnyLeg reports whether ANY leg of (user, symbol) is
// mid-liquidation — symbol-scoped config ops must not race any leg's forced
// close.
func (s *Service) hasLiquidationAnyLeg(user uint64, symbol string) bool {
	for idx := perpstate.IdxNet; idx <= perpstate.IdxShort; idx++ {
		if s.hasLiquidation(liqKey(user, symbol, idx)) {
			return true
		}
	}
	return false
}

// --- shared helpers ----------------------------------------------------------

// hasActiveOrders reports whether (user, symbol) has any non-terminal order.
// v1 deliberately rejects config changes on ANY live order, not only
// position-increasing ones (ADR-0074 alternatives C: provability over UX).
func (s *Service) hasActiveOrders(user uint64, symbol string) bool {
	for _, o := range s.ordersFor(user, symbol) {
		if !isTerminal(o.Status) {
			return true
		}
	}
	return false
}

// activeOrderNotional sums the unfilled exposure of live orders, valued at
// order price (market orders: current mark) — the §9 tier-cap input.
func (s *Service) activeOrderNotional(user uint64, symbol string) dec.Decimal {
	total := zero
	for _, o := range s.ordersFor(user, symbol) {
		if isTerminal(o.Status) {
			continue
		}
		remaining := o.Qty.Sub(o.FilledQty)
		if remaining.Sign() <= 0 {
			continue
		}
		price := o.Price
		if price.Sign() <= 0 {
			price = s.eng.MarkOf(symbol)
		}
		total = total.Add(price.Mul(remaining))
	}
	return total
}

// emitPositionConfig journals the post-op config state (§13). The echoed
// per-leg fields (margin mode / leverage / risk_id) are uniform across legs
// (ADR-0077 §7); the first existing leg supplies them and position_idx
// records that provenance. position_mode is the symbol's mode after the op.
func (s *Service) emitPositionConfig(user uint64, symbol, reason, opID string) {
	views := s.eng.PositionConfigsOf(user, symbol)
	if len(views) == 0 {
		return
	}
	v := views[0]
	p, ok := s.eng.PositionRaw(user, symbol, v.PositionIdx)
	if !ok {
		return
	}
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_PositionConfig{PositionConfig: &eventpb.PerpPositionConfigEvent{
			UserId: user, Symbol: symbol,
			MarginMode: toWireMarginMode(p.Mode), Leverage: p.Leverage.String(),
			RiskId: p.RiskID, AutoAddMargin: p.AutoAddMargin, AutoAddMax: p.AutoAddMax.String(),
			PositionVersion: p.Version, Reason: reason, ClientOpId: opID,
			PositionMode: toWirePositionMode(v.PosMode), PositionIdx: uint32(p.PositionIdx),
		}},
	})
}

// emitMarginAdjustmentLeg journals one §6/§7 cash movement between the wallet
// and ONE leg's position margin (or the mode-switch cash leg) — money
// movement stays attributable per (user, symbol, position_idx) (ADR-0077 §6).
func (s *Service) emitMarginAdjustmentLeg(user uint64, symbol string, idx uint8,
	kind eventpb.PerpMarginAdjustmentEvent_Kind,
	moved, marginBefore, marginAfter, walletAfter dec.Decimal, version uint64, opID string) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_MarginAdjustment{MarginAdjustment: &eventpb.PerpMarginAdjustmentEvent{
			UserId: user, Symbol: symbol, Kind: kind,
			Amount:       moved.String(),
			MarginBefore: marginBefore.String(), MarginAfter: marginAfter.String(),
			WalletAfter: walletAfter.String(), PositionVersion: version,
			ClientOpId: opID, MarkPrice: s.eng.MarkOf(symbol).String(),
			PositionIdx: uint32(idx),
		}},
	})
}

func customerLimitWire(l engine.CustomerLimitRow) *perprpc.CustomerLeverageLimit {
	return &perprpc.CustomerLeverageLimit{
		UserId: l.UserID, Symbol: l.Symbol, MaxLeverage: l.MaxLeverage.String(),
		Reason: l.Reason, UpdatedBy: l.UpdatedBy, UpdatedAtUnixMs: l.UpdatedMs,
	}
}

func toWireMarginMode(m perpstate.MarginMode) eventpb.PerpMarginMode {
	switch m {
	case perpstate.MarginIsolated:
		return eventpb.PerpMarginMode_PERP_MARGIN_MODE_ISOLATED
	case perpstate.MarginCross:
		return eventpb.PerpMarginMode_PERP_MARGIN_MODE_CROSS
	default:
		return eventpb.PerpMarginMode_PERP_MARGIN_MODE_UNSPECIFIED
	}
}

func toWireMode(m perpstate.MarginMode) perprpc.MarginMode {
	switch m {
	case perpstate.MarginIsolated:
		return perprpc.MarginMode_MARGIN_MODE_ISOLATED
	case perpstate.MarginCross:
		return perprpc.MarginMode_MARGIN_MODE_CROSS
	default:
		return perprpc.MarginMode_MARGIN_MODE_UNSPECIFIED
	}
}

// toWirePositionMode maps to the journal enum (ADR-0077).
func toWirePositionMode(m perpstate.PositionMode) eventpb.PerpPositionMode {
	if m == perpstate.PositionHedge {
		return eventpb.PerpPositionMode_PERP_POSITION_MODE_HEDGE
	}
	return eventpb.PerpPositionMode_PERP_POSITION_MODE_ONE_WAY
}

// toWirePositionModeRPC maps to the rpc enum (ADR-0077).
func toWirePositionModeRPC(m perpstate.PositionMode) perprpc.PositionMode {
	if m == perpstate.PositionHedge {
		return perprpc.PositionMode_POSITION_MODE_HEDGE
	}
	return perprpc.PositionMode_POSITION_MODE_ONE_WAY
}

func fillLeverageResp(resp *perprpc.SetPositionLeverageResponse, out engine.OpOutcome) {
	resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
	resp.Leverage = out.Leverage.String()
	resp.PositionMargin = out.MarginAfter.String()
	resp.FreeBalanceAfter = out.FreeAfter.String()
}

func fillAdjustResp(resp *perprpc.AdjustIsolatedMarginResponse, out engine.OpOutcome) {
	resp.Accepted, resp.RejectReason = out.Accepted, out.Reason
	resp.PositionMargin = out.MarginAfter.String()
	resp.FreeBalanceAfter = out.FreeAfter.String()
}

func requireUserSymbol(user uint64, symbol string) error {
	if user == 0 || symbol == "" {
		return errInvalid("user_id and symbol required")
	}
	return nil
}
