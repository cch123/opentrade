package service

// close_all.go is the ADR-0078 §5 conservative two-phase close-all:
//
//	CANCELING  cancel every active order in scope, reject new ones
//	PLACING    scope quiesced → reduce-only protected market per non-flat leg
//	DONE(_WITH_ERRORS) close orders terminal → guard lifts
//
// Sizing happens AFTER every in-scope order is terminal, so in-flight fills
// between the cancel dispatch and the terminal event are naturally included
// and no other order can race the close orders for close capacity — the
// design that removes the ADR-0081 dependency (修订 #1). The close order ids
// are pre-allocated at request time and snapshot-persisted: a crash-replay
// re-dispatch converges through Match's DUPLICATE_ORDER_ID reject (修订 #6).

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// closeAllLeg is one leg's outcome row (mirrors perprpc.CloseAllLeg).
type closeAllLeg struct {
	Symbol  string
	Idx     uint8
	OrderID uint64
	Qty     dec.Decimal
	Reject  string
}

// closeAllState is one user's close-all run. Terminal entries stay in the
// registry for idempotent re-reads until a new request replaces them.
type closeAllState struct {
	UserID      uint64
	OpID        string
	Symbol      string   // request scope filter ("" = all)
	Symbols     []string // resolved scope
	SlippageBps uint32
	Phase       perprpc.CloseAllPhase

	PendingCancels map[uint64]struct{} // in-scope orders awaiting terminal
	PendingCloses  map[uint64]struct{} // close orders awaiting terminal
	LegIDs         map[string]uint64   // "symbol|idx" → pre-allocated close order id
	Legs           []closeAllLeg       // filled at PLACING
}

func (ca *closeAllState) terminal() bool {
	return ca.Phase == perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE ||
		ca.Phase == perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE_WITH_ERRORS
}

func legKey(symbol string, idx uint8) string {
	return symbol + "|" + string(rune('0'+idx))
}

func (s *Service) closeAllOf(user uint64) *closeAllState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeAlls[user]
}

func (s *Service) setCloseAll(ca *closeAllState) {
	s.mu.Lock()
	s.closeAlls[ca.UserID] = ca
	s.mu.Unlock()
}

// closeAllBlocksLocked reports whether a running close-all owns (user,
// symbol) — every other placement in scope is rejected until it finishes
// (ADR-0078 §5). Caller holds the user's seq lock.
func (s *Service) closeAllBlocksLocked(user uint64, symbol string) bool {
	ca := s.closeAllOf(user)
	if ca == nil || ca.terminal() {
		return false
	}
	if ca.Symbol == "" {
		return true // all-symbol scope owns everything
	}
	for _, sym := range ca.Symbols {
		if sym == symbol {
			return true
		}
	}
	return false
}

// CloseAllPositions is the §5 entry point. Idempotent on client_op_id: a
// repeat returns the run's current state instead of starting a new one.
func (s *Service) CloseAllPositions(req *perprpc.CloseAllPositionsRequest) (*perprpc.CloseAllPositionsResponse, error) {
	user := req.GetUserId()
	if user == 0 {
		return nil, errInvalid("user_id required")
	}
	if req.GetClientOpId() == "" {
		return nil, errInvalid("client_op_id required")
	}
	// V1 close orders are protected market only (ADR-0078 §5) — the collar
	// is mandatory, not optional.
	if req.GetSlippageBps() == 0 || req.GetSlippageBps() > 10_000 {
		return nil, errInvalid("invalid slippage_bps")
	}
	resp := &perprpc.CloseAllPositionsResponse{CloseAllId: req.GetClientOpId()}
	s.seq.do(user, func() {
		if ca := s.closeAllOf(user); ca != nil {
			if ca.OpID == req.GetClientOpId() {
				resp.Accepted = true
				fillCloseAllResp(resp, ca)
				return
			}
			if !ca.terminal() {
				resp.RejectReason = "close_all_in_progress"
				return
			}
		}
		symbols := s.closeAllScope(user, req.GetSymbol())
		if len(symbols) == 0 {
			resp.RejectReason = "no_scope"
			return
		}
		for _, sym := range symbols {
			// Cancel-phase feasibility is all-or-nothing: a partially
			// cancellable scope would leave unknown live orders racing the
			// close orders (provability over UX, ADR-0074 alternatives C).
			if !s.cancelAllowed(sym) {
				resp.RejectReason = "symbol_not_cancelable"
				return
			}
			if s.hasLiquidationAnyLeg(user, sym) {
				resp.RejectReason = "liquidation_in_flight"
				return
			}
		}
		ca := &closeAllState{
			UserID: user, OpID: req.GetClientOpId(), Symbol: req.GetSymbol(),
			Symbols: symbols, SlippageBps: req.GetSlippageBps(),
			Phase:          perprpc.CloseAllPhase_CLOSE_ALL_PHASE_CANCELING,
			PendingCancels: map[uint64]struct{}{},
			PendingCloses:  map[uint64]struct{}{},
			LegIDs:         map[string]uint64{},
		}
		// Pre-allocate every possible leg's close order id NOW (修订 #6):
		// the ids must exist in the snapshot before any dispatch so a
		// crash-replay re-dispatches the SAME ids. The position mode cannot
		// change for the scope's duration — SetPositionMode rejects under
		// active orders, and the scope always holds active orders (ours)
		// until the run finishes.
		for _, sym := range symbols {
			for _, idx := range s.legIdxsOf(user, sym) {
				ca.LegIDs[legKey(sym, idx)] = s.nextID()
			}
		}
		// Cancel pass. Any dispatch failure rejects the whole run before it
		// is registered — already-dispatched cancels stand (they only reduce
		// exposure) and no close order will ever follow them.
		for _, sym := range symbols {
			for _, o := range s.activeOrdersOf(user, sym) {
				if s.liquidationFor(o.OrderID) != nil {
					continue // defensive: guarded above
				}
				if pa := s.takeAmend(o.OrderID); pa != nil {
					s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_ABORTED_BY_CANCEL, "", zero)
				}
				if o.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL {
					if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
						resp.RejectReason = "dispatch_failed"
						return
					}
					old := o.Status
					o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
					o.UpdatedMs = s.now()
					s.emitOrderStatus(o, old, o.Status)
				}
				ca.PendingCancels[o.OrderID] = struct{}{}
			}
		}
		s.setCloseAll(ca)
		s.emitCloseAll(ca)
		if len(ca.PendingCancels) == 0 {
			s.closeAllPlaceLocked(ca)
		}
		resp.Accepted = true
		fillCloseAllResp(resp, ca)
	})
	return resp, nil
}

// closeAllScope resolves the request scope: one symbol, or every symbol the
// user holds a position or an active order in.
func (s *Service) closeAllScope(user uint64, symbol string) []string {
	if symbol != "" {
		return []string{symbol}
	}
	seen := map[string]struct{}{}
	var out []string
	for _, p := range s.eng.PositionsOf(user) {
		if _, ok := seen[p.Symbol]; !ok {
			seen[p.Symbol] = struct{}{}
			out = append(out, p.Symbol)
		}
	}
	for _, o := range s.activeOrdersOf(user, "") {
		if _, ok := seen[o.Symbol]; !ok {
			seen[o.Symbol] = struct{}{}
			out = append(out, o.Symbol)
		}
	}
	return out
}

// legIdxsOf lists the addressable leg indexes under the symbol's current
// position mode (ADR-0077: one-way = idx 0, hedge = idx 1/2).
func (s *Service) legIdxsOf(user uint64, symbol string) []uint8 {
	if s.eng.PositionModeOf(user, symbol) == perpstate.PositionHedge {
		return []uint8{perpstate.IdxLong, perpstate.IdxShort}
	}
	return []uint8{perpstate.IdxNet}
}

// closeAllOnTerminalLocked advances the run when an in-scope order reaches
// terminal. Caller holds the user's seq lock.
func (s *Service) closeAllOnTerminalLocked(o *Order) {
	ca := s.closeAllOf(o.UserID)
	if ca == nil || ca.terminal() {
		return
	}
	if _, ok := ca.PendingCancels[o.OrderID]; ok {
		delete(ca.PendingCancels, o.OrderID)
		if len(ca.PendingCancels) == 0 && ca.Phase == perprpc.CloseAllPhase_CLOSE_ALL_PHASE_CANCELING {
			s.closeAllPlaceLocked(ca)
		}
		return
	}
	if _, ok := ca.PendingCloses[o.OrderID]; ok {
		delete(ca.PendingCloses, o.OrderID)
		if len(ca.PendingCloses) == 0 && ca.Phase == perprpc.CloseAllPhase_CLOSE_ALL_PHASE_PLACING {
			s.closeAllFinishLocked(ca)
		}
	}
}

// closeAllPlaceLocked is the PLACING phase: the scope is quiescent (no live
// orders, new ones rejected), so each non-flat leg's size is exact. Caller
// holds the user's seq lock.
func (s *Service) closeAllPlaceLocked(ca *closeAllState) {
	ca.Phase = perprpc.CloseAllPhase_CLOSE_ALL_PHASE_PLACING
	for _, sym := range ca.Symbols {
		for _, idx := range s.legIdxsOf(ca.UserID, sym) {
			pos, ok := s.eng.PositionRaw(ca.UserID, sym, idx)
			if !ok || pos.Size.Sign() == 0 {
				continue
			}
			leg := closeAllLeg{Symbol: sym, Idx: idx, OrderID: ca.LegIDs[legKey(sym, idx)], Qty: pos.Size}
			if s.hasLiquidation(liqKey(ca.UserID, sym, idx)) {
				// The liquidation engine owns this leg's close now; close-all
				// must not race the bankruptcy order (ADR-0078 §5).
				leg.Reject = "liquidation_in_flight"
				ca.Legs = append(ca.Legs, leg)
				continue
			}
			sp := orderSpec{
				User: ca.UserID, Symbol: sym,
				Side: pos.Side.Opposite(), Type: eventpb.OrderType_ORDER_TYPE_MARKET,
				TIF: eventpb.TimeInForce_TIME_IN_FORCE_IOC,
				Qty: pos.Size, ReduceOnly: true, PosIdx: idx,
				SlippageBps: ca.SlippageBps,
			}
			if _, reason := s.placeOrderLocked(sp, leg.OrderID); reason != "" {
				leg.Reject = reason
			} else {
				ca.PendingCloses[leg.OrderID] = struct{}{}
			}
			ca.Legs = append(ca.Legs, leg)
		}
	}
	if len(ca.PendingCloses) == 0 {
		s.closeAllFinishLocked(ca)
		return
	}
	s.emitCloseAll(ca)
}

// closeAllFinishLocked stamps the terminal phase and lifts the guard.
func (s *Service) closeAllFinishLocked(ca *closeAllState) {
	ca.Phase = perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE
	for _, leg := range ca.Legs {
		if leg.Reject != "" {
			ca.Phase = perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE_WITH_ERRORS
			break
		}
	}
	s.emitCloseAll(ca)
}

// emitCloseAll journals the run's current state (ADR-0078 §5).
func (s *Service) emitCloseAll(ca *closeAllState) {
	evt := &eventpb.PerpCloseAllEvent{
		UserId: ca.UserID, CloseAllId: ca.OpID, Symbol: ca.Symbol,
		State: closeAllEventState(ca.Phase),
	}
	for _, leg := range ca.Legs {
		evt.Legs = append(evt.Legs, &eventpb.PerpCloseAllEvent_Leg{
			Symbol: leg.Symbol, PositionIdx: uint32(leg.Idx), OrderId: leg.OrderID,
			Qty: leg.Qty.String(), RejectReason: leg.Reject,
		})
	}
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_CloseAll{CloseAll: evt},
	})
}

func closeAllEventState(p perprpc.CloseAllPhase) eventpb.PerpCloseAllEvent_State {
	switch p {
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_CANCELING:
		return eventpb.PerpCloseAllEvent_STATE_REQUESTED
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_PLACING:
		return eventpb.PerpCloseAllEvent_STATE_PLACING
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE:
		return eventpb.PerpCloseAllEvent_STATE_DONE
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE_WITH_ERRORS:
		return eventpb.PerpCloseAllEvent_STATE_DONE_WITH_ERRORS
	default:
		return eventpb.PerpCloseAllEvent_STATE_UNSPECIFIED
	}
}

func fillCloseAllResp(resp *perprpc.CloseAllPositionsResponse, ca *closeAllState) {
	resp.Phase = ca.Phase
	resp.Legs = resp.Legs[:0]
	for _, leg := range ca.Legs {
		qty := ""
		if leg.Qty.Sign() != 0 {
			qty = leg.Qty.String()
		}
		resp.Legs = append(resp.Legs, &perprpc.CloseAllLeg{
			Symbol: leg.Symbol, PositionIdx: uint32(leg.Idx), OrderId: leg.OrderID,
			Qty: qty, RejectReason: leg.Reject,
		})
	}
}
