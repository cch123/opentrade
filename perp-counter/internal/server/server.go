// Package server is the Connect-Go entry point into perp-counter
// (ADR-0068 / ADR-0074): order + cancel + queries, the account/position
// config surface, and the admin customer-leverage plane.
package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Server satisfies perprpcconnect.PerpServiceHandler.
type Server struct {
	eng        *engine.Engine
	svc        *service.Service
	defaultMMR dec.Decimal // fallback maintenance margin rate when no risk model is installed
}

// New wires a Server. A zero defaultMMR disables the derived liq-price field
// when the engine has no risk model either.
func New(eng *engine.Engine, svc *service.Service, defaultMMR dec.Decimal) *Server {
	return &Server{eng: eng, svc: svc, defaultMMR: defaultMMR}
}

var _ perprpcconnect.PerpServiceHandler = (*Server)(nil)

func (s *Server) PlaceOrder(_ context.Context, req *connect.Request[perprpc.PlaceOrderRequest]) (*connect.Response[perprpc.PlaceOrderResponse], error) {
	resp, err := s.svc.PlaceOrder(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) CancelOrder(_ context.Context, req *connect.Request[perprpc.CancelOrderRequest]) (*connect.Response[perprpc.CancelOrderResponse], error) {
	resp, err := s.svc.CancelOrder(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) QueryOrder(_ context.Context, req *connect.Request[perprpc.QueryOrderRequest]) (*connect.Response[perprpc.QueryOrderResponse], error) {
	resp, ok := s.svc.QueryOrder(req.Msg)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound,
			errors.New("order not found (live orders only; terminal via history)"))
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) QueryPositions(_ context.Context, req *connect.Request[perprpc.QueryPositionsRequest]) (*connect.Response[perprpc.QueryPositionsResponse], error) {
	user := req.Msg.GetUserId()
	if user == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	var src []perpstate.Position
	if sym := req.Msg.GetSymbol(); sym != "" {
		// One row per non-flat leg (ADR-0077): net mode yields at most idx 0,
		// hedge mode up to both legs.
		for idx := perpstate.IdxNet; idx <= perpstate.IdxShort; idx++ {
			if p, ok := s.eng.PositionOf(user, sym, idx); ok {
				src = append(src, p)
			}
		}
	} else {
		src = s.eng.PositionsOf(user)
	}
	out := make([]*perprpc.Position, 0, len(src))
	for i := range src {
		p := &src[i]
		out = append(out, s.positionView(p))
	}
	return connect.NewResponse(&perprpc.QueryPositionsResponse{Positions: out}), nil
}

func (s *Server) QueryMargin(_ context.Context, req *connect.Request[perprpc.QueryMarginRequest]) (*connect.Response[perprpc.QueryMarginResponse], error) {
	user := req.Msg.GetUserId()
	if user == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	w := s.eng.WalletOf(user)
	isoMargin := dec.FromInt(0)
	isoUPnL := dec.FromInt(0)
	crossUPnL := dec.FromInt(0)
	for _, p := range s.eng.PositionsOf(user) {
		mark := s.eng.MarkOf(p.Symbol)
		u := dec.FromInt(0)
		if mark.Sign() > 0 {
			u = p.UnrealizedPnL(mark)
		}
		if p.Mode == perpstate.MarginCross {
			crossUPnL = crossUPnL.Add(u)
			continue
		}
		isoMargin = isoMargin.Add(p.Margin)
		isoUPnL = isoUPnL.Add(u)
	}
	// Derived availability (ADR-0074 §2): ledger buckets above, requirement /
	// available_* fields computed from the cross pool risk model here.
	zero := dec.FromInt(0)
	crossIM := zero
	crossMM := zero
	if h, ok := s.eng.CrossPoolHealth(user); ok {
		crossIM = h.InitialRequirement
		crossMM = h.MaintenanceRequirement
	}
	availTrade := w.Available.Add(dec.Min(crossUPnL, zero)).Sub(crossIM)
	availWithdraw := dec.Min(w.Available, availTrade)
	if availWithdraw.Sign() < 0 {
		availWithdraw = zero
	}
	return connect.NewResponse(&perprpc.QueryMarginResponse{
		Asset:                    "USDT",
		FreeBalance:              w.Available.String(),
		OrderMarginReserved:      w.Reserved.Add(w.CrossReserved).String(),
		IsolatedMarginLocked:     isoMargin.String(),
		IsolatedUnrealizedPnl:    isoUPnL.String(),
		CrossUnrealizedPnl:       crossUPnL.String(),
		CrossInitialRequired:     crossIM.String(),
		CrossMaintenanceRequired: crossMM.String(),
		AvailableToTrade:         availTrade.String(),
		AvailableToWithdraw:      availWithdraw.String(),
	}), nil
}

// --- ADR-0074 config surface --------------------------------------------

func (s *Server) SetMarginMode(_ context.Context, req *connect.Request[perprpc.SetMarginModeRequest]) (*connect.Response[perprpc.SetMarginModeResponse], error) {
	resp, err := s.svc.SetMarginMode(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

// SetPositionMode is the ADR-0077 §3 ONE_WAY ↔ HEDGE switch.
func (s *Server) SetPositionMode(_ context.Context, req *connect.Request[perprpc.SetPositionModeRequest]) (*connect.Response[perprpc.SetPositionModeResponse], error) {
	resp, err := s.svc.SetPositionMode(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) AdjustIsolatedMargin(_ context.Context, req *connect.Request[perprpc.AdjustIsolatedMarginRequest]) (*connect.Response[perprpc.AdjustIsolatedMarginResponse], error) {
	resp, err := s.svc.AdjustIsolatedMargin(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) SetAutoAddMargin(_ context.Context, req *connect.Request[perprpc.SetAutoAddMarginRequest]) (*connect.Response[perprpc.SetAutoAddMarginResponse], error) {
	resp, err := s.svc.SetAutoAddMargin(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) SetPositionLeverage(_ context.Context, req *connect.Request[perprpc.SetPositionLeverageRequest]) (*connect.Response[perprpc.SetPositionLeverageResponse], error) {
	resp, err := s.svc.SetPositionLeverage(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) SetRiskId(_ context.Context, req *connect.Request[perprpc.SetRiskIdRequest]) (*connect.Response[perprpc.SetRiskIdResponse], error) {
	resp, err := s.svc.SetRiskId(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) QueryPositionConfig(_ context.Context, req *connect.Request[perprpc.QueryPositionConfigRequest]) (*connect.Response[perprpc.QueryPositionConfigResponse], error) {
	resp, err := s.svc.QueryPositionConfig(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) QueryAccountConfig(_ context.Context, req *connect.Request[perprpc.QueryAccountConfigRequest]) (*connect.Response[perprpc.QueryAccountConfigResponse], error) {
	resp, err := s.svc.QueryAccountConfig(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) SetCustomerLeverageLimit(_ context.Context, req *connect.Request[perprpc.SetCustomerLeverageLimitRequest]) (*connect.Response[perprpc.SetCustomerLeverageLimitResponse], error) {
	resp, err := s.svc.SetCustomerLeverageLimit(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) ListCustomerLeverageLimits(_ context.Context, req *connect.Request[perprpc.ListCustomerLeverageLimitsRequest]) (*connect.Response[perprpc.ListCustomerLeverageLimitsResponse], error) {
	resp, err := s.svc.ListCustomerLeverageLimits(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

// positionView projects a position into the wire type, computing the
// mark-derived fields (ADR-0068 invariant #2: unrealized/ratio use mark).
func (s *Server) positionView(p *perpstate.Position) *perprpc.Position {
	mark := s.eng.MarkOf(p.Symbol)
	v := &perprpc.Position{
		Symbol:      p.Symbol,
		PositionIdx: uint32(p.PositionIdx),
		PositionMode: toWirePositionMode(
			s.eng.PositionModeOf(p.UserID, p.Symbol)),
		Side:        toEventSide(p.Side),
		Size:        p.Size.String(),
		EntryPrice:  p.Entry.String(),
		Margin:      p.Margin.String(),
		Leverage:    p.Leverage.String(),
		MarginMode:  toWireMode(p.Mode),
		MarkPrice:   mark.String(),
		RealizedPnl: p.Realized.String(),
		RiskId:      p.RiskID,
	}
	if mark.Sign() > 0 {
		v.UnrealizedPnl = p.UnrealizedPnL(mark).String()
	}
	// Per-position ratio / liq price are isolated concepts; a cross
	// position's health lives in the account pool (QueryMargin carries the
	// pool requirements, ADR-0074 §4 rule #6).
	if p.Mode != perpstate.MarginCross {
		if mark.Sign() > 0 {
			v.MarginRatio = p.MarginRatio(mark).String()
		}
		if mmrOf, ok := s.eng.MMRFuncForView(*p); ok {
			v.LiqPrice = p.LiqPrice(mmrOf).String()
		} else if s.defaultMMR.Sign() > 0 {
			v.LiqPrice = p.LiqPrice(perpstate.ConstantMMR(s.defaultMMR)).String()
		}
	}
	return v
}

func toEventSide(s perpstate.Side) eventpb.Side {
	switch s {
	case perpstate.SideBuy:
		return eventpb.Side_SIDE_BUY
	case perpstate.SideSell:
		return eventpb.Side_SIDE_SELL
	default:
		return eventpb.Side_SIDE_UNSPECIFIED
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

func toWirePositionMode(m perpstate.PositionMode) perprpc.PositionMode {
	if m == perpstate.PositionHedge {
		return perprpc.PositionMode_POSITION_MODE_HEDGE
	}
	return perprpc.PositionMode_POSITION_MODE_ONE_WAY
}
