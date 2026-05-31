// Package server is the Connect-Go entry point into perp-counter
// (ADR-0068). The read paths (QueryPositions / QueryMargin) are wired to the
// engine; the write paths (PlaceOrder / CancelOrder) return Unimplemented
// until the Match dispatch + per-user sequencer land in M3.
package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Server satisfies perprpcconnect.PerpServiceHandler.
type Server struct {
	eng        *engine.Engine
	svc        *service.Service
	defaultMMR dec.Decimal // maintenance margin rate for derived liq/ratio (M6: per-symbol)
}

// New wires a Server. A zero defaultMMR disables the derived liq-price field.
func New(eng *engine.Engine, svc *service.Service, defaultMMR dec.Decimal) *Server {
	return &Server{eng: eng, svc: svc, defaultMMR: defaultMMR}
}

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
		if p, ok := s.eng.PositionOf(user, sym); ok {
			src = []perpstate.Position{p}
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
	posMargin := dec.FromInt(0)
	uPnL := dec.FromInt(0)
	for _, p := range s.eng.PositionsOf(user) {
		posMargin = posMargin.Add(p.Margin)
		mark := s.eng.MarkOf(p.Symbol)
		if mark.Sign() > 0 {
			uPnL = uPnL.Add(p.UnrealizedPnL(mark))
		}
	}
	return connect.NewResponse(&perprpc.QueryMarginResponse{
		Asset:          "USDT",
		Available:      w.Available.String(),
		Reserved:       w.Reserved.String(),
		PositionMargin: posMargin.String(),
		UnrealizedPnl:  uPnL.String(),
	}), nil
}

// positionView projects a position into the wire type, computing the
// mark-derived fields (ADR-0068 invariant #2: unrealized/ratio use mark).
func (s *Server) positionView(p *perpstate.Position) *perprpc.Position {
	mark := s.eng.MarkOf(p.Symbol)
	v := &perprpc.Position{
		Symbol:      p.Symbol,
		Side:        toEventSide(p.Side),
		Size:        p.Size.String(),
		EntryPrice:  p.Entry.String(),
		Margin:      p.Margin.String(),
		Leverage:    p.Leverage.String(),
		MarginMode:  toWireMode(p.Mode),
		MarkPrice:   mark.String(),
		RealizedPnl: p.Realized.String(),
	}
	if mark.Sign() > 0 {
		v.UnrealizedPnl = p.UnrealizedPnL(mark).String()
		v.MarginRatio = p.MarginRatio(mark).String()
	}
	if s.defaultMMR.Sign() > 0 {
		v.LiqPrice = p.LiqPrice(perpstate.ConstantMMR(s.defaultMMR)).String()
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
