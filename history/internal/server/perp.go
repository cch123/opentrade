package server

// perp.go implements the perp (futures) history RPCs over the perp_* projection
// (ADR-0068 M7). Same thin shape as the spot handlers: validate user scope,
// delegate to the store, map errors.

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
)

func (s *Server) ListPerpPositions(ctx context.Context, req *connect.Request[historypb.ListPerpPositionsRequest]) (*connect.Response[historypb.ListPerpPositionsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, err := s.store.ListPerpPositions(ctx, m.UserId, m.Symbol)
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpPositionsResponse{Positions: rows}), nil
}

func (s *Server) ListPerpFunding(ctx context.Context, req *connect.Request[historypb.ListPerpFundingRequest]) (*connect.Response[historypb.ListPerpFundingResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpFunding(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpFundingResponse{Funding: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpLiquidations(ctx context.Context, req *connect.Request[historypb.ListPerpLiquidationsRequest]) (*connect.Response[historypb.ListPerpLiquidationsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpLiquidations(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpLiquidationsResponse{Liquidations: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpADL(ctx context.Context, req *connect.Request[historypb.ListPerpADLRequest]) (*connect.Response[historypb.ListPerpADLResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpADL(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpADLResponse{Adl: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpMarginAdjustments(ctx context.Context, req *connect.Request[historypb.ListPerpMarginAdjustmentsRequest]) (*connect.Response[historypb.ListPerpMarginAdjustmentsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpMarginAdjustments(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpMarginAdjustmentsResponse{Adjustments: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpConfigLogs(ctx context.Context, req *connect.Request[historypb.ListPerpConfigLogsRequest]) (*connect.Response[historypb.ListPerpConfigLogsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpConfigLogs(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpConfigLogsResponse{Logs: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpSettlements(ctx context.Context, req *connect.Request[historypb.ListPerpSettlementsRequest]) (*connect.Response[historypb.ListPerpSettlementsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpSettlements(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpSettlementsResponse{Settlements: rows, NextCursor: next}), nil
}

func (s *Server) ListPerpDailyStats(ctx context.Context, req *connect.Request[historypb.ListPerpDailyStatsRequest]) (*connect.Response[historypb.ListPerpDailyStatsResponse], error) {
	m := req.Msg
	if m.GetUserId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListPerpDailyStats(ctx, mysqlstore.PerpLedgerFilter{
		UserID: m.UserId, Symbol: m.Symbol, SinceMs: m.SinceMs, UntilMs: m.UntilMs,
	}, m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListPerpDailyStatsResponse{Stats: rows, NextCursor: next}), nil
}
