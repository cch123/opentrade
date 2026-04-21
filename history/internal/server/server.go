// Package server wraps the MySQL store as a historypb.HistoryServiceServer.
// Translation between external enums and the store's internal codes lives
// here; the store speaks int8 status codes, the wire speaks external
// history.OrderStatus.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/cursor"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
)

// Server is the HistoryService gRPC handler.
type Server struct {
	historypb.UnimplementedHistoryServiceServer
	store *mysqlstore.Store
}

// New wires a handler over the given store.
func New(store *mysqlstore.Store) *Server {
	return &Server{store: store}
}

// GetOrder fetches one order by id scoped to user_id.
func (s *Server) GetOrder(ctx context.Context, req *historypb.GetOrderRequest) (*historypb.GetOrderResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	if req.GetOrderId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id required")
	}
	o, err := s.store.GetOrder(ctx, req.UserId, req.OrderId)
	if err != nil {
		if errors.Is(err, mysqlstore.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &historypb.GetOrderResponse{Order: o}, nil
}

// ListOrders folds scope → statuses, then delegates to the store.
func (s *Server) ListOrders(ctx context.Context, req *historypb.ListOrdersRequest) (*historypb.ListOrdersResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	// Pick statuses: explicit list wins; else fall back to scope.
	var external []historypb.OrderStatus
	switch {
	case len(req.GetStatuses()) > 0:
		external = req.GetStatuses()
	default:
		external = mysqlstore.StatusesForScope(req.GetScope())
	}

	rows, next, err := s.store.ListOrders(ctx,
		mysqlstore.OrdersFilter{
			UserID:   req.UserId,
			Symbol:   req.Symbol,
			Statuses: mysqlstore.InternalStatusesForFilters(external),
			SinceMs:  req.SinceMs,
			UntilMs:  req.UntilMs,
		},
		req.Cursor, int(req.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return &historypb.ListOrdersResponse{Orders: rows, NextCursor: next}, nil
}

// ListTrades pages user fills (maker + taker).
func (s *Server) ListTrades(ctx context.Context, req *historypb.ListTradesRequest) (*historypb.ListTradesResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	rows, next, err := s.store.ListTrades(ctx,
		mysqlstore.TradesFilter{
			UserID:  req.UserId,
			Symbol:  req.Symbol,
			SinceMs: req.SinceMs,
			UntilMs: req.UntilMs,
		},
		req.Cursor, int(req.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return &historypb.ListTradesResponse{Trades: rows, NextCursor: next}, nil
}

// GetConditional fetches one conditional by id scoped to user_id.
func (s *Server) GetConditional(ctx context.Context, req *historypb.GetConditionalRequest) (*historypb.GetConditionalResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	c, err := s.store.GetConditional(ctx, req.UserId, req.Id)
	if err != nil {
		if errors.Is(err, mysqlstore.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "conditional not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &historypb.GetConditionalResponse{Conditional: c}, nil
}

// ListConditionals folds scope → statuses, then delegates to the store.
func (s *Server) ListConditionals(ctx context.Context, req *historypb.ListConditionalsRequest) (*historypb.ListConditionalsResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	var external = req.GetStatuses()
	if len(external) == 0 {
		external = mysqlstore.ConditionalStatusesForScope(req.GetScope())
	}
	rows, next, err := s.store.ListConditionals(ctx,
		mysqlstore.ConditionalsFilter{
			UserID:   req.UserId,
			Symbol:   req.Symbol,
			Statuses: mysqlstore.InternalConditionalStatuses(external),
			SinceMs:  req.SinceMs,
			UntilMs:  req.UntilMs,
		},
		req.Cursor, int(req.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return &historypb.ListConditionalsResponse{Conditionals: rows, NextCursor: next}, nil
}

// GetTransfer fetches one saga row by transfer_id scoped to user_id.
func (s *Server) GetTransfer(ctx context.Context, req *historypb.GetTransferRequest) (*historypb.GetTransferResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	if req.GetTransferId() == "" {
		return nil, status.Error(codes.InvalidArgument, "transfer_id required")
	}
	t, err := s.store.GetTransfer(ctx, req.UserId, req.TransferId)
	if err != nil {
		if errors.Is(err, mysqlstore.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "transfer not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &historypb.GetTransferResponse{Transfer: t}, nil
}

// ListTransfers folds scope → states, then delegates to the store.
func (s *Server) ListTransfers(ctx context.Context, req *historypb.ListTransfersRequest) (*historypb.ListTransfersResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	states := req.GetStates()
	if len(states) == 0 {
		states = mysqlstore.StatesForTransferScope(req.GetScope())
	}
	rows, next, err := s.store.ListTransfers(ctx,
		mysqlstore.TransfersFilter{
			UserID:  req.UserId,
			FromBiz: req.FromBiz,
			ToBiz:   req.ToBiz,
			Asset:   req.Asset,
			States:  states,
			SinceMs: req.SinceMs,
			UntilMs: req.UntilMs,
		},
		req.Cursor, int(req.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return &historypb.ListTransfersResponse{Transfers: rows, NextCursor: next}, nil
}

// ListAccountLogs pages the user's funds-flow journal.
func (s *Server) ListAccountLogs(ctx context.Context, req *historypb.ListAccountLogsRequest) (*historypb.ListAccountLogsResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	rows, next, err := s.store.ListAccountLogs(ctx,
		mysqlstore.AccountLogsFilter{
			UserID:   req.UserId,
			Asset:    req.Asset,
			BizTypes: req.BizTypes,
			SinceMs:  req.SinceMs,
			UntilMs:  req.UntilMs,
		},
		req.Cursor, int(req.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return &historypb.ListAccountLogsResponse{Logs: rows, NextCursor: next}, nil
}

func translateErr(err error) error {
	switch {
	case errors.Is(err, cursor.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid cursor")
	}
	return status.Error(codes.Internal, err.Error())
}
