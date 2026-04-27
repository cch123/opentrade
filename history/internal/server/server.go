// Package server wraps the MySQL store as a historyrpcconnect.HistoryServiceHandler.
// Translation between external enums and the store's internal codes lives
// here; the store speaks int8 status codes, the wire speaks external
// history.OrderStatus.
package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/cursor"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
)

// Server satisfies historyrpcconnect.HistoryServiceHandler.
type Server struct {
	store *mysqlstore.Store
}

// New wires a handler over the given store.
func New(store *mysqlstore.Store) *Server {
	return &Server{store: store}
}

// GetOrder fetches one order by id scoped to user_id.
func (s *Server) GetOrder(ctx context.Context, req *connect.Request[historypb.GetOrderRequest]) (*connect.Response[historypb.GetOrderResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	if m.GetOrderId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id required"))
	}
	o, err := s.store.GetOrder(ctx, m.UserId, m.OrderId)
	if err != nil {
		if errors.Is(err, mysqlstore.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("order not found"))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&historypb.GetOrderResponse{Order: o}), nil
}

// ListOrders folds scope → statuses, then delegates to the store.
func (s *Server) ListOrders(ctx context.Context, req *connect.Request[historypb.ListOrdersRequest]) (*connect.Response[historypb.ListOrdersResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	// Pick statuses: explicit list wins; else fall back to scope.
	var external []historypb.OrderStatus
	switch {
	case len(m.GetStatuses()) > 0:
		external = m.GetStatuses()
	default:
		external = mysqlstore.StatusesForScope(m.GetScope())
	}

	rows, next, err := s.store.ListOrders(ctx,
		mysqlstore.OrdersFilter{
			UserID:   m.UserId,
			Symbol:   m.Symbol,
			Statuses: mysqlstore.InternalStatusesForFilters(external),
			SinceMs:  m.SinceMs,
			UntilMs:  m.UntilMs,
		},
		m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListOrdersResponse{Orders: rows, NextCursor: next}), nil
}

// ListTrades pages user fills (maker + taker).
func (s *Server) ListTrades(ctx context.Context, req *connect.Request[historypb.ListTradesRequest]) (*connect.Response[historypb.ListTradesResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListTrades(ctx,
		mysqlstore.TradesFilter{
			UserID:  m.UserId,
			Symbol:  m.Symbol,
			SinceMs: m.SinceMs,
			UntilMs: m.UntilMs,
		},
		m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListTradesResponse{Trades: rows, NextCursor: next}), nil
}

// GetTrigger fetches one trigger by id scoped to user_id.
func (s *Server) GetTrigger(ctx context.Context, req *connect.Request[historypb.GetTriggerRequest]) (*connect.Response[historypb.GetTriggerResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	if m.GetId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}
	c, err := s.store.GetTrigger(ctx, m.UserId, m.Id)
	if err != nil {
		if errors.Is(err, mysqlstore.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("trigger not found"))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&historypb.GetTriggerResponse{Trigger: c}), nil
}

// ListTriggers folds scope → statuses, then delegates to the store.
func (s *Server) ListTriggers(ctx context.Context, req *connect.Request[historypb.ListTriggersRequest]) (*connect.Response[historypb.ListTriggersResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	var external = m.GetStatuses()
	if len(external) == 0 {
		external = mysqlstore.TriggerStatusesForScope(m.GetScope())
	}
	rows, next, err := s.store.ListTriggers(ctx,
		mysqlstore.TriggersFilter{
			UserID:   m.UserId,
			Symbol:   m.Symbol,
			Statuses: mysqlstore.InternalTriggerStatuses(external),
			SinceMs:  m.SinceMs,
			UntilMs:  m.UntilMs,
		},
		m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListTriggersResponse{Triggers: rows, NextCursor: next}), nil
}

// ListAccountLogs pages the user's funds-flow journal.
func (s *Server) ListAccountLogs(ctx context.Context, req *connect.Request[historypb.ListAccountLogsRequest]) (*connect.Response[historypb.ListAccountLogsResponse], error) {
	m := req.Msg
	if m.GetUserId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	rows, next, err := s.store.ListAccountLogs(ctx,
		mysqlstore.AccountLogsFilter{
			UserID:   m.UserId,
			Asset:    m.Asset,
			BizTypes: m.BizTypes,
			SinceMs:  m.SinceMs,
			UntilMs:  m.UntilMs,
		},
		m.Cursor, int(m.Limit))
	if err != nil {
		return nil, translateErr(err)
	}
	return connect.NewResponse(&historypb.ListAccountLogsResponse{Logs: rows, NextCursor: next}), nil
}

func translateErr(err error) error {
	switch {
	case errors.Is(err, cursor.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("invalid cursor"))
	}
	return connect.NewError(connect.CodeInternal, err)
}
