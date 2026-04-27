// Package server adapts counter/internal/service.Service to the
// CounterService Connect-Go handler interface.
package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/pkg/dec"
)

// Router is the dispatcher contract every RPC handler depends on.
// Implementations resolve a user_id to the per-vshard Service owned by
// this node. Returning (nil, false) means "not my vshard" or "not yet
// ready" — the handler replies FailedPrecondition so the BFF refreshes
// its routing view and retries (ADR-0058 §BFF 客户端路由).
type Router interface {
	Lookup(userID string) (*service.Service, bool)
}

// Server satisfies counterrpcconnect.CounterServiceHandler.
type Server struct {
	router Router
	logger *zap.Logger
}

// New creates a Server backed by a Router. In single-vshard tests pass
// NewSingleServiceRouter(svc); production wiring passes
// *worker.Manager.
func New(router Router, logger *zap.Logger) *Server {
	return &Server{router: router, logger: logger}
}

// routeOrFail resolves userID → Service, returning a FailedPrecondition
// status when this node does not currently serve the vshard. Every RPC
// handler in this file calls it as its first step.
func (s *Server) routeOrFail(userID string) (*service.Service, error) {
	if userID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	svc, ok := s.router.Lookup(userID)
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("service: user does not belong to this node"))
	}
	return svc, nil
}

// SingleServiceRouter is a fixed Router that always returns the same
// Service. Used by in-process tests (and by legacy paths during
// migration). Not suitable for production where different users land on
// different vshards.
type SingleServiceRouter struct{ svc *service.Service }

// NewSingleServiceRouter wraps svc into a Router that answers Lookup
// with (svc, true) for every user.
func NewSingleServiceRouter(svc *service.Service) *SingleServiceRouter {
	return &SingleServiceRouter{svc: svc}
}

// Lookup implements Router.
func (r *SingleServiceRouter) Lookup(_ string) (*service.Service, bool) {
	return r.svc, r.svc != nil
}

// PlaceOrder implements CounterService.PlaceOrder (MVP-3).
func (s *Server) PlaceOrder(ctx context.Context, req *connect.Request[counterrpc.PlaceOrderRequest]) (*connect.Response[counterrpc.PlaceOrderResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	internalReq, err := placeOrderFromProto(m)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	res, err := svc.PlaceOrder(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !res.Accepted && res.RejectReason != "" && res.OrderID == 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(res.RejectReason))
	}
	return connect.NewResponse(&counterrpc.PlaceOrderResponse{
		OrderId:          res.OrderID,
		ClientOrderId:    res.ClientOrderID,
		Accepted:         res.Accepted,
		ReceivedTsUnixMs: res.ReceivedAtMS,
	}), nil
}

// CancelOrder implements CounterService.CancelOrder (MVP-3).
func (s *Server) CancelOrder(ctx context.Context, req *connect.Request[counterrpc.CancelOrderRequest]) (*connect.Response[counterrpc.CancelOrderResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	res, err := svc.CancelOrder(ctx, service.CancelOrderRequest{
		UserID:  m.UserId,
		OrderID: m.OrderId,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !res.Accepted && res.RejectReason != "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(res.RejectReason))
	}
	return connect.NewResponse(&counterrpc.CancelOrderResponse{
		OrderId:  res.OrderID,
		Accepted: res.Accepted,
	}), nil
}

// QueryOrder implements CounterService.QueryOrder (MVP-3).
func (s *Server) QueryOrder(_ context.Context, req *connect.Request[counterrpc.QueryOrderRequest]) (*connect.Response[counterrpc.QueryOrderResponse], error) {
	m := req.Msg
	if m == nil || m.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	o, err := svc.QueryOrder(m.UserId, m.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	return connect.NewResponse(orderToProto(o)), nil
}

// QueryBalance implements CounterService.QueryBalance. If Asset is empty,
// returns all assets for the user.
func (s *Server) QueryBalance(_ context.Context, req *connect.Request[counterrpc.QueryBalanceRequest]) (*connect.Response[counterrpc.QueryBalanceResponse], error) {
	m := req.Msg
	if m == nil || m.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id is required"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	resp := &counterrpc.QueryBalanceResponse{}
	if m.Asset != "" {
		bal, err := svc.QueryBalance(m.UserId, m.Asset)
		if err != nil {
			return nil, mapServiceError(err)
		}
		resp.Balances = []*counterrpc.Balance{{
			Asset:     m.Asset,
			Available: bal.Available.String(),
			Frozen:    bal.Frozen.String(),
		}}
		return connect.NewResponse(resp), nil
	}
	account, err := svc.QueryAccount(m.UserId)
	if err != nil {
		return nil, mapServiceError(err)
	}
	for asset, bal := range account {
		resp.Balances = append(resp.Balances, &counterrpc.Balance{
			Asset:     asset,
			Available: bal.Available.String(),
			Frozen:    bal.Frozen.String(),
		})
	}
	return connect.NewResponse(resp), nil
}

// ---------------------------------------------------------------------------
// proto <-> internal helpers
// ---------------------------------------------------------------------------

// mapServiceError converts service-layer errors to Connect codes.
func mapServiceError(err error) error {
	switch {
	case errors.Is(err, service.ErrMissingUserID),
		errors.Is(err, service.ErrMissingTransferID),
		errors.Is(err, service.ErrMissingAsset),
		errors.Is(err, service.ErrInvalidSymbol),
		errors.Is(err, service.ErrReservationIDRequired),
		errors.Is(err, counterstate.ErrInvalidSymbol),
		errors.Is(err, counterstate.ErrInvalidSide),
		errors.Is(err, counterstate.ErrInvalidQty),
		errors.Is(err, counterstate.ErrInvalidPrice),
		errors.Is(err, counterstate.ErrMarketBuyNeedsQuote),
		errors.Is(err, counterstate.ErrInvalidAmount):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, service.ErrOrderDepsNotConfigured):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, service.ErrWrongShard):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, counterstate.ErrReservationNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, counterstate.ErrReservationUserMismatch):
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, counterstate.ErrReservationMismatch),
		errors.Is(err, counterstate.ErrInsufficientAvailable),
		errors.Is(err, counterstate.ErrInsufficientFrozen):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

// ---------------------------------------------------------------------------
// PlaceOrder / CancelOrder / QueryOrder helpers (MVP-3)
// ---------------------------------------------------------------------------

// Reserve implements CounterService.Reserve (ADR-0041).
func (s *Server) Reserve(ctx context.Context, req *connect.Request[counterrpc.ReserveRequest]) (*connect.Response[counterrpc.ReserveResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	side, err := sideFromProto(m.Side)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ot, err := orderTypeFromProto(m.OrderType)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	var price dec.Decimal
	if m.Price != "" {
		p, perr := dec.Parse(m.Price)
		if perr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid price %q: %v", m.Price, perr))
		}
		price = p
	}
	qty, err := dec.Parse(m.Qty)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid qty %q: %v", m.Qty, err))
	}
	quoteQty, err := dec.Parse(m.QuoteQty)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid quote_qty %q: %v", m.QuoteQty, err))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	res, err := svc.Reserve(ctx, service.ReserveRequest{
		UserID:        m.UserId,
		ReservationID: m.ReservationId,
		Symbol:        m.Symbol,
		Side:          side,
		OrderType:     ot,
		Price:         price,
		Qty:           qty,
		QuoteQty:      quoteQty,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&counterrpc.ReserveResponse{
		ReservationId: res.ReservationID,
		Asset:         res.Asset,
		Amount:        res.Amount.String(),
		Accepted:      res.Accepted,
	}), nil
}

// AdminCancelOrders implements CounterService.AdminCancelOrders (ADR-0052).
// ADR-0058: under vshard routing we require UserID so dispatch is
// deterministic; symbol-only fan-out moves to an operator tool that
// targets specific nodes directly.
func (s *Server) AdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	res, err := svc.AdminCancelOrders(ctx, service.AdminCancelFilter{
		UserID: m.UserId,
		Symbol: m.Symbol,
	})
	if err != nil {
		if errors.Is(err, service.ErrAdminCancelFilterEmpty) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&counterrpc.AdminCancelOrdersResponse{
		Cancelled: res.Cancelled,
		Skipped:   res.Skipped,
		ShardId:   int32(svc.ShardID()),
	}), nil
}

// CancelMyOrders implements CounterService.CancelMyOrders. User-facing
// bulk cancel: user_id is required, symbol optional (empty = every active
// order owned by the user on this shard).
func (s *Server) CancelMyOrders(ctx context.Context, req *connect.Request[counterrpc.CancelMyOrdersRequest]) (*connect.Response[counterrpc.CancelMyOrdersResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	res, err := svc.CancelMyOrders(ctx, m.UserId, m.Symbol)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&counterrpc.CancelMyOrdersResponse{
		Cancelled: res.Cancelled,
		Skipped:   res.Skipped,
	}), nil
}

// ReleaseReservation implements CounterService.ReleaseReservation (ADR-0041).
func (s *Server) ReleaseReservation(ctx context.Context, req *connect.Request[counterrpc.ReleaseReservationRequest]) (*connect.Response[counterrpc.ReleaseReservationResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	svc, err := s.routeOrFail(m.UserId)
	if err != nil {
		return nil, err
	}
	res, err := svc.ReleaseReservation(ctx, service.ReleaseReservationRequest{
		UserID:        m.UserId,
		ReservationID: m.ReservationId,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	resp := &counterrpc.ReleaseReservationResponse{
		ReservationId: res.ReservationID,
		Accepted:      res.Accepted,
	}
	if !res.Amount.IsZero() {
		resp.Asset = res.Asset
		resp.Amount = res.Amount.String()
	}
	return connect.NewResponse(resp), nil
}

func placeOrderFromProto(req *counterrpc.PlaceOrderRequest) (service.PlaceOrderRequest, error) {
	side, err := sideFromProto(req.Side)
	if err != nil {
		return service.PlaceOrderRequest{}, err
	}
	ot, err := orderTypeFromProto(req.OrderType)
	if err != nil {
		return service.PlaceOrderRequest{}, err
	}
	tif, err := tifFromProto(req.Tif)
	if err != nil {
		return service.PlaceOrderRequest{}, err
	}
	var price dec.Decimal
	if req.Price != "" {
		p, perr := dec.Parse(req.Price)
		if perr != nil {
			return service.PlaceOrderRequest{}, fmt.Errorf("invalid price %q: %w", req.Price, perr)
		}
		price = p
	}
	qty, err := dec.Parse(req.Qty)
	if err != nil {
		return service.PlaceOrderRequest{}, fmt.Errorf("invalid qty %q: %w", req.Qty, err)
	}
	quoteQty, err := dec.Parse(req.QuoteQty)
	if err != nil {
		return service.PlaceOrderRequest{}, fmt.Errorf("invalid quote_qty %q: %w", req.QuoteQty, err)
	}
	var refPrice dec.Decimal
	if req.ReferencePrice != "" {
		p, perr := dec.Parse(req.ReferencePrice)
		if perr != nil {
			return service.PlaceOrderRequest{}, fmt.Errorf("invalid reference_price %q: %w", req.ReferencePrice, perr)
		}
		refPrice = p
	}
	return service.PlaceOrderRequest{
		UserID:         req.UserId,
		ClientOrderID:  req.ClientOrderId,
		Symbol:         req.Symbol,
		Side:           side,
		OrderType:      ot,
		TIF:            tif,
		Price:          price,
		Qty:            qty,
		QuoteQty:       quoteQty,
		ReservationID:  req.ReservationId,
		ReferencePrice: refPrice,
	}, nil
}

func orderToProto(o *counterstate.Order) *counterrpc.QueryOrderResponse {
	side, _ := sideToProto(o.Side)
	ot, _ := orderTypeToProto(o.Type)
	tif, _ := tifToProto(o.TIF)
	return &counterrpc.QueryOrderResponse{
		OrderId:         o.ID,
		ClientOrderId:   o.ClientOrderID,
		Symbol:          o.Symbol,
		Side:            side,
		OrderType:       ot,
		Tif:             tif,
		Price:           o.Price.String(),
		Qty:             o.Qty.String(),
		FilledQty:       o.FilledQty.String(),
		FrozenAmt:       o.FrozenAmount.String(),
		Status:          internalStatusToProto(o.Status),
		CreatedAtUnixMs: o.CreatedAt,
		UpdatedAtUnixMs: o.UpdatedAt,
	}
}

func sideFromProto(s eventpb.Side) (counterstate.Side, error) {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return counterstate.SideBid, nil
	case eventpb.Side_SIDE_SELL:
		return counterstate.SideAsk, nil
	}
	return 0, fmt.Errorf("invalid side: %v", s)
}

func sideToProto(s counterstate.Side) (eventpb.Side, error) {
	switch s {
	case counterstate.SideBid:
		return eventpb.Side_SIDE_BUY, nil
	case counterstate.SideAsk:
		return eventpb.Side_SIDE_SELL, nil
	}
	return eventpb.Side_SIDE_UNSPECIFIED, fmt.Errorf("invalid side: %d", s)
}

func orderTypeFromProto(t eventpb.OrderType) (counterstate.OrderType, error) {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return counterstate.OrderTypeLimit, nil
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return counterstate.OrderTypeMarket, nil
	}
	return 0, fmt.Errorf("invalid order type: %v", t)
}

func orderTypeToProto(t counterstate.OrderType) (eventpb.OrderType, error) {
	switch t {
	case counterstate.OrderTypeLimit:
		return eventpb.OrderType_ORDER_TYPE_LIMIT, nil
	case counterstate.OrderTypeMarket:
		return eventpb.OrderType_ORDER_TYPE_MARKET, nil
	}
	return eventpb.OrderType_ORDER_TYPE_UNSPECIFIED, fmt.Errorf("invalid order type: %d", t)
}

func tifFromProto(t eventpb.TimeInForce) (counterstate.TIF, error) {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC, eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED:
		return counterstate.TIFGTC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return counterstate.TIFIOC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return counterstate.TIFFOK, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return counterstate.TIFPostOnly, nil
	}
	return 0, fmt.Errorf("invalid TIF: %v", t)
}

func tifToProto(t counterstate.TIF) (eventpb.TimeInForce, error) {
	switch t {
	case counterstate.TIFGTC:
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC, nil
	case counterstate.TIFIOC:
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC, nil
	case counterstate.TIFFOK:
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK, nil
	case counterstate.TIFPostOnly:
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, nil
	}
	return eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED, fmt.Errorf("invalid TIF: %d", t)
}

func internalStatusToProto(s counterstate.OrderStatus) eventpb.InternalOrderStatus {
	switch s {
	case counterstate.OrderStatusPendingNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW
	case counterstate.OrderStatusNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW
	case counterstate.OrderStatusPartiallyFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED
	case counterstate.OrderStatusFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED
	case counterstate.OrderStatusPendingCancel:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
	case counterstate.OrderStatusCanceled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED
	case counterstate.OrderStatusRejected:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED
	case counterstate.OrderStatusExpired:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED
	}
	return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED
}
