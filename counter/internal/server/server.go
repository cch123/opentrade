// Package server adapts counter/internal/service.Service to the protobuf
// CounterService gRPC interface.
package server

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
)

// Server implements counterrpc.CounterServiceServer. PlaceOrder / CancelOrder /
// QueryOrder land in MVP-3; they currently return Unimplemented via the
// embedded UnimplementedCounterServiceServer.
type Server struct {
	counterrpc.UnimplementedCounterServiceServer

	svc    *service.Service
	logger *zap.Logger
}

// New creates a Server.
func New(svc *service.Service, logger *zap.Logger) *Server {
	return &Server{svc: svc, logger: logger}
}

// PlaceOrder implements CounterService.PlaceOrder (MVP-3).
func (s *Server) PlaceOrder(ctx context.Context, req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	internalReq, err := placeOrderFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	res, err := s.svc.PlaceOrder(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !res.Accepted && res.RejectReason != "" && res.OrderID == 0 {
		return nil, status.Error(codes.FailedPrecondition, res.RejectReason)
	}
	return &counterrpc.PlaceOrderResponse{
		OrderId:         res.OrderID,
		ClientOrderId:   res.ClientOrderID,
		Accepted:        res.Accepted,
		ReceivedTsUnixMs: res.ReceivedAtMS,
	}, nil
}

// CancelOrder implements CounterService.CancelOrder (MVP-3).
func (s *Server) CancelOrder(ctx context.Context, req *counterrpc.CancelOrderRequest) (*counterrpc.CancelOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	res, err := s.svc.CancelOrder(ctx, service.CancelOrderRequest{
		UserID:  req.UserId,
		OrderID: req.OrderId,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !res.Accepted && res.RejectReason != "" {
		return nil, status.Error(codes.FailedPrecondition, res.RejectReason)
	}
	return &counterrpc.CancelOrderResponse{
		OrderId:  res.OrderID,
		Accepted: res.Accepted,
	}, nil
}

// QueryOrder implements CounterService.QueryOrder (MVP-3).
func (s *Server) QueryOrder(_ context.Context, req *counterrpc.QueryOrderRequest) (*counterrpc.QueryOrderResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	o, err := s.svc.QueryOrder(req.UserId, req.OrderId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return orderToProto(o), nil
}

// Transfer implements CounterService.Transfer.
func (s *Server) Transfer(ctx context.Context, req *counterrpc.TransferRequest) (*counterrpc.TransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	internalReq, err := transferRequestFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	res, err := s.svc.Transfer(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return transferResponseToProto(res), nil
}

// QueryBalance implements CounterService.QueryBalance. If Asset is empty,
// returns all assets for the user.
func (s *Server) QueryBalance(_ context.Context, req *counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	resp := &counterrpc.QueryBalanceResponse{}
	if req.Asset != "" {
		bal, err := s.svc.QueryBalance(req.UserId, req.Asset)
		if err != nil {
			return nil, mapServiceError(err)
		}
		resp.Balances = []*counterrpc.Balance{{
			Asset:     req.Asset,
			Available: bal.Available.String(),
			Frozen:    bal.Frozen.String(),
		}}
		return resp, nil
	}
	account, err := s.svc.QueryAccount(req.UserId)
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
	return resp, nil
}

// ---------------------------------------------------------------------------
// proto <-> internal helpers
// ---------------------------------------------------------------------------

func transferRequestFromProto(req *counterrpc.TransferRequest) (engine.TransferRequest, error) {
	amount, err := dec.Parse(req.Amount)
	if err != nil {
		return engine.TransferRequest{}, fmt.Errorf("invalid amount %q: %w", req.Amount, err)
	}
	t, err := transferTypeFromProto(req.Type)
	if err != nil {
		return engine.TransferRequest{}, err
	}
	return engine.TransferRequest{
		TransferID: req.TransferId,
		UserID:     req.UserId,
		Asset:      req.Asset,
		Amount:     amount,
		Type:       t,
		BizRefID:   req.BizRefId,
		Memo:       req.Memo,
	}, nil
}

func transferTypeFromProto(t counterrpc.TransferType) (engine.TransferType, error) {
	switch t {
	case counterrpc.TransferType_TRANSFER_TYPE_DEPOSIT:
		return engine.TransferDeposit, nil
	case counterrpc.TransferType_TRANSFER_TYPE_WITHDRAW:
		return engine.TransferWithdraw, nil
	case counterrpc.TransferType_TRANSFER_TYPE_FREEZE:
		return engine.TransferFreeze, nil
	case counterrpc.TransferType_TRANSFER_TYPE_UNFREEZE:
		return engine.TransferUnfreeze, nil
	default:
		return 0, fmt.Errorf("invalid transfer type: %v", t)
	}
}

func transferResponseToProto(res *engine.TransferResult) *counterrpc.TransferResponse {
	return &counterrpc.TransferResponse{
		TransferId:     res.TransferID,
		Status:         transferStatusToProto(res.Status),
		RejectReason:   res.RejectReason,
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}
}

func transferStatusToProto(s engine.TransferStatus) counterrpc.TransferStatus {
	switch s {
	case engine.TransferStatusConfirmed:
		return counterrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED
	case engine.TransferStatusRejected:
		return counterrpc.TransferStatus_TRANSFER_STATUS_REJECTED
	case engine.TransferStatusDuplicated:
		return counterrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED
	default:
		return counterrpc.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
	}
}

// mapServiceError converts service-layer errors to gRPC status codes.
func mapServiceError(err error) error {
	switch {
	case errors.Is(err, service.ErrMissingUserID),
		errors.Is(err, service.ErrMissingTransferID),
		errors.Is(err, service.ErrMissingAsset),
		errors.Is(err, service.ErrInvalidSymbol),
		errors.Is(err, service.ErrReservationIDRequired),
		errors.Is(err, engine.ErrInvalidSymbol),
		errors.Is(err, engine.ErrInvalidSide),
		errors.Is(err, engine.ErrInvalidQty),
		errors.Is(err, engine.ErrInvalidPrice),
		errors.Is(err, engine.ErrMarketBuyNeedsQuote),
		errors.Is(err, engine.ErrInvalidAmount):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrOrderDepsNotConfigured):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, service.ErrWrongShard):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, engine.ErrReservationNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrReservationUserMismatch):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, engine.ErrReservationMismatch),
		errors.Is(err, engine.ErrInsufficientAvailable),
		errors.Is(err, engine.ErrInsufficientFrozen):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// ---------------------------------------------------------------------------
// PlaceOrder / CancelOrder / QueryOrder helpers (MVP-3)
// ---------------------------------------------------------------------------

// Reserve implements CounterService.Reserve (ADR-0041).
func (s *Server) Reserve(ctx context.Context, req *counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	side, err := sideFromProto(req.Side)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ot, err := orderTypeFromProto(req.OrderType)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var price dec.Decimal
	if req.Price != "" {
		p, perr := dec.Parse(req.Price)
		if perr != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid price %q: %v", req.Price, perr))
		}
		price = p
	}
	qty, err := dec.Parse(req.Qty)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid qty %q: %v", req.Qty, err))
	}
	quoteQty, err := dec.Parse(req.QuoteQty)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid quote_qty %q: %v", req.QuoteQty, err))
	}
	res, err := s.svc.Reserve(ctx, service.ReserveRequest{
		UserID:        req.UserId,
		ReservationID: req.ReservationId,
		Symbol:        req.Symbol,
		Side:          side,
		OrderType:     ot,
		Price:         price,
		Qty:           qty,
		QuoteQty:      quoteQty,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &counterrpc.ReserveResponse{
		ReservationId: res.ReservationID,
		Asset:         res.Asset,
		Amount:        res.Amount.String(),
		Accepted:      res.Accepted,
	}, nil
}

// AdminCancelOrders implements CounterService.AdminCancelOrders (ADR-0052).
func (s *Server) AdminCancelOrders(ctx context.Context, req *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	res, err := s.svc.AdminCancelOrders(ctx, service.AdminCancelFilter{
		UserID: req.UserId,
		Symbol: req.Symbol,
	})
	if err != nil {
		if errors.Is(err, service.ErrAdminCancelFilterEmpty) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, mapServiceError(err)
	}
	return &counterrpc.AdminCancelOrdersResponse{
		Cancelled: res.Cancelled,
		Skipped:   res.Skipped,
		ShardId:   int32(s.svc.ShardID()),
	}, nil
}

// ReleaseReservation implements CounterService.ReleaseReservation (ADR-0041).
func (s *Server) ReleaseReservation(ctx context.Context, req *counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	res, err := s.svc.ReleaseReservation(ctx, service.ReleaseReservationRequest{
		UserID:        req.UserId,
		ReservationID: req.ReservationId,
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
	return resp, nil
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
	return service.PlaceOrderRequest{
		UserID:        req.UserId,
		ClientOrderID: req.ClientOrderId,
		Symbol:        req.Symbol,
		Side:          side,
		OrderType:     ot,
		TIF:           tif,
		Price:         price,
		Qty:           qty,
		QuoteQty:      quoteQty,
		ReservationID: req.ReservationId,
	}, nil
}

func orderToProto(o *engine.Order) *counterrpc.QueryOrderResponse {
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

func sideFromProto(s eventpb.Side) (engine.Side, error) {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return engine.SideBid, nil
	case eventpb.Side_SIDE_SELL:
		return engine.SideAsk, nil
	}
	return 0, fmt.Errorf("invalid side: %v", s)
}

func sideToProto(s engine.Side) (eventpb.Side, error) {
	switch s {
	case engine.SideBid:
		return eventpb.Side_SIDE_BUY, nil
	case engine.SideAsk:
		return eventpb.Side_SIDE_SELL, nil
	}
	return eventpb.Side_SIDE_UNSPECIFIED, fmt.Errorf("invalid side: %d", s)
}

func orderTypeFromProto(t eventpb.OrderType) (engine.OrderType, error) {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return engine.OrderTypeLimit, nil
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return engine.OrderTypeMarket, nil
	}
	return 0, fmt.Errorf("invalid order type: %v", t)
}

func orderTypeToProto(t engine.OrderType) (eventpb.OrderType, error) {
	switch t {
	case engine.OrderTypeLimit:
		return eventpb.OrderType_ORDER_TYPE_LIMIT, nil
	case engine.OrderTypeMarket:
		return eventpb.OrderType_ORDER_TYPE_MARKET, nil
	}
	return eventpb.OrderType_ORDER_TYPE_UNSPECIFIED, fmt.Errorf("invalid order type: %d", t)
}

func tifFromProto(t eventpb.TimeInForce) (engine.TIF, error) {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC, eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED:
		return engine.TIFGTC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return engine.TIFIOC, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return engine.TIFFOK, nil
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return engine.TIFPostOnly, nil
	}
	return 0, fmt.Errorf("invalid TIF: %v", t)
}

func tifToProto(t engine.TIF) (eventpb.TimeInForce, error) {
	switch t {
	case engine.TIFGTC:
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC, nil
	case engine.TIFIOC:
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC, nil
	case engine.TIFFOK:
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK, nil
	case engine.TIFPostOnly:
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, nil
	}
	return eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED, fmt.Errorf("invalid TIF: %d", t)
}

func internalStatusToProto(s engine.OrderStatus) eventpb.InternalOrderStatus {
	switch s {
	case engine.OrderStatusPendingNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW
	case engine.OrderStatusNew:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW
	case engine.OrderStatusPartiallyFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED
	case engine.OrderStatusFilled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED
	case engine.OrderStatusPendingCancel:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
	case engine.OrderStatusCanceled:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED
	case engine.OrderStatusRejected:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED
	case engine.OrderStatusExpired:
		return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED
	}
	return eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED
}
