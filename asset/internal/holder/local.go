package holder

import (
	"context"
	"errors"
	"fmt"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
)

// LocalFundingClient is the in-process Client for biz_line=funding. It
// goes directly through *service.Service instead of looping back
// through gRPC — saves one hop and a serialization/deserialization on
// the hot path (every saga touches funding on one side).
type LocalFundingClient struct {
	svc *service.Service
}

// NewLocalFundingClient wires a local Client on top of svc.
func NewLocalFundingClient(svc *service.Service) *LocalFundingClient {
	return &LocalFundingClient{svc: svc}
}

// TransferOut implements Client.
func (c *LocalFundingClient) TransferOut(ctx context.Context, req Request) (Result, error) {
	return c.run(ctx, req, c.svc.TransferOut)
}

// TransferIn implements Client.
func (c *LocalFundingClient) TransferIn(ctx context.Context, req Request) (Result, error) {
	return c.run(ctx, req, c.svc.TransferIn)
}

// CompensateTransferOut implements Client.
func (c *LocalFundingClient) CompensateTransferOut(ctx context.Context, req Request) (Result, error) {
	return c.run(ctx, req, c.svc.Compensate)
}

// run is the shared parse-then-dispatch for all three methods.
func (c *LocalFundingClient) run(ctx context.Context, req Request, call func(context.Context, service.HolderRequest) (service.Result, error)) (Result, error) {
	hreq, buildErr := buildServiceReq(req)
	if buildErr != nil {
		return Result{
			Status:       StatusRejected,
			Reason:       ReasonAmountInvalid,
			ReasonDetail: buildErr.Error(),
		}, nil
	}
	res, err := call(ctx, hreq)
	if err != nil {
		if errors.Is(err, service.ErrIdempotencyConflict) {
			return Result{
				Status:       StatusRejected,
				Reason:       ReasonInternal,
				ReasonDetail: err.Error(),
			}, nil
		}
		return Result{}, err
	}
	return serviceResultToHolder(res), nil
}

// buildServiceReq mirrors server.buildHolderReq but without gRPC status
// wrapping — the saga driver consumes errors as rejected Results, not
// gRPC codes.
func buildServiceReq(req Request) (service.HolderRequest, error) {
	if req.UserID == "" {
		return service.HolderRequest{}, errors.New("user_id required")
	}
	if req.TransferID == "" {
		return service.HolderRequest{}, errors.New("transfer_id required")
	}
	if req.Asset == "" {
		return service.HolderRequest{}, errors.New("asset required")
	}
	amt, err := dec.Parse(req.Amount)
	if err != nil {
		return service.HolderRequest{}, fmt.Errorf("invalid amount %q: %w", req.Amount, err)
	}
	if amt.Sign() <= 0 {
		return service.HolderRequest{}, errors.New("amount must be positive")
	}
	return service.HolderRequest{
		UserID:          req.UserID,
		TransferID:      req.TransferID,
		Asset:           req.Asset,
		PeerBiz:         req.PeerBiz,
		Memo:            req.Memo,
		CompensateCause: req.CompensateCause,
		Amount: engine.TransferRequest{
			UserID:     req.UserID,
			TransferID: req.TransferID,
			Asset:      req.Asset,
			Amount:     amt,
		},
	}, nil
}

// serviceResultToHolder translates service.Result into holder.Result.
// Engine errors fold into the RejectReason enum the same way the server
// layer does so downstream code sees a consistent reason set regardless
// of local vs remote path.
func serviceResultToHolder(res service.Result) Result {
	switch res.Status {
	case service.StatusConfirmed:
		return Result{Status: StatusConfirmed}
	case service.StatusDuplicated:
		return Result{Status: StatusDuplicated}
	case service.StatusRejected:
		detail := ""
		if res.RejectReason != nil {
			detail = res.RejectReason.Error()
		}
		return Result{
			Status:       StatusRejected,
			Reason:       engineErrToReason(res.RejectReason),
			ReasonDetail: detail,
		}
	}
	return Result{Status: StatusUnspecified, Reason: ReasonInternal}
}

func engineErrToReason(err error) RejectReason {
	switch {
	case err == nil:
		return ReasonUnspecified
	case errors.Is(err, engine.ErrInsufficientAvailable):
		return ReasonInsufficientBalance
	case errors.Is(err, engine.ErrInvalidAmount):
		return ReasonAmountInvalid
	}
	return ReasonInternal
}
