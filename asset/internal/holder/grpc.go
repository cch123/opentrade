package holder

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
)

// GRPCClient wraps an assetholderrpc.AssetHolderClient. It is the
// client implementation used when the peer biz_line lives in a
// different process (e.g. counter for biz_line=spot).
type GRPCClient struct {
	conn *grpc.ClientConn
	rpc  assetholderrpc.AssetHolderClient
}

// NewGRPCClient dials target and returns a Client wrapping the
// resulting connection. The caller must Close the underlying conn
// when the asset-service shuts down.
func NewGRPCClient(target string, opts ...grpc.DialOption) (*GRPCClient, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("holder: dial %s: %w", target, err)
	}
	return &GRPCClient{conn: conn, rpc: assetholderrpc.NewAssetHolderClient(conn)}, nil
}

// NewGRPCClientWithConn is for tests (sharing a bufconn) and for callers
// that want to manage the dial themselves. The GRPCClient does NOT take
// ownership: Close() is a no-op.
func NewGRPCClientWithConn(conn *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{conn: nil, rpc: assetholderrpc.NewAssetHolderClient(conn)}
}

// Close closes the owned conn (nil-safe for bufconn-injected clients).
func (c *GRPCClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// TransferOut implements Client.
func (c *GRPCClient) TransferOut(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.TransferOut(ctx, &assetholderrpc.TransferOutRequest{
		UserId:     req.UserID,
		TransferId: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
		PeerBiz:    req.PeerBiz,
		Memo:       req.Memo,
	})
	return wrapResp(resp, err)
}

// TransferIn implements Client.
func (c *GRPCClient) TransferIn(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.TransferIn(ctx, &assetholderrpc.TransferInRequest{
		UserId:     req.UserID,
		TransferId: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
		PeerBiz:    req.PeerBiz,
		Memo:       req.Memo,
	})
	return wrapInResp(resp, err)
}

// CompensateTransferOut implements Client.
func (c *GRPCClient) CompensateTransferOut(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.CompensateTransferOut(ctx, &assetholderrpc.CompensateTransferOutRequest{
		UserId:          req.UserID,
		TransferId:      req.TransferID,
		Asset:           req.Asset,
		Amount:          req.Amount,
		PeerBiz:         req.PeerBiz,
		CompensateCause: req.CompensateCause,
	})
	return wrapCompResp(resp, err)
}

// wrapResp / wrapInResp / wrapCompResp fold the proto responses into
// the internal Result. Duplicated response types share fields but are
// distinct proto messages; proto generation doesn't give us a shared
// interface so we write three tiny helpers.

func wrapResp(resp *assetholderrpc.TransferOutResponse, err error) (Result, error) {
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status:       statusFromProto(resp.Status),
		Reason:       reasonFromProto(resp.RejectReason),
		ReasonDetail: "",
	}, nil
}

func wrapInResp(resp *assetholderrpc.TransferInResponse, err error) (Result, error) {
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status: statusFromProto(resp.Status),
		Reason: reasonFromProto(resp.RejectReason),
	}, nil
}

func wrapCompResp(resp *assetholderrpc.CompensateTransferOutResponse, err error) (Result, error) {
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status: statusFromProto(resp.Status),
		Reason: reasonFromProto(resp.RejectReason),
	}, nil
}

func statusFromProto(s assetholderrpc.TransferStatus) Status {
	switch s {
	case assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED:
		return StatusConfirmed
	case assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED:
		return StatusRejected
	case assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED:
		return StatusDuplicated
	}
	return StatusUnspecified
}

func reasonFromProto(r assetholderrpc.RejectReason) RejectReason {
	switch r {
	case assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE:
		return ReasonInsufficientBalance
	case assetholderrpc.RejectReason_REJECT_REASON_UNKNOWN_USER:
		return ReasonUnknownUser
	case assetholderrpc.RejectReason_REJECT_REASON_UNKNOWN_ASSET:
		return ReasonUnknownAsset
	case assetholderrpc.RejectReason_REJECT_REASON_ASSET_FROZEN:
		return ReasonAssetFrozen
	case assetholderrpc.RejectReason_REJECT_REASON_AMOUNT_INVALID:
		return ReasonAmountInvalid
	case assetholderrpc.RejectReason_REJECT_REASON_INTERNAL:
		return ReasonInternal
	}
	return ReasonUnspecified
}
