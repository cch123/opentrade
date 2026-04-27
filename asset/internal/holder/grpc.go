package holder

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/api/gen/rpc/assetholder/assetholderrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// GRPCClient wraps an assetholderrpcconnect.AssetHolderClient. The peer
// biz_line lives in a different process (e.g. counter for biz_line=spot)
// and we speak the gRPC wire protocol over h2c via connect-go.
type GRPCClient struct {
	httpClient *http.Client
	owns       bool // true => Close() releases httpClient idle conns
	rpc        assetholderrpcconnect.AssetHolderClient
}

// NewGRPCClient builds a Client targeting the given host:port endpoint
// over plaintext h2c. The returned Client owns the underlying
// http.Client; Close() drops idle connections.
func NewGRPCClient(target string) (*GRPCClient, error) {
	httpClient := connectx.NewH2CClient()
	rpc := assetholderrpcconnect.NewAssetHolderClient(
		httpClient,
		connectx.BaseURL(target),
		connect.WithGRPC(),
	)
	return &GRPCClient{httpClient: httpClient, owns: true, rpc: rpc}, nil
}

// NewGRPCClientFromHTTP wraps an existing *http.Client with a Connect
// AssetHolder stub bound to baseURL (scheme + host[:port], no path).
// Used by tests that share an httptest.Server transport across multiple
// holders. Close() is a no-op — the caller owns the http.Client.
func NewGRPCClientFromHTTP(httpClient *http.Client, baseURL string) *GRPCClient {
	rpc := assetholderrpcconnect.NewAssetHolderClient(
		httpClient,
		baseURL,
		connect.WithGRPC(),
	)
	return &GRPCClient{httpClient: httpClient, owns: false, rpc: rpc}
}

// Close releases idle connections on owned http.Clients. Safe to call
// on borrowed clients (no-op).
func (c *GRPCClient) Close() error {
	if c.owns && c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}

// TransferOut implements Client.
func (c *GRPCClient) TransferOut(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.TransferOut(ctx, connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId:     req.UserID,
		TransferId: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
		PeerBiz:    req.PeerBiz,
		Memo:       req.Memo,
	}))
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status: statusFromProto(resp.Msg.Status),
		Reason: reasonFromProto(resp.Msg.RejectReason),
	}, nil
}

// TransferIn implements Client.
func (c *GRPCClient) TransferIn(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.TransferIn(ctx, connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId:     req.UserID,
		TransferId: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
		PeerBiz:    req.PeerBiz,
		Memo:       req.Memo,
	}))
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status: statusFromProto(resp.Msg.Status),
		Reason: reasonFromProto(resp.Msg.RejectReason),
	}, nil
}

// CompensateTransferOut implements Client.
func (c *GRPCClient) CompensateTransferOut(ctx context.Context, req Request) (Result, error) {
	resp, err := c.rpc.CompensateTransferOut(ctx, connect.NewRequest(&assetholderrpc.CompensateTransferOutRequest{
		UserId:          req.UserID,
		TransferId:      req.TransferID,
		Asset:           req.Asset,
		Amount:          req.Amount,
		PeerBiz:         req.PeerBiz,
		CompensateCause: req.CompensateCause,
	}))
	if err != nil {
		return Result{}, err
	}
	return Result{
		Status: statusFromProto(resp.Msg.Status),
		Reason: reasonFromProto(resp.Msg.RejectReason),
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
