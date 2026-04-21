package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
)

// Asset is the narrow surface BFF needs from asset-service (ADR-0057).
// Tests substitute a fake implementation so the REST layer can be
// exercised without dialing.
type Asset interface {
	Transfer(ctx context.Context, in *assetrpc.TransferRequest, opts ...grpc.CallOption) (*assetrpc.TransferResponse, error)
	QueryFundingBalance(ctx context.Context, in *assetrpc.QueryFundingBalanceRequest, opts ...grpc.CallOption) (*assetrpc.QueryFundingBalanceResponse, error)
	QueryTransfer(ctx context.Context, in *assetrpc.QueryTransferRequest, opts ...grpc.CallOption) (*assetrpc.QueryTransferResponse, error)
}

// DialAsset opens a plaintext gRPC connection to asset-service. mTLS /
// auth credentials come with the broader security work.
func DialAsset(_ context.Context, endpoint string) (*grpc.ClientConn, Asset, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc Dial %s: %w", endpoint, err)
	}
	return conn, assetrpc.NewAssetServiceClient(conn), nil
}
