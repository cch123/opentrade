package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xargin/opentrade/api/gen/rpc/asset/assetrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Asset is the narrow surface BFF needs from asset-service (ADR-0057).
// Aliased to the generated Connect client interface so fakes and the
// real stub share the same shape.
type Asset = assetrpcconnect.AssetServiceClient

// DialAsset opens a plaintext h2c connection to asset-service. mTLS /
// auth credentials come with the broader security work.
func DialAsset(_ context.Context, endpoint string) (*http.Client, Asset, error) {
	httpClient := connectx.NewH2CClient()
	cli := assetrpcconnect.NewAssetServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}
