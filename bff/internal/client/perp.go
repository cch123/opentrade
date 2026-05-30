package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Perp is the minimal perp-counter surface BFF needs (ADR-0068 M7). Aliased to
// the Connect-generated client so fakes implement the alias and hot paths
// invoke the same stub (mirrors client.Counter).
type Perp = perprpcconnect.PerpServiceClient

// DialPerp wires a perp-counter Connect client over plaintext h2c.
func DialPerp(_ context.Context, endpoint string) (*http.Client, Perp, error) {
	httpClient := connectx.NewH2CClient()
	cli := perprpcconnect.NewPerpServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}
