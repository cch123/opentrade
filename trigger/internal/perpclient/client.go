// Package perpclient wraps the perp-counter Connect client behind the
// engine's narrow PerpOrderPlacer surface (ADR-0078 §6). perp-counter is a
// single instance today — no shard routing (mirrors counterclient's module
// boundary: trigger does not import bff's client).
package perpclient

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Dial returns a perp-counter Connect client for endpoint.
func Dial(_ context.Context, endpoint string) (*http.Client, perprpcconnect.PerpServiceClient, error) {
	if endpoint == "" {
		return nil, nil, fmt.Errorf("dial: empty endpoint")
	}
	httpClient := connectx.NewH2CClient()
	cli := perprpcconnect.NewPerpServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}

// Placer adapts the generated client to engine.PerpOrderPlacer (unwraps the
// connect request/response envelopes).
type Placer struct {
	cli perprpcconnect.PerpServiceClient
}

// NewPlacer wraps a dialed client.
func NewPlacer(cli perprpcconnect.PerpServiceClient) *Placer { return &Placer{cli: cli} }

// PlaceOrder forwards the fired inner order to perp-counter.
func (p *Placer) PlaceOrder(ctx context.Context, in *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
	resp, err := p.cli.PlaceOrder(ctx, connect.NewRequest(in))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
