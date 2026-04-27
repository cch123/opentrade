// Package client wraps the Counter Connect-Go stub so REST handlers
// depend on an interface (easy to fake in tests) rather than on the
// generated stub directly.
package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xargin/opentrade/api/gen/rpc/counter/counterrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Counter is the minimal surface BFF needs. The Connect-generated
// CounterServiceClient already carries all the methods, so we alias
// rather than redeclaring them — fakes implement the alias and hot
// paths invoke the same generated stub.
//
// ADR-0057 M4: Transfer was removed — all user-facing fund movement
// goes through asset-service now (see client.Asset).
type Counter = counterrpcconnect.CounterServiceClient

// DialCounter wires a Counter Connect client over plaintext h2c. The
// returned *http.Client owns the transport; callers drop idle conns
// via CloseIdleConnections at shutdown. mTLS / auth credentials arrive
// in a later MVP.
func DialCounter(_ context.Context, endpoint string) (*http.Client, Counter, error) {
	httpClient := connectx.NewH2CClient()
	cli := counterrpcconnect.NewCounterServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}

// Dial preserves the legacy entry name for callers still using
// `client.Dial`.
func Dial(ctx context.Context, endpoint string) (*http.Client, Counter, error) {
	return DialCounter(ctx, endpoint)
}
