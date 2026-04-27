package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xargin/opentrade/api/gen/rpc/history/historyrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// History is the narrow surface BFF needs from the history service
// (MVP-15, ADR-0046 / ADR-0057 M4). Aliased to the generated Connect
// client; tests fake the alias.
type History = historyrpcconnect.HistoryServiceClient

// DialHistory opens a plaintext h2c connection to the history service.
// mTLS / auth land with the broader credentials work later.
func DialHistory(_ context.Context, endpoint string) (*http.Client, History, error) {
	httpClient := connectx.NewH2CClient()
	cli := historyrpcconnect.NewHistoryServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}
