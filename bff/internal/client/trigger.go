package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xargin/opentrade/api/gen/rpc/trigger/triggerrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Trigger is the narrow surface BFF needs from the trigger service.
// Aliased to the generated Connect client; fakes implement the alias.
type Trigger = triggerrpcconnect.TriggerServiceClient

// DialTrigger opens a plaintext h2c connection to the trigger service.
// mTLS / auth land with the broader credentials work later.
func DialTrigger(_ context.Context, endpoint string) (*http.Client, Trigger, error) {
	httpClient := connectx.NewH2CClient()
	cli := triggerrpcconnect.NewTriggerServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}
