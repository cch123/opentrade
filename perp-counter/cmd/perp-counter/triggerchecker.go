package main

// triggerchecker.go adapts the trigger service's CountActiveTriggers RPC to
// the service.TriggerChecker seam (ADR-0077 §3 / ADR-0078 §6): the
// SetPositionMode guard against orphaning position-bound triggers. RPC
// failures surface as errors so the caller fails CLOSED — "couldn't ask"
// must never read as "no triggers". The guard is best-effort UX; the hard
// guarantee is the fail-closed admission matrix at fire time.

import (
	"context"
	"time"

	"connectrpc.com/connect"

	triggerrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/api/gen/rpc/trigger/triggerrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// triggerChecker is the Connect client adapter behind the seam.
type triggerChecker struct {
	cli     triggerrpcconnect.TriggerServiceClient
	timeout time.Duration
}

// newTriggerChecker dials lazily (Connect clients connect per call); the
// per-query timeout keeps the user sequencer from hanging on a dead trigger
// service — the error path is the fail-closed reject.
func newTriggerChecker(endpoint string) *triggerChecker {
	return &triggerChecker{
		cli: triggerrpcconnect.NewTriggerServiceClient(
			connectx.NewH2CClient(),
			connectx.BaseURL(endpoint),
			connect.WithGRPC(),
		),
		timeout: 2 * time.Second,
	}
}

// HasActiveTriggers implements service.TriggerChecker.
func (t *triggerChecker) HasActiveTriggers(user uint64, symbol string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	resp, err := t.cli.CountActiveTriggers(ctx, connect.NewRequest(&triggerrpc.CountActiveTriggersRequest{
		UserId: user, Symbol: symbol,
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetCount() > 0, nil
}
