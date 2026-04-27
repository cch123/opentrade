// Package counterclient is admin-gateway's thin wrapper around the
// generated CounterService Connect-Go client. Kept separate from
// bff/internal/client (ADR-0052 decides admin-gateway is an independent
// module) even though the shape is similar — future admin-only changes
// (stricter timeouts / dedicated deadline budgets / admin-only retries)
// can land here without touching the user-facing client.
package counterclient

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"connectrpc.com/connect"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/api/gen/rpc/counter/counterrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// Counter is the minimal Counter-service surface admin-gateway uses.
// Only AdminCancelOrders is called today; additional admin RPCs can be
// added here as MVP-17 follow-ups.
type Counter interface {
	AdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error)
}

// Dial wires a Connect client targeting one Counter shard endpoint over
// plaintext h2c. mTLS / auth credentials arrive in a later MVP.
func Dial(_ context.Context, endpoint string) (*http.Client, Counter, error) {
	httpClient := connectx.NewH2CClient()
	cli := counterrpcconnect.NewCounterServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, narrowCounter{cli: cli}, nil
}

// narrowCounter projects the full CounterServiceClient down to the
// admin-only AdminCancelOrders surface. Keeps the Counter interface
// intentionally narrow so other Counter RPCs can't sneak into the admin
// gateway by accident.
type narrowCounter struct {
	cli counterrpcconnect.CounterServiceClient
}

func (n narrowCounter) AdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	return n.cli.AdminCancelOrders(ctx, req)
}

// Sharded is a lightweight admin-gateway ShardedCounter. Unlike
// bff/internal/client.ShardedCounter this does not hash users to a
// single shard — admin batch cancel either targets a single shard
// (caller picks by user ownership, same xxhash routing as bff) or
// broadcasts to every shard. We keep the routing decision at the
// handler level so the client is a simple ordered shard list.
type Sharded struct {
	clients []Counter
}

// NewSharded wraps an ordered list of per-shard Counter clients.
func NewSharded(clients []Counter) (*Sharded, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("sharded counter: at least one shard required")
	}
	for i, c := range clients {
		if c == nil {
			return nil, fmt.Errorf("sharded counter: shard %d is nil", i)
		}
	}
	return &Sharded{clients: clients}, nil
}

// Shards returns the configured shard count.
func (s *Sharded) Shards() int { return len(s.clients) }

// Shard returns the i-th shard client. Callers that pick by user
// ownership hash outside this package.
func (s *Sharded) Shard(i int) Counter {
	return s.clients[i]
}

// BroadcastAdminCancelOrders calls every shard in parallel and returns
// per-shard responses (unwrapped from connect.Response) in shard-id
// order. Shards that error return nil at their slot and the first
// error is surfaced; callers can still read the non-nil slots for
// partial-success visibility.
func (s *Sharded) BroadcastAdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) ([]*counterrpc.AdminCancelOrdersResponse, error) {
	out := make([]*counterrpc.AdminCancelOrdersResponse, len(s.clients))
	errs := make([]error, len(s.clients))
	var wg sync.WaitGroup
	wg.Add(len(s.clients))
	for i, c := range s.clients {
		i, c := i, c
		go func() {
			defer wg.Done()
			resp, err := c.AdminCancelOrders(ctx, req)
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = resp.Msg
		}()
	}
	wg.Wait()
	var first error
	for _, e := range errs {
		if e != nil {
			first = e
			break
		}
	}
	return out, first
}
