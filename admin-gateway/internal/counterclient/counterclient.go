// Package counterclient is admin-gateway's thin wrapper around the
// generated CounterService gRPC stub. Kept separate from bff/internal/
// client (ADR-0052 decides admin-gateway is an independent module) even
// though the shape is similar — future admin-only changes (stricter
// timeouts / dedicated deadline budgets / admin-only retries) can land
// here without touching the user-facing client.
package counterclient

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
)

// Counter is the minimal Counter-service surface admin-gateway uses. Only
// AdminCancelOrders is called today; additional admin RPCs can be added
// here as MVP-17 follow-ups.
type Counter interface {
	AdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error)
}

// Dial opens a plaintext RPC transport to one Counter shard endpoint.
// mTLS / auth credentials arrive in a later MVP.
func Dial(_ context.Context, endpoint string) (io.Closer, Counter, error) {
	httpClient, closer := connectutil.NewHTTPClient()
	return closer, counterrpc.NewCounterServiceConnectClient(httpClient, endpoint), nil
}

// Sharded is a lightweight admin-gateway ShardedCounter. Unlike
// bff/internal/client.ShardedCounter this does not hash users to a single
// shard — admin batch cancel either targets a single shard (caller picks
// by user ownership, same xxhash routing as bff) or broadcasts to every
// shard. We keep the routing decision at the handler level so the client
// is a simple ordered shard list.
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

// Shard returns the i-th shard client. Callers that pick by user ownership
// hash outside this package.
func (s *Sharded) Shard(i int) Counter {
	return s.clients[i]
}

// BroadcastAdminCancelOrders calls every shard in parallel and returns
// per-shard responses in shard-id order. Shards that error return nil at
// their slot and the first error is surfaced; callers can still read the
// non-nil slots for partial-success visibility.
func (s *Sharded) BroadcastAdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) ([]*counterrpc.AdminCancelOrdersResponse, error) {
	out := make([]*counterrpc.AdminCancelOrdersResponse, len(s.clients))
	errs := make([]error, len(s.clients))
	var wg sync.WaitGroup
	wg.Add(len(s.clients))
	for i, c := range s.clients {
		i, c := i, c
		go func() {
			defer wg.Done()
			resp, err := c.AdminCancelOrders(ctx, in, opts...)
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = resp
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
