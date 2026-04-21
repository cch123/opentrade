package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
)

type fakePub struct {
	mu     sync.Mutex
	events []*eventpb.CounterJournalEvent
}

func (f *fakePub) Publish(_ context.Context, _ string, evt *eventpb.CounterJournalEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, evt)
	return nil
}

func newServer(t *testing.T) *Server {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &fakePub{}
	svc := service.New(service.Config{ShardID: 0, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	return New(svc, zap.NewNop())
}

// Counter's round-trip transfer handler was removed in ADR-0057 M4; its
// coverage lives in asset/internal/server/server_test.go (end-to-end via
// AssetHolder) and counter/internal/server/assetholder_test.go (holder
// surface — idempotency, reject mapping, wrong shard). Only QueryBalance
// round-trips through the Counter gRPC server directly now:

func TestQueryBalanceReturnsZeroForUnknownAsset(t *testing.T) {
	s := newServer(t)
	resp, err := s.QueryBalance(context.Background(), &counterrpc.QueryBalanceRequest{
		UserId: "u1", Asset: "USDT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Balances) != 1 || resp.Balances[0].Available != "0" {
		t.Fatalf("balance = %+v", resp.Balances)
	}
}

func TestPlaceOrderWithoutOrderDepsReturnsUnavailable(t *testing.T) {
	s := newServer(t)
	// newServer wires Transfer only; PlaceOrder requires SetOrderDeps.
	_, err := s.PlaceOrder(context.Background(), &counterrpc.PlaceOrderRequest{
		UserId: "u1", Symbol: "BTC-USDT",
		Side:      eventpb.Side_SIDE_BUY,
		OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
		Tif:       eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Price:     "100", Qty: "1",
	})
	if err == nil {
		t.Fatal("expected Unavailable")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable", status.Code(err))
	}
}
