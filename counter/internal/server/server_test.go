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

func TestTransferRoundTrip(t *testing.T) {
	s := newServer(t)

	resp, err := s.Transfer(context.Background(), &counterrpc.TransferRequest{
		UserId: "u1", TransferId: "tx-1", Asset: "USDT",
		Amount: "100", Type: counterrpc.TransferType_TRANSFER_TYPE_DEPOSIT,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != counterrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Status)
	}
	if resp.AvailableAfter != "100" {
		t.Fatalf("available_after = %q", resp.AvailableAfter)
	}

	// Query the balance via the server.
	q, err := s.QueryBalance(context.Background(), &counterrpc.QueryBalanceRequest{
		UserId: "u1", Asset: "USDT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Balances) != 1 || q.Balances[0].Available != "100" {
		t.Fatalf("query = %+v", q.Balances)
	}
}

func TestTransferInvalidAmount(t *testing.T) {
	s := newServer(t)
	_, err := s.Transfer(context.Background(), &counterrpc.TransferRequest{
		UserId: "u1", TransferId: "tx-1", Asset: "USDT",
		Amount: "not-a-number", Type: counterrpc.TransferType_TRANSFER_TYPE_DEPOSIT,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestTransferInvalidType(t *testing.T) {
	s := newServer(t)
	_, err := s.Transfer(context.Background(), &counterrpc.TransferRequest{
		UserId: "u1", TransferId: "tx-1", Asset: "USDT",
		Amount: "1", Type: counterrpc.TransferType_TRANSFER_TYPE_UNSPECIFIED,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s", status.Code(err))
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
