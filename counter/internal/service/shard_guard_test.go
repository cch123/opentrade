package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/shard"
)

func newShardedFixture(t *testing.T, shardID, totalShards int) *Service {
	t.Helper()
	state := counterstate.NewShardState(shardID)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &mockPublisher{}
	return New(Config{ShardID: shardID, TotalShards: totalShards, ProducerID: "counter-shard-X-main"},
		state, seq, dt, pub, zap.NewNop())
}

// Pick a user id that does NOT land on shardID under totalShards.
func userForOtherShard(shardID, totalShards int) string {
	for i := 0; ; i++ {
		u := "probe-" + itoa(i)
		if shard.Index(u, totalShards) != shardID {
			return u
		}
	}
}

func TestOwnsUser_TotalShardsZero_ClaimsEverything(t *testing.T) {
	svc := newShardedFixture(t, 0, 0)
	if !svc.OwnsUser("anyone") {
		t.Error("TotalShards=0 must claim every user")
	}
}

func TestOwnsUser_SelectsByIndex(t *testing.T) {
	total := 10
	for sid := 0; sid < total; sid++ {
		svc := newShardedFixture(t, sid, total)
		owned := "probe-" + itoa(sid*31+1)
		want := shard.Index(owned, total) == sid
		if got := svc.OwnsUser(owned); got != want {
			t.Errorf("shard=%d user=%q OwnsUser=%v want %v", sid, owned, got, want)
		}
	}
}

func TestTransfer_WrongShardReturnsError(t *testing.T) {
	total := 10
	shardID := 0
	svc := newShardedFixture(t, shardID, total)
	otherUser := userForOtherShard(shardID, total)

	_, err := svc.Transfer(context.Background(), counterstate.TransferRequest{
		TransferID: "tx-1", UserID: otherUser, Asset: "USDT",
		Amount: dec.New("10"), Type: counterstate.TransferDeposit,
	})
	if !errors.Is(err, ErrWrongShard) {
		t.Fatalf("got %v, want ErrWrongShard", err)
	}
}

func TestQueryBalance_WrongShardReturnsError(t *testing.T) {
	total := 10
	shardID := 3
	svc := newShardedFixture(t, shardID, total)
	otherUser := userForOtherShard(shardID, total)
	if _, err := svc.QueryBalance(otherUser, "USDT"); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryBalance: got %v want ErrWrongShard", err)
	}
	if _, err := svc.QueryAccount(otherUser); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryAccount: got %v want ErrWrongShard", err)
	}
	if _, err := svc.QueryOrder(otherUser, 1); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryOrder: got %v want ErrWrongShard", err)
	}
}

// TestHandleTradeEvent_ForeignShardSkipped verifies that when the consumer
// sees a trade-event whose parties are not owned by this shard, each handler
// returns nil without entering the user sequencer or publishing a journal
// event. The guard lets every Counter instance consume the full trade-event
// topic safely (MVP-8 implicit filter → explicit per-party filter).
func TestHandleTradeEvent_ForeignShardSkipped(t *testing.T) {
	total := 10
	shardID := 0
	svc := newShardedFixture(t, shardID, total)
	pub := svc.publisher.(*mockPublisher)
	foreign := userForOtherShard(shardID, total)

	cases := []struct {
		name    string
		payload *eventpb.TradeEvent
	}{
		{"accepted", &eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Accepted{
			Accepted: &eventpb.OrderAccepted{UserId: foreign, OrderId: 1},
		}}},
		{"rejected", &eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Rejected{
			Rejected: &eventpb.OrderRejected{UserId: foreign, OrderId: 1},
		}}},
		{"cancelled", &eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Cancelled{
			Cancelled: &eventpb.OrderCancelled{UserId: foreign, OrderId: 1},
		}}},
		{"expired", &eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Expired{
			Expired: &eventpb.OrderExpired{UserId: foreign, OrderId: 1},
		}}},
		{"trade", &eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Trade{
			Trade: &eventpb.Trade{
				TradeId: "t1", Symbol: "BTC-USDT", Price: "100", Qty: "1",
				MakerUserId: foreign, MakerOrderId: 1,
				TakerUserId: foreign, TakerOrderId: 2,
				MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
				TakerSide: eventpb.Side_SIDE_BUY,
			},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := svc.HandleTradeEvent(context.Background(), tc.payload); err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
	if got := len(pub.Events()); got != 0 {
		t.Errorf("publisher saw %d events; want 0 for foreign-shard inputs", got)
	}
}

func TestTransfer_CorrectShardPasses(t *testing.T) {
	total := 10
	shardID := 7
	svc := newShardedFixture(t, shardID, total)
	// Find a user owned by this shard.
	var owned string
	for i := 0; ; i++ {
		u := "mine-" + itoa(i)
		if shard.Index(u, total) == shardID {
			owned = u
			break
		}
	}
	res, err := svc.Transfer(context.Background(), counterstate.TransferRequest{
		TransferID: "tx-1", UserID: owned, Asset: "USDT",
		Amount: dec.New("50"), Type: counterstate.TransferDeposit,
	})
	if err != nil {
		t.Fatalf("Transfer failed for owned user: %v", err)
	}
	if res.Status != counterstate.TransferStatusConfirmed {
		t.Errorf("expected Confirmed, got %d", res.Status)
	}
}

