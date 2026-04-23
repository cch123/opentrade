package journal

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestOffsetStore_AdjustUsesSavedAndKeepsCurrentForMissing(t *testing.T) {
	store := newOffsetStore(map[string]map[int32]int64{
		"order-event-BTC-USDT": {0: 42},
	})

	got := store.adjust(map[string]map[int32]kgo.Offset{
		"order-event-BTC-USDT": {
			0: kgo.NewOffset().AtStart(),
			1: kgo.NewOffset().At(9),
		},
	})

	if off := got["order-event-BTC-USDT"][0]; off != kgo.NewOffset().At(42) {
		t.Fatalf("saved offset = %v, want 42", off)
	}
	if off := got["order-event-BTC-USDT"][1]; off != kgo.NewOffset().At(9) {
		t.Fatalf("missing saved offset = %v, want current 9", off)
	}
}

func TestOffsetStore_MergeUpdatesDynamicTopicOffsets(t *testing.T) {
	store := newOffsetStore(nil)

	store.merge(map[string]map[int32]int64{
		"order-event-ETH-USDT": {3: 88},
	})
	got := store.adjust(map[string]map[int32]kgo.Offset{
		"order-event-ETH-USDT": {3: kgo.NewOffset().AtStart()},
	})

	if off := got["order-event-ETH-USDT"][3]; off != kgo.NewOffset().At(88) {
		t.Fatalf("dynamic offset = %v, want 88", off)
	}
}
