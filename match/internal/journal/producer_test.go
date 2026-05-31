package journal

import (
	"testing"

	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/shard"
)

// TestOutputTargets_NonTrade: every OutputKind other than Trade carries
// exactly one user_id and targets one vshard.
func TestOutputTargets_NonTrade(t *testing.T) {
	const vshards = 256
	cases := []struct {
		name string
		out  *sequencer.Output
	}{
		{"Accepted", &sequencer.Output{Kind: sequencer.OutputOrderAccepted, UserID: 6001}},
		{"Rejected", &sequencer.Output{Kind: sequencer.OutputOrderRejected, UserID: 6002}},
		{"Cancelled", &sequencer.Output{Kind: sequencer.OutputOrderCancelled, UserID: 6003}},
		{"Expired", &sequencer.Output{Kind: sequencer.OutputOrderExpired, UserID: 6004}},
	}
	for _, c := range cases {
		got := outputTargets(c.out, vshards)
		if len(got) != 1 {
			t.Errorf("%s: got %d targets, want 1", c.name, len(got))
			continue
		}
		if got[0].userID != c.out.UserID {
			t.Errorf("%s: userID = %d, want %d", c.name, got[0].userID, c.out.UserID)
		}
		wantPart := shard.Index(c.out.UserID, vshards)
		if got[0].partition != wantPart {
			t.Errorf("%s: partition = %d, want %d", c.name, got[0].partition, wantPart)
		}
	}
}

// TestOutputTargets_Trade_Dual: a normal Trade (maker != taker) emits
// two targets — one for maker, one for taker — each at their own
// vshard.
func TestOutputTargets_Trade_Dual(t *testing.T) {
	const vshards = 256
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      5001, // taker side (see convert.go)
		MakerUserID: 4001,
	}
	got := outputTargets(out, vshards)
	if len(got) != 2 {
		t.Fatalf("got %d targets, want 2: %+v", len(got), got)
	}
	// First entry is maker, second is taker — contract matters for
	// downstream debug / log readers.
	if got[0].userID != 4001 {
		t.Errorf("target[0] = %d, want maker", got[0].userID)
	}
	if got[1].userID != 5001 {
		t.Errorf("target[1] = %d, want taker", got[1].userID)
	}
	if got[0].partition != shard.Index(4001, vshards) {
		t.Errorf("maker partition = %d, want %d",
			got[0].partition, shard.Index(4001, vshards))
	}
	if got[1].partition != shard.Index(5001, vshards) {
		t.Errorf("taker partition = %d, want %d",
			got[1].partition, shard.Index(5001, vshards))
	}
}

// TestOutputTargets_Trade_SelfTrade: maker_user_id == taker_user_id
// collapses to a single emit so Counter doesn't apply a duplicate
// settlement to the same user (the downstream match_seq guard would
// drop the second copy anyway — this avoids the wasted event).
func TestOutputTargets_Trade_SelfTrade(t *testing.T) {
	const vshards = 256
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      6001,
		MakerUserID: 6001,
	}
	got := outputTargets(out, vshards)
	if len(got) != 1 {
		t.Fatalf("got %d targets, want 1: %+v", len(got), got)
	}
	if got[0].userID != 6001 || got[0].partition != shard.Index(6001, vshards) {
		t.Errorf("self-trade = %+v", got[0])
	}
}

// TestOutputTargets_Trade_ZeroMakerCollapses: if maker_user_id is
// zero (shouldn't happen in production but defensively), we fall back
// to single-emit rather than writing to partition for user 0.
func TestOutputTargets_Trade_EmptyMakerCollapses(t *testing.T) {
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      5002,
		MakerUserID: 0,
	}
	got := outputTargets(out, 256)
	if len(got) != 1 {
		t.Errorf("empty maker should collapse to single, got %d targets", len(got))
	}
}
