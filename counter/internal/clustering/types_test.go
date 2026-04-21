package clustering

import (
	"encoding/json"
	"testing"
)

func TestNode_JSONRoundTrip(t *testing.T) {
	in := Node{
		ID:          "counter-a",
		Endpoint:    "10.0.0.1:8081",
		Capacity:    32,
		StartedAtMS: 1713660000000,
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Node
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("mismatch: in=%+v out=%+v", in, out)
	}
}

func TestAssignment_JSONRoundTrip(t *testing.T) {
	in := Assignment{
		VShardID: 42,
		Owner:    "counter-a",
		Epoch:    7,
		State:    StateMigrating,
		Target:   "counter-b",
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Assignment
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("mismatch: in=%+v out=%+v", in, out)
	}
}

// TestAssignment_JSONOmitsEmptyTarget confirms the non-migrating common
// case keeps the wire payload slim (Target field dropped).
func TestAssignment_JSONOmitsEmptyTarget(t *testing.T) {
	in := Assignment{VShardID: 0, Owner: "x", Epoch: 1, State: StateActive}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); contains(got, "target") {
		t.Errorf("expected target to be omitted, got %s", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
