// Package clustering implements the ADR-0058 cluster-membership control
// plane for Counter: every process registers itself in etcd under a
// lease, competes for the coordinator role, and — when elected —
// maintains the vshard → owner assignment table used by the rest of the
// codebase to route work.
//
// Scope of phase 2 (current file set):
//
//   - Node registration + keepalive (the "instance lock").
//   - Coordinator election + initial assignment (HRW hashing from the
//     current live-nodes set).
//
// Not yet in scope (later phases of ADR-0058):
//
//   - Automatic failover on node churn (phase 7).
//   - Active migration / epoch handoff (phase 6).
//   - Counter nodes reacting to assignment changes to start/stop
//     per-vshard consumers (phase 3).
//
// Design principle: lean on etcd primitives (Session, Election, Txn
// CAS) rather than re-implementing distributed algorithms.

package clustering

// VShardID identifies one of the VShardCount virtual shards (typically
// 0..255). Stable for the lifetime of the cluster — users map to a
// vshard by `farmhash(user_id) % VShardCount` and the mapping never
// changes (ADR-0058).
type VShardID int

// State is the lifecycle phase of a single vshard's ownership.
type State string

const (
	// StateActive means the owner is processing this vshard's traffic.
	StateActive State = "ACTIVE"

	// StateMigrating means the owner has been told to hand off to
	// Target. It must stop accepting new work, flush its producer,
	// snapshot, upload, then advance the assignment to
	// StateHandoffReady.
	StateMigrating State = "MIGRATING"

	// StateHandoffReady means the old owner is finished and the
	// coordinator can CAS the assignment onto Target (Owner=Target,
	// Epoch+1, State=Active). Reserved for phase 6.
	StateHandoffReady State = "HANDOFF_READY"
)

// Node is a counter process's self-description. Serialised as JSON under
// /{root}/nodes/{id} with a lease — absence of the key means the node is
// gone. Capacity lets the coordinator skip overloaded nodes during
// assignment (phase 2 treats every live node as willing).
type Node struct {
	ID          string `json:"id"`
	Endpoint    string `json:"endpoint"`
	Capacity    int    `json:"capacity"`
	StartedAtMS int64  `json:"started_at_ms"`
}

// Assignment records "who owns which vshard, at what epoch, in what
// phase". Serialised as JSON under /{root}/assignment/vshard-{NNN}.
// Epoch is the fencing token: any reader/writer acting on behalf of a
// vshard carries the epoch so an outdated owner's Kafka transactional
// id is rejected when a new owner takes over (ADR-0058 §Fencing).
type Assignment struct {
	VShardID VShardID `json:"vshard_id"`
	Owner    string   `json:"owner"`
	Epoch    uint64   `json:"epoch"`
	State    State    `json:"state"`
	// Target is set only while State == StateMigrating /
	// StateHandoffReady. Empty otherwise.
	Target string `json:"target,omitempty"`
}
