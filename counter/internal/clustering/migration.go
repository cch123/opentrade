package clustering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The three functions in this file drive the ADR-0058 §7 cold-handoff
// state machine:
//
//     ACTIVE(owner=A, ep=N)
//         │  StartMigration(target=C)         (operator tool)
//         ▼
//     MIGRATING(owner=A, ep=N, target=C)
//         │  MarkHandoffReady(ep=N)           (old owner after snapshot)
//         ▼
//     HANDOFF_READY(owner=A, ep=N, target=C)
//         │  CompleteMigration()              (coordinator)
//         ▼
//     ACTIVE(owner=C, ep=N+1)
//
// Each step is an etcd CAS (guarded by ModRevision) so we cannot
// accidentally collapse states if two actors race. Callers get a
// typed error on precondition mismatch so retries / skips are easy
// to reason about.

// ErrPreconditionMismatch is returned when the assignment's current
// state doesn't satisfy the caller's expectations (wrong state, wrong
// owner, wrong epoch, or the key changed between Get and Txn).
var ErrPreconditionMismatch = errors.New("clustering: migration precondition mismatch")

// ErrAssignmentMissing is returned when the vshard has no assignment
// key in etcd. This shouldn't happen post-initial-reconcile; surface
// it so operators notice.
var ErrAssignmentMissing = errors.New("clustering: assignment not found")

// StartMigration is the operator-facing entry point: move vshard from
// its current ACTIVE owner to `target`. Fails if the vshard is not in
// ACTIVE state, or if target is already the owner.
func StartMigration(ctx context.Context, client *clientv3.Client, keys *Keys, vshardID VShardID, target string) error {
	if target == "" {
		return errors.New("clustering: migration target required")
	}
	return casUpdateAssignment(ctx, client, keys, vshardID,
		func(a Assignment) (Assignment, error) {
			if a.State != StateActive {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d state is %s, need ACTIVE",
					ErrPreconditionMismatch, vshardID, a.State)
			}
			if a.Owner == target {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d already owned by %s",
					ErrPreconditionMismatch, vshardID, target)
			}
			a.State = StateMigrating
			a.Target = target
			return a, nil
		})
}

// MarkHandoffReady is called by the old owner after it has flushed
// its transactional producer and uploaded the final snapshot. The
// caller asserts ownership (Owner == expectedOwner && Epoch ==
// expectedEpoch && State == MIGRATING); any mismatch indicates the
// migration was torn down under us and we must NOT advance the state.
func MarkHandoffReady(ctx context.Context, client *clientv3.Client, keys *Keys, vshardID VShardID, expectedOwner string, expectedEpoch uint64) error {
	return casUpdateAssignment(ctx, client, keys, vshardID,
		func(a Assignment) (Assignment, error) {
			if a.State != StateMigrating {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d state is %s, need MIGRATING",
					ErrPreconditionMismatch, vshardID, a.State)
			}
			if a.Owner != expectedOwner {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d owner is %q, expected %q",
					ErrPreconditionMismatch, vshardID, a.Owner, expectedOwner)
			}
			if a.Epoch != expectedEpoch {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d epoch is %d, expected %d",
					ErrPreconditionMismatch, vshardID, a.Epoch, expectedEpoch)
			}
			a.State = StateHandoffReady
			return a, nil
		})
}

// CompleteMigration is called by the coordinator once it observes a
// HANDOFF_READY assignment: swap Owner over to Target, bump Epoch,
// clear Target, set State back to ACTIVE. After this the new owner's
// Manager will see the vshard appear in its ACTIVE set and start a
// Worker with the fresh epoch-stamped transactional id.
func CompleteMigration(ctx context.Context, client *clientv3.Client, keys *Keys, vshardID VShardID) error {
	return casUpdateAssignment(ctx, client, keys, vshardID,
		func(a Assignment) (Assignment, error) {
			if a.State != StateHandoffReady {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d state is %s, need HANDOFF_READY",
					ErrPreconditionMismatch, vshardID, a.State)
			}
			if a.Target == "" {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d has empty Target at HANDOFF_READY",
					ErrPreconditionMismatch, vshardID)
			}
			a.Owner = a.Target
			a.Epoch = a.Epoch + 1
			a.State = StateActive
			a.Target = ""
			return a, nil
		})
}

// casUpdateAssignment is the shared CAS loop: Get → apply transform →
// Txn-compare ModRevision → Put. Retries a small number of times if
// the key changed between Get and Txn (benign race). All "we saw a
// state we didn't want" cases surface as ErrPreconditionMismatch
// without retry.
func casUpdateAssignment(
	ctx context.Context,
	client *clientv3.Client,
	keys *Keys,
	vshardID VShardID,
	transform func(Assignment) (Assignment, error),
) error {
	key := keys.Assignment(vshardID)
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("get %s: %w", key, err)
		}
		if len(resp.Kvs) == 0 {
			return fmt.Errorf("%w: %s", ErrAssignmentMissing, key)
		}
		kv := resp.Kvs[0]
		var current Assignment
		if err := json.Unmarshal(kv.Value, &current); err != nil {
			return fmt.Errorf("unmarshal %s: %w", key, err)
		}
		updated, err := transform(current)
		if err != nil {
			return err
		}
		data, err := json.Marshal(updated)
		if err != nil {
			return fmt.Errorf("marshal updated assignment: %w", err)
		}
		txnResp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)).
			Then(clientv3.OpPut(key, string(data))).
			Commit()
		if err != nil {
			return fmt.Errorf("cas %s: %w", key, err)
		}
		if txnResp.Succeeded {
			return nil
		}
		// Key raced; loop will re-read and retry.
	}
	return fmt.Errorf("clustering: CAS for %s failed after %d attempts", key, maxAttempts)
}

// ForceReassign moves a vshard to newOwner untriggerly — used by
// the coordinator's failover loop (ADR-0058 phase 7) when the current
// owner's etcd lease has expired. Unlike the ACTIVE → MIGRATING →
// HANDOFF_READY path, there is no flush / snapshot handoff here: the
// old owner is presumed dead, so newOwner must rely on the most
// recent snapshot in S3 plus Kafka replay to rebuild state (ADR-0001
// Kafka-as-SoT + ADR-0048 offset-in-snapshot cover the gap).
//
// Bumps Epoch so any zombie producer from the dead owner that comes
// back online gets fenced on its next InitProducerID (ADR-0058 §4).
func ForceReassign(ctx context.Context, client *clientv3.Client, keys *Keys, vshardID VShardID, newOwner string) error {
	if newOwner == "" {
		return errors.New("clustering: ForceReassign newOwner required")
	}
	return casUpdateAssignment(ctx, client, keys, vshardID,
		func(a Assignment) (Assignment, error) {
			if a.Owner == newOwner {
				return Assignment{}, fmt.Errorf(
					"%w: vshard %d already owned by %s",
					ErrPreconditionMismatch, vshardID, newOwner)
			}
			a.Owner = newOwner
			a.Epoch = a.Epoch + 1
			a.State = StateActive
			a.Target = ""
			return a, nil
		})
}

// ReadAssignment is a small helper for callers (CLI, tests, Manager
// handoff check) that just want to see the current assignment.
func ReadAssignment(ctx context.Context, client *clientv3.Client, keys *Keys, vshardID VShardID) (Assignment, error) {
	resp, err := client.Get(ctx, keys.Assignment(vshardID))
	if err != nil {
		return Assignment{}, err
	}
	if len(resp.Kvs) == 0 {
		return Assignment{}, fmt.Errorf("%w: vshard %d", ErrAssignmentMissing, vshardID)
	}
	var a Assignment
	if err := json.Unmarshal(resp.Kvs[0].Value, &a); err != nil {
		return Assignment{}, err
	}
	return a, nil
}
