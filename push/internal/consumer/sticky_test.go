package consumer

import (
	"testing"

	"github.com/xargin/opentrade/pkg/shard"
)

func TestOwnsUser_DisabledByDefault(t *testing.T) {
	// TotalInstances=0 or 1 means single-instance mode; ownsUser always
	// returns true so the filter does nothing.
	for _, total := range []int{0, 1} {
		c := &PrivateConsumer{instanceOrdinal: 0, totalInstances: total}
		if !c.ownsUser(1001) {
			t.Errorf("total=%d: expected ownsUser true", total)
		}
	}
}

func TestOwnsUser_FiltersByHash(t *testing.T) {
	const total = 10
	userID := uint64(1001)
	owner := shard.Index(userID, total)

	ownerCons := &PrivateConsumer{instanceOrdinal: owner, totalInstances: total}
	if !ownerCons.ownsUser(userID) {
		t.Errorf("owner shard %d should claim %d", owner, userID)
	}
	for i := 0; i < total; i++ {
		if i == owner {
			continue
		}
		c := &PrivateConsumer{instanceOrdinal: i, totalInstances: total}
		if c.ownsUser(userID) {
			t.Errorf("shard %d must not claim %d (owner is %d)", i, userID, owner)
		}
	}
}

func TestOwnsUser_ZeroUserIDTreatedAsStable(t *testing.T) {
	const total = 10
	owner := shard.Index(uint64(0), total)
	c := &PrivateConsumer{instanceOrdinal: owner, totalInstances: total}
	if !c.ownsUser(0) {
		t.Error("zero user id is a valid bucket; owner should claim it")
	}
	nonOwner := (owner + 1) % total
	c2 := &PrivateConsumer{instanceOrdinal: nonOwner, totalInstances: total}
	if c2.ownsUser(0) {
		t.Error("non-owner must not claim zero user id")
	}
}
