package main

import "testing"

func TestOwnedSnapshotVShards_AllWhenSingleOwner(t *testing.T) {
	got := ownedSnapshotVShards(4, 0, 1)
	want := []int{0, 1, 2, 3}
	if !sameInts(got, want) {
		t.Fatalf("owned = %v, want %v", got, want)
	}
}

func TestOwnedSnapshotVShards_ModuloSplit(t *testing.T) {
	got := ownedSnapshotVShards(8, 1, 3)
	want := []int{1, 4, 7}
	if !sameInts(got, want) {
		t.Fatalf("owned = %v, want %v", got, want)
	}
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
