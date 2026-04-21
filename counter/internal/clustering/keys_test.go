package clustering

import "testing"

func TestNewKeys_DefaultRoot(t *testing.T) {
	k := NewKeys("")
	if k.Root != DefaultRoot {
		t.Errorf("empty root should fall back to %q, got %q", DefaultRoot, k.Root)
	}
}

func TestNewKeys_StripsTrailingSlash(t *testing.T) {
	cases := map[string]string{
		"/x":         "/x",
		"/x/":        "/x",
		"/x///":      "/x",
		"/cex/test/": "/cex/test",
	}
	for in, want := range cases {
		if got := NewKeys(in).Root; got != want {
			t.Errorf("NewKeys(%q).Root = %q, want %q", in, got, want)
		}
	}
}

func TestKeys_Layout(t *testing.T) {
	k := NewKeys("/cex/counter")
	if got := k.CoordinatorLeader(); got != "/cex/counter/coordinator/leader" {
		t.Errorf("CoordinatorLeader = %q", got)
	}
	if got := k.NodesPrefix(); got != "/cex/counter/nodes/" {
		t.Errorf("NodesPrefix = %q", got)
	}
	if got := k.Node("node-A"); got != "/cex/counter/nodes/node-A" {
		t.Errorf("Node = %q", got)
	}
	if got := k.AssignmentsPrefix(); got != "/cex/counter/assignment/" {
		t.Errorf("AssignmentsPrefix = %q", got)
	}
	// Zero-padded so lexicographic order matches numeric order.
	if got := k.Assignment(0); got != "/cex/counter/assignment/vshard-000" {
		t.Errorf("Assignment(0) = %q", got)
	}
	if got := k.Assignment(7); got != "/cex/counter/assignment/vshard-007" {
		t.Errorf("Assignment(7) = %q", got)
	}
	if got := k.Assignment(255); got != "/cex/counter/assignment/vshard-255" {
		t.Errorf("Assignment(255) = %q", got)
	}
}
