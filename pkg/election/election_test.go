package election

import (
	"testing"
)

// Real etcd leader election is integration-only (requires an etcd server).
// These unit tests cover config validation and construction paths that do
// not hit the network.

func TestNew_RequiresPath(t *testing.T) {
	if _, err := New(Config{Value: "v"}); err == nil {
		t.Error("expected error without Path")
	}
}

func TestNew_RequiresValue(t *testing.T) {
	if _, err := New(Config{Path: "/p"}); err == nil {
		t.Error("expected error without Value")
	}
}

func TestNew_RequiresClientOrEndpoints(t *testing.T) {
	if _, err := New(Config{Path: "/p", Value: "v"}); err == nil {
		t.Error("expected error when neither Client nor Endpoints set")
	}
}

// Real dial / campaign / resign / observe paths require a running etcd;
// they are exercised by manual smoke test (deploy/docker/docker-compose.yml)
// and by match/counter HA integration when MVP-12 is up.
