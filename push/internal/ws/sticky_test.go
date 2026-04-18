package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/shard"
	"github.com/xargin/opentrade/push/internal/hub"
	"github.com/xargin/opentrade/push/internal/ws"
)

// Handler should reject a user whose hash belongs to another instance with
// HTTP 403 and an X-Correct-Instance header pointing at the owner.
func TestSticky_RejectsMisroutedUser(t *testing.T) {
	const total = 10
	user := "alice"
	owner := shard.Index(user, total)
	nonOwner := (owner + 1) % total

	logger := zap.NewNop()
	h := hub.New(logger)
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{
		SendBuffer:      4,
		WriteTimeout:    time.Second,
		InstanceOrdinal: nonOwner,
		TotalInstances:  total,
	}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	header := http.Header{}
	header.Set(ws.HeaderUserID, user)
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err == nil {
		t.Fatal("expected dial to fail")
	}
	if resp == nil {
		t.Fatalf("expected response body; err=%v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status=%d want 403", resp.StatusCode)
	}
	got := resp.Header.Get(ws.HeaderCorrectInstance)
	if got == "" {
		t.Fatal("missing X-Correct-Instance header")
	}
	if gotOwner, _ := strconv.Atoi(got); gotOwner != owner {
		t.Errorf("X-Correct-Instance=%s want %d", got, owner)
	}
}

// Owner accepts the connection; basic subscribe still works.
func TestSticky_OwnerAccepts(t *testing.T) {
	const total = 10
	user := "alice"
	owner := shard.Index(user, total)

	logger := zap.NewNop()
	h := hub.New(logger)
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{
		SendBuffer:      4,
		WriteTimeout:    time.Second,
		InstanceOrdinal: owner,
		TotalInstances:  total,
	}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	header := http.Header{}
	header.Set(ws.HeaderUserID, user)
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("owner should accept: %v", err)
	}
	_ = c.Close(websocket.StatusNormalClosure, "done")
}

// Anonymous connections (no X-User-Id) bypass the sticky check entirely —
// they can only subscribe to public streams so there's no private-delivery
// invariant to protect.
func TestSticky_AnonymousBypassesCheck(t *testing.T) {
	logger := zap.NewNop()
	h := hub.New(logger)
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{
		SendBuffer:      4,
		WriteTimeout:    time.Second,
		InstanceOrdinal: 0,
		TotalInstances:  10,
	}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("anonymous should pass: %v", err)
	}
	_ = c.Close(websocket.StatusNormalClosure, "done")
}

// Single-instance mode disables the sticky check.
func TestSticky_SingleInstanceNoFilter(t *testing.T) {
	logger := zap.NewNop()
	h := hub.New(logger)
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{
		SendBuffer:      4,
		WriteTimeout:    time.Second,
		InstanceOrdinal: 0,
		TotalInstances:  1,
	}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	header := http.Header{}
	header.Set(ws.HeaderUserID, "any-user-any-shard")
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("single-instance mode should accept everyone: %v", err)
	}
	_ = c.Close(websocket.StatusNormalClosure, "done")
}
