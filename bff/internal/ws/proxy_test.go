package ws_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/bff/internal/auth"
	bffws "github.com/xargin/opentrade/bff/internal/ws"
)

// fakeUpstream is a minimal push stand-in. It records the X-User-Id it
// received and echoes every client frame back with a "server:" prefix so
// tests can verify both directions without depending on the real push hub.
type fakeUpstream struct {
	receivedUserID string
	recvMu         sync.Mutex
	recv           [][]byte
	ready          chan struct{}
}

func (u *fakeUpstream) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u.receivedUserID = r.Header.Get("X-User-Id")
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("upstream accept: %v", err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		close(u.ready)

		for {
			mt, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			u.recvMu.Lock()
			u.recv = append(u.recv, append([]byte(nil), data...))
			u.recvMu.Unlock()
			if err := c.Write(r.Context(), mt, append([]byte("server:"), data...)); err != nil {
				return
			}
		}
	}
}

func (u *fakeUpstream) frames() [][]byte {
	u.recvMu.Lock()
	defer u.recvMu.Unlock()
	cp := make([][]byte, len(u.recv))
	copy(cp, u.recv)
	return cp
}

func startUpstream(t *testing.T) (*fakeUpstream, *httptest.Server) {
	t.Helper()
	u := &fakeUpstream{ready: make(chan struct{})}
	srv := httptest.NewServer(u.handler(t))
	t.Cleanup(srv.Close)
	return u, srv
}

func startProxy(t *testing.T, upstreamURL string) *httptest.Server {
	t.Helper()
	proxy, err := bffws.New(bffws.Config{UpstreamURL: upstreamURL}, zap.NewNop())
	if err != nil {
		t.Fatalf("proxy new: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", auth.Middleware(proxy.Handler()))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/ws"
}

func TestProxy_ForwardsBothDirectionsAndInjectsUserID(t *testing.T) {
	upstream, upstreamSrv := startUpstream(t)
	proxySrv := startProxy(t, upstreamSrv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	header := http.Header{}
	header.Set("X-User-Id", "alice")
	client, _, err := websocket.Dial(ctx, wsURL(proxySrv.URL), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "done")

	if err := client.Write(ctx, websocket.MessageText, []byte("hello")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	_, got, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(got) != "server:hello" {
		t.Errorf("unexpected echo: %q", string(got))
	}

	// Give the upstream handler a moment to record the frame before asserting.
	<-upstream.ready
	if upstream.receivedUserID != "alice" {
		t.Errorf("upstream user id: %q", upstream.receivedUserID)
	}
	if frames := upstream.frames(); len(frames) != 1 || string(frames[0]) != "hello" {
		t.Errorf("upstream frames: %q", frames)
	}
}

func TestProxy_BadGatewayOnUpstreamDown(t *testing.T) {
	// Point the proxy at a URL that's not listening.
	proxySrv := startProxy(t, "ws://127.0.0.1:1/ws")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(proxySrv.URL), nil)
	if err == nil {
		t.Fatal("expected dial to fail")
	}
	if resp == nil {
		t.Fatalf("expected response, got nil err=%v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		b, _ := io.ReadAll(resp.Body)
		t.Errorf("status=%d body=%q", resp.StatusCode, string(b))
	}
}

func TestProxy_RejectsBadUpstreamScheme(t *testing.T) {
	if _, err := bffws.New(bffws.Config{UpstreamURL: "ftp://nope"}, zap.NewNop()); err == nil {
		t.Error("expected scheme rejection")
	}
}
