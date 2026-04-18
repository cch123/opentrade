package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/push/internal/hub"
	"github.com/xargin/opentrade/push/internal/ws"
)

// End-to-end: start an HTTP server with the ws handler, dial a real WebSocket
// client, send a subscribe, push a broadcast, verify it's delivered.
func TestE2E_SubscribeAndBroadcast(t *testing.T) {
	logger := zap.NewNop()
	h := hub.New(logger)

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{SendBuffer: 16, WriteTimeout: 2 * time.Second}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	header := http.Header{}
	header.Set(ws.HeaderUserID, "alice")
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "done")

	// Subscribe.
	sub, _ := json.Marshal(ws.ClientMsg{Op: ws.OpSubscribe, Streams: []string{"trade@X"}})
	if err := c.Write(ctx, websocket.MessageText, sub); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Read the ack.
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	var ack ws.ControlMsg
	if err := json.Unmarshal(data, &ack); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	if ack.Op != ws.OpAck || len(ack.Streams) != 1 || ack.Streams[0] != "trade@X" {
		t.Fatalf("ack: %+v", ack)
	}

	// Wait until the server side has registered the subscription.
	if err := waitFor(ctx, func() bool { return h.StreamSubscribers("trade@X") == 1 }); err != nil {
		t.Fatalf("waiting for subscriber: %v", err)
	}

	// Broadcast.
	frame, _ := ws.EncodeData("trade@X", []byte(`{"p":1}`))
	sent, dropped := h.BroadcastStream("trade@X", frame)
	if sent != 1 || dropped != 0 {
		t.Fatalf("broadcast: sent=%d dropped=%d", sent, dropped)
	}

	_, data, err = c.Read(ctx)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	var dm ws.DataMsg
	if err := json.Unmarshal(data, &dm); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if dm.Stream != "trade@X" || string(dm.Data) != `{"p":1}` {
		t.Errorf("data frame: %+v", dm)
	}
}

func TestE2E_UserStreamAutoSubscribe(t *testing.T) {
	logger := zap.NewNop()
	h := hub.New(logger)

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{SendBuffer: 16, WriteTimeout: 2 * time.Second}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	header := http.Header{}
	header.Set(ws.HeaderUserID, "bob")
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "done")

	// Wait for connection + auto subscribe.
	if err := waitFor(ctx, func() bool { return h.StreamSubscribers(ws.StreamUser) == 1 }); err != nil {
		t.Fatalf("waiting auto-sub: %v", err)
	}

	frame, _ := ws.EncodeData(ws.StreamUser, []byte(`{"priv":true}`))
	sent, _ := h.SendUser("bob", frame)
	if sent != 1 {
		t.Fatalf("SendUser sent=%d", sent)
	}

	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var dm ws.DataMsg
	_ = json.Unmarshal(data, &dm)
	if dm.Stream != ws.StreamUser {
		t.Errorf("stream: %q", dm.Stream)
	}
}

func TestE2E_UnknownOpReturnsError(t *testing.T) {
	logger := zap.NewNop()
	h := hub.New(logger)
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(baseCtx, h, ws.Config{}, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "done")

	payload, _ := json.Marshal(ws.ClientMsg{Op: "nope"})
	if err := c.Write(ctx, websocket.MessageText, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var cm ws.ControlMsg
	_ = json.Unmarshal(data, &cm)
	if cm.Op != ws.OpError {
		t.Errorf("expected error op, got %+v", cm)
	}
}

// waitFor polls until cond returns true or ctx fires. 10ms cadence.
func waitFor(ctx context.Context, cond func() bool) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
