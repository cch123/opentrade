package ws

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/push/internal/hub"
)

// HeaderUserID mirrors bff/internal/auth.HeaderUserID for MVP parity — the
// upstream LB / BFF is the trust boundary (ADR-0022 / BFF auth middleware).
const HeaderUserID = "X-User-Id"

// Handler returns an http.HandlerFunc that upgrades incoming requests to
// WebSocket and spawns a Conn bound to the given Hub.
//
// The connection's ctx inherits from the server's base ctx, so
// Server.Shutdown can cascade-cancel live connections.
func Handler(base context.Context, h *hub.Hub, cfg Config, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get(HeaderUserID)

		wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			// MVP: skip origin checks; upstream LB terminates TLS and filters.
			InsecureSkipVerify: true,
		})
		if err != nil {
			logger.Warn("ws accept", zap.Error(err))
			return
		}

		id := newConnID()
		c := NewConn(id, userID, wsConn, h, cfg, logger.With(
			zap.String("conn", id),
			zap.String("user", userID),
		))
		logger.Debug("ws connected",
			zap.String("conn", id), zap.String("user", userID),
			zap.String("remote", r.RemoteAddr))

		// Run in the caller goroutine to keep the handler busy until the
		// connection closes — net/http will not reuse the hijacked conn.
		c.Start(base)

		logger.Debug("ws disconnected", zap.String("conn", id))
	}
}

func newConnID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
