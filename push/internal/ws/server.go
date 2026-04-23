package ws

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/shard"
	"github.com/xargin/opentrade/push/internal/hub"
)

// HeaderUserID mirrors bff/internal/auth.HeaderUserID for MVP parity — the
// upstream LB / BFF is the trust boundary (ADR-0022 / BFF auth middleware).
const HeaderUserID = "X-User-Id"

// HeaderCorrectInstance is set on a 403 response so a misrouted client can
// resolve where to reconnect (ADR-0033).
const HeaderCorrectInstance = "X-Correct-Instance"

// HeaderTrustedAuth is stamped by BFF/LB when forwarding authenticated users
// to Push. Configure TrustedHeaderSecret to require this header whenever
// X-User-Id is present.
const HeaderTrustedAuth = "X-OpenTrade-Internal-Auth"

// Handler returns an http.HandlerFunc that upgrades incoming requests to
// WebSocket and spawns a Conn bound to the given Hub.
//
// When cfg.TotalInstances > 1 the handler enforces sticky routing: an
// authenticated user whose shard.Index(userID, TotalInstances) is not this
// instance gets 403 + X-Correct-Instance. Anonymous connections (no
// X-User-Id) are always accepted because they can only subscribe to public
// streams — no private delivery depends on sticky routing for them.
//
// The connection's ctx inherits from the server's base ctx, so
// Server.Shutdown can cascade-cancel live connections.
func Handler(base context.Context, h *hub.Hub, cfg Config, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get(HeaderUserID)
		if userID != "" && cfg.TrustedHeaderSecret != "" && !trustedHeaderOK(r, cfg.TrustedHeaderSecret) {
			http.Error(w, "invalid trusted header", http.StatusUnauthorized)
			logger.Warn("ws trusted header rejected", zap.String("remote", r.RemoteAddr))
			return
		}

		if cfg.TotalInstances > 1 && userID != "" {
			if owner := shard.Index(userID, cfg.TotalInstances); owner != cfg.InstanceOrdinal {
				w.Header().Set(HeaderCorrectInstance, strconv.Itoa(owner))
				http.Error(w, "wrong push instance", http.StatusForbidden)
				logger.Debug("ws sticky reject",
					zap.String("user", userID),
					zap.Int("owner", owner),
					zap.Int("self", cfg.InstanceOrdinal))
				return
			}
		}

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

func trustedHeaderOK(r *http.Request, secret string) bool {
	raw := strings.TrimSpace(r.Header.Get(HeaderTrustedAuth))
	if raw == "" {
		return false
	}
	token := raw
	if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
		token = strings.TrimSpace(raw[len("Bearer "):])
	}
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(secret)) == 1
}

func newConnID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
