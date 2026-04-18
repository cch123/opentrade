// Package ws hosts the BFF WebSocket reverse proxy.
//
// MVP-10 scope: accept an authenticated client WS connection, dial a single
// upstream push instance with X-User-Id stamped, and pump frames in both
// directions. Multi-instance sticky routing arrives with MVP-13 — this
// package already injects user_id on the upstream handshake so the eventual
// LB hash has a stable key.
package ws

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/bff/internal/auth"
)

// Config tunes the proxy.
type Config struct {
	// UpstreamURL points at a push instance's /ws endpoint
	// (e.g. "ws://push:8081/ws"). Schemes http/ws both accepted.
	UpstreamURL string
	// DialTimeout bounds the upstream handshake. Defaults to 5s.
	DialTimeout time.Duration
}

// Proxy is the BFF-side WS reverse-proxy handler.
type Proxy struct {
	upstream    *url.URL
	dialTimeout time.Duration
	logger      *zap.Logger
}

// New constructs a Proxy. Returns an error when UpstreamURL is malformed.
func New(cfg Config, logger *zap.Logger) (*Proxy, error) {
	if cfg.UpstreamURL == "" {
		return nil, errors.New("ws proxy: upstream url required")
	}
	u, err := url.Parse(cfg.UpstreamURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "ws", "wss":
		// keep as-is
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, errors.New("ws proxy: upstream scheme must be ws/wss/http/https")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	return &Proxy{upstream: u, dialTimeout: cfg.DialTimeout, logger: logger}, nil
}

// Handler returns the http.HandlerFunc that upgrades the client connection
// and pumps frames to the upstream.
func (p *Proxy) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, _ := auth.UserID(r.Context())
		// Anonymous connections are allowed for public streams; auth middleware
		// already stamps ctx when X-User-Id is present. Upstream push treats
		// empty user_id as anonymous too.

		upstreamConn, err := p.dialUpstream(r.Context(), userID)
		if err != nil {
			p.logger.Warn("ws upstream dial failed", zap.Error(err))
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		clientConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			// Same trust boundary as push — LB/BFF is the auth layer.
			InsecureSkipVerify: true,
		})
		if err != nil {
			_ = upstreamConn.Close(websocket.StatusInternalError, "client accept failed")
			p.logger.Warn("ws client accept failed", zap.Error(err))
			return
		}

		p.pump(r.Context(), clientConn, upstreamConn, userID)
	}
}

// dialUpstream opens a WS connection to push, injecting X-User-Id.
func (p *Proxy) dialUpstream(ctx context.Context, userID string) (*websocket.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, p.dialTimeout)
	defer cancel()

	header := http.Header{}
	if userID != "" {
		header.Set("X-User-Id", userID)
	}
	conn, _, err := websocket.Dial(dialCtx, p.upstream.String(), &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// pump runs two copy loops — client→upstream and upstream→client — and
// returns when either side closes. Closing either connection unblocks the
// other via coder/websocket's context propagation.
func (p *Proxy) pump(ctx context.Context, client, upstream *websocket.Conn, userID string) {
	pumpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		copyFrames(pumpCtx, client, upstream, p.logger, "client→upstream", userID)
		cancel()
	}()
	go func() {
		defer wg.Done()
		copyFrames(pumpCtx, upstream, client, p.logger, "upstream→client", userID)
		cancel()
	}()
	wg.Wait()

	_ = client.Close(websocket.StatusNormalClosure, "bye")
	_ = upstream.Close(websocket.StatusNormalClosure, "bye")
}

// copyFrames reads from src and writes to dst until either end returns an
// error or ctx is cancelled.
func copyFrames(ctx context.Context, src, dst *websocket.Conn, logger *zap.Logger, dir, userID string) {
	for {
		mt, data, err := src.Read(ctx)
		if err != nil {
			if ctx.Err() == nil && !isClosedError(err) {
				logger.Debug("ws proxy read ended",
					zap.String("dir", dir),
					zap.String("user", userID),
					zap.Error(err))
			}
			return
		}
		if err := dst.Write(ctx, mt, data); err != nil {
			if ctx.Err() == nil {
				logger.Debug("ws proxy write ended",
					zap.String("dir", dir),
					zap.String("user", userID),
					zap.Error(err))
			}
			return
		}
	}
}

// isClosedError reports whether err looks like a normal close (so we don't
// log it at warning levels).
func isClosedError(err error) bool {
	if err == nil {
		return true
	}
	// coder/websocket returns CloseError for server-initiated closes; any
	// mention of "closed" in the string covers client close + ctx cancelled.
	msg := err.Error()
	return strings.Contains(msg, "closed") || strings.Contains(msg, "EOF")
}
