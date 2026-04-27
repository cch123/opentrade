// Package connectx contains shared helpers for serving and dialing
// Connect-Go RPCs over cleartext HTTP/2 (h2c). All OpenTrade services
// speak the gRPC wire protocol over h2c — encryption / mTLS land later
// with broader credentials work — so every server and client funnels
// through these helpers to keep transport setup consistent.
package connectx

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// NewH2CServer wraps handler in an h2c-capable http.Server bound to addr.
// Caller drives lifecycle via Server.ListenAndServe / Server.Shutdown.
func NewH2CServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h2c.NewHandler(handler, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// NewH2CClient returns an *http.Client wired to speak HTTP/2 over plain
// TCP. The TLS config passed by net/http is intentionally discarded —
// Connect-Go uses the http://host:port URL form to signal cleartext.
func NewH2CClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}

// BaseURL turns a "host:port" endpoint into the http://host:port form
// Connect clients want. Empty input returns "" so callers can treat
// unconfigured endpoints as a no-op.
func BaseURL(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	return "http://" + endpoint
}
