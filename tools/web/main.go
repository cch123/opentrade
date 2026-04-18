// Command opentrade-web is a zero-build browser UI for exercising the
// OpenTrade order flow end-to-end against a running BFF + push stack.
//
// It hosts a single embedded HTML page and reverse-proxies /v1/* and /ws to
// BFF. The WS proxy converts a `?user_id=...` query parameter into an
// `X-User-Id` header when dialing BFF — browsers can't set custom headers on
// the WebSocket handshake, and the dev-mode auth (ADR-0039 header scheme)
// needs that header.
//
// Usage (from repo root):
//
//	./bin/opentrade-web                                  # serves on :7070, BFF at :8080
//	./bin/opentrade-web --addr :9000 --bff http://host:8080
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

//go:embed index.html
var indexHTML []byte

func main() {
	var (
		addr = flag.String("addr", ":7070", "dev server listen address")
		bff  = flag.String("bff", "http://localhost:8080", "BFF base URL")
	)
	flag.Parse()

	bffURL, err := url.Parse(*bff)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid --bff:", err)
		os.Exit(2)
	}

	mux := http.NewServeMux()
	// Go 1.22 ServeMux: bare "/" is a catch-all prefix, so method-qualified
	// "GET /" conflicts with unqualified "/v1/". Register "/" without a
	// method — more-specific prefixes ("/v1/", "/ws", "/healthz") still win.
	mux.HandleFunc("/", serveIndex)
	mux.Handle("/v1/", restProxy(bffURL))
	mux.HandleFunc("/ws", wsProxy(bffURL))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("opentrade-web listening on %s → proxying to %s", *addr, bffURL)
		log.Printf("open http://localhost%s/ in your browser", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// serveIndex returns the embedded single-page app.
func serveIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

// restProxy reverse-proxies /v1/* to BFF. Browser fetches already carry the
// X-User-Id header set by app.js; httputil.ReverseProxy preserves headers.
func restProxy(target *url.URL) http.Handler {
	p := httputil.NewSingleHostReverseProxy(target)
	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		r.Host = target.Host
	}
	return p
}

// wsProxy accepts a browser WS connection, dials BFF /ws with X-User-Id set
// from the `user_id` query param, and pumps frames both ways.
func wsProxy(target *url.URL) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user_id")

		upstream := *target
		switch upstream.Scheme {
		case "http":
			upstream.Scheme = "ws"
		case "https":
			upstream.Scheme = "wss"
		}
		upstream.Path = "/ws"
		upstream.RawQuery = ""

		header := http.Header{}
		if user != "" {
			header.Set("X-User-Id", user)
		}

		dialCtx, dialCancel := context.WithTimeout(r.Context(), 5*time.Second)
		upConn, _, err := websocket.Dial(dialCtx, upstream.String(), &websocket.DialOptions{
			HTTPHeader: header,
		})
		dialCancel()
		if err != nil {
			http.Error(w, "upstream dial: "+err.Error(), http.StatusBadGateway)
			return
		}

		browserConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // same-origin dev tool
		})
		if err != nil {
			_ = upConn.Close(websocket.StatusInternalError, "accept failed")
			return
		}

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); pump(ctx, browserConn, upConn); cancel() }()
		go func() { defer wg.Done(); pump(ctx, upConn, browserConn); cancel() }()
		wg.Wait()

		_ = browserConn.Close(websocket.StatusNormalClosure, "bye")
		_ = upConn.Close(websocket.StatusNormalClosure, "bye")
	}
}

func pump(ctx context.Context, src, dst *websocket.Conn) {
	for {
		mt, data, err := src.Read(ctx)
		if err != nil {
			return
		}
		if err := dst.Write(ctx, mt, data); err != nil {
			return
		}
	}
}

