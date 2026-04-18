// Command opentrade-tui is a terminal UI for exercising the OpenTrade order
// flow end-to-end against a running BFF + push stack. It uses the dev-mode
// X-User-Id auth (ADR-0039 header scheme) — no login required.
//
// Usage (from repo root):
//
//	./bin/opentrade-tui                              # defaults: BFF localhost:8080
//	./bin/opentrade-tui --bff http://host:8080
//	./bin/opentrade-tui --users alice,bob,carol      # first is active at start
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	var (
		bff   = flag.String("bff", "http://localhost:8080", "BFF base URL (REST + /ws reverse proxy)")
		wsArg = flag.String("ws", "", "WebSocket URL override (default: derive from --bff + /ws)")
		users = flag.String("users", "alice,bob", "comma-separated user IDs to rotate through (ctrl+u)")
	)
	flag.Parse()

	us := splitCSV(*users)
	if len(us) == 0 {
		fmt.Fprintln(os.Stderr, "--users must not be empty")
		os.Exit(2)
	}

	wsURL := *wsArg
	if wsURL == "" {
		derived, err := deriveWS(*bff)
		if err != nil {
			fmt.Fprintln(os.Stderr, "derive ws url:", err)
			os.Exit(2)
		}
		wsURL = derived
	}

	m := initialModel(*bff, wsURL, us)
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())
	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// deriveWS converts "http://host:port" → "ws://host:port/ws".
func deriveWS(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	u.Path = "/ws"
	u.RawQuery = ""
	return u.String(), nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
