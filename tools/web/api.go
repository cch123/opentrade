package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// api.go is the launcher's own control-plane HTTP surface, mounted under
// /api/. It is distinct from the BFF trading API (/v1/*, proxied elsewhere):
// these endpoints start/stop the stack, report status/logs, and run the dev
// faucet. The browser drives orchestration entirely through here.

type apiServer struct {
	stack  *Stack
	sup    *Supervisor
	faucet *faucet
}

func (a *apiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/stack/up", a.handleUp)
	mux.HandleFunc("POST /api/stack/down", a.handleDown)
	mux.HandleFunc("GET /api/stack/status", a.handleStatus)
	mux.HandleFunc("GET /api/stack/logs", a.handleLogs)
	mux.HandleFunc("POST /api/faucet", a.handleFaucet)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// handleUp starts orchestration. Body selects optional services:
//
//	{"asset": true, "trigger": true}
func (a *apiServer) handleUp(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Asset   bool `json:"asset"`
		Trigger bool `json:"trigger"`
	}
	// An empty body is valid (spot-only stack); ignore decode EOF.
	_ = json.NewDecoder(r.Body).Decode(&body)
	optIn := map[string]bool{"asset": body.Asset, "trigger": body.Trigger}
	if err := a.stack.Up(optIn); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, a.stack.status(r.Context()))
}

// handleDown stops services. Body {"deps": true} also stops docker compose.
// Runs in the background so a slow teardown doesn't block the response.
func (a *apiServer) handleDown(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Deps bool `json:"deps"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	go a.stack.Down(body.Deps)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stopping"})
}

func (a *apiServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.stack.status(r.Context()))
}

// handleLogs returns the recent log tail for one service. svc="docker" returns
// the compose output.
func (a *apiServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("svc")
	if svc == "" {
		writeErr(w, http.StatusBadRequest, "svc query param required")
		return
	}
	tail := 200
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			tail = n
		}
	}
	var (
		lines []string
		err   error
	)
	if svc == "docker" {
		lines = a.stack.depsLogTail(tail)
	} else {
		lines, err = a.sup.logs(svc, tail)
	}
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"svc": svc, "lines": lines})
}

// handleFaucet credits a dev balance. Body:
//
//	{"user":"alice","target":"spot","asset":"USDT","amount":"30000"}
func (a *apiServer) handleFaucet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		User   string `json:"user"`
		Target string `json:"target"`
		Asset  string `json:"asset"`
		Amount string `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if body.User == "" || body.Asset == "" || body.Amount == "" {
		writeErr(w, http.StatusBadRequest, "user, asset and amount are required")
		return
	}
	if body.Target == "" {
		body.Target = "spot"
	}
	transferID := fmt.Sprintf("faucet-%s-%s-%d", body.Target, body.User, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	avail, err := a.faucet.credit(ctx, body.Target, body.User, transferID, body.Asset, body.Amount)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "faucet failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user":            body.User,
		"target":          body.Target,
		"asset":           body.Asset,
		"available_after": avail,
		"transfer_id":     transferID,
	})
}
