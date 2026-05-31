package server

import (
	"encoding/json"
	"net/http"

	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perprisk"
)

// RegisterRiskHandlers exposes the narrow internal RPC surface ADR-0071 needs
// from a perp-counter shard. It deliberately reuses the service's sequencer
// entrypoints instead of offering generic position mutation APIs; the
// coordinator can only ask for candidates or submit a version-stamped ADL task.
func RegisterRiskHandlers(mux *http.ServeMux, svc *service.Service) {
	mux.HandleFunc(perprisk.RiskCandidatesPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req perprisk.CandidateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		price, err := dec.Parse(req.AdlPrice)
		if err != nil || req.Symbol == "" || price.Sign() <= 0 {
			http.Error(w, "symbol and positive adl_price required", http.StatusBadRequest)
			return
		}
		candidates := svc.ADLCandidates(req.Symbol, price, req.ExcludeUser)
		resp := perprisk.CandidateResponse{Candidates: make([]perprisk.ADLCandidateWire, 0, len(candidates))}
		for _, c := range candidates {
			resp.Candidates = append(resp.Candidates, perprisk.CandidateToWire(c))
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc(perprisk.RiskTaskPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var wire perprisk.ADLTaskWire
		if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		task, err := perprisk.TaskFromWire(wire)
		if err != nil || task.LotID == "" || task.UserID == 0 || task.Symbol == "" ||
			task.Qty.Sign() <= 0 || task.Price.Sign() <= 0 || task.AdlRound == 0 {
			http.Error(w, "valid task with lot_id, user_id, symbol, qty, price, and adl_round required", http.StatusBadRequest)
			return
		}
		result := svc.ExecuteAdlTask(task)
		writeJSON(w, perprisk.ADLTaskResponse{
			Applied: result.Applied, FactQty: result.FactQty.String(), RealizedPnL: result.RealizedPnL.String(),
		})
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
