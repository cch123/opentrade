package rest

import (
	"net/http"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/auth"
)

type transferBody struct {
	TransferID string `json:"transfer_id"`
	Asset      string `json:"asset"`
	Amount     string `json:"amount"`
	Type       string `json:"type"` // "deposit" / "withdraw" / "freeze" / "unfreeze"
	BizRefID   string `json:"biz_ref_id,omitempty"`
	Memo       string `json:"memo,omitempty"`
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var body transferBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tt, err := parseTransferType(body.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.counter.Transfer(r.Context(), &counterrpc.TransferRequest{
		UserId:     userID,
		TransferId: body.TransferID,
		Asset:      body.Asset,
		Amount:     body.Amount,
		Type:       tt,
		BizRefId:   body.BizRefID,
		Memo:       body.Memo,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"transfer_id":     resp.TransferId,
		"status":          transferStatusToString(resp.Status),
		"reject_reason":   resp.RejectReason,
		"available_after": resp.AvailableAfter,
		"frozen_after":    resp.FrozenAfter,
	})
}

func (s *Server) handleQueryBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	asset := r.URL.Query().Get("asset")
	resp, err := s.counter.QueryBalance(r.Context(), &counterrpc.QueryBalanceRequest{
		UserId: userID,
		Asset:  asset,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	balances := make([]map[string]string, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		balances = append(balances, map[string]string{
			"asset":     b.Asset,
			"available": b.Available,
			"frozen":    b.Frozen,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"balances": balances,
	})
}

// ---------------------------------------------------------------------------
// parsing helpers
// ---------------------------------------------------------------------------

func parseTransferType(t string) (counterrpc.TransferType, error) {
	switch t {
	case "deposit", "DEPOSIT":
		return counterrpc.TransferType_TRANSFER_TYPE_DEPOSIT, nil
	case "withdraw", "WITHDRAW":
		return counterrpc.TransferType_TRANSFER_TYPE_WITHDRAW, nil
	case "freeze", "FREEZE":
		return counterrpc.TransferType_TRANSFER_TYPE_FREEZE, nil
	case "unfreeze", "UNFREEZE":
		return counterrpc.TransferType_TRANSFER_TYPE_UNFREEZE, nil
	}
	return counterrpc.TransferType_TRANSFER_TYPE_UNSPECIFIED, badRequest("type", t)
}

func transferStatusToString(s counterrpc.TransferStatus) string {
	switch s {
	case counterrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED:
		return "confirmed"
	case counterrpc.TransferStatus_TRANSFER_STATUS_REJECTED:
		return "rejected"
	case counterrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED:
		return "duplicated"
	}
	return "unspecified"
}
