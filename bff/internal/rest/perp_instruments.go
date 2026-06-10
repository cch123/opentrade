package rest

// perp_instruments.go serves the ADR-0075 public product metadata (Bybit
// instruments-info shape): contract spec + the ACTIVE SymbolConfig version's
// trading parameters. Display only — BFF may briefly show an older version
// during the propagation window; authoritative admission is always the
// perp-counter + Match version handshake.

import (
	"net/http"
	"time"

	"github.com/xargin/opentrade/pkg/perpcfg"
)

// SetPerpCatalog installs the catalog cache backing /v1/perp/instruments.
// Same setter pattern as SetPerp: nil keeps the routes responding 503.
func (s *Server) SetPerpCatalog(c *perpcfg.Cache) { s.perpCatalog = c }

type instrumentView struct {
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	ContractType  string `json:"contract_type"`
	BaseAsset     string `json:"base_asset"`
	QuoteAsset    string `json:"quote_asset"`
	SettleAsset   string `json:"settle_asset"`
	ContractSize  string `json:"contract_size"`
	PriceScale    int32  `json:"price_scale"`
	QtyScale      int32  `json:"qty_scale"`
	Alias         string `json:"alias,omitempty"`
	ConfigVersion uint64 `json:"config_version"`

	TickSize    string `json:"tick_size"`
	QtyStep     string `json:"qty_step"`
	MinOrderQty string `json:"min_order_qty"`
	MaxOrderQty string `json:"max_order_qty"`
	MinNotional string `json:"min_notional"`
	MinPrice    string `json:"min_price"`
	MaxPrice    string `json:"max_price"`

	FundingIntervalSeconds int64  `json:"funding_interval_seconds"`
	FundingCap             string `json:"funding_cap"`
	FundingFloor           string `json:"funding_floor"`
	MakerFeeRate           string `json:"maker_fee_rate"`
	TakerFeeRate           string `json:"taker_fee_rate"`

	RiskTiers []perpcfg.RiskTier `json:"risk_tiers"`
}

func instrumentFromView(v perpcfg.View) instrumentView {
	cfg := v.Cfg
	return instrumentView{
		Symbol:        v.Spec.Symbol,
		Status:        string(cfg.Status),
		ContractType:  string(v.Spec.ContractType),
		BaseAsset:     v.Spec.BaseAsset,
		QuoteAsset:    v.Spec.QuoteAsset,
		SettleAsset:   v.Spec.SettleAsset,
		ContractSize:  v.Spec.ContractSize.String(),
		PriceScale:    v.Spec.PriceScale,
		QtyScale:      v.Spec.QtyScale,
		Alias:         v.Spec.Alias,
		ConfigVersion: cfg.ConfigVersion,

		TickSize:    cfg.Precision.TickSize.String(),
		QtyStep:     cfg.Precision.QtyStep.String(),
		MinOrderQty: cfg.OrderLimits.MinOrderQty.String(),
		MaxOrderQty: cfg.OrderLimits.MaxOrderQty.String(),
		MinNotional: cfg.OrderLimits.MinNotional.String(),
		MinPrice:    cfg.OrderLimits.MinPrice.String(),
		MaxPrice:    cfg.OrderLimits.MaxPrice.String(),

		FundingIntervalSeconds: cfg.Funding.IntervalSeconds,
		FundingCap:             cfg.Funding.Cap.String(),
		FundingFloor:           cfg.Funding.Floor.String(),
		MakerFeeRate:           cfg.Fees.MakerFeeRate.String(),
		TakerFeeRate:           cfg.Fees.TakerFeeRate.String(),

		RiskTiers: cfg.RiskTiers,
	}
}

func (s *Server) handlePerpInstruments(w http.ResponseWriter, r *http.Request) {
	if s.perpCatalog == nil {
		writeError(w, http.StatusServiceUnavailable, "perp catalog not configured")
		return
	}
	nowMs := time.Now().UnixMilli()
	out := make([]instrumentView, 0)
	for _, sym := range s.perpCatalog.Symbols() {
		if v, ok := s.perpCatalog.ActiveAt(sym, nowMs); ok {
			out = append(out, instrumentFromView(v))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"instruments": out})
}

func (s *Server) handlePerpInstrument(w http.ResponseWriter, r *http.Request) {
	if s.perpCatalog == nil {
		writeError(w, http.StatusServiceUnavailable, "perp catalog not configured")
		return
	}
	symbol := r.PathValue("symbol")
	v, ok := s.perpCatalog.Active(symbol)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown or not yet effective symbol")
		return
	}
	writeJSON(w, http.StatusOK, instrumentFromView(v))
}
