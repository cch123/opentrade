// Package metrics centralises asset-service's Prometheus metrics
// (ADR-0057 M6). All Saga-specific series live here so the driver /
// orchestrator / reconciler share one factory and one registry.
//
// Exposed series:
//
//   saga_state_count{state=}             — gauge, refreshed periodically
//                                          by reconciler; how many sagas
//                                          currently sit in each state.
//   saga_transitions_total{from,to}      — counter, one inc per
//                                          Driver.transition that wins
//                                          the optimistic-lock update.
//   saga_compensate_attempts_total       — counter, one inc per
//                                          CompensateTransferOut RPC
//                                          call (including retries).
//   saga_stuck_total                     — counter, one inc per saga
//                                          that terminates in
//                                          COMPENSATE_STUCK.
//   saga_terminal_duration_seconds       — histogram, observed in the
//                                          Orchestrator when a saga
//                                          reaches a terminal state,
//                                          labelled by final state.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Saga is the handle every saga component uses to emit telemetry. Fields
// are exported so tests can inspect counter values via the prometheus
// testutil helpers.
type Saga struct {
	StateCount          *prometheus.GaugeVec
	TransitionsTotal    *prometheus.CounterVec
	CompensateAttempts  prometheus.Counter
	StuckTotal          prometheus.Counter
	TerminalDurationSec *prometheus.HistogramVec
}

// NewSaga constructs and registers all saga series on reg. Pass nil to
// use prometheus.DefaultRegisterer — tests typically pass a fresh
// prometheus.NewRegistry so series don't leak across test runs.
func NewSaga(reg prometheus.Registerer) *Saga {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	const svc = "asset"

	stateCount := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "saga_state_count",
		Help:        "Number of sagas currently in each transferledger state. Refreshed by the reconciler loop.",
		ConstLabels: prometheus.Labels{"service": svc},
	}, []string{"state"})
	reg.MustRegister(stateCount)

	transitions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "saga_transitions_total",
		Help:        "Total saga state transitions by (from, to) pair.",
		ConstLabels: prometheus.Labels{"service": svc},
	}, []string{"from", "to"})
	reg.MustRegister(transitions)

	compensate := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "saga_compensate_attempts_total",
		Help:        "Total CompensateTransferOut RPC attempts (including retries).",
		ConstLabels: prometheus.Labels{"service": svc},
	})
	reg.MustRegister(compensate)

	stuck := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "saga_stuck_total",
		Help:        "Total sagas that terminated in COMPENSATE_STUCK state and require operator action.",
		ConstLabels: prometheus.Labels{"service": svc},
	})
	reg.MustRegister(stuck)

	// Buckets: 10ms .. 60s. Most happy-path sagas complete in <200ms
	// (two RPCs + ledger writes); compensation can stretch to tens of
	// seconds under exponential backoff; STUCK sagas are capped by the
	// configured retry budget but we want the tail visible.
	terminalDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "saga_terminal_duration_seconds",
		Help:        "Time from saga INIT to terminal state, by final state.",
		Buckets:     prometheus.ExponentialBucketsRange(0.01, 60, 14),
		ConstLabels: prometheus.Labels{"service": svc},
	}, []string{"state"})
	reg.MustRegister(terminalDur)

	return &Saga{
		StateCount:          stateCount,
		TransitionsTotal:    transitions,
		CompensateAttempts:  compensate,
		StuckTotal:          stuck,
		TerminalDurationSec: terminalDur,
	}
}
