// Package metrics centralises Counter's ADR-0060 M8 + ADR-0062 M8
// Prometheus series. All counter-service-specific observables
// (pendingList depth, TECheckpoint publish success/fail, Publish
// retry attempts, evictor throughput, snapshot duration, catch-up
// applied count) share one factory and one registry so main.go can
// expose /metrics with a single handler.
//
// Series:
//
//	counter_pendinglist_size{vshard}      — gauge, updated by the
//	                                        async handler + advancer
//	                                        on every Enqueue/Pop.
//	counter_checkpoint_publish_total{vshard, result}
//	                                      — counter, one inc per
//	                                        advancer publish (result
//	                                        = "ok" / "err").
//	counter_publish_retry_total{op, attempt_range}
//	                                      — counter, one inc per
//	                                        retry attempt inside
//	                                        TxnProducer.runTxnWithRetry
//	                                        (op = Publish /
//	                                        PublishOrderPlacement /
//	                                        PublishToVShard;
//	                                        attempt_range = 1 /
//	                                        "2-5" / "6+" so the
//	                                        histogram stays compact).
//	counter_catchup_events_applied_total{vshard}
//	                                      — counter, one inc per
//	                                        journal record replayed
//	                                        during recovery.
//
// ADR-0064 M2d on-demand startup:
//
//	counter_startup_duration_seconds{vshard, mode, result}
//	                                      — histogram, observed by
//	                                        worker.Run at close(ready).
//	                                        mode = on-demand | fallback;
//	                                        result = success | error.
//	                                        _count rate → boot rate;
//	                                        _bucket → p99 boot latency;
//	                                        combined filter on mode +
//	                                        result answers "is
//	                                        on-demand actually faster
//	                                        than fallback?"
//	counter_ondemand_rpc_duration_seconds{vshard, result}
//	                                      — histogram, wraps just the
//	                                        TakeSnapshot gRPC call in
//	                                        loadOnDemand. result maps
//	                                        to one of: ok | timeout |
//	                                        unavailable |
//	                                        failed_precondition |
//	                                        resource_exhausted |
//	                                        canceled | unimplemented |
//	                                        internal | other. Splits
//	                                        network vs trade-dump
//	                                        server-side issues.
//	counter_sentinel_produce_duration_seconds{vshard, result}
//	                                      — histogram, wraps just the
//	                                        TxnProducer.ProduceFenceSentinel
//	                                        call. result = ok | error.
//	                                        Isolates the Kafka
//	                                        InitProducerID / fencing
//	                                        path from the rest of
//	                                        startup.
//
// Callers pass *Counter around; nil is a valid zero value that
// makes every Record* method a cheap no-op. Tests and legacy paths
// that predate M8 pass nil; only main.go (and future ops tooling)
// construct a real Counter.
package metrics

import (
	"context"
	"errors"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Counter bundles the Prometheus series emitted by ADR-0060 and
// ADR-0064 observability. Nil receivers skip all work.
type Counter struct {
	PendingListSize        *prometheus.GaugeVec
	CheckpointPublishTotal *prometheus.CounterVec
	PublishRetryTotal      *prometheus.CounterVec
	CatchUpEventsApplied   *prometheus.CounterVec

	// ADR-0064 M2d startup observability.
	StartupDurationSec         *prometheus.HistogramVec // labels: vshard, mode, result
	OnDemandRPCDurationSec     *prometheus.HistogramVec // labels: vshard, result
	SentinelProduceDurationSec *prometheus.HistogramVec // labels: vshard, result
}

// NewCounter constructs and registers all counter-service series on
// reg. Pass nil to use prometheus.DefaultRegisterer.
func NewCounter(reg prometheus.Registerer) *Counter {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	const svc = "counter"
	labels := prometheus.Labels{"service": svc}

	c := &Counter{
		PendingListSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "counter_pendinglist_size",
			Help:        "Per-vshard pendingList depth (ADR-0060 M8).",
			ConstLabels: labels,
		}, []string{"vshard"}),

		CheckpointPublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_checkpoint_publish_total",
			Help:        "TECheckpointEvent publish outcomes emitted by the advancer (ADR-0060).",
			ConstLabels: labels,
		}, []string{"vshard", "result"}),

		PublishRetryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_publish_retry_total",
			Help:        "Retry attempts inside TxnProducer.runTxnWithRetry, bucketed by op + attempt range (ADR-0060 §3).",
			ConstLabels: labels,
		}, []string{"op", "attempt_range"}),

		CatchUpEventsApplied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_catchup_events_applied_total",
			Help:        "Journal records re-applied during catch-up recovery (ADR-0060 M6).",
			ConstLabels: labels,
		}, []string{"vshard"}),

		// ADR-0064 M2d startup observability. Buckets span 50ms..60s
		// exponentially — on-demand hot path is ~1-3s, legacy
		// catchUpJournal can run 10-30s on a large restore, and the
		// ADR-0058 TransactionTimeout backstop is 10s, so 60s upper
		// leaves headroom for pathological Kafka abort+rebalance
		// worst-case.
		StartupDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "counter_startup_duration_seconds",
			Help:        "Wall-clock from worker.Run start to close(ready). Split by mode (on-demand | fallback) and result (success | error) — ADR-0064 §实施约束.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBucketsRange(0.05, 60, 14),
		}, []string{"vshard", "mode", "result"}),

		// TakeSnapshot RPC is the single most load-bearing step on
		// the on-demand hot path; separate histogram so operators
		// can isolate trade-dump / network issues from the sentinel
		// produce or the blob download.
		OnDemandRPCDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "counter_ondemand_rpc_duration_seconds",
			Help:        "TakeSnapshot RPC duration. result = ok | timeout | unavailable | failed_precondition | resource_exhausted | canceled | unimplemented | internal | other — ADR-0064 §4.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBucketsRange(0.005, 10, 12),
		}, []string{"vshard", "result"}),

		// Sentinel produce is the other independent hot-path step
		// (fence + LEO stabilisation). Short tail when healthy
		// (50-200ms), long tail when Kafka is degraded — separate
		// series makes the distinction observable without parsing
		// logs.
		SentinelProduceDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "counter_sentinel_produce_duration_seconds",
			Help:        "TxnProducer.ProduceFenceSentinel duration. result = ok | error — ADR-0064 §3 Phase 1 step ③.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBucketsRange(0.005, 10, 12),
		}, []string{"vshard", "result"}),
	}

	reg.MustRegister(c.PendingListSize)
	reg.MustRegister(c.CheckpointPublishTotal)
	reg.MustRegister(c.PublishRetryTotal)
	reg.MustRegister(c.CatchUpEventsApplied)
	reg.MustRegister(c.StartupDurationSec)
	reg.MustRegister(c.OnDemandRPCDurationSec)
	reg.MustRegister(c.SentinelProduceDurationSec)
	return c
}

// RecordPendingSize sets the per-vshard pendingList depth gauge.
// Nil receiver is a no-op.
func (c *Counter) RecordPendingSize(vshard int32, n int) {
	if c == nil || c.PendingListSize == nil {
		return
	}
	c.PendingListSize.WithLabelValues(vshardLabel(vshard)).Set(float64(n))
}

// RecordCheckpointPublish bumps the checkpoint publish outcome
// counter. Nil receiver is a no-op.
func (c *Counter) RecordCheckpointPublish(vshard int32, ok bool) {
	if c == nil || c.CheckpointPublishTotal == nil {
		return
	}
	result := "ok"
	if !ok {
		result = "err"
	}
	c.CheckpointPublishTotal.WithLabelValues(vshardLabel(vshard), result).Inc()
}

// RecordPublishRetry bumps the retry-attempt counter. attempt is the
// 1-indexed attempt number (1 means "first try", 2+ means retried).
// Nil receiver is a no-op.
func (c *Counter) RecordPublishRetry(op string, attempt int) {
	if c == nil || c.PublishRetryTotal == nil {
		return
	}
	c.PublishRetryTotal.WithLabelValues(op, attemptRange(attempt)).Inc()
}

// RecordCatchUpApplied bumps the catch-up replay counter by n events.
// Nil receiver is a no-op.
func (c *Counter) RecordCatchUpApplied(vshard int32, n int) {
	if c == nil || c.CatchUpEventsApplied == nil || n <= 0 {
		return
	}
	c.CatchUpEventsApplied.WithLabelValues(vshardLabel(vshard)).Add(float64(n))
}

// StartupMode is the wire-string form of worker.StartupMode used in
// metric labels. Duplicated here to avoid a metrics → worker import
// cycle; tests enforce the two stay in sync.
const (
	StartupModeLabelOnDemand = "on-demand"
	StartupModeLabelFallback = "fallback"
)

// RecordStartupDuration observes the wall-clock from worker.Run
// entry to close(ready). mode is "on-demand" or "fallback" —
// "fallback" covers both the ErrFallback-after-on-demand path and
// the configured-legacy path (they produce identical metrics since
// both hit ADR-0060 §4.2 load + catchUpJournal). success
// discriminates the final Run return: true for a ready worker,
// false for a bail-out before close(ready).
// Nil receiver is a no-op.
func (c *Counter) RecordStartupDuration(vshard int32, mode string, seconds float64, success bool) {
	if c == nil || c.StartupDurationSec == nil {
		return
	}
	result := "success"
	if !success {
		result = "error"
	}
	c.StartupDurationSec.WithLabelValues(vshardLabel(vshard), mode, result).Observe(seconds)
}

// RecordOnDemandRPCDuration observes the TakeSnapshot RPC
// round-trip from the Counter side. result is the concrete
// outcome label — use OnDemandRPCResultLabel(err) to derive it
// from an error so the taxonomy stays consistent across call
// sites. Nil receiver is a no-op.
func (c *Counter) RecordOnDemandRPCDuration(vshard int32, result string, seconds float64) {
	if c == nil || c.OnDemandRPCDurationSec == nil {
		return
	}
	c.OnDemandRPCDurationSec.WithLabelValues(vshardLabel(vshard), result).Observe(seconds)
}

// RecordSentinelProduceDuration observes one sentinel produce call
// (success + failure both recorded so dashboards show the failure
// rate against total attempts). Nil receiver is a no-op.
func (c *Counter) RecordSentinelProduceDuration(vshard int32, success bool, seconds float64) {
	if c == nil || c.SentinelProduceDurationSec == nil {
		return
	}
	result := "ok"
	if !success {
		result = "error"
	}
	c.SentinelProduceDurationSec.WithLabelValues(vshardLabel(vshard), result).Observe(seconds)
}

func vshardLabel(v int32) string {
	return strconv.FormatInt(int64(v), 10)
}

// OnDemandRPCResultLabel maps a TakeSnapshot RPC error to a
// stable metric label. Pass nil for a successful call. The label
// taxonomy matches ADR-0064 §4 fallback-decision codes so
// operators can filter on the same names they see in the ADR and
// in worker logs. Exported so other callers (future health
// probes, integration tests) get the same classification.
func OnDemandRPCResultLabel(err error) string {
	if err == nil {
		return "ok"
	}
	// Raw ctx errors can leak past gRPC wrapping on Dial failures;
	// treat them first so "timeout" doesn't collapse into "other".
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return "timeout"
	case codes.Canceled:
		return "canceled"
	case codes.Unavailable:
		return "unavailable"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.ResourceExhausted:
		return "resource_exhausted"
	case codes.Unimplemented:
		return "unimplemented"
	case codes.Internal:
		return "internal"
	}
	return "other"
}

func attemptRange(n int) string {
	switch {
	case n <= 1:
		return "1"
	case n <= 5:
		return "2-5"
	default:
		return "6+"
	}
}
