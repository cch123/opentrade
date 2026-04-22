// Package metrics centralises Counter's ADR-0060 M8 + ADR-0062 M8
// Prometheus series. All counter-service-specific observables
// (pendingList depth, TECheckpoint publish success/fail, Publish
// retry attempts, evictor throughput, snapshot duration, catch-up
// applied count) share one factory and one registry so main.go can
// expose /metrics with a single handler.
//
// Series:
//
//   counter_pendinglist_size{vshard}      — gauge, updated by the
//                                           async handler + advancer
//                                           on every Enqueue/Pop.
//   counter_checkpoint_publish_total{vshard, result}
//                                         — counter, one inc per
//                                           advancer publish (result
//                                           = "ok" / "err").
//   counter_publish_retry_total{op, attempt_range}
//                                         — counter, one inc per
//                                           retry attempt inside
//                                           TxnProducer.runTxnWithRetry
//                                           (op = Publish /
//                                           PublishOrderPlacement /
//                                           PublishToVShard;
//                                           attempt_range = 1 /
//                                           "2-5" / "6+" so the
//                                           histogram stays compact).
//   counter_evict_processed_total{vshard, result}
//                                         — counter, one inc per
//                                           evicted order (result =
//                                           "ok" / "err").
//   counter_snapshot_duration_seconds{vshard}
//                                         — histogram, observed by
//                                           writeSnapshot around the
//                                           Flush + Capture + Save
//                                           critical section.
//   counter_catchup_events_applied_total{vshard}
//                                         — counter, one inc per
//                                           journal record replayed
//                                           during recovery.
//   counter_seq_high_watermark{vshard}    — gauge, mirrors the
//                                           sequencer's counterSeq;
//                                           refreshed per snapshot.
//
// Callers pass *Counter around; nil is a valid zero value that
// makes every Record* method a cheap no-op. Tests and legacy paths
// that predate M8 pass nil; only main.go (and future ops tooling)
// construct a real Counter.
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Counter bundles the Prometheus series emitted by ADR-0060 /
// ADR-0062 observability. Nil receivers skip all work.
type Counter struct {
	PendingListSize         *prometheus.GaugeVec
	CheckpointPublishTotal  *prometheus.CounterVec
	PublishRetryTotal       *prometheus.CounterVec
	EvictProcessedTotal     *prometheus.CounterVec
	EvictHaltedTotal        *prometheus.CounterVec
	SnapshotDurationSec     *prometheus.HistogramVec
	CatchUpEventsApplied    *prometheus.CounterVec
	CounterSeqHighWatermark *prometheus.GaugeVec
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

		EvictProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_evict_processed_total",
			Help:        "Terminal orders processed by the evictor (ADR-0062 M8).",
			ConstLabels: labels,
		}, []string{"vshard", "result"}),

		EvictHaltedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_evict_halted_total",
			Help:        "Evict rounds skipped by the health-check circuit breaker (ADR-0062 §5). reason=snapshot_stale | checkpoint_stale.",
			ConstLabels: labels,
		}, []string{"vshard", "reason"}),

		SnapshotDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "counter_snapshot_duration_seconds",
			Help:        "writeSnapshot critical-section duration (Flush + Capture + Save) — bounds M7 stop-the-world window.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBucketsRange(0.001, 5, 10),
		}, []string{"vshard"}),

		CatchUpEventsApplied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "counter_catchup_events_applied_total",
			Help:        "Journal records re-applied during catch-up recovery (ADR-0060 M6).",
			ConstLabels: labels,
		}, []string{"vshard"}),

		CounterSeqHighWatermark: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "counter_seq_high_watermark",
			Help:        "UserSequencer.CounterSeq() sampled on each snapshot — monotonic across restarts (ADR-0018).",
			ConstLabels: labels,
		}, []string{"vshard"}),
	}

	reg.MustRegister(c.PendingListSize)
	reg.MustRegister(c.CheckpointPublishTotal)
	reg.MustRegister(c.PublishRetryTotal)
	reg.MustRegister(c.EvictProcessedTotal)
	reg.MustRegister(c.EvictHaltedTotal)
	reg.MustRegister(c.SnapshotDurationSec)
	reg.MustRegister(c.CatchUpEventsApplied)
	reg.MustRegister(c.CounterSeqHighWatermark)
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

// RecordEvictProcessed bumps the evictor outcome counter.
// Nil receiver is a no-op.
func (c *Counter) RecordEvictProcessed(vshard int32, ok bool) {
	if c == nil || c.EvictProcessedTotal == nil {
		return
	}
	result := "ok"
	if !ok {
		result = "err"
	}
	c.EvictProcessedTotal.WithLabelValues(vshardLabel(vshard), result).Inc()
}

// RecordEvictHalted bumps the circuit-breaker counter with the
// specific reason label. Nil receiver is a no-op.
func (c *Counter) RecordEvictHalted(vshard int32, reason string) {
	if c == nil || c.EvictHaltedTotal == nil {
		return
	}
	c.EvictHaltedTotal.WithLabelValues(vshardLabel(vshard), reason).Inc()
}

// RecordSnapshotDuration observes the writeSnapshot critical-section
// duration in seconds. Nil receiver is a no-op.
func (c *Counter) RecordSnapshotDuration(vshard int32, seconds float64) {
	if c == nil || c.SnapshotDurationSec == nil {
		return
	}
	c.SnapshotDurationSec.WithLabelValues(vshardLabel(vshard)).Observe(seconds)
}

// RecordCatchUpApplied bumps the catch-up replay counter by n events.
// Nil receiver is a no-op.
func (c *Counter) RecordCatchUpApplied(vshard int32, n int) {
	if c == nil || c.CatchUpEventsApplied == nil || n <= 0 {
		return
	}
	c.CatchUpEventsApplied.WithLabelValues(vshardLabel(vshard)).Add(float64(n))
}

// RecordCounterSeq updates the sequencer high-watermark gauge.
// Nil receiver is a no-op.
func (c *Counter) RecordCounterSeq(vshard int32, seq uint64) {
	if c == nil || c.CounterSeqHighWatermark == nil {
		return
	}
	c.CounterSeqHighWatermark.WithLabelValues(vshardLabel(vshard)).Set(float64(seq))
}

func vshardLabel(v int32) string {
	return strconv.FormatInt(int64(v), 10)
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
