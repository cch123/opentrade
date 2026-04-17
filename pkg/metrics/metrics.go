// Package metrics wraps Prometheus client_golang with framework-level defaults
// shared by all OpenTrade services.
//
// Every service automatically exposes:
//   - cex_rpc_requests_total{service,method,code}
//   - cex_rpc_duration_seconds{service,method}
//   - cex_kafka_produce_total{topic,result}
//   - cex_kafka_consume_lag{topic,partition}
//   - cex_kafka_txn_abort_total{service}
//   - Go runtime metrics (via collectors.NewGoCollector)
//
// Services add their own metrics on top via the shared Registry.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Framework returns the shared framework metrics, registered on the given
// Registry. It is safe to call once per service at startup.
type Framework struct {
	RPCRequestsTotal  *prometheus.CounterVec
	RPCDurationSec    *prometheus.HistogramVec
	KafkaProduceTotal *prometheus.CounterVec
	KafkaConsumeLag   *prometheus.GaugeVec
	KafkaTxnAbortTotal *prometheus.CounterVec
}

// NewFramework constructs and registers the framework metrics.
func NewFramework(service string, reg prometheus.Registerer) *Framework {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto{reg: reg}

	return &Framework{
		RPCRequestsTotal: factory.counterVec(prometheus.CounterOpts{
			Name: "cex_rpc_requests_total",
			Help: "Total number of RPC requests.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"method", "code"}),

		RPCDurationSec: factory.histogramVec(prometheus.HistogramOpts{
			Name:    "cex_rpc_duration_seconds",
			Help:    "RPC request handling duration.",
			Buckets: prometheus.ExponentialBucketsRange(0.0005, 2, 14),
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"method"}),

		KafkaProduceTotal: factory.counterVec(prometheus.CounterOpts{
			Name: "cex_kafka_produce_total",
			Help: "Kafka produce attempts by result (ok, err).",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"topic", "result"}),

		KafkaConsumeLag: factory.gaugeVec(prometheus.GaugeOpts{
			Name: "cex_kafka_consume_lag",
			Help: "Kafka consumer lag by topic and partition.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"topic", "partition"}),

		KafkaTxnAbortTotal: factory.counterVec(prometheus.CounterOpts{
			Name: "cex_kafka_txn_abort_total",
			Help: "Kafka transaction aborts.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{}),
	}
}

// NewRegistry returns a fresh registry with Go + process collectors attached.
// Callers typically use one registry per service.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Handler returns an http.Handler exposing metrics for the given registry.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// --- tiny promauto-lite to avoid pulling promauto for a few helpers ---------

type promauto struct {
	reg prometheus.Registerer
	mu  sync.Mutex
}

func (p *promauto) counterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	p.reg.MustRegister(c)
	return c
}

func (p *promauto) histogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	p.reg.MustRegister(h)
	return h
}

func (p *promauto) gaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(opts, labels)
	p.reg.MustRegister(g)
	return g
}
