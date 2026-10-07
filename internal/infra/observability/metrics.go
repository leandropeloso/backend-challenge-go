// Package observability reúne logs estruturados e métricas Prometheus.
package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

// Metrics implementa port.Metrics sobre um registry próprio (sem estado global).
type Metrics struct {
	Registry *prometheus.Registry

	results      *prometheus.CounterVec
	duplicates   *prometheus.CounterVec
	retries      *prometheus.CounterVec
	dlq          *prometheus.CounterVec
	conflicts    prometheus.Counter
	divergences  prometheus.Counter
	duration     *prometheus.HistogramVec
	outboxLag    prometheus.Gauge
	outboxQueued prometheus.Gauge
	published    prometheus.Counter
	publishFails prometheus.Counter
}

var _ port.Metrics = (*Metrics)(nil)

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Operações concluídas ou em espera, por tipo, status e código de falha.",
		}, []string{"kind", "status", "failure_code"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_duplicates_total", Help: "Recebimentos repetidos respondidos pelo resultado persistido.",
		}, []string{"source"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_retries_total", Help: "Novas tentativas por componente.",
		}, []string{"component"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_dlq_total", Help: "Mensagens enviadas à DLQ, por motivo.",
		}, []string{"reason"}),
		conflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_concurrency_conflicts_total", Help: "Transações repetidas por deadlock ou falha de serialização.",
		}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_reconciliation_divergences_total", Help: "Reconciliações que encontraram saldo divergente do ledger.",
		}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_duration_seconds", Help: "Latência de processamento por origem.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"source"}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wager_outbox_lag_seconds", Help: "Idade do evento não publicado mais antigo.",
		}),
		outboxQueued: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wager_outbox_pending", Help: "Eventos aguardando publicação.",
		}),
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_outbox_published_total", Help: "Eventos publicados com sucesso.",
		}),
		publishFails: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_outbox_publish_failures_total", Help: "Falhas de publicação (o evento será tentado de novo).",
		}),
	}
	m.Registry.MustRegister(m.results, m.duplicates, m.retries, m.dlq, m.conflicts, m.divergences,
		m.duration, m.outboxLag, m.outboxQueued, m.published, m.publishFails)
	return m
}

func (m *Metrics) TransactionResult(kind wager.Kind, status wager.Status, code wager.FailureCode) {
	m.results.WithLabelValues(string(kind), string(status), string(code)).Inc()
}

func (m *Metrics) Duplicate(source string)   { m.duplicates.WithLabelValues(source).Inc() }
func (m *Metrics) Retry(component string)    { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) DLQ(reason string)         { m.dlq.WithLabelValues(reason).Inc() }
func (m *Metrics) ConcurrencyConflict()      { m.conflicts.Inc() }
func (m *Metrics) ReconciliationDivergence() { m.divergences.Inc() }

func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.duration.WithLabelValues(source).Observe(d.Seconds())
}

func (m *Metrics) OutboxStats(pending int64, oldest time.Duration) {
	m.outboxQueued.Set(float64(pending))
	m.outboxLag.Set(oldest.Seconds())
}

func (m *Metrics) OutboxPublished()     { m.published.Inc() }
func (m *Metrics) OutboxPublishFailed() { m.publishFails.Inc() }

// SystemClock é o relógio de produção, sempre em UTC.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
