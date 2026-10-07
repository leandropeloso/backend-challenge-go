package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/config"
	"github.com/leandropeloso/wager-service/internal/fault"
)

// maxBatch é o limite de mensagens por chamada de publicação em lote.
const maxBatch = 10

// Publisher entrega mensagens da outbox ao broker. PublishBatch devolve um
// resultado por mensagem (nil = aceita) e nunca recebe duas mensagens da mesma
// carteira no mesmo lote.
type Publisher interface {
	PublishBatch(ctx context.Context, msgs []port.OutboxMessage) []error
}

// OutboxMetrics recebe os números do relay (fila pendente, publicações e falhas).
type OutboxMetrics interface {
	OutboxStats(pending int64, oldest time.Duration)
	OutboxPublished()
	OutboxPublishFailed()
}

// OutboxRelay publica os eventos pendentes da outbox. Vários relays (em várias
// instâncias) podem rodar ao mesmo tempo: cada evento é reservado por lease e,
// se o dono morrer, o lease expira e outro relay assume o evento.
type OutboxRelay struct {
	store   port.OutboxStore
	pub     Publisher
	metrics OutboxMetrics
	cfg     config.Outbox
	owner   string
	log     *slog.Logger

	statsAt time.Time
}

// NewOutboxRelay cria um relay; owner identifica esta instância nos leases.
func NewOutboxRelay(store port.OutboxStore, pub Publisher, metrics OutboxMetrics, cfg config.Outbox, owner string, log *slog.Logger) *OutboxRelay {
	return &OutboxRelay{store: store, pub: pub, metrics: metrics, cfg: cfg, owner: owner, log: log.With("worker", "outbox", "owner", owner)}
}

// RunOnce reserva e publica um lote. Devolve true se havia eventos (há chance de ter mais).
func (o *OutboxRelay) RunOnce(ctx context.Context) bool {
	if time.Since(o.statsAt) >= time.Second { // gauges: uma leitura por segundo basta
		o.statsAt = time.Now()
		if stats, err := o.store.Stats(ctx); err == nil {
			o.metrics.OutboxStats(stats.Pending, stats.OldestAge)
		}
	}
	msgs, err := o.store.Claim(ctx, o.owner, o.cfg.BatchSize, o.cfg.Lease)
	if err != nil {
		if ctx.Err() == nil {
			o.log.Warn("outbox claim failed", "error", err)
		}
		return false
	}
	if len(msgs) == 0 {
		return false
	}

	// Agrupa por carteira, preservando a ordem de reserva.
	var order []string
	groups := map[string][]port.OutboxMessage{}
	for _, m := range msgs {
		if _, ok := groups[m.PartitionKey]; !ok {
			order = append(order, m.PartitionKey)
		}
		groups[m.PartitionKey] = append(groups[m.PartitionKey], m)
	}

	// Rodada r = o r-ésimo evento de cada carteira. Um lote nunca repete carteira,
	// e a rodada seguinte só começa quando a anterior terminou, então a ordem por
	// carteira é estrita mesmo com falhas parciais.
	var (
		mu     sync.Mutex
		failed = map[string]bool{}
	)
	for round := 0; ; round++ {
		var batch []port.OutboxMessage
		for _, key := range order {
			g := groups[key]
			if round >= len(g) {
				continue
			}
			mu.Lock()
			skip := failed[key]
			mu.Unlock()
			if skip { // um evento anterior da carteira falhou: este espera
				o.release(g[round], "", time.Now())
				continue
			}
			batch = append(batch, g[round])
		}
		if len(batch) == 0 {
			if round >= longest(groups) {
				break
			}
			continue
		}
		o.publishRound(ctx, batch, failed, &mu)
	}
	return true
}

func longest(groups map[string][]port.OutboxMessage) int {
	n := 0
	for _, g := range groups {
		n = max(n, len(g))
	}
	return n
}

// publishRound envia os lotes da rodada em paralelo (até cfg.Concurrency).
func (o *OutboxRelay) publishRound(ctx context.Context, batch []port.OutboxMessage, failed map[string]bool, mu *sync.Mutex) {
	workers := max(o.cfg.Concurrency, 1)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for start := 0; start < len(batch); start += maxBatch {
		chunk := batch[start:min(start+maxBatch, len(batch))]
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			o.publishChunk(ctx, chunk, failed, mu)
		}()
	}
	wg.Wait()
}

func (o *OutboxRelay) publishChunk(ctx context.Context, chunk []port.OutboxMessage, failed map[string]bool, mu *sync.Mutex) {
	if ctx.Err() != nil {
		for _, m := range chunk {
			o.release(m, "shutdown", time.Now())
		}
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, o.cfg.PublishTimeout)
	errs := o.pub.PublishBatch(pubCtx, chunk)
	cancel()

	var done []uuid.UUID
	for i, m := range chunk {
		if i < len(errs) && errs[i] != nil {
			o.metrics.OutboxPublishFailed()
			mu.Lock()
			failed[m.PartitionKey] = true
			mu.Unlock()
			o.log.Warn("event publish failed", "eventId", m.ID, "eventType", m.EventType, "attempts", m.Attempts, "error", errs[i])
			o.release(m, errs[i].Error(), time.Now().Add(o.backoff(m.Attempts)))
			continue
		}
		// Ponto de falha dos testes: publicado no broker, ainda não confirmado na outbox.
		fault.Hit("outbox.after_publish")
		done = append(done, m.ID)
	}
	if len(done) == 0 {
		return
	}
	if err := o.markPublished(done); err != nil {
		o.log.Warn("could not confirm publication; the events will be republished with the same eventIds", "count", len(done), "error", err)
		return
	}
	for range done {
		o.metrics.OutboxPublished()
	}
}

func (o *OutboxRelay) markPublished(ids []uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return o.store.MarkPublishedMany(ctx, ids, o.owner)
}

func (o *OutboxRelay) release(m port.OutboxMessage, cause string, retryAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.store.Release(ctx, m.ID, o.owner, cause, retryAt); err != nil {
		o.log.Warn("could not release outbox event; the lease will expire", "eventId", m.ID, "error", err)
	}
}

// backoff: base * 2^(tentativa-1), limitado a BackoffMax. Eventos nunca são descartados.
func (o *OutboxRelay) backoff(attempts int) time.Duration {
	d := o.cfg.BackoffBase
	for i := 1; i < attempts && d < o.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > o.cfg.BackoffMax {
		d = o.cfg.BackoffMax
	}
	return d
}
