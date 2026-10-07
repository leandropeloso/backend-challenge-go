package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/config"
)

var quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))

type fakeStore struct {
	mu        sync.Mutex
	batch     []port.OutboxMessage
	published []uuid.UUID
	released  map[uuid.UUID]time.Time
	causes    map[uuid.UUID]string
}

func newFakeStore(msgs ...port.OutboxMessage) *fakeStore {
	return &fakeStore{batch: msgs, released: map[uuid.UUID]time.Time{}, causes: map[uuid.UUID]string{}}
}

func (f *fakeStore) Claim(context.Context, string, int, time.Duration) ([]port.OutboxMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.batch
	f.batch = nil
	return out, nil
}

func (f *fakeStore) MarkPublishedMany(_ context.Context, ids []uuid.UUID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, ids...)
	return nil
}

func (f *fakeStore) Release(_ context.Context, id uuid.UUID, _ string, cause string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released[id] = at
	f.causes[id] = cause
	return nil
}

func (f *fakeStore) Stats(context.Context) (port.OutboxStats, error) { return port.OutboxStats{}, nil }

type fakePublisher struct {
	mu      sync.Mutex
	fail    map[uuid.UUID]bool
	sent    []uuid.UUID
	batches [][]port.OutboxMessage
}

func (p *fakePublisher) PublishBatch(_ context.Context, msgs []port.OutboxMessage) []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches = append(p.batches, append([]port.OutboxMessage(nil), msgs...))
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		if p.fail[m.ID] {
			errs[i] = errors.New("broker unavailable")
			continue
		}
		p.sent = append(p.sent, m.ID)
	}
	return errs
}

type fakeMetrics struct {
	mu                sync.Mutex
	published, failed int
}

func (m *fakeMetrics) OutboxStats(int64, time.Duration) {}
func (m *fakeMetrics) OutboxPublished()                 { m.mu.Lock(); m.published++; m.mu.Unlock() }
func (m *fakeMetrics) OutboxPublishFailed()             { m.mu.Lock(); m.failed++; m.mu.Unlock() }

func relay(store port.OutboxStore, pub Publisher, m OutboxMetrics) *OutboxRelay {
	return NewOutboxRelay(store, pub, m, config.Outbox{
		BatchSize: 10, Concurrency: 4, PublishTimeout: time.Second, BackoffBase: time.Second, BackoffMax: 8 * time.Second,
	}, "test-owner", quiet)
}

func msg(partition string, attempts int) port.OutboxMessage {
	return port.OutboxMessage{ID: uuid.New(), PartitionKey: partition, EventType: "X", Attempts: attempts}
}

func TestRelayConfirmsOnlyAfterPublishing(t *testing.T) {
	a, b := msg("w1", 1), msg("w2", 1)
	store, pub, m := newFakeStore(a, b), &fakePublisher{}, &fakeMetrics{}
	if !relay(store, pub, m).RunOnce(context.Background()) {
		t.Fatal("havia eventos")
	}
	if len(pub.sent) != 2 || len(store.published) != 2 || m.published != 2 {
		t.Fatalf("publicados=%d confirmados=%d métrica=%d", len(pub.sent), len(store.published), m.published)
	}
	if len(store.released) != 0 {
		t.Fatal("nada deveria ser liberado")
	}
}

func TestRelayFailureKeepsOrderWithinAPartition(t *testing.T) {
	first, second, other := msg("w1", 3), msg("w1", 1), msg("w2", 1)
	store := newFakeStore(first, second, other)
	pub := &fakePublisher{fail: map[uuid.UUID]bool{first.ID: true}}
	m := &fakeMetrics{}

	before := time.Now()
	relay(store, pub, m).RunOnce(context.Background())

	if len(pub.sent) != 1 || pub.sent[0] != other.ID {
		t.Fatalf("só a outra carteira pode ser publicada: %v", pub.sent)
	}
	for _, id := range pub.sent {
		if id == second.ID {
			t.Fatal("o evento seguinte da mesma carteira não pode furar a ordem")
		}
	}
	if _, ok := store.released[first.ID]; !ok || store.causes[first.ID] == "" {
		t.Fatal("o evento que falhou deve ser liberado com a causa")
	}
	// tentativa 3 → backoff de 4s (1s, 2s, 4s)
	if d := store.released[first.ID].Sub(before); d < 3500*time.Millisecond || d > 5*time.Second {
		t.Fatalf("backoff = %s, quer ~4s", d)
	}
	// o seguinte é devolvido sem erro e sem espera
	if at, ok := store.released[second.ID]; !ok || at.Sub(before) > time.Second || store.causes[second.ID] != "" {
		t.Fatalf("o evento bloqueado deve voltar imediatamente: %v %q", at, store.causes[second.ID])
	}
	if m.failed != 1 || m.published != 1 {
		t.Fatalf("métricas: falhas=%d publicados=%d", m.failed, m.published)
	}
	if len(store.published) != 1 {
		t.Fatal("só o evento publicado pode ser confirmado")
	}
}

func TestRelayBackoffIsCapped(t *testing.T) {
	r := relay(newFakeStore(), &fakePublisher{}, &fakeMetrics{})
	want := map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 9: 8 * time.Second, 500: 8 * time.Second}
	for attempts, w := range want {
		if got := r.backoff(attempts); got != w {
			t.Errorf("backoff(%d) = %s, quer %s", attempts, got, w)
		}
	}
}

func TestRelayReleasesEverythingOnShutdown(t *testing.T) {
	a, b := msg("w1", 1), msg("w2", 1)
	store, pub := newFakeStore(a, b), &fakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	relay(store, pub, &fakeMetrics{}).RunOnce(ctx)
	if len(pub.sent) != 0 {
		t.Fatal("nada deve ser publicado com o contexto cancelado")
	}
	if len(store.released) != 2 {
		t.Fatalf("liberados = %d, quer 2", len(store.released))
	}
}

func TestLoopStopWaitsForTheRunningIteration(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var iterations int
	var mu sync.Mutex
	loop := NewLoop("t", time.Millisecond, quiet, func(ctx context.Context) bool {
		mu.Lock()
		iterations++
		first := iterations == 1
		mu.Unlock()
		if first {
			close(started)
			<-release // trabalho em andamento
		}
		return false
	})
	loop.Start()
	<-started

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- loop.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		t.Fatalf("Stop não deveria terminar com trabalho em andamento (%v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if iterations != 1 {
		t.Fatalf("nenhuma iteração nova após o Stop (iterações=%d)", iterations)
	}
}

func TestLoopStopCancelsWorkWhenDeadlineExpires(t *testing.T) {
	cancelled := make(chan struct{})
	started := make(chan struct{})
	loop := NewLoop("t", time.Millisecond, quiet, func(ctx context.Context) bool {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return false
	})
	loop.Start()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := loop.Stop(ctx); err == nil {
		t.Fatal("o prazo estourou: Stop deveria sinalizar erro")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("o contexto do trabalho deveria ter sido cancelado")
	}
}

type countingResumer struct {
	left int
	err  error
}

func (c *countingResumer) ResumeOne(context.Context) (bool, error) {
	if c.err != nil {
		return false, c.err
	}
	if c.left == 0 {
		return false, nil
	}
	c.left--
	return true, nil
}

func TestPendingRunnerReportsRemainingWork(t *testing.T) {
	r := NewPendingRunner(&countingResumer{left: 2}, quiet)
	for i, want := range []bool{true, true, false} {
		if got := r.RunOnce(context.Background()); got != want {
			t.Fatalf("passo %d: %v, quer %v", i, got, want)
		}
	}
	if NewPendingRunner(&countingResumer{err: errors.New("db down")}, quiet).RunOnce(context.Background()) {
		t.Fatal("em erro o loop deve esperar o intervalo")
	}
}

// Um lote nunca repete carteira e respeita o limite do broker; a ordem de cada
// carteira é mantida entre as rodadas.
func TestRelayBatchesNeverRepeatAPartitionAndRespectOrder(t *testing.T) {
	var msgs []port.OutboxMessage
	var perWallet = map[string][]uuid.UUID{}
	for round := 0; round < 3; round++ {
		for w := 0; w < 25; w++ {
			key := "wallet-" + string(rune('a'+w))
			m := msg(key, 1)
			msgs = append(msgs, m)
			perWallet[key] = append(perWallet[key], m.ID)
		}
	}
	store, pub := newFakeStore(msgs...), &fakePublisher{}
	relay(store, pub, &fakeMetrics{}).RunOnce(context.Background())

	if len(pub.sent) != 75 || len(store.published) != 75 {
		t.Fatalf("publicados=%d confirmados=%d", len(pub.sent), len(store.published))
	}
	position := map[uuid.UUID]int{}
	for i, id := range pub.sent {
		position[id] = i
	}
	for _, b := range pub.batches {
		if len(b) > maxBatch {
			t.Fatalf("lote de %d excede o limite %d", len(b), maxBatch)
		}
		seen := map[string]bool{}
		for _, m := range b {
			if seen[m.PartitionKey] {
				t.Fatalf("o lote repete a carteira %s", m.PartitionKey)
			}
			seen[m.PartitionKey] = true
		}
	}
	for key, ids := range perWallet {
		for i := 1; i < len(ids); i++ {
			if position[ids[i]] < position[ids[i-1]] {
				t.Fatalf("carteira %s publicada fora de ordem", key)
			}
		}
	}
}
