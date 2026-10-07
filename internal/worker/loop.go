// Package worker contém os loops em segundo plano (outbox, referências pendentes).
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Loop executa fn repetidamente até Stop. fn devolve true quando ainda há
// trabalho imediato (o loop então não espera o intervalo).
//
// O encerramento é em duas fases: Stop impede novas iterações e aguarda a
// atual terminar; só se o prazo de ctx estourar o contexto de trabalho é
// cancelado, interrompendo a iteração em curso.
type Loop struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) bool
	log      *slog.Logger

	gateCtx  context.Context
	stopGate context.CancelFunc
	workCtx  context.Context
	stopWork context.CancelFunc

	once sync.Once
	done chan struct{}
}

func NewLoop(name string, interval time.Duration, log *slog.Logger, fn func(ctx context.Context) bool) *Loop {
	l := &Loop{name: name, interval: interval, fn: fn, log: log.With("worker", name), done: make(chan struct{})}
	l.gateCtx, l.stopGate = context.WithCancel(context.Background())
	l.workCtx, l.stopWork = context.WithCancel(context.Background())
	return l
}

func (l *Loop) Start() {
	l.once.Do(func() {
		go func() {
			defer close(l.done)
			l.log.Info("worker started")
			for l.gateCtx.Err() == nil {
				if l.fn(l.workCtx) {
					continue
				}
				select {
				case <-l.gateCtx.Done():
				case <-time.After(l.interval):
				}
			}
			l.log.Info("worker stopped")
		}()
	})
}

// Stop espera o término do loop até o prazo de ctx.
func (l *Loop) Stop(ctx context.Context) error {
	l.stopGate()
	select {
	case <-l.done:
		l.stopWork()
		return nil
	case <-ctx.Done():
		l.stopWork()
		select {
		case <-l.done:
		case <-time.After(3 * time.Second):
		}
		return fmt.Errorf("worker %s did not finish in time: %w", l.name, ctx.Err())
	}
}
