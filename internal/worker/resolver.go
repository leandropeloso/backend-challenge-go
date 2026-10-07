package worker

import (
	"context"
	"log/slog"
)

// PendingResumer retoma uma transação pendente vencida; false quando não há nenhuma.
type PendingResumer interface {
	ResumeOne(ctx context.Context) (bool, error)
}

// PendingRunner é o passo do worker de referências pendentes: processa tudo
// que já venceu e só então espera o próximo intervalo.
type PendingRunner struct {
	svc PendingResumer
	log *slog.Logger
}

func NewPendingRunner(svc PendingResumer, log *slog.Logger) *PendingRunner {
	return &PendingRunner{svc: svc, log: log.With("worker", "pending-resolver")}
}

// RunOnce devolve true enquanto houver trabalho vencido.
func (p *PendingRunner) RunOnce(ctx context.Context) bool {
	found, err := p.svc.ResumeOne(ctx)
	if err != nil {
		if ctx.Err() == nil {
			p.log.Warn("could not resume pending transaction", "error", err)
		}
		return false
	}
	return found
}
