// Comando server inicia o serviço (API HTTP, consumidor SQS, relay da outbox e
// worker de referências pendentes, conforme RUN_*), com shutdown gracioso em
// SIGTERM/SIGINT.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/fx"

	"github.com/leandropeloso/wager-service/internal/config"
	"github.com/leandropeloso/wager-service/internal/platform"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
}

func run() error {
	// O tratamento de sinais vale desde o início: um SIGTERM durante a
	// inicialização cancela o Start, e o Fx desfaz em ordem o que já subiu.
	sig, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var cfg *config.Config
	app := platform.New(fx.Populate(&cfg))
	if err := app.Err(); err != nil {
		return err
	}

	startCtx, cancelStart := context.WithTimeout(sig, 90*time.Second)
	defer cancelStart()
	if err := app.Start(startCtx); err != nil {
		if sig.Err() != nil && errors.Is(err, context.Canceled) {
			return nil // interrompido durante a inicialização; recursos já liberados pelo Fx
		}
		return fmt.Errorf("start: %w", err)
	}

	select {
	case <-sig.Done():
	case <-app.Done():
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	return nil
}
