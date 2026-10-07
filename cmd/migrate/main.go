// Comando migrate aplica ou reverte as migrations do banco.
//
//	migrate up             aplica todas as pendentes
//	migrate down [n]       reverte as últimas n (padrão 1; 0 = todas)
//	migrate status         lista as migrations e se estão aplicadas
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/leandropeloso/wager-service/internal/infra/migrate"
	"github.com/leandropeloso/wager-service/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: migrate up | down [n] | status")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	r, err := connectWithRetry(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close(context.WithoutCancel(ctx)) }()

	switch os.Args[1] {
	case "up":
		n, err := r.Up(ctx)
		fmt.Printf("applied %d migration(s)\n", n)
		return err
	case "down":
		steps := 1
		if len(os.Args) > 2 {
			if steps, err = strconv.Atoi(os.Args[2]); err != nil || steps < 0 {
				return fmt.Errorf("down expects a non-negative number of steps")
			}
		}
		n, err := r.Down(ctx, steps)
		fmt.Printf("reverted %d migration(s)\n", n)
		return err
	case "status":
		list, err := r.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range list {
			mark := "pending"
			if s.Applied {
				mark = "applied"
			}
			fmt.Printf("%04d_%s\t%s\n", s.Version, s.Name, mark)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", os.Args[1])
}

// connectWithRetry espera o PostgreSQL aceitar conexões (útil logo após o compose subir).
func connectWithRetry(ctx context.Context, url string) (*migrate.Runner, error) {
	for {
		r, err := migrate.New(ctx, url, migrations.FS)
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("database not reachable: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(2 * time.Second):
		}
	}
}
