// Package platform compõe a aplicação com Uber Fx: configuração, conexões,
// repositórios, casos de uso, API HTTP e workers, todos com ciclo de vida
// gerenciado por fx.Lifecycle.
package platform

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"

	"github.com/leandropeloso/wager-service/internal/app/openwallet"
	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/app/query"
	"github.com/leandropeloso/wager-service/internal/config"
	"github.com/leandropeloso/wager-service/internal/infra/auth"
	"github.com/leandropeloso/wager-service/internal/infra/httpapi"
	"github.com/leandropeloso/wager-service/internal/infra/ids"
	"github.com/leandropeloso/wager-service/internal/infra/observability"
	"github.com/leandropeloso/wager-service/internal/infra/postgres"
	"github.com/leandropeloso/wager-service/internal/infra/sqsx"
	"github.com/leandropeloso/wager-service/internal/telemetry"
	"github.com/leandropeloso/wager-service/internal/worker"
)

// New devolve a aplicação completa. opts permite acrescentar ou substituir
// peças (os testes usam fx.Populate e fx.Replace).
func New(opts ...fx.Option) *fx.App {
	return fx.New(Options(), fx.Options(opts...))
}

// Options reúne todos os módulos da aplicação.
func Options() fx.Option {
	return fx.Options(
		fx.NopLogger,
		ConfigModule,
		ObservabilityModule,
		PostgresModule,
		AuthModule,
		SQSModule,
		AppModule,
		WorkersModule,
		HTTPModule,
	)
}

// ConfigModule fornece a configuração validada.
var ConfigModule = fx.Module("config", fx.Provide(config.Load))

// newTelemetry configura o rastreamento e garante o flush dos spans ao parar. O
// Invoke abaixo o constrói primeiro, então seu OnStop roda por último.
func newTelemetry(lc fx.Lifecycle, cfg *config.Config) (*telemetry.Provider, error) {
	p, err := telemetry.Setup(context.Background(), telemetry.Config{
		Endpoint: cfg.Telemetry.Endpoint, ServiceName: cfg.Telemetry.ServiceName,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: p.Shutdown})
	return p, nil
}

var ObservabilityModule = fx.Module("observability",
	fx.Invoke(func(*telemetry.Provider) {}),
	fx.Provide(
		newTelemetry,
		observability.NewLogger,
		observability.NewMetrics,
		func(m *observability.Metrics) port.Metrics { return m },
		func(m *observability.Metrics) worker.OutboxMetrics { return m },
		func() port.Clock { return observability.SystemClock{} },
		func() port.IDGenerator { return ids.Generator{} },
	),
)

var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		postgres.NewStore,
		postgres.NewReader,
		postgres.NewOutboxStore,
		func(s *postgres.Store) port.UnitOfWork { return s },
		func(r *postgres.Reader) port.Reader { return r },
		func(o *postgres.OutboxStore) port.OutboxStore { return o },
	),
)

// newPool cria o pool e valida a conexão no início; o fechamento é o último
// passo do shutdown, depois que servidor e workers pararam.
func newPool(lc fx.Lifecycle, cfg *config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), cfg.DatabaseURL, cfg.DatabaseMaxConns)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for {
				err := pool.Ping(ctx)
				if err == nil {
					log.Info("postgres connected")
					return nil
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("postgres not reachable: %w (last error: %v)", ctx.Err(), err)
				case <-time.After(time.Second):
				}
			}
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

var AuthModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg *config.Config) *auth.Validator {
		v := auth.NewValidator(cfg.Auth)
		lc.Append(fx.Hook{
			OnStart: v.Start,
			OnStop:  func(context.Context) error { v.Stop(); return nil },
		})
		return v
	}),
)

var SQSModule = fx.Module("sqs",
	fx.Provide(
		func(cfg *config.Config) (*sqs.Client, error) { return sqsx.NewClient(context.Background(), cfg) },
		func(c *sqs.Client) sqsx.API { return c },
		newQueues,
	),
)

// newQueues resolve as URLs das filas na inicialização; consumidor e publisher
// só leem o valor depois dessa etapa.
func newQueues(lc fx.Lifecycle, api sqsx.API, cfg *config.Config, log *slog.Logger) *sqsx.Queues {
	q := &sqsx.Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		resolved, err := sqsx.ResolveQueues(ctx, api, cfg, 45*time.Second)
		if err != nil {
			return err
		}
		*q = resolved
		log.Info("sqs queues resolved", "wager", resolved.Wager, "dlq", resolved.DLQ, "events", resolved.Events)
		return nil
	}})
	return q
}

var AppModule = fx.Module("app",
	fx.Provide(
		func(cfg *config.Config) processwager.Config {
			return processwager.Config{
				PendingBaseBackoff: cfg.Pending.BaseBackoff, PendingMaxBackoff: cfg.Pending.MaxBackoff,
				PendingTTL: cfg.Pending.TTL, PendingMaxAttempts: cfg.Pending.MaxAttempts,
			}
		},
		processwager.NewService,
		openwallet.NewService,
		query.NewService,
	),
)

var WorkersModule = fx.Module("workers",
	fx.Provide(
		func(cfg *config.Config, api sqsx.API, q *sqsx.Queues, svc *processwager.Service, m port.Metrics, log *slog.Logger) *sqsx.Consumer {
			return sqsx.NewConsumer(api, q, svc, m, log, cfg)
		},
		func(api sqsx.API, q *sqsx.Queues) worker.Publisher { return sqsx.NewEventPublisher(api, q) },
	),
	// A ordem dos Invoke define a ordem de parada (inversa): HTTP e consumidor
	// deixam de aceitar trabalho primeiro; depois o resolver e a outbox.
	fx.Invoke(registerOutbox, registerResolver, registerConsumer),
)

func registerOutbox(lc fx.Lifecycle, cfg *config.Config, store port.OutboxStore, pub worker.Publisher, m worker.OutboxMetrics, log *slog.Logger) {
	if !cfg.RunOutbox {
		return
	}
	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	relay := worker.NewOutboxRelay(store, pub, m, cfg.Outbox, owner, log)
	loop := worker.NewLoop("outbox", cfg.Outbox.PollInterval, log, relay.RunOnce)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { loop.Start(); return nil },
		OnStop:  loop.Stop,
	})
}

func registerResolver(lc fx.Lifecycle, cfg *config.Config, svc *processwager.Service, log *slog.Logger) {
	if !cfg.RunResolver {
		return
	}
	runner := worker.NewPendingRunner(svc, log)
	loop := worker.NewLoop("pending-resolver", cfg.Pending.PollInterval, log, runner.RunOnce)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { loop.Start(); return nil },
		OnStop:  loop.Stop,
	})
}

func registerConsumer(lc fx.Lifecycle, cfg *config.Config, c *sqsx.Consumer) {
	if !cfg.RunConsumer {
		return
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { c.Start(); return nil },
		OnStop:  c.Stop,
	})
}

var HTTPModule = fx.Module("http",
	fx.Provide(newAPI),
	fx.Invoke(registerHTTP),
)

func newAPI(wagers *processwager.Service, wallets *openwallet.Service, queries *query.Service, v *auth.Validator,
	store *postgres.Store, api sqsx.API, q *sqsx.Queues, m *observability.Metrics, log *slog.Logger) *httpapi.API {
	checks := []httpapi.ReadyCheck{
		{Name: "postgres", Check: store.Ping},
		{Name: "sqs", Check: func(ctx context.Context) error { return sqsx.Ping(ctx, api, q.Wager) }},
		{Name: "idp-keys", Check: func(context.Context) error {
			if !v.Ready() {
				return fmt.Errorf("signing keys not loaded")
			}
			return nil
		}},
	}
	metricsHandler := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	return httpapi.New(wagers, wallets, queries, v, checks, metricsHandler, log)
}

func registerHTTP(lc fx.Lifecycle, cfg *config.Config, api *httpapi.API, log *slog.Logger) {
	if !cfg.RunHTTP {
		return
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
			}
			go func() {
				if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
					log.Error("http server stopped unexpectedly", "error", err)
				}
			}()
			log.Info("http server listening", "addr", ln.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("http server shutting down")
			return srv.Shutdown(ctx)
		},
	})
}
