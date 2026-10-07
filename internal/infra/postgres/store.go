// Package postgres implementa os repositórios sobre pgx com SQL explícito.
//
// Delimitação da transação: Store.Do abre uma transação pgx e entrega aos
// repositórios (wallets, transações, ledger, inbox, outbox) a mesma pgx.Tx.
// Nenhum repositório abre, confirma ou desfaz transações por conta própria.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

// querier é o subconjunto comum a pgx.Tx e *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func NewPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.HealthCheckPeriod = 15 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Do executa fn em uma transação READ COMMITTED. O isolamento é suficiente
// porque cada carteira é serializada por SELECT ... FOR NO KEY UPDATE.
func (s *Store) Do(ctx context.Context, fn func(ctx context.Context, r port.Repositories) error) (err error) {
	ctx, span := telemetry.Start(ctx, "db.transaction", attribute.String("db.system", "postgresql"))
	defer func() {
		telemetry.Fail(span, err)
		span.End()
	}()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return transient(err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// O rollback precisa acontecer mesmo com o contexto já cancelado.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rctx)
	}()

	if err := fn(ctx, newRepos(tx)); err != nil {
		return translate(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return translate(transient(err))
	}
	committed = true
	return nil
}

// translate marca como retentáveis ou transitórios os erros do banco; erros de
// negócio passam intactos.
func translate(err error) error {
	if err == nil || errors.Is(err, port.ErrRetryable) || errors.Is(err, port.ErrTransient) {
		return err
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "40001", "40P01":
			return fmt.Errorf("%w: %w", port.ErrRetryable, err)
		}
		switch pg.Code[:2] {
		case "08", "53", "57", "58":
			return fmt.Errorf("%w: %w", port.ErrTransient, err)
		}
		return err
	}
	var ne net.Error
	var ce *pgconn.ConnectError
	if errors.As(err, &ne) || errors.As(err, &ce) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", port.ErrTransient, err)
	}
	return err
}

// transient classifica falhas de Begin/Commit: tudo que não for erro SQL
// determinístico é tratado como indisponibilidade temporária.
func transient(err error) error {
	if err == nil {
		return nil
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, port.ErrTransient) {
		return err
	}
	return fmt.Errorf("%w: %w", port.ErrTransient, err)
}

type repos struct {
	wallets *walletRepo
	txs     *transactionRepo
	ledger  *ledgerRepo
	journal *journalRepo
	inbox   *inboxRepo
	outbox  *outboxRepo
}

func newRepos(q querier) *repos {
	return &repos{
		wallets: &walletRepo{q: q}, txs: &transactionRepo{q: q}, ledger: &ledgerRepo{q: q}, journal: &journalRepo{q: q},
		inbox: &inboxRepo{q: q}, outbox: &outboxRepo{q: q},
	}
}

func (r *repos) Wallets() port.WalletRepository           { return r.wallets }
func (r *repos) Transactions() port.TransactionRepository { return r.txs }
func (r *repos) Ledger() port.LedgerRepository            { return r.ledger }
func (r *repos) Journal() port.JournalRepository          { return r.journal }
func (r *repos) Inbox() port.InboxRepository              { return r.inbox }
func (r *repos) Outbox() port.OutboxRepository            { return r.outbox }

func isUniqueViolation(err error, constraint string) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505" && (constraint == "" || pg.ConstraintName == constraint)
}
