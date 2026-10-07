package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/journal"
	"github.com/leandropeloso/wager-service/internal/domain/money"
)

type journalRepo struct{ q querier }

func (r *journalRepo) Append(ctx context.Context, e *journal.Entry) error {
	if _, err := r.q.Exec(ctx, `INSERT INTO journal_entries (id, transaction_id, created_at) VALUES ($1, $2, $3)`,
		e.ID(), e.TransactionID(), e.CreatedAt()); err != nil {
		return fmt.Errorf("insert journal entry: %w", err)
	}
	for _, p := range e.Postings() {
		var walletID *uuid.UUID
		if p.Account.IsWallet() {
			id, err := uuid.Parse(string(p.Account)[len("wallet:"):])
			if err != nil {
				return fmt.Errorf("wallet account %q: %w", p.Account, err)
			}
			walletID = &id
		}
		if _, err := r.q.Exec(ctx, `INSERT INTO journal_postings (entry_id, account, wallet_id, direction, amount, currency)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID(), string(p.Account), walletID, string(p.Direction), p.Amount.Minor(), string(p.Amount.Currency())); err != nil {
			return fmt.Errorf("insert journal posting: %w", err)
		}
	}
	return nil
}

// TrialBalance totaliza débitos e créditos por moeda num único snapshot.
func (r *Reader) TrialBalance(ctx context.Context) ([]port.TrialLine, error) {
	tx, err := r.s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, translate(transient(err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, `SELECT p.currency,
			(COALESCE(SUM(p.amount) FILTER (WHERE p.direction = 'DEBIT'), 0)::numeric / 100)::numeric(40,2)::text,
			(COALESCE(SUM(p.amount) FILTER (WHERE p.direction = 'CREDIT'), 0)::numeric / 100)::numeric(40,2)::text,
			count(DISTINCT p.entry_id),
			COALESCE(SUM(p.amount) FILTER (WHERE p.direction = 'DEBIT'), 0)
			    = COALESCE(SUM(p.amount) FILTER (WHERE p.direction = 'CREDIT'), 0)
		FROM journal_postings p GROUP BY p.currency ORDER BY p.currency`)
	if err != nil {
		return nil, translate(transient(fmt.Errorf("trial balance: %w", err)))
	}
	defer rows.Close()
	var out []port.TrialLine
	for rows.Next() {
		var l port.TrialLine
		var cur string
		if err := rows.Scan(&cur, &l.Debits, &l.Credits, &l.Entries, &l.Balanced); err != nil {
			return nil, fmt.Errorf("scan trial balance: %w", err)
		}
		l.Currency = money.Currency(cur)
		out = append(out, l)
	}
	return out, translate(transient(rows.Err()))
}
