package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

// Reader implementa port.Reader.
type Reader struct{ s *Store }

func NewReader(s *Store) *Reader { return &Reader{s: s} }

func (r *Reader) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	w, err := scanWallet(r.s.pool.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1`, id))
	return w, translate((err))
}

func (r *Reader) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := scanTransaction(r.s.pool.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions WHERE id = $1`, id))
	return t, translate((err))
}

func (r *Reader) GetTransactionByProvider(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	t, err := scanTransaction(r.s.pool.QueryRow(ctx,
		`SELECT `+txCols+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID))
	return t, translate((err))
}

func (r *Reader) ListLedger(ctx context.Context, walletID uuid.UUID, after int64, limit int) ([]port.LedgerItem, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT id, transaction_id, direction, amount, balance_before, balance_after,
		currency, wallet_version, created_at
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND wallet_version > $2
		ORDER BY wallet_version ASC LIMIT $3`, walletID, after, limit)
	if err != nil {
		return nil, translate((fmt.Errorf("list ledger: %w", err)))
	}
	defer rows.Close()

	var items []port.LedgerItem
	for rows.Next() {
		var (
			id, txID              uuid.UUID
			dir, cur              string
			amount, before, after int64
			version               int64
			createdAt             time.Time
		)
		if err := rows.Scan(&id, &txID, &dir, &amount, &before, &after, &cur, &version, &createdAt); err != nil {
			return nil, fmt.Errorf("scan ledger: %w", err)
		}
		c := money.Currency(cur)
		a, err1 := money.FromMinor(amount, c)
		b, err2 := money.FromMinor(before, c)
		f, err3 := money.FromMinor(after, c)
		if err := errors.Join(err1, err2, err3); err != nil {
			return nil, err
		}
		entry, err := ledger.Rehydrate(id, walletID, txID, ledger.Direction(dir), a, b, f, createdAt)
		if err != nil {
			return nil, fmt.Errorf("rehydrate ledger entry: %w", err)
		}
		items = append(items, port.LedgerItem{Entry: entry, WalletVersion: version})
	}
	if err := rows.Err(); err != nil {
		return nil, translate((fmt.Errorf("list ledger: %w", err)))
	}
	return items, nil
}

// Reconcile lê saldo e ledger no mesmo snapshot (REPEATABLE READ, somente leitura).
func (r *Reader) Reconcile(ctx context.Context, walletID uuid.UUID) (port.Reconciliation, error) {
	var rec port.Reconciliation
	tx, err := r.s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return rec, translate(transient(err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		stored int64
		cur    string
	)
	if err := tx.QueryRow(ctx, `SELECT balance, currency FROM wallets WHERE id = $1`, walletID).Scan(&stored, &cur); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rec, port.ErrNotFound
		}
		return rec, translate((err))
	}
	var calculated, entries int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::bigint, count(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&calculated, &entries)
	if err != nil {
		return rec, translate((err))
	}
	c := money.Currency(cur)
	if rec.Stored, err = money.FromMinor(stored, c); err != nil {
		return rec, err
	}
	if rec.Calculated, err = money.FromMinor(calculated, c); err != nil {
		return rec, err
	}
	rec.Entries = entries
	return rec, nil
}
