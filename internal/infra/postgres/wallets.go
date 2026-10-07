package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

type walletRepo struct{ q querier }

const walletCols = `id, player_id, currency, balance, version, created_at, updated_at`

func (r *walletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO wallets (`+walletCols+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return port.ErrWalletExists
	}
	if err != nil {
		return fmt.Errorf("insert wallet: %w", err)
	}
	return nil
}

func (r *walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	// NO KEY UPDATE serializa os escritores da carteira sem bloquear inserts que
	// apenas referenciam a linha por chave estrangeira.
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1 FOR NO KEY UPDATE`, id))
}

func (r *walletRepo) Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx,
		`UPDATE wallets SET balance = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return fmt.Errorf("update wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return port.ErrConcurrentUpdate
	}
	return nil
}

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		s       wallet.Snapshot
		cur     string
		balance int64
	)
	if err := row.Scan(&s.ID, &s.PlayerID, &cur, &balance, &s.Version, &s.CreatedAt, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, port.ErrNotFound
		}
		return nil, fmt.Errorf("scan wallet: %w", err)
	}
	m, err := money.FromMinor(balance, money.Currency(cur))
	if err != nil {
		return nil, fmt.Errorf("wallet balance: %w", err)
	}
	s.Balance = m
	return wallet.Rehydrate(s)
}
