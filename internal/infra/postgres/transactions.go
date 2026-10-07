package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

type transactionRepo struct{ q querier }

const txCols = `id, kind, provider_id, external_transaction_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, amount, currency, reference_external_transaction_id,
	resolved_reference_id, status, failure_code, result_balance, attempts, next_attempt_at,
	expires_at, created_at, updated_at, completed_at`

func (r *transactionRepo) InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error) {
	tag, err := r.q.Exec(ctx, insertSQL+` ON CONFLICT DO NOTHING`, insertArgs(t, "EXTERNAL")...)
	if err != nil {
		return false, fmt.Errorf("insert transaction: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *transactionRepo) InsertInternal(ctx context.Context, t *wager.Transaction) error {
	if _, err := r.q.Exec(ctx, insertSQL, insertArgs(t, "INTERNAL")...); err != nil {
		return fmt.Errorf("insert opening transaction: %w", err)
	}
	return nil
}

const insertSQL = `INSERT INTO wager_transactions (
	id, origin, kind, provider_id, external_transaction_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, amount, currency, reference_external_transaction_id,
	resolved_reference_id, status, failure_code, result_balance, attempts, next_attempt_at,
	expires_at, created_at, updated_at, completed_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`

func insertArgs(t *wager.Transaction, origin string) []any {
	return []any{
		t.ID(), origin, string(t.Kind()), nullable(t.ProviderID()), nullable(t.ExternalID()),
		nullable(t.IdempotencyKey()), nullable(t.PayloadHash()), t.WalletID(), t.PlayerID(),
		nullable(t.RoundID()), nullable(t.GameID()), t.Money().Minor(), string(t.Money().Currency()),
		nullable(t.ReferenceExternalID()), t.ResolvedReferenceID(), string(t.Status()),
		nullable(string(t.FailureCode())), resultMinor(t), t.Attempts(), t.NextAttemptAt(), t.ExpiresAt(),
		t.CreatedAt(), t.UpdatedAt(), t.CompletedAt(),
	}
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func resultMinor(t *wager.Transaction) *int64 {
	if rb := t.ResultBalance(); rb != nil {
		v := rb.Minor()
		return &v
	}
	return nil
}

func (r *transactionRepo) Update(ctx context.Context, t *wager.Transaction) error {
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions SET
		resolved_reference_id = $2, status = $3, failure_code = $4, result_balance = $5, attempts = $6,
		next_attempt_at = $7, expires_at = $8, updated_at = $9, completed_at = $10
		WHERE id = $1`,
		t.ID(), t.ResolvedReferenceID(), string(t.Status()), nullable(string(t.FailureCode())),
		resultMinor(t), t.Attempts(), t.NextAttemptAt(), t.ExpiresAt(), t.UpdatedAt(), t.CompletedAt())
	if err != nil {
		return fmt.Errorf("update transaction: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return port.ErrNotFound
	}
	return nil
}

func (r *transactionRepo) GetByID(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions WHERE id = $1`, id))
}

func (r *transactionRepo) GetByProviderKey(ctx context.Context, providerID, key string) (*wager.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx,
		`SELECT `+txCols+` FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (r *transactionRepo) GetByProviderExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx,
		`SELECT `+txCols+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID))
}

func (r *transactionRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM wager_transactions
		WHERE resolved_reference_id = $1 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`,
		referenceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check reversal: %w", err)
	}
	return exists, nil
}

func (r *transactionRepo) ClaimDue(ctx context.Context, now time.Time) (*wager.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions
		WHERE status IN ('PENDING', 'PENDING_REFERENCE') AND COALESCE(next_attempt_at, created_at) <= $1
		ORDER BY COALESCE(next_attempt_at, created_at)
		FOR UPDATE SKIP LOCKED LIMIT 1`, now))
}

func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var (
		s                                          wager.Snapshot
		kind, status, cur                          string
		provider, external, key, hash, round, game *string
		reference, failure                         *string
		amount                                     int64
		result                                     *int64
	)
	err := row.Scan(&s.ID, &kind, &provider, &external, &key, &hash, &s.WalletID, &s.PlayerID, &round, &game,
		&amount, &cur, &reference, &s.ResolvedReferenceID, &status, &failure, &result, &s.Attempts,
		&s.NextAttemptAt, &s.ExpiresAt, &s.CreatedAt, &s.UpdatedAt, &s.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, port.ErrNotFound
		}
		return nil, fmt.Errorf("scan transaction: %w", err)
	}
	currency := money.Currency(cur)
	if s.Money, err = money.FromMinor(amount, currency); err != nil {
		return nil, fmt.Errorf("transaction amount: %w", err)
	}
	if result != nil {
		rb, err := money.FromMinor(*result, currency)
		if err != nil {
			return nil, fmt.Errorf("transaction result balance: %w", err)
		}
		s.ResultBalance = &rb
	}
	s.Kind, s.Status = wager.Kind(kind), wager.Status(status)
	s.ProviderID, s.ExternalID, s.IdempotencyKey = deref(provider), deref(external), deref(key)
	s.PayloadHash, s.RoundID, s.GameID = deref(hash), deref(round), deref(game)
	s.ReferenceExternalID, s.FailureCode = deref(reference), wager.FailureCode(deref(failure))
	return wager.Rehydrate(s)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
