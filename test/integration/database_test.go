//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http"
	neturl "net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/leandropeloso/wager-service/internal/infra/migrate"
	"github.com/leandropeloso/wager-service/migrations"
)

func pgCode(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

func expectDBError(t *testing.T, what, wantCode, wantText string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: o banco deveria ter recusado", what)
	}
	if wantCode != "" && pgCode(err) != wantCode {
		t.Fatalf("%s: SQLSTATE %q, quer %q (%v)", what, pgCode(err), wantCode, err)
	}
	if wantText != "" && !strings.Contains(err.Error(), wantText) {
		t.Fatalf("%s: erro %q não contém %q", what, err, wantText)
	}
}

// As invariantes valem no próprio banco, independentemente da aplicação.
func TestDatabaseConstraintsEnforceInvariants(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false"))
	w := openWallet(t, inst, "100.00")
	mustStatus(t, mustSubmit(t, inst, w, op{external: "db-bet-" + w.id, kind: "BET", amount: "10.00"}), http.StatusOK)
	ctx := context.Background()

	var betTx string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.id).Scan(&betTx); err != nil {
		t.Fatal(err)
	}

	t.Run("ledger é append-only", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount = 1 WHERE wallet_id = $1`, w.id)
		expectDBError(t, "UPDATE", "23000", "append-only", err)
		_, err = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id)
		expectDBError(t, "DELETE", "23000", "append-only", err)
		_, err = pool.Exec(ctx, `TRUNCATE wallet_ledger_entries`)
		expectDBError(t, "TRUNCATE", "23000", "append-only", err)
	})

	t.Run("lançamento precisa respeitar balanceAfter = balanceBefore ± valor", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, currency, wallet_version, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 100, 9000, 8000, 'BRL', 99, now())`, uuid.New(), w.id, uuid.New())
		expectDBError(t, "invariante", "23514", "wallet_ledger_balance_invariant", err)
	})

	t.Run("lançamento precisa continuar o anterior", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, currency, wallet_version, created_at)
			VALUES ($1, $2, $3, 'CREDIT', 100, 5, 105, 'BRL', 99, now())`, uuid.New(), w.id, betTx)
		expectDBError(t, "cadeia", "23000", "does not continue", err)
	})

	t.Run("(walletId, transactionId) é único e há chaves estrangeiras", func(t *testing.T) {
		// continua a cadeia (balance_before = 9000) e repete a transação do débito
		_, err := pool.Exec(ctx, `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, currency, wallet_version, created_at)
			VALUES ($1, $2, $3, 'CREDIT', 100, 9000, 9100, 'BRL', 99, now())`, uuid.New(), w.id, betTx)
		expectDBError(t, "unicidade", "23505", "wallet_ledger_wallet_transaction_key", err)
		_, err = pool.Exec(ctx, `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, currency, wallet_version, created_at)
			VALUES ($1, $2, $3, 'CREDIT', 100, 9000, 9100, 'BRL', 99, now())`, uuid.New(), w.id, uuid.New())
		expectDBError(t, "FK de transação", "23503", "", err)
	})

	t.Run("saldo não pode ser negativo", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE wallets SET balance = -1, version = version + 1 WHERE id = $1`, w.id)
		expectDBError(t, "saldo negativo", "23514", "wallets_balance_check", err)
	})

	t.Run("saldo e versão só mudam juntos e com ledger", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE wallets SET balance = balance + 1 WHERE id = $1`, w.id)
		expectDBError(t, "saldo sem versão", "23000", "version must advance", err)
		_, err = pool.Exec(ctx, `UPDATE wallets SET version = version + 1 WHERE id = $1`, w.id)
		expectDBError(t, "versão sem saldo", "23000", "without a balance change", err)
		// saldo e versão coerentes, mas sem lançamento no ledger: o commit falha
		_, err = pool.Exec(ctx, `UPDATE wallets SET balance = balance + 1, version = version + 1 WHERE id = $1`, w.id)
		expectDBError(t, "saldo sem ledger", "23000", "does not match its ledger", err)
		_, err = pool.Exec(ctx, `UPDATE wallets SET currency = 'USD' WHERE id = $1`, w.id)
		expectDBError(t, "troca de moeda", "23000", "immutable", err)
		_, err = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, w.id)
		expectDBError(t, "DELETE", "23000", "cannot be deleted", err)
		if bal := dbBalance(t, w.id); bal != 9000 {
			t.Fatalf("saldo = %d", bal)
		}
	})

	t.Run("par (jogador, moeda) é único", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
			VALUES ($1, $2, 'BRL', 0, 1, now(), now())`, uuid.New(), w.player)
		expectDBError(t, "carteira duplicada", "23505", "wallets_player_currency_key", err)
	})

	t.Run("transações: unicidade, estado terminal e abertura", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, status, created_at, updated_at)
			VALUES ($1, 'EXTERNAL', 'BET', 'provider-a', $2, 'outra-chave', 'h', $3, $4, 'r', 'g', 100, 'BRL', 'PENDING', now(), now())`,
			uuid.New(), "db-bet-"+w.id, w.id, w.player)
		expectDBError(t, "externalId duplicado", "23505", "wager_transactions_provider_external_key", err)

		_, err = pool.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE id = $1`, betTx)
		expectDBError(t, "transação terminal", "23000", "terminal state", err)
		_, err = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE id = $1`, betTx)
		expectDBError(t, "DELETE", "23000", "cannot be deleted", err)

		_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, wallet_id, player_id, amount, currency, status,
			result_balance, created_at, updated_at, completed_at)
			VALUES ($1, 'INTERNAL', 'OPENING', $2, $3, 100, 'BRL', 'PROCESSED', 100, now(), now(), now())`, uuid.New(), w.id, w.player)
		expectDBError(t, "crédito inicial duplicado", "23505", "wager_transactions_one_opening_per_wallet", err)

		_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, wallet_id, player_id, amount, currency, status,
			result_balance, created_at, updated_at, completed_at)
			VALUES ($1, 'INTERNAL', 'OPENING', 'provider-a', $2, $3, 100, 'BRL', 'PROCESSED', 100, now(), now(), now())`, uuid.New(), uuid.New(), w.player)
		expectDBError(t, "OPENING com metadados externos", "23514", "wager_transactions_origin_shape", err)

		_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, status, created_at, updated_at)
			VALUES ($1, 'EXTERNAL', 'LOSS', 'provider-a', 'loss-x', 'loss-x', 'h', $2, $3, 'r', 'g', 5, 'BRL', 'PENDING', now(), now())`,
			uuid.New(), w.id, w.player)
		expectDBError(t, "LOSS com valor", "23514", "wager_transactions_amount_policy", err)
	})

	t.Run("uma referência não recebe duas reversões processadas", func(t *testing.T) {
		refund := op{kind: "REFUND", amount: "10.00", ref: "db-bet-" + w.id}
		mustStatus(t, mustSubmit(t, inst, w, refund), http.StatusOK)
		var refundTx string
		if err := pool.QueryRow(ctx, `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'REFUND'`, w.id).Scan(&refundTx); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, reference_external_transaction_id,
			resolved_reference_id, status, result_balance, created_at, updated_at, completed_at)
			VALUES ($1, 'EXTERNAL', 'ROLLBACK', 'provider-a', 'rb-forced', 'rb-forced', 'h', $2, $3, 'r', 'g', 1000, 'BRL', 'x',
			(SELECT resolved_reference_id FROM wager_transactions WHERE id = $4), 'PROCESSED', 1000, now(), now(), now())`,
			uuid.New(), w.id, w.player, refundTx)
		expectDBError(t, "segunda reversão", "23505", "wager_transactions_one_reversal_per_reference", err)
	})

	t.Run("outbox: conteúdo imutável", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE outbox_events SET payload = '{}'::jsonb WHERE partition_key = $1`, w.id)
		expectDBError(t, "payload", "23000", "immutable", err)
		_, err = pool.Exec(ctx, `DELETE FROM outbox_events WHERE partition_key = $1`, w.id)
		expectDBError(t, "DELETE", "23000", "cannot be deleted", err)
	})

	assertLedgerMatchesBalance(t, w.id)
}

// Aplicação e reversão das migrations, num banco descartável.
func TestMigrationsApplyAndRevert(t *testing.T) {
	ctx := context.Background()
	scratch := "wager_migrate_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:10]
	if _, err := pool.Exec(ctx, `CREATE DATABASE `+scratch); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP DATABASE IF EXISTS `+scratch+` WITH (FORCE)`)
	})
	parsed, err := neturl.Parse(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + scratch
	url := parsed.String()

	run := func(fn func(r *migrate.Runner)) {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		r, err := migrate.New(ctx, url, migrations.FS)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close(ctx)
		fn(r)
	}
	tableCount := func() int {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	run(func(r *migrate.Runner) {
		n, err := r.Up(ctx)
		if err != nil || n != 9 {
			t.Fatalf("up: n=%d err=%v", n, err)
		}
		if again, err := r.Up(ctx); err != nil || again != 0 {
			t.Fatalf("up é idempotente: n=%d err=%v", again, err)
		}
	})
	if n := tableCount(); n != 7 {
		t.Fatalf("tabelas após up = %d", n)
	}
	steps := []struct{ revert, tables int }{{1, 7}, {1, 7}, {1, 5}, {1, 5}, {1, 4}} // 0009 (trigger), 0008 (coluna), 0007 (diário), 0006 (índice), 0005 (outbox)
	for i, s := range steps {
		run(func(r *migrate.Runner) {
			if n, err := r.Down(ctx, s.revert); err != nil || n != s.revert {
				t.Fatalf("down passo %d: n=%d err=%v", i+1, n, err)
			}
		})
		if n := tableCount(); n != s.tables {
			t.Fatalf("tabelas após down passo %d = %d, quer %d", i+1, n, s.tables)
		}
	}
	run(func(r *migrate.Runner) {
		if n, err := r.Down(ctx, 0); err != nil || n != 4 {
			t.Fatalf("down todas: n=%d err=%v", n, err)
		}
	})
	if n := tableCount(); n != 0 {
		t.Fatalf("tabelas após down total = %d", n)
	}
	run(func(r *migrate.Runner) {
		if n, err := r.Up(ctx); err != nil || n != 9 {
			t.Fatalf("up de novo: n=%d err=%v", n, err)
		}
		st, err := r.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range st {
			if !s.Applied {
				t.Errorf("migration %d não aplicada", s.Version)
			}
		}
	})
}
