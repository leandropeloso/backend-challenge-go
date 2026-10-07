//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// Regressão da brecha apontada na revisão: um lançamento de ledger encadeado, com diário
// válido, mas SEM atualizar a carteira, tem de falhar no commit. Antes só as escritas em
// `wallets` eram conferidas contra o ledger.
func TestLedgerEntryWithoutMatchingWalletUpdateIsRejected(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false", "RUN_RESOLVER", "false"))
	w := openWallet(t, inst, "100.00")
	ctx := context.Background()

	// uma transação PROCESSED à qual o ledger/diário vão se referir
	newProcessedTx := func(amount int64, resultBalance int64) string {
		id := uuid.NewString()
		if _, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, status, result_balance, created_at, updated_at, completed_at)
			VALUES ($1, 'EXTERNAL', 'BET', 'provider-a', $2, $2, 'h', $3, $4, 'r', 'g', $5, 'BRL', 'PROCESSED', $6, now(), now(), now())`,
			id, "gap-"+id, w.id, w.player, amount, resultBalance); err != nil {
			t.Fatal(err)
		}
		return id
	}
	journalFor := func(txID string, amount int64) [][]any {
		e := uuid.NewString()
		return [][]any{
			{`INSERT INTO journal_entries (id, transaction_id, created_at) VALUES ($1, $2, now())`, e, txID},
			{`INSERT INTO journal_postings (entry_id, account, wallet_id, direction, amount, currency) VALUES ($1, $2, $3, 'DEBIT', $4, 'BRL')`, e, "wallet:" + w.id, w.id, amount},
			{`INSERT INTO journal_postings (entry_id, account, direction, amount, currency) VALUES ($1, 'house:provider-a', 'CREDIT', $2, 'BRL')`, e, amount},
		}
	}
	ledgerStmt := func(txID string, before, after, version int64) []any {
		return []any{`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after,
			currency, wallet_version, created_at) VALUES ($1, $2, $3, 'DEBIT', $4, $5, $6, 'BRL', $7, now())`,
			uuid.New(), w.id, txID, before - after, before, after, version}
	}

	t.Run("ledger + diário sem atualizar a carteira não commita", func(t *testing.T) {
		tx := newProcessedTx(100, 9900)
		stmts := append([][]any{ledgerStmt(tx, 10000, 9900, 2)}, journalFor(tx, 100)...)
		err := commitStatements(t, stmts)
		expectDBError(t, "commit", "23000", "does not match its latest ledger entry", err)
		if bal := dbBalance(t, w.id); bal != 10000 {
			t.Fatalf("saldo = %d", bal)
		}
		if entries, _, _ := dbLedger(t, w.id); entries != 1 {
			t.Fatalf("o lançamento rejeitado não pode ficar: %d", entries)
		}
	})

	t.Run("contraprova: ledger, diário e carteira juntos commitam", func(t *testing.T) {
		tx := newProcessedTx(100, 9900)
		stmts := append([][]any{ledgerStmt(tx, 10000, 9900, 2)}, journalFor(tx, 100)...)
		stmts = append(stmts, []any{`UPDATE wallets SET balance = 9900, version = 2 WHERE id = $1`, w.id})
		if err := commitStatements(t, stmts); err != nil {
			t.Fatalf("o conjunto consistente deveria commitar: %v", err)
		}
		if bal := dbBalance(t, w.id); bal != 9900 {
			t.Fatalf("saldo = %d", bal)
		}
		assertLedgerMatchesBalance(t, w.id)
	})

}
