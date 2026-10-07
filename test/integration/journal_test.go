//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// Cada movimento gera um lançamento de partidas dobradas equilibrado, ligado ao ledger.
func TestDoubleEntryJournalFollowsEveryMovement(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	bet := op{external: "j-bet-" + w.id, kind: "BET", amount: "40.00"}
	mustStatus(t, mustSubmit(t, inst, w, bet), http.StatusOK)
	win := op{external: "j-win-" + w.id, kind: "WIN", amount: "15.00"}
	mustStatus(t, mustSubmit(t, inst, w, win), http.StatusOK)
	mustStatus(t, mustSubmit(t, inst, w, op{kind: "REFUND", amount: "40.00", ref: bet.external}), http.StatusOK)
	mustStatus(t, mustSubmit(t, inst, w, op{kind: "LOSS", amount: "0.00"}), http.StatusOK)
	// rejeitada: não gera lançamento
	mustStatus(t, mustSubmit(t, inst, w, op{kind: "BET", amount: "9999.00"}), http.StatusUnprocessableEntity)

	// OPENING + BET + WIN + REFUND = 4 lançamentos (LOSS e rejeição não movimentam)
	if n := dbCount(t, `SELECT count(*) FROM journal_entries e JOIN wager_transactions t ON t.id = e.transaction_id WHERE t.wallet_id = $1`, w.id); n != 4 {
		t.Fatalf("lançamentos do diário = %d, quer 4", n)
	}
	type posting struct {
		account, direction string
		amount             int64
	}
	read := func(external string) []posting {
		rows, err := pool.Query(context.Background(), `SELECT p.account, p.direction, p.amount FROM journal_postings p
			JOIN journal_entries e ON e.id = p.entry_id JOIN wager_transactions t ON t.id = e.transaction_id
			WHERE t.wallet_id = $1 AND (t.external_transaction_id = $2 OR ($2 = '' AND t.kind = 'OPENING'))
			ORDER BY p.direction, p.account`, w.id, external)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []posting
		for rows.Next() {
			var p posting
			_ = rows.Scan(&p.account, &p.direction, &p.amount)
			out = append(out, p)
		}
		return out
	}
	wantWallet, wantHouse := "wallet:"+w.id, "house:provider-a"
	expect := map[string][]posting{ // ordenados por direção (CREDIT antes de DEBIT)
		"":           {{wantWallet, "CREDIT", 10000}, {"funding:opening", "DEBIT", 10000}},
		bet.external: {{wantHouse, "CREDIT", 4000}, {wantWallet, "DEBIT", 4000}},
		win.external: {{wantWallet, "CREDIT", 1500}, {wantHouse, "DEBIT", 1500}},
	}
	for ext, want := range expect {
		got := read(ext)
		if len(got) != len(want) {
			t.Fatalf("%q: postings %+v", ext, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%q: posting %d = %+v, quer %+v", ext, i, got[i], want[i])
			}
		}
	}

	// balancete geral: débitos = créditos
	tb := call(t, inst.base, "GET", "/accounting/trial-balance", internalTok(t), "", nil)
	mustStatus(t, tb, http.StatusOK)
	if !tb.boolean("balanced") {
		t.Fatalf("o balancete deveria estar equilibrado: %s", tb.raw)
	}
	if lines, _ := tb.body["lines"].([]any); len(lines) == 0 {
		t.Fatal("balancete sem linhas")
	}
	if bal := dbBalance(t, w.id); bal != 11500 { // 100 − 40 (aposta) + 15 (prêmio) + 40 (reembolso)
		t.Fatalf("saldo = %d, quer 11500", bal)
	}
	assertLedgerMatchesBalance(t, w.id)

	// o balancete é restrito ao serviço interno
	mustStatus(t, call(t, inst.base, "GET", "/accounting/trial-balance", providerA(t), "", nil), http.StatusForbidden)
	mustStatus(t, call(t, inst.base, "GET", "/accounting/trial-balance", "", "", nil), http.StatusUnauthorized)
}

// commitStatements executa os comandos numa transação e devolve o erro do COMMIT.
// Se algum comando falhar antes disso, o teste falha com o erro real do comando.
func commitStatements(t *testing.T, stmts [][]any) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for i, s := range stmts {
		if _, err := tx.Exec(ctx, s[0].(string), s[1:]...); err != nil {
			t.Fatalf("comando %d falhou antes do commit: %v", i+1, err)
		}
	}
	return tx.Commit(ctx)
}

// O diário é imposto pelo banco: desequilíbrio, imutabilidade e vínculo com o ledger.
func TestJournalConstraintsAreEnforcedByTheDatabase(t *testing.T) {
	q := newQueues(t)
	// sem resolver: as linhas PENDING criadas aqui são apostas reais e seriam processadas por ele
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false", "RUN_RESOLVER", "false"))
	w := openWallet(t, inst, "100.00")
	ctx := context.Background()

	var txID, entryID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.id).Scan(&txID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id::text FROM journal_entries WHERE transaction_id = $1`, txID).Scan(&entryID); err != nil {
		t.Fatal(err)
	}

	t.Run("append-only", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE journal_postings SET amount = 1 WHERE entry_id = $1`, entryID)
		expectDBError(t, "UPDATE", "23000", "append-only", err)
		_, err = pool.Exec(ctx, `DELETE FROM journal_postings WHERE entry_id = $1`, entryID)
		expectDBError(t, "DELETE", "23000", "append-only", err)
		_, err = pool.Exec(ctx, `DELETE FROM journal_entries WHERE id = $1`, entryID)
		expectDBError(t, "DELETE entry", "23000", "append-only", err)
		_, err = pool.Exec(ctx, `TRUNCATE journal_postings`)
		expectDBError(t, "TRUNCATE", "23000", "append-only", err)
	})

	newTx := func() string {
		id := uuid.NewString()
		// as linhas PENDING criadas aqui não podem sobrar para o resolver das outras instâncias
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'WALLET_NOT_FOUND', completed_at = now(), updated_at = now()
				WHERE id = $1 AND status = 'PENDING'`, id)
		})
		if _, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
			payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, status, created_at, updated_at)
			VALUES ($1, 'EXTERNAL', 'BET', 'provider-a', $2, $2, 'h', $3, $4, 'r', 'g', 100, 'BRL', 'PENDING', now(), now())`,
			id, "jc-"+id, w.id, w.player); err != nil {
			t.Fatal(err)
		}
		return id
	}
	posting := func(entry, account, dir string, amount int64, cur string) []any {
		return []any{`INSERT INTO journal_postings (entry_id, account, direction, amount, currency) VALUES ($1, $2, $3, $4, $5)`,
			entry, account, dir, amount, cur}
	}
	entryStmt := func(entry, tx string) []any {
		return []any{`INSERT INTO journal_entries (id, transaction_id, created_at) VALUES ($1, $2, now())`, entry, tx}
	}

	t.Run("lançamento desequilibrado não commita", func(t *testing.T) {
		id, e := newTx(), uuid.NewString()
		err := commitStatements(t, [][]any{entryStmt(e, id), posting(e, "house:x", "DEBIT", 100, "BRL"), posting(e, "house:y", "CREDIT", 99, "BRL")})
		expectDBError(t, "commit", "23000", "not balanced", err)
	})

	t.Run("lançamento com um só posting não commita", func(t *testing.T) {
		id, e := newTx(), uuid.NewString()
		err := commitStatements(t, [][]any{entryStmt(e, id), posting(e, "house:x", "DEBIT", 100, "BRL")})
		expectDBError(t, "commit", "23000", "not balanced", err)
	})

	t.Run("moedas misturadas não equilibram", func(t *testing.T) {
		id, e := newTx(), uuid.NewString()
		err := commitStatements(t, [][]any{entryStmt(e, id), posting(e, "house:x", "DEBIT", 100, "BRL"), posting(e, "house:y", "CREDIT", 100, "USD")})
		expectDBError(t, "commit", "23000", "not balanced", err)
	})

	t.Run("lançamento equilibrado commita (contraprova)", func(t *testing.T) {
		id, e := newTx(), uuid.NewString()
		if err := commitStatements(t, [][]any{entryStmt(e, id), posting(e, "house:x", "DEBIT", 100, "BRL"), posting(e, "house:y", "CREDIT", 100, "BRL")}); err != nil {
			t.Fatalf("um lançamento equilibrado deveria commitar: %v", err)
		}
	})

	t.Run("conta de carteira exige wallet_id", func(t *testing.T) {
		id, e := newTx(), uuid.NewString()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (id, transaction_id, created_at) VALUES ($1, $2, now())`, e, id); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO journal_postings (entry_id, account, direction, amount, currency) VALUES ($1, 'wallet:x', 'DEBIT', 100, 'BRL')`, e)
		expectDBError(t, "wallet sem wallet_id", "23514", "journal_postings_wallet_account", err)
	})

	t.Run("movimento do ledger sem posting correspondente não commita", func(t *testing.T) {
		id := newTx()
		// ledger e carteira coerentes entre si, mas sem o lançamento no diário
		err := commitStatements(t, [][]any{
			{`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after,
				currency, wallet_version, created_at) VALUES ($1, $2, $3, 'DEBIT', 100, 10000, 9900, 'BRL', 2, now())`, uuid.New(), w.id, id},
			{`UPDATE wallets SET balance = 9900, version = 2 WHERE id = $1`, w.id},
		})
		expectDBError(t, "commit", "23000", "no matching journal posting", err)
	})

	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
}
