//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Uma transação pendente que falha sempre (aqui: já existe um lançamento de diário
// para ela) não pode monopolizar o worker de pendências: as demais continuam sendo
// retomadas e a "venenosa" recebe backoff até virar FAILED, auditável.
func TestPoisonPendingTransactionDoesNotStarveTheResolver(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_RESOLVER", "false", "RUN_OUTBOX", "false"))
	w := openWallet(t, writer, "100.00")
	ctx := context.Background()

	// transação PENDING antiga, mas com um lançamento de diário já gravado: reprocessá-la
	// colide sempre com journal_entries_transaction_id_key
	poisonID, poisonExt := uuid.NewString(), "poison-"+w.id
	if _, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, provider_id, external_transaction_id, idempotency_key,
		payload_hash, wallet_id, player_id, round_id, game_id, amount, currency, status, created_at, updated_at)
		VALUES ($1, 'EXTERNAL', 'BET', 'provider-a', $2, $2, 'h', $3, $4, 'r', 'g', 1000, 'BRL', 'PENDING',
		        now() - interval '1 hour', now() - interval '1 hour')`, poisonID, poisonExt, w.id, w.player); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `WITH e AS (INSERT INTO journal_entries (id, transaction_id, created_at) VALUES (gen_random_uuid(), $1, now()) RETURNING id)
		INSERT INTO journal_postings (entry_id, account, direction, amount, currency)
		SELECT id, 'house:x', 'DEBIT', 1, 'BRL' FROM e UNION ALL SELECT id, 'house:y', 'CREDIT', 1, 'BRL' FROM e`, poisonID); err != nil {
		t.Fatal(err)
	}

	// uma pendência legítima, criada depois (portanto atrás da venenosa na fila)
	betExt := "legit-bet-" + w.id
	refund, refundOp := submit(t, writer, w, op{kind: "REFUND", amount: "10.00", ref: betExt})
	mustStatus(t, refund, http.StatusAccepted)
	mustStatus(t, mustSubmit(t, writer, w, op{external: betExt, kind: "BET", amount: "10.00"}), http.StatusOK)

	// agora sobe o resolver, com poucas tentativas e backoff curto
	startInstance(t, q, withEnv("RUN_CONSUMER", "false", "PENDING_MAX_ATTEMPTS", "3",
		"PENDING_BASE_BACKOFF", "200ms", "PENDING_MAX_BACKOFF", "400ms"))

	eventually(t, 40*time.Second, "a pendência legítima ser resolvida apesar da venenosa", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", refundOp.external)
		return s == "PROCESSED"
	})
	eventually(t, 40*time.Second, "a venenosa virar FAILED", func() bool {
		s, code, _ := dbTxStatus(t, "provider-a", poisonExt)
		return s == "FAILED" && code == "INTERNAL_ERROR"
	})
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d: a venenosa não pode ter efeito financeiro", bal)
	}
	assertLedgerMatchesBalance(t, w.id)
}
