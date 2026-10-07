//go:build integration

package integration

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// A mesma aposta enviada 50 vezes em paralelo, por três processos distintos,
// gera um único débito e o mesmo resultado para todos.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "1000.00")

	external := "dup-" + w.id
	o := op{external: external, key: "provider-a:" + external, kind: "BET", amount: "25.00"}

	var mu sync.Mutex
	var results []response
	parallel(50, func(i int) {
		r, _ := submit(t, nodes[i%3], w, o)
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	})

	fresh := 0
	var txID string
	for _, r := range results {
		mustStatus(t, r, http.StatusOK)
		if r.str("balance", "amount") != "975.00" {
			t.Fatalf("saldo devolvido = %s, quer 975.00", r.str("balance", "amount"))
		}
		if txID == "" {
			txID = r.str("transactionId")
		} else if r.str("transactionId") != txID {
			t.Fatalf("transactionId diverge: %s vs %s", r.str("transactionId"), txID)
		}
		if !r.boolean("idempotentReplay") {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("esperava exatamente 1 processamento original, veio %d", fresh)
	}
	if bal := dbBalance(t, w.id); bal != 97500 {
		t.Fatalf("saldo no banco = %d", bal)
	}
	if entries, debits, _ := dbLedger(t, w.id); entries != 2 || debits != 1 {
		t.Fatalf("ledger: %d lançamentos, %d débitos", entries, debits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Duas apostas distintas de 80.00 sobre 100.00, simultâneas e em processos
// diferentes: uma processa, a outra é rejeitada; saldo final 20.00.
func TestTwoBetsRaceForTheSameBalance(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))

	for round := 0; round < 8; round++ {
		w := openWallet(t, nodes[0], "100.00")
		ops := []op{
			{external: "race-a-" + w.id, kind: "BET", amount: "80.00"},
			{external: "race-b-" + w.id, kind: "BET", amount: "80.00"},
		}
		got := make([]response, 2)
		parallel(2, func(i int) { got[i], _ = submit(t, nodes[(round+i)%3], w, ops[i]) })

		var ok, rejected int
		for _, r := range got {
			switch r.status {
			case http.StatusOK:
				ok++
			case http.StatusUnprocessableEntity:
				rejected++
				if r.str("failureCode") != "INSUFFICIENT_FUNDS" {
					t.Fatalf("failureCode = %s", r.str("failureCode"))
				}
			default:
				t.Fatalf("status inesperado %d: %s", r.status, r.raw)
			}
		}
		if ok != 1 || rejected != 1 {
			t.Fatalf("rodada %d: %d processadas, %d rejeitadas", round, ok, rejected)
		}
		if bal := dbBalance(t, w.id); bal != 2000 {
			t.Fatalf("saldo final = %d, quer 2000", bal)
		}
		if _, debits, _ := dbLedger(t, w.id); debits != 1 {
			t.Fatalf("débitos no ledger = %d", debits)
		}

		// Reenvios não alteram o resultado.
		for i, o := range ops {
			o.key = "provider-a:" + o.external
			again, _ := submit(t, nodes[(round+i+1)%3], w, o)
			if !again.boolean("idempotentReplay") || again.status != got[i].status {
				t.Fatalf("replay diverge: %d/%s vs %d", again.status, again.raw, got[i].status)
			}
		}
		if bal := dbBalance(t, w.id); bal != 2000 {
			t.Fatalf("saldo mudou após replays: %d", bal)
		}
		assertLedgerMatchesBalance(t, w.id)
	}
}

// Carteiras diferentes avançam em paralelo, sem se bloquear nem se contaminar.
func TestDifferentWalletsProceedInParallel(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))

	const wallets = 40
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, nodes[i%3], "500.00")
	}
	parallel(wallets*5, func(i int) {
		w := ws[i%wallets]
		r, _ := submit(t, nodes[i%3], w, op{kind: "BET", amount: "10.00", external: "par-" + w.id + "-" + string(rune('a'+i/wallets))})
		if r.status != http.StatusOK {
			t.Errorf("status %d: %s", r.status, r.raw)
		}
	})
	for _, w := range ws {
		if bal := dbBalance(t, w.id); bal != 45000 {
			t.Errorf("carteira %s: saldo %d, quer 45000", w.id, bal)
		}
		if _, debits, _ := dbLedger(t, w.id); debits != 5 {
			t.Errorf("carteira %s: %d débitos", w.id, debits)
		}
		assertLedgerMatchesBalance(t, w.id)
	}
}

// Muitas apostas pequenas disputando o mesmo saldo nunca o deixam negativo.
func TestBalanceNeverGoesNegativeUnderContention(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "100.00")

	var mu sync.Mutex
	processed := 0
	parallel(30, func(i int) {
		r, _ := submit(t, nodes[i%3], w, op{kind: "BET", amount: "7.00"})
		mu.Lock()
		defer mu.Unlock()
		switch r.status {
		case http.StatusOK:
			processed++
		case http.StatusUnprocessableEntity:
		default:
			t.Errorf("status %d: %s", r.status, r.raw)
		}
	})
	if processed != 14 { // 14 × 7.00 = 98.00
		t.Fatalf("processadas = %d, quer 14", processed)
	}
	if bal := dbBalance(t, w.id); bal != 200 {
		t.Fatalf("saldo = %d, quer 200", bal)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// O replay devolve o saldo observado no processamento original, mesmo que a
// carteira já tenha avançado, e conflitos de idempotência são detectados.
func TestIdempotencyReplayAndConflicts(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "1000.00")

	first := op{external: "idem-1-" + w.id, kind: "BET", amount: "25.00"}
	r1, first := submit(t, nodes[0], w, first)
	mustStatus(t, r1, http.StatusOK)

	second, _ := submit(t, nodes[1], w, op{kind: "BET", amount: "10.00"})
	mustStatus(t, second, http.StatusOK)
	if second.str("balance", "amount") != "965.00" {
		t.Fatalf("saldo = %s", second.str("balance", "amount"))
	}

	replay, _ := submit(t, nodes[1], w, first)
	mustStatus(t, replay, http.StatusOK)
	if !replay.boolean("idempotentReplay") || replay.str("balance", "amount") != "975.00" {
		t.Fatalf("replay deve repetir o saldo original 975.00: %s", replay.raw)
	}
	if replay.str("transactionId") != r1.str("transactionId") {
		t.Fatal("replay deve devolver a mesma transação")
	}

	// mesma chave, conteúdo diferente
	changed := first
	changed.amount = "30.00"
	conflict, _ := submit(t, nodes[0], w, changed)
	mustStatus(t, conflict, http.StatusConflict)
	if conflict.str("error", "code") != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("code = %s", conflict.str("error", "code"))
	}

	// mesma operação externa com outra chave
	otherKey := first
	otherKey.key = "outra-chave-" + w.id
	dup, _ := submit(t, nodes[1], w, otherKey)
	mustStatus(t, dup, http.StatusConflict)
	if dup.str("error", "code") != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("code = %s", dup.str("error", "code"))
	}

	// a chave pertence ao provedor: o provedor B não enxerga o resultado do A
	b, _ := submit(t, nodes[0], w, op{provider: "provider-b", external: first.external, key: first.key, kind: "BET", amount: "1.00"})
	mustStatus(t, b, http.StatusOK)
	if b.boolean("idempotentReplay") || b.str("transactionId") == r1.str("transactionId") {
		t.Fatal("a chave de A não pode produzir replay para B")
	}

	if _, debits, _ := dbLedger(t, w.id); debits != 3 {
		t.Fatalf("débitos = %d, quer 3 (25 + 10 + 1)", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Reiniciar todos os processos preserva idempotência e consistência financeira.
func TestRestartKeepsIdempotencyAndBalances(t *testing.T) {
	q := newQueues(t)
	a := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, a, "300.00")

	o := op{external: "restart-" + w.id, kind: "BET", amount: "50.00"}
	r, o := submit(t, a, w, o)
	mustStatus(t, r, http.StatusOK)

	a.kill() // encerramento abrupto, sem shutdown

	b := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	replay, _ := submit(t, b, w, o)
	mustStatus(t, replay, http.StatusOK)
	if !replay.boolean("idempotentReplay") || replay.str("balance", "amount") != "250.00" ||
		replay.str("transactionId") != r.str("transactionId") {
		t.Fatalf("replay após reinício: %s", replay.raw)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != 1 {
		t.Fatalf("débitos = %d", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Desligamento gracioso: SIGTERM encerra o processo com código 0, sem perder trabalho.
func TestGracefulShutdown(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q)
	w := openWallet(t, inst, "100.00")
	r, _ := submit(t, inst, w, op{kind: "BET", amount: "5.00"})
	mustStatus(t, r, http.StatusOK)

	start := time.Now()
	if code := inst.terminate(20 * time.Second); code != 0 {
		t.Fatalf("exit code = %d\n%s", code, inst.out.String())
	}
	if time.Since(start) > 15*time.Second {
		t.Fatalf("shutdown lento: %s", time.Since(start))
	}
	logs := inst.out.String()
	for _, want := range []string{"http server shutting down", `"worker":"outbox"`, "postgres pool closed"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log de shutdown sem %q", want)
		}
	}
	// o fechamento do pool é o último passo
	if strings.Index(logs, "http server shutting down") > strings.Index(logs, "postgres pool closed") {
		t.Error("o pool deve fechar depois do servidor HTTP")
	}
}
