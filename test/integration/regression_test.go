//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func scrape(t *testing.T, inst *instance) string {
	t.Helper()
	resp, err := http.Get(inst.base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

// lockWallet segura um lock exclusivo na linha da carteira, simulando uma
// dependência lenta/indisponível para quem precisa escrever nela.
func lockWallet(t *testing.T, walletID string) (release func()) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, walletID); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(release)
	return release
}

// A mesma operação chegando ao mesmo tempo por HTTP (três instâncias) e por SQS
// produz um único efeito e respostas coerentes.
func TestSameOperationSimultaneouslyViaHTTPAndSQS(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3)
	w := openWallet(t, nodes[0], "1000.00")

	const n = 15
	ops := make([]op, n)
	for i := range ops {
		ops[i] = op{external: "hs-" + w.id + "-" + uuid.NewString()[:8], kind: "BET", amount: "10.00"}
		ops[i].key = "provider-a:" + ops[i].external
	}
	var mu sync.Mutex
	byOp := make([][]response, n)
	parallel(n*4, func(j int) {
		i, k := j/4, j%4
		if k == 3 {
			sendOp(t, q, w, ops[i], "msg-hs-"+uuid.NewString())
			return
		}
		r, _ := submit(t, nodes[k], w, ops[i])
		mu.Lock()
		byOp[i] = append(byOp[i], r)
		mu.Unlock()
	})

	for i, rs := range byOp {
		var txID, bal string
		for _, r := range rs {
			mustStatus(t, r, http.StatusOK)
			if txID == "" {
				txID, bal = r.str("transactionId"), r.str("balance", "amount")
			} else if r.str("transactionId") != txID || r.str("balance", "amount") != bal {
				t.Fatalf("op %d: respostas divergentes (%s/%s vs %s/%s)", i, txID, bal, r.str("transactionId"), r.str("balance", "amount"))
			}
		}
	}
	waitQueueEmpty(t, q.wager, 30*time.Second)
	if bal := dbBalance(t, w.id); bal != 85000 {
		t.Fatalf("saldo = %d, quer 85000", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != n {
		t.Fatalf("débitos = %d, quer %d", debits, n)
	}
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.id); n != 15 {
		t.Fatalf("transações BET = %d", n)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// SIGTERM com o consumidor trabalhando: sai com código 0 e nada se perde nem se duplica.
func TestSIGTERMWhileConsumingLosesNothing(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false"))
	const wallets, perWallet = 10, 25
	run := uuid.NewString()[:8]
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, writer, "100.00")
	}
	parallel(wallets*perWallet, func(i int) {
		w := ws[i%wallets]
		sendOp(t, q, w, op{external: "term-" + w.id + "-" + pad(i/wallets), kind: "BET", amount: "1.00"}, "msg-term-"+run+"-"+uuid.NewString())
	})

	a := startInstance(t, q, withEnv("RUN_OUTBOX", "false"), noWait)
	processed := func() int {
		return dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND kind = 'BET' AND status = 'PROCESSED'`, partitions(ws))
	}
	eventually(t, 60*time.Second, "o consumidor começar a processar", func() bool { return processed() >= 5 })
	atTerm := processed()
	if code := a.terminate(25 * time.Second); code != 0 {
		t.Fatalf("exit code = %d\n%s", code, a.out.String())
	}
	t.Logf("processadas antes do SIGTERM: %d de %d", atTerm, wallets*perWallet)
	afterStop := processed()

	startInstance(t, q, withEnv("RUN_OUTBOX", "false"))
	eventually(t, 90*time.Second, "todas as mensagens processadas", func() bool { return processed() == wallets*perWallet })
	waitQueueEmpty(t, q.wager, 60*time.Second)
	_ = afterStop

	for _, w := range ws {
		if bal := dbBalance(t, w.id); bal != 7500 {
			t.Fatalf("carteira %s: saldo %d, quer 7500", w.id, bal)
		}
		if _, debits, _ := dbLedger(t, w.id); debits != perWallet {
			t.Fatalf("carteira %s: %d débitos", w.id, debits)
		}
		assertLedgerMatchesBalance(t, w.id)
	}
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatalf("DLQ recebeu %d mensagens", v+f)
	}
	if n := dbCount(t, `SELECT count(*) FROM inbox_messages WHERE consumer_name = 'it-consumer' AND completed_at IS NOT NULL
		AND message_id LIKE 'msg-term-' || $1 || '-%'`, run); n != wallets*perWallet {
		t.Fatalf("inbox concluída = %d", n)
	}
}

func pad(i int) string { return fmt.Sprintf("%03d", i) }

var slowConsumer = []string{
	"CONSUMER_PROCESS_TIMEOUT", "2s", "CONSUMER_VISIBILITY_TIMEOUT", "5s",
	"CONSUMER_RETRY_BASE_DELAY", "1s", "CONSUMER_MAX_ATTEMPTS", "3", "RUN_OUTBOX", "false",
}

// Falha transitória (a carteira está travada): a mensagem volta com backoff e,
// quando o problema passa, é processada uma única vez.
func TestTransientFailureIsRetriedThenSucceeds(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv(slowConsumer...))
	w := openWallet(t, inst, "100.00")

	release := lockWallet(t, w.id)
	o := op{external: "transient-" + w.id, kind: "BET", amount: "10.00"}
	sendOp(t, q, w, o, "msg-transient-"+uuid.NewString())
	time.Sleep(3 * time.Second) // ao menos uma tentativa estoura o timeout
	if _, _, found := dbTxStatus(t, "provider-a", o.external); found {
		t.Fatal("nada pode ser commitado enquanto a carteira está travada")
	}
	release()

	eventually(t, 40*time.Second, "mensagem ser processada após o problema passar", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "PROCESSED"
	})
	waitQueueEmpty(t, q.wager, 30*time.Second)
	if bal := dbBalance(t, w.id); bal != 9000 {
		t.Fatalf("saldo = %d", bal)
	}
	if !strings.Contains(inst.out.String(), "transient failure, will retry") {
		t.Fatal("o consumidor deveria ter registrado o retry com backoff")
	}
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatal("a DLQ não deve receber uma mensagem que acabou processada")
	}
}

// Tentativas esgotadas chegam à DLQ, sem nenhum efeito financeiro.
func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv(slowConsumer...))
	w := openWallet(t, inst, "100.00")

	release := lockWallet(t, w.id)
	o := op{external: "exhaust-" + w.id, kind: "BET", amount: "10.00"}
	sendOp(t, q, w, o, "msg-exhaust-"+uuid.NewString())

	got := drain(t, q.dlq, 90*time.Second, func(b []string) bool { return len(b) >= 1 })
	release()
	if len(got) != 1 {
		t.Fatalf("DLQ recebeu %d mensagens, quer 1\n%s", len(got), inst.out.String())
	}
	waitQueueEmpty(t, q.wager, 30*time.Second)
	if _, _, found := dbTxStatus(t, "provider-a", o.external); found {
		t.Fatal("a mensagem que foi para a DLQ não pode ter efeito")
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
	if !strings.Contains(inst.out.String(), "retries exhausted") {
		t.Fatal("o motivo da DLQ deveria estar no log")
	}
}

// O mesmo messageId com outro conteúdo é conflito (hash da inbox), não duplicata.
func TestMessageIDReusedWithDifferentContentGoesToDLQ(t *testing.T) {
	q := newQueues(t)
	startInstance(t, q)
	w := openWallet(t, startInstance(t, q, withEnv("RUN_CONSUMER", "false")), "100.00")

	id := "msg-reuse-" + uuid.NewString()
	first := op{external: "reuse-1-" + w.id, kind: "BET", amount: "5.00"}
	q.send(t, w.id, "d1-"+id, requestMsg(w, first, id))
	eventually(t, 30*time.Second, "primeira mensagem", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", first.external)
		return s == "PROCESSED"
	})
	second := op{external: "reuse-2-" + w.id, kind: "BET", amount: "7.00"}
	q.send(t, w.id, "d2-"+id, requestMsg(w, second, id))

	if got := drain(t, q.dlq, 40*time.Second, func(b []string) bool { return len(b) >= 1 }); len(got) != 1 {
		t.Fatalf("DLQ = %d", len(got))
	}
	if _, _, found := dbTxStatus(t, "provider-a", second.external); found {
		t.Fatal("o conteúdo conflitante não pode ser aplicado")
	}
	if bal := dbBalance(t, w.id); bal != 9500 {
		t.Fatalf("saldo = %d", bal)
	}
}

func TestMetricsReflectWhatHappened(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q)
	w := openWallet(t, inst, "10.00")

	bet := op{external: "m-" + w.id, kind: "BET", amount: "4.00"}
	mustStatus(t, mustSubmit(t, inst, w, bet), http.StatusOK)
	mustStatus(t, mustSubmit(t, inst, w, bet), http.StatusOK)                                               // replay HTTP
	mustStatus(t, mustSubmit(t, inst, w, op{kind: "BET", amount: "99.00"}), http.StatusUnprocessableEntity) // rejeição
	q.send(t, w.id, "bad-metric", "lixo que não é JSON")
	dup := "msg-m-" + uuid.NewString()
	o := op{external: "m2-" + w.id, kind: "BET", amount: "1.00"}
	q.send(t, w.id, "m-a", requestMsg(w, o, dup))
	eventually(t, 30*time.Second, "mensagem processada", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "PROCESSED"
	})
	q.send(t, w.id, "m-b", requestMsg(w, o, "msg-m2-"+uuid.NewString())) // replay por SQS
	waitQueueEmpty(t, q.wager, 30*time.Second)
	eventually(t, 30*time.Second, "outbox esvaziar", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND published_at IS NULL`, w.id) == 0
	})
	time.Sleep(time.Second)

	text := scrape(t, inst)
	for _, want := range []string{
		`wager_transactions_total{failure_code="",kind="BET",status="PROCESSED"}`,
		`wager_transactions_total{failure_code="INSUFFICIENT_FUNDS",kind="BET",status="REJECTED"}`,
		`wager_duplicates_total{source="http"} 1`,
		`wager_duplicates_total{source="sqs"} 1`,
		`wager_dlq_total{reason="invalid_message"} 1`,
		`wager_processing_duration_seconds_count{source="http"}`,
		`wager_outbox_published_total`,
		`wager_outbox_lag_seconds`,
		`wager_outbox_pending `,
		`wager_concurrency_conflicts_total`,
		`wager_reconciliation_divergences_total 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("métrica ausente ou com valor inesperado: %s", want)
		}
	}
}

// Vários resolvers (três processos) processam cada pendência exatamente uma vez.
func TestConcurrentResolversProcessEachPendingOnce(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "1000.00")

	const n = 12
	bets := make([]string, n)
	for i := range bets {
		bets[i] = "pend-bet-" + w.id + "-" + pad(i)
		r, _ := submit(t, nodes[i%3], w, op{kind: "REFUND", amount: "10.00", ref: bets[i]})
		mustStatus(t, r, http.StatusAccepted)
	}
	parallel(n, func(i int) {
		mustStatus(t, mustSubmit(t, nodes[i%3], w, op{external: bets[i], kind: "BET", amount: "10.00"}), http.StatusOK)
	})
	eventually(t, 60*time.Second, "todas as pendências resolvidas", func() bool {
		return dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'REFUND' AND status = 'PROCESSED'`, w.id) == n
	})
	if bal := dbBalance(t, w.id); bal != 100000 {
		t.Fatalf("saldo = %d, quer 100000", bal)
	}
	if _, debits, credits := dbLedger(t, w.id); debits != n || credits != n+1 {
		t.Fatalf("ledger: %d débitos / %d créditos", debits, credits)
	}
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND status IN ('PENDING','PENDING_REFERENCE')`, w.id); n != 0 {
		t.Fatalf("restaram %d pendências", n)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// REFUND e ROLLBACK da mesma aposta, simultâneos em instâncias diferentes: só um vence.
func TestRefundAndRollbackRaceAcrossInstances(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "500.00")

	const n = 20
	bets := make([]string, n)
	for i := range bets {
		bets[i] = "race-bet-" + w.id + "-" + pad(i)
		mustStatus(t, mustSubmit(t, nodes[0], w, op{external: bets[i], kind: "BET", amount: "10.00"}), http.StatusOK)
	}
	var mu sync.Mutex
	wins, already := 0, 0
	parallel(n*2, func(j int) {
		i := j / 2
		kind := "REFUND"
		if j%2 == 1 {
			kind = "ROLLBACK"
		}
		r, _ := submit(t, nodes[(j+i)%3], w, op{kind: kind, amount: "10.00", ref: bets[i]})
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.status == http.StatusOK:
			wins++
		case r.status == http.StatusUnprocessableEntity && r.str("failureCode") == "ALREADY_REVERSED":
			already++
		default:
			t.Errorf("status %d: %s", r.status, r.raw)
		}
	})
	if wins != n || already != n {
		t.Fatalf("vencedores=%d rejeitadas=%d, quer %d/%d", wins, already, n, n)
	}
	if bal := dbBalance(t, w.id); bal != 50000 {
		t.Fatalf("saldo = %d: a aposta foi devolvida mais de uma vez ou nenhuma", bal)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Estouro aritmético vira falha permanente auditável, sem alterar saldo nem ledger.
func TestBalanceOverflowIsAPermanentAuditableFailure(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "92233720368547758.07")

	o := op{external: "ovf-" + w.id, kind: "WIN", amount: "0.01"}
	r, o := submit(t, inst, w, o)
	mustStatus(t, r, http.StatusInternalServerError)
	if r.str("status") != "FAILED" || r.str("failureCode") != "INTERNAL_ERROR" {
		t.Fatalf("resposta: %s", r.raw)
	}
	again, _ := submit(t, inst, w, o)
	if again.status != http.StatusInternalServerError || !again.boolean("idempotentReplay") {
		t.Fatalf("replay da falha: %d %s", again.status, again.raw)
	}
	if s, code, _ := dbTxStatus(t, "provider-a", o.external); s != "FAILED" || code != "INTERNAL_ERROR" {
		t.Fatalf("auditoria: %s %s", s, code)
	}
	if entries, _, _ := dbLedger(t, w.id); entries != 1 {
		t.Fatalf("ledger = %d", entries)
	}
	rec := call(t, inst.base, "POST", "/wallets/"+w.id+"/reconciliation", internalTok(t), "", nil)
	if !rec.boolean("consistent") || rec.str("storedBalance", "amount") != "92233720368547758.07" {
		t.Fatalf("reconciliação: %s", rec.raw)
	}
	// uma aposta normal na mesma carteira continua funcionando
	mustStatus(t, mustSubmit(t, inst, w, op{kind: "BET", amount: "0.01"}), http.StatusOK)
}

func TestHTTPInputNormalizationAndLimits(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	// UUID em maiúsculas é a mesma operação: replay, não conflito
	o := op{external: "norm-" + w.id, kind: "BET", amount: "3.00"}
	first, o := submit(t, inst, w, o)
	mustStatus(t, first, http.StatusOK)
	upper := w
	upper.id, upper.player = strings.ToUpper(w.id), strings.ToUpper(w.player)
	replay := call(t, inst.base, "POST", "/wagering/transactions", providerA(t), o.key, upper.req(o))
	mustStatus(t, replay, http.StatusOK)
	if !replay.boolean("idempotentReplay") {
		t.Fatalf("UUID maiúsculo deveria ser replay: %s", replay.raw)
	}

	// chave de idempotência gigante
	long := strings.Repeat("k", 300)
	r := call(t, inst.base, "POST", "/wagering/transactions", providerA(t), long, w.req(op{kind: "BET", amount: "1.00"}))
	mustStatus(t, r, http.StatusBadRequest)

	// espaços nas pontas não são normalizados em silêncio
	r = call(t, inst.base, "POST", "/wagering/transactions", providerA(t), "provider-a:sp", w.req(op{external: " sp-" + w.id, kind: "BET", amount: "1.00"}))
	mustStatus(t, r, http.StatusBadRequest)

	// corpo gigante
	big := `{"providerId":"provider-a","gameId":"` + strings.Repeat("x", 70<<10) + `"}`
	r = call(t, inst.base, "POST", "/wagering/transactions", providerA(t), "k-big", big)
	mustStatus(t, r, http.StatusRequestEntityTooLarge)

	// método não permitido e rota desconhecida
	mustStatus(t, call(t, inst.base, "DELETE", "/wallets/"+w.id, internalTok(t), "", nil), http.StatusMethodNotAllowed)
	mustStatus(t, call(t, inst.base, "GET", "/nada", internalTok(t), "", nil), http.StatusNotFound)

	if bal := dbBalance(t, w.id); bal != 9700 {
		t.Fatalf("saldo = %d", bal)
	}
}
