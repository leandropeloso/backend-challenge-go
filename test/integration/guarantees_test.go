//go:build integration

package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

// Um PENDING commitado (por exemplo, por uma versão com aceite assíncrono que
// morreu antes de executar) é retomado por outra instância, sem duplicar efeito.
func TestCommittedPendingIsResumedByAnotherInstance(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_RESOLVER", "false", "RUN_OUTBOX", "false"))
	w := openWallet(t, writer, "100.00")

	ext := "pending-" + w.id
	if _, err := pool.Exec(context.Background(), `INSERT INTO wager_transactions
		(id, origin, kind, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
		 round_id, game_id, amount, currency, status, created_at, updated_at)
		VALUES ($1, 'EXTERNAL', 'BET', 'provider-a', $2, $3, 'hash-from-an-earlier-accept', $4, $5, 'r', 'g', 3000, 'BRL',
		        'PENDING', now() - interval '1 minute', now() - interval '1 minute')`,
		uuid.New(), ext, "provider-a:"+ext, w.id, w.player); err != nil {
		t.Fatal(err)
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatal("o PENDING ainda não pode ter efeito")
	}

	// só agora sobem os workers: duas instâncias disputam a retomada
	startCluster(t, q, 2, withEnv("RUN_CONSUMER", "false"))
	eventually(t, 30*time.Second, "PENDING ser retomado", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", ext)
		return s == "PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 7000 {
		t.Fatalf("saldo = %d, quer 7000", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != 1 {
		t.Fatalf("débitos = %d", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Uma reversão recebida por SQS antes da referência é aceita (a mensagem sai
// da fila) assim que a pendência está persistida; o worker assume a continuidade.
func TestSQSReversalBeforeReferenceIsAckedAndResolved(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2)
	w := openWallet(t, nodes[0], "100.00")

	betExt := "sqs-late-bet-" + w.id
	refund := op{external: "sqs-early-refund-" + w.id, kind: "REFUND", amount: "15.00", ref: betExt}
	sendOp(t, q, w, refund, "msg-early-"+uuid.NewString())
	eventually(t, 30*time.Second, "pendência persistida", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", refund.external)
		return s == "PENDING_REFERENCE"
	})
	waitQueueEmpty(t, q.wager, 30*time.Second) // concluída na entrada, não presa na fila
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatal("pendência não é erro: a DLQ deve ficar vazia")
	}

	sendOp(t, q, w, op{external: betExt, kind: "BET", amount: "15.00"}, "msg-late-"+uuid.NewString())
	eventually(t, 40*time.Second, "reversão resolvida", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", refund.external)
		return s == "PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// A reconciliação reporta a divergência (resposta, log e métrica) sem alterar o saldo.
// A divergência é forçada desabilitando temporariamente os triggers da tabela.
func TestReconciliationReportsDivergenceWithoutFixingIt(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "50.00")

	tamper := func(delta int64) {
		t.Helper()
		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		for _, stmt := range []string{
			`ALTER TABLE wallets DISABLE TRIGGER USER`,
		} {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + $2 WHERE id = $1`, w.id, delta); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE wallets ENABLE TRIGGER USER`); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	tamper(123)
	t.Cleanup(func() {
		if dbBalance(t, w.id) != 5000 {
			tamper(5000 - dbBalance(t, w.id))
		}
	})

	rec := call(t, inst.base, "POST", "/wallets/"+w.id+"/reconciliation", internalTok(t), "", nil)
	mustStatus(t, rec, http.StatusOK)
	if rec.boolean("consistent") || rec.str("storedBalance", "amount") != "51.23" ||
		rec.str("calculatedBalance", "amount") != "50.00" || rec.str("difference", "amount") != "1.23" {
		t.Fatalf("a divergência deveria aparecer na resposta: %s", rec.raw)
	}
	if bal := dbBalance(t, w.id); bal != 5123 {
		t.Fatalf("a reconciliação não pode alterar o saldo (agora %d)", bal)
	}
	if !strings.Contains(scrape(t, inst), "wager_reconciliation_divergences_total 1") {
		t.Error("a métrica de divergências deveria ser 1")
	}
	if !strings.Contains(inst.out.String(), "wallet reconciliation divergence") {
		t.Error("a divergência deveria ser registrada no log")
	}
}

// Atomicidade: uma falha no último passo (a outbox) desfaz saldo, ledger, estado
// da transação e inbox; repetir depois aplica o efeito uma única vez.
func TestFailureAtTheLastStepLeavesNoPartialState(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2)
	w := openWallet(t, nodes[0], "100.00")
	ctx := context.Background()
	sfx := strings.ReplaceAll(uuid.NewString(), "-", "")[:10]

	setup := []string{
		`CREATE FUNCTION it_fail_` + sfx + `() RETURNS trigger AS $$ BEGIN
			IF NEW.partition_key = '` + w.id + `' THEN RAISE EXCEPTION 'injected failure at the last step'; END IF;
			RETURN NEW; END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER it_fail_` + sfx + ` BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION it_fail_` + sfx + `()`,
	}
	for _, s := range setup {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	armed := true
	disarm := func() {
		if !armed {
			return
		}
		armed = false
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS it_fail_`+sfx+` ON outbox_events`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS it_fail_`+sfx+`()`)
	}
	t.Cleanup(disarm)

	versionBefore := dbVersion(t, w.id)
	outboxBefore := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, w.id)

	// HTTP: 500 e nenhum rastro
	httpOp := op{external: "atomic-http-" + w.id, kind: "BET", amount: "10.00"}
	r, httpOp := submit(t, nodes[0], w, httpOp)
	mustStatus(t, r, http.StatusInternalServerError)
	// SQS: a mensagem não é concluída (vai para retry), sem inbox nem efeito
	sqsMsg := "msg-atomic-" + uuid.NewString()
	sqsOp := op{external: "atomic-sqs-" + w.id, kind: "BET", amount: "20.00"}
	q.send(t, w.id, sqsMsg, requestMsg(w, sqsOp, sqsMsg))
	time.Sleep(3 * time.Second)

	assertClean := func(when string) {
		t.Helper()
		if bal := dbBalance(t, w.id); bal != 10000 {
			t.Fatalf("%s: saldo = %d", when, bal)
		}
		if v := dbVersion(t, w.id); v != versionBefore {
			t.Fatalf("%s: versão mudou %d → %d", when, versionBefore, v)
		}
		if entries, debits, _ := dbLedger(t, w.id); entries != 1 || debits != 0 {
			t.Fatalf("%s: ledger %d/%d", when, entries, debits)
		}
		for _, ext := range []string{httpOp.external, sqsOp.external} {
			if _, _, found := dbTxStatus(t, "provider-a", ext); found {
				t.Fatalf("%s: transação %s ficou gravada", when, ext)
			}
		}
		if n := dbCount(t, `SELECT count(*) FROM inbox_messages WHERE message_id = $1`, sqsMsg); n != 0 {
			t.Fatalf("%s: inbox ficou gravada", when)
		}
		if n := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, w.id); n != outboxBefore {
			t.Fatalf("%s: eventos de outbox parciais (%d → %d)", when, outboxBefore, n)
		}
	}
	assertClean("com a falha armada")

	// a falha passa: HTTP repetido e SQS reentregue aplicam cada operação uma vez
	disarm()
	again, _ := submit(t, nodes[1], w, httpOp)
	mustStatus(t, again, http.StatusOK)
	if again.boolean("idempotentReplay") {
		t.Fatal("a primeira execução bem-sucedida não é replay")
	}
	eventually(t, 60*time.Second, "mensagem reentregue ser processada", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", sqsOp.external)
		return s == "PROCESSED"
	})
	waitQueueEmpty(t, q.wager, 30*time.Second)
	if bal := dbBalance(t, w.id); bal != 7000 {
		t.Fatalf("saldo final = %d, quer 7000", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != 2 {
		t.Fatalf("débitos = %d", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// /health/ready reflete o estado do SQS; /health/live continua verdadeiro.
func TestReadinessReflectsSQSAvailability(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))

	if r := call(t, inst.base, "GET", "/health/ready", "", "", nil); r.status != http.StatusOK {
		t.Fatalf("ready = %d: %s", r.status, r.raw)
	}
	if _, err := sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(q.wager)}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "readiness indicar o SQS indisponível", func() bool {
		r := call(t, inst.base, "GET", "/health/ready", "", "", nil)
		checks, _ := r.body["checks"].(map[string]any)
		return r.status == http.StatusServiceUnavailable && checks["sqs"] == "unavailable" && checks["postgres"] == "ok"
	})
	if r := call(t, inst.base, "GET", "/health/live", "", "", nil); r.status != http.StatusOK {
		t.Fatalf("live = %d: o processo continua vivo", r.status)
	}
	_, err := sqsClient.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName:  aws.String(q.wagerName),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "readiness voltar ao normal", func() bool {
		return call(t, inst.base, "GET", "/health/ready", "", "", nil).status == http.StatusOK
	})
}
