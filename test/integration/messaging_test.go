//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

// requestMsg monta o corpo de WagerTransactionRequested para a operação.
func requestMsg(w testWallet, o op, messageID string) map[string]any {
	if o.provider == "" {
		o.provider = "provider-a"
	}
	if o.round == "" {
		o.round = "round-1"
	}
	if o.key == "" {
		o.key = o.provider + ":" + o.external
	}
	data := map[string]any{
		"providerId": o.provider, "externalTransactionId": o.external, "idempotencyKey": o.key,
		"playerId": w.player, "walletId": w.id, "roundId": o.round, "gameId": "game-1",
		"kind": o.kind, "money": money(o.amount),
	}
	if o.ref != "" {
		data["referenceExternalTransactionId"] = o.ref
	}
	return map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	}
}

func sendOp(t *testing.T, q *queues, w testWallet, o op, messageID string) {
	t.Helper()
	q.send(t, w.id, messageID, requestMsg(w, o, messageID))
}

func waitQueueEmpty(t *testing.T, queueURL string, wait time.Duration) {
	t.Helper()
	eventually(t, wait, "fila esvaziar", func() bool {
		v, f := queueDepth(t, queueURL)
		return v == 0 && f == 0
	})
}

func TestSQSConsumerFlowAndDeduplication(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3) // três consumidores disputando a mesma fila
	w := openWallet(t, nodes[0], "100.00")

	// operação normal
	bet := op{external: "sqs-bet-" + w.id, kind: "BET", amount: "30.00"}
	sendOp(t, q, w, bet, "msg-"+uuid.NewString())
	eventually(t, 20*time.Second, "aposta via SQS ser processada", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", bet.external)
		return s == "PROCESSED"
	})
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if bal := dbBalance(t, w.id); bal != 7000 {
		t.Fatalf("saldo = %d", bal)
	}

	// o mesmo messageId reentregue (deduplicação do broker contornada com outro dedup id)
	dupID := "msg-dup-" + uuid.NewString()
	rejectOp := op{external: "sqs-dup-" + w.id, kind: "BET", amount: "10.00"}
	body := requestMsg(w, rejectOp, dupID)
	q.send(t, w.id, "dedup-1-"+dupID, body)
	q.send(t, w.id, "dedup-2-"+dupID, body)
	q.send(t, w.id, "dedup-3-"+dupID, body)
	eventually(t, 20*time.Second, "mensagem duplicada ser processada", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", rejectOp.external)
		return s == "PROCESSED"
	})
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if bal := dbBalance(t, w.id); bal != 6000 {
		t.Fatalf("saldo = %d: reentrega aplicou duas vezes", bal)
	}
	if n := dbCount(t, `SELECT count(*) FROM inbox_messages WHERE consumer_name = 'it-consumer' AND message_id = $1`, dupID); n != 1 {
		t.Fatalf("inbox = %d", n)
	}

	// a mesma operação com outro messageId: replay financeiro, sem novo efeito
	sendOp(t, q, w, bet, "msg-other-"+uuid.NewString())
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if bal := dbBalance(t, w.id); bal != 6000 {
		t.Fatalf("saldo = %d após replay por SQS", bal)
	}

	// cruzando HTTP: replay da operação que entrou por SQS
	replay, _ := submit(t, nodes[1], w, bet)
	mustStatus(t, replay, http.StatusOK)
	if !replay.boolean("idempotentReplay") || replay.str("balance", "amount") != "70.00" {
		t.Fatalf("replay HTTP: %s", replay.raw)
	}
	// e o contrário: operação que entrou por HTTP, reenviada por SQS
	httpOp := op{external: "http-first-" + w.id, kind: "BET", amount: "5.00"}
	mustStatus(t, mustSubmit(t, nodes[2], w, httpOp), http.StatusOK)
	sendOp(t, q, w, httpOp, "msg-cross-"+uuid.NewString())
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if bal := dbBalance(t, w.id); bal != 5500 {
		t.Fatalf("saldo = %d após HTTP+SQS", bal)
	}

	// rejeição de negócio é terminal: a mensagem sai da fila, não vai para a DLQ
	broke := op{external: "sqs-broke-" + w.id, kind: "BET", amount: "9999.00"}
	sendOp(t, q, w, broke, "msg-broke-"+uuid.NewString())
	eventually(t, 20*time.Second, "rejeição ser registrada", func() bool {
		s, code, _ := dbTxStatus(t, "provider-a", broke.external)
		return s == "REJECTED" && code == "INSUFFICIENT_FUNDS"
	})
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatalf("DLQ recebeu %d mensagens por uma rejeição de negócio", v+f)
	}
	assertLedgerMatchesBalance(t, w.id)
}

func TestInvalidAndConflictingMessagesGoToDLQ(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2)
	w := openWallet(t, nodes[0], "100.00")

	good := op{external: "dlq-good-" + w.id, kind: "BET", amount: "10.00"}
	sendOp(t, q, w, good, "msg-good-"+uuid.NewString())
	eventually(t, 20*time.Second, "mensagem válida", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", good.external)
		return s == "PROCESSED"
	})

	bad := []struct {
		name string
		body any
	}{
		{"não é JSON", "isto não é json"},
		{"tipo errado", map[string]any{"messageId": "m1", "type": "Outro", "occurredAt": time.Now().Format(time.RFC3339), "data": map[string]any{}}},
		{"sem messageId", requestMsg(w, op{external: "x1", kind: "BET", amount: "1.00"}, "")},
		{"valor com uma casa", requestMsg(w, op{external: "x2", kind: "BET", amount: "1.5"}, "m-x2-"+uuid.NewString())},
		{"OPENING", requestMsg(w, op{external: "x3", kind: "OPENING", amount: "1.00"}, "m-x3-"+uuid.NewString())},
		{"valor numérico", `{"messageId":"m-x4","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"x4","idempotencyKey":"k4","playerId":"` + w.player + `","walletId":"` + w.id + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":1.50,"currency":"BRL"}}}`},
		{"campo desconhecido", `{"messageId":"m-x5","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","extra":1,"data":{}}`},
		// mesma chave de idempotência, conteúdo diferente
		{"conflito de chave", requestMsg(w, op{external: good.external, key: "provider-a:" + good.external, kind: "BET", amount: "11.00"}, "m-conf-"+uuid.NewString())},
	}
	for i, b := range bad {
		q.send(t, w.id, "bad-"+string(rune('a'+i)), b.body)
	}
	got := drain(t, q.dlq, 40*time.Second, func(b []string) bool { return len(b) >= len(bad) })
	if len(got) != len(bad) {
		t.Fatalf("DLQ recebeu %d mensagens, quer %d", len(got), len(bad))
	}
	waitQueueEmpty(t, q.wager, 20*time.Second)
	if bal := dbBalance(t, w.id); bal != 9000 {
		t.Fatalf("mensagens inválidas alteraram o saldo: %d", bal)
	}
	// nenhuma delas deixa inbox nem transação
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE external_transaction_id IN ('x1','x2','x3','x4','x5')`); n != 0 {
		t.Fatalf("transações indevidas: %d", n)
	}
}

// Consumidor morre depois do commit e antes de remover a mensagem: a reentrega
// é tratada como duplicata, sem segundo débito.
func TestConsumerCrashAfterCommitIsRedeliveredSafely(t *testing.T) {
	q := newQueues(t)
	api := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, api, "100.00")

	crasher := startInstance(t, q, withEnv("FAULT_POINT", "consumer.after_commit", "RUN_OUTBOX", "false"))
	o := op{external: "crash-" + w.id, kind: "BET", amount: "40.00"}
	msgID := "msg-crash-" + uuid.NewString()
	sendOp(t, q, w, o, msgID)

	code, exited := crasher.waitExit(30 * time.Second)
	if !exited || code != 137 {
		t.Fatalf("o consumidor deveria ter morrido após o commit (exited=%v code=%d)\n%s", exited, code, crasher.out.String())
	}
	// o efeito já está commitado...
	if s, _, _ := dbTxStatus(t, "provider-a", o.external); s != "PROCESSED" {
		t.Fatalf("status = %s", s)
	}
	if n := dbCount(t, `SELECT count(*) FROM inbox_messages WHERE message_id = $1 AND completed_at IS NOT NULL`, msgID); n != 1 {
		t.Fatalf("inbox concluída = %d", n)
	}
	// ...mas a mensagem continua na fila
	if v, f := queueDepth(t, q.wager); v+f != 1 {
		t.Fatalf("a mensagem deveria continuar na fila (visíveis=%d, em voo=%d)", v, f)
	}

	// outro consumidor recebe a reentrega depois do visibility timeout
	startInstance(t, q, withEnv("RUN_OUTBOX", "false"))
	waitQueueEmpty(t, q.wager, 40*time.Second)
	if bal := dbBalance(t, w.id); bal != 6000 {
		t.Fatalf("saldo = %d: a reentrega foi aplicada de novo", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != 1 {
		t.Fatalf("débitos = %d", debits)
	}
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $1`, o.external); n != 1 {
		t.Fatalf("transações = %d", n)
	}
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatal("a reentrega não deve ir para a DLQ")
	}
}

type outboxRow struct {
	id        string
	attempts  int
	published bool
	lastError string
}

func outboxRows(t *testing.T, partition string) []outboxRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id::text, attempts, published_at IS NOT NULL, COALESCE(last_error, '')
		FROM outbox_events WHERE partition_key = $1 ORDER BY seq`, partition)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.attempts, &r.published, &r.lastError); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func eventIDs(bodies []string) []string {
	var ids []string
	for _, b := range bodies {
		var e struct {
			EventID string `json:"eventId"`
		}
		_ = json.Unmarshal([]byte(b), &e)
		ids = append(ids, e.EventID)
	}
	return ids
}

// O publisher morre entre publicar e confirmar na outbox: outro publisher
// assume o evento (após o lease) e republica com o mesmo eventId.
func TestOutboxRecoversAfterPublisherCrash(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false"))
	w := openWallet(t, writer, "100.00")
	if rows := outboxRows(t, w.id); len(rows) != 2 || rows[0].published {
		t.Fatalf("outbox inicial: %+v", rows)
	}

	// morre ao publicar, possivelmente antes de abrir a porta HTTP
	crasher := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "FAULT_POINT", "outbox.after_publish"), noWait)
	if code, exited := crasher.waitExit(30 * time.Second); !exited || code != 137 {
		t.Fatalf("publisher deveria ter morrido após publicar (exited=%v code=%d)", exited, code)
	}
	// publicado no broker, mas a confirmação não aconteceu
	if rows := outboxRows(t, w.id); rows[0].published {
		t.Fatalf("evento não deveria estar confirmado: %+v", rows)
	}
	if v, f := queueDepth(t, q.events); v+f < 1 {
		t.Fatal("o evento deveria ter chegado ao broker antes da queda")
	}

	startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	eventually(t, 40*time.Second, "outro publisher assumir os eventos", func() bool {
		for _, r := range outboxRows(t, w.id) {
			if !r.published {
				return false
			}
		}
		return true
	})
	want := map[string]bool{}
	for _, r := range outboxRows(t, w.id) {
		want[r.id] = true
	}
	got := drain(t, q.events, 15*time.Second, nil)
	seen := map[string]bool{}
	for _, id := range eventIDs(got) {
		if !want[id] {
			t.Fatalf("eventId desconhecido no broker: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("eventos distintos no broker = %d, quer %d", len(seen), len(want))
	}
}

// Três publishers disputando a mesma outbox: cada evento é publicado uma vez,
// na ordem de cada carteira.
func TestConcurrentPublishersShareTheOutbox(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false", "RUN_RESOLVER", "false"))

	const wallets = 25
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, writer, "50.00")
		mustStatus(t, mustSubmit(t, writer, ws[i], op{kind: "BET", amount: "5.00"}), http.StatusOK)
	}
	total := wallets * 4 // abertura (2 eventos) + aposta (2 eventos)

	startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false", "RUN_RESOLVER", "false", "OUTBOX_BATCH_SIZE", "7"))

	eventually(t, 60*time.Second, "todos os eventos publicados", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = ANY($1) AND published_at IS NOT NULL`, partitions(ws)) == total
	})
	if n := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = ANY($1) AND attempts <> 1`, partitions(ws)); n != 0 {
		t.Fatalf("%d eventos foram reservados mais de uma vez", n)
	}

	var mu sync.Mutex
	var all []string
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ { // leitura paralela, a fila FIFO reparte por grupo
		wg.Add(1)
		go func() {
			defer wg.Done()
			part := drain(t, q.events, 12*time.Second, nil)
			mu.Lock()
			all = append(all, part...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	counts := map[string]int{}
	for _, id := range eventIDs(all) {
		counts[id]++
	}
	if len(counts) != total {
		t.Fatalf("eventos distintos = %d, quer %d", len(counts), total)
	}
	for id, c := range counts {
		if c != 1 {
			t.Fatalf("evento %s publicado %d vezes", id, c)
		}
	}
}

func partitions(ws []testWallet) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = w.id
	}
	sort.Strings(out)
	return out
}

// Se o SQS fica indisponível, os eventos permanecem na outbox, com tentativas
// e backoff, e saem quando o broker volta.
func TestOutboxSurvivesSQSOutage(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false"))
	startInstance(t, q, withEnv("RUN_CONSUMER", "false")) // relay saudável, ainda sem eventos

	if _, err := sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(q.events)}); err != nil {
		t.Fatal(err)
	}
	w := openWallet(t, writer, "10.00") // o relay passa a falhar ao publicar
	eventually(t, 30*time.Second, "falhas de publicação registradas", func() bool {
		rows := outboxRows(t, w.id)
		return len(rows) == 2 && rows[0].attempts >= 2 && rows[0].lastError != "" && !rows[0].published
	})

	// o broker volta
	_, err := sqsClient.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName:  aws.String(q.eventsName),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 40*time.Second, "eventos publicados após o retorno do SQS", func() bool {
		for _, r := range outboxRows(t, w.id) {
			if !r.published {
				return false
			}
		}
		return true
	})
	if got := drain(t, q.events, 10*time.Second, func(b []string) bool { return len(b) >= 2 }); len(got) != 2 {
		t.Fatalf("mensagens recebidas = %d", len(got))
	}
}

// Conexões do banco derrubadas no meio do tráfego: nenhuma operação é aplicada
// duas vezes e todas terminam com o resultado correto após novas tentativas.
func TestSurvivesDatabaseConnectionLoss(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "1000.00")

	stop := make(chan struct{})
	var killer sync.WaitGroup
	killer.Add(1)
	go func() {
		defer killer.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(150 * time.Millisecond):
				_, _ = pool.Exec(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
					WHERE datname = current_database() AND pid <> pg_backend_pid() AND application_name = ''
					AND usename = current_user AND backend_type = 'client backend'
					AND query NOT LIKE '%pg_terminate_backend%'`)
			}
		}
	}()

	const ops = 20
	var attemptsMu sync.Mutex
	failures := 0
	parallel(ops, func(i int) {
		o := op{external: "flaky-" + w.id + "-" + string(rune('a'+i)), kind: "BET", amount: "10.00"}
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			r, _ := submit(t, nodes[i%2], w, o)
			if r.status == http.StatusOK {
				return
			}
			if r.status < 500 {
				t.Errorf("status inesperado %d: %s", r.status, r.raw)
				return
			}
			attemptsMu.Lock()
			failures++
			attemptsMu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
		t.Errorf("operação %d não terminou", i)
	})
	close(stop)
	killer.Wait()

	if bal := dbBalance(t, w.id); bal != 80000 {
		t.Fatalf("saldo = %d, quer 80000 (20 apostas de 10.00 exatamente uma vez)", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != ops {
		t.Fatalf("débitos = %d", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
	t.Logf("tentativas repetidas por indisponibilidade transitória: %d", failures)
	if !strings.Contains(nodes[0].out.String()+nodes[1].out.String(), "transient failure") && failures > 0 {
		t.Log("as falhas foram devolvidas como 5xx sem log de falha transitória")
	}
}

// Uma pendência sobrevive à morte do processo e é retomada por outra instância.
func TestPendingReferenceSurvivesRestart(t *testing.T) {
	q := newQueues(t)
	a := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, a, "100.00")

	betExt := "restart-bet-" + w.id
	rollback, o := submit(t, a, w, op{kind: "ROLLBACK", amount: "20.00", ref: betExt})
	mustStatus(t, rollback, http.StatusAccepted)
	a.kill()

	b := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	mustStatus(t, mustSubmit(t, b, w, op{external: betExt, kind: "BET", amount: "20.00"}), http.StatusOK)
	eventually(t, 30*time.Second, "pendência retomada pela nova instância", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
	assertLedgerMatchesBalance(t, w.id)
}
