//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type scripted struct {
	wallet    int
	o         op
	viaSQS    bool
	duplicate bool // reenvio de uma operação anterior (mesma chave e conteúdo)
}

func cents(c int) string { return fmt.Sprintf("%d.%02d", c/100, c%100) }

// buildScript gera, de forma determinística para a semente, uma sequência de
// operações com todos os tipos, referências corretas e incorretas e reenvios.
func buildScript(rng *rand.Rand, run string, wallets, total int) []scripted {
	type sent struct {
		external, kind string
		amount         int
	}
	history := make([][]sent, wallets)
	var out []scripted
	for len(out) < total {
		wi := rng.Intn(wallets)
		viaSQS := rng.Intn(100) < 30

		if n := len(out); n > 0 && rng.Intn(100) < 10 { // reenvio idêntico
			prev := out[rng.Intn(n)]
			if !prev.duplicate {
				out = append(out, scripted{wallet: prev.wallet, o: prev.o, viaSQS: rng.Intn(2) == 0, duplicate: true})
				continue
			}
		}
		roll := rng.Intn(100)
		ext := fmt.Sprintf("s-%s-%d-%d", run, wi, len(out))
		c := 100 + rng.Intn(6000)
		switch {
		case roll < 45:
			out = append(out, scripted{wi, op{external: ext, kind: "BET", amount: cents(c)}, viaSQS, false})
			history[wi] = append(history[wi], sent{ext, "BET", c})
		case roll < 60:
			out = append(out, scripted{wi, op{external: ext, kind: "WIN", amount: cents(c)}, viaSQS, false})
			history[wi] = append(history[wi], sent{ext, "WIN", c})
		case roll < 67:
			out = append(out, scripted{wi, op{external: ext, kind: "LOSS", amount: "0.00"}, viaSQS, false})
		default:
			h := history[wi]
			if len(h) == 0 {
				continue
			}
			ref := h[rng.Intn(len(h))]
			kind := "REFUND"
			if rng.Intn(2) == 0 {
				kind = "ROLLBACK"
			}
			amount := ref.amount
			if rng.Intn(100) < 8 { // valor errado de propósito
				amount++
			}
			out = append(out, scripted{wi, op{external: ext, kind: kind, amount: cents(amount), ref: ref.external}, viaSQS, false})
			if kind == "REFUND" || kind == "ROLLBACK" {
				history[wi] = append(history[wi], sent{ext, kind, amount})
			}
		}
	}
	return out
}

// Carga aleatória e concorrente por três processos, HTTP e SQS ao mesmo tempo.
// As invariantes são conferidas por consultas independentes da lógica da aplicação.
func TestRandomizedConcurrentWorkloadKeepsInvariants(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	q := newQueues(t)
	nodes := startCluster(t, q, 3)
	const wallets, total = 6, 300
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, nodes[i%3], "300.00")
	}
	script := buildScript(rng, uuid.NewString()[:8], wallets, total)
	rng.Shuffle(len(script), func(i, j int) { script[i], script[j] = script[j], script[i] })

	// reconciliações em paralelo à carga: sempre consistentes (snapshot único)
	stop := make(chan struct{})
	var recWG sync.WaitGroup
	recWG.Add(1)
	go func() {
		defer recWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(40 * time.Millisecond):
			}
			w := ws[i%wallets]
			r := call(t, nodes[i%3].base, "POST", "/wallets/"+w.id+"/reconciliation", internalTok(t), "", nil)
			if r.status != http.StatusOK || !r.boolean("consistent") {
				t.Errorf("reconciliação durante a carga: %d %s", r.status, r.raw)
				return
			}
		}
	}()

	type observed struct {
		status int
		txID   string
		code   string
	}
	var mu sync.Mutex
	httpSeen := map[string]observed{} // por chave
	var work sync.WaitGroup
	jobs := make(chan scripted)
	for i := 0; i < 12; i++ {
		work.Add(1)
		go func(worker int) {
			defer work.Done()
			for s := range jobs {
				w := ws[s.wallet]
				if s.viaSQS {
					sendOp(t, q, w, s.o, "msg-st-"+uuid.NewString())
					continue
				}
				r, o := submit(t, nodes[worker%3], w, s.o)
				if r.status >= 500 {
					t.Errorf("erro de servidor: %d %s", r.status, r.raw)
					continue
				}
				mu.Lock()
				prev, ok := httpSeen[o.external]
				cur := observed{r.status, r.str("transactionId"), r.str("failureCode")}
				if ok && prev.txID != cur.txID {
					t.Errorf("replay devolveu outra transação (%s vs %s)", prev.txID, cur.txID)
				}
				if ok && (prev.status == 200 || prev.status == 422) && prev.status != cur.status {
					t.Errorf("resultado terminal mudou entre replays: %d → %d (%s)", prev.status, cur.status, r.raw)
				}
				if !ok || cur.status == 200 || cur.status == 422 {
					httpSeen[o.external] = cur
				}
				mu.Unlock()
			}
		}(i)
	}
	for _, s := range script {
		jobs <- s
	}
	close(jobs)
	work.Wait()
	close(stop)
	recWG.Wait()

	// quiescência: sem pendências, fila vazia, outbox publicada
	eventually(t, 120*time.Second, "o sistema ficar quiescente", func() bool {
		v, f := queueDepth(t, q.wager)
		pending := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND status IN ('PENDING','PENDING_REFERENCE')`, partitions(ws))
		return v+f == 0 && pending == 0
	})
	eventually(t, 60*time.Second, "outbox publicada", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = ANY($1) AND published_at IS NULL`, partitions(ws)) == 0
	})
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatalf("a DLQ recebeu %d mensagens numa carga válida", v+f)
	}

	ctx := context.Background()
	processed := map[string]int{}
	for _, w := range ws {
		checkWalletInvariants(t, ctx, w)

		var proc, rej, tx int
		_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'PROCESSED'), count(*) FILTER (WHERE status = 'REJECTED'), count(*)
			FROM wager_transactions WHERE wallet_id = $1`, w.id).Scan(&proc, &rej, &tx)
		processed[w.id] = proc
		t.Logf("carteira %s: %d transações (%d processadas, %d rejeitadas)", w.id[:8], tx, proc, rej)

		// eventos: um por resultado, um por lançamento
		counts := map[string]int{}
		rows, err := pool.Query(ctx, `SELECT event_type, count(*) FROM outbox_events WHERE partition_key = $1 GROUP BY 1`, w.id)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var et string
			var n int
			_ = rows.Scan(&et, &n)
			counts[et] = n
		}
		rows.Close()
		var entries int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id).Scan(&entries)
		if counts["WagerTransactionProcessed"] != proc {
			t.Errorf("carteira %s: %d eventos Processed, %d transações processadas", w.id, counts["WagerTransactionProcessed"], proc)
		}
		if counts["WagerTransactionRejected"] != rej {
			t.Errorf("carteira %s: %d eventos Rejected, %d rejeitadas", w.id, counts["WagerTransactionRejected"], rej)
		}
		if counts["WalletBalanceChanged"] != entries {
			t.Errorf("carteira %s: %d eventos de saldo, %d lançamentos", w.id, counts["WalletBalanceChanged"], entries)
		}
	}

	// o que o HTTP viu de terminal bate com o banco
	for external, o := range httpSeen {
		status, code, found := dbTxStatus(t, "provider-a", external)
		if !found {
			t.Errorf("%s respondido (%d) mas não gravado", external, o.status)
			continue
		}
		if o.status == 200 && status != "PROCESSED" || o.status == 422 && (status != "REJECTED" || code != o.code) {
			t.Errorf("%s: HTTP %d/%s mas banco %s/%s", external, o.status, o.code, status, code)
		}
	}

	// eventos no broker: sem duplicar eventId, e em ordem de versão por carteira
	checkBrokerOrdering(t, q, ws)

	// toda operação enviada tem exatamente um registro
	expected := map[string]bool{}
	for _, s := range script {
		expected[s.o.external] = true
	}
	var got int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND kind <> 'OPENING'`, partitions(ws)).Scan(&got)
	if got != len(expected) {
		t.Fatalf("transações gravadas = %d, operações distintas enviadas = %d", got, len(expected))
	}
}

// checkWalletInvariants confere, por SQL independente, tudo que o enunciado exige do dinheiro.
func checkWalletInvariants(t *testing.T, ctx context.Context, w testWallet) {
	t.Helper()
	var balance, version int64
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, w.id).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	if balance < 0 {
		t.Fatalf("carteira %s com saldo negativo: %d", w.id, balance)
	}
	assertLedgerMatchesBalance(t, w.id)

	// saldo recomputado a partir das transações processadas, sem olhar o ledger
	var recomputed int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(CASE t.kind
			WHEN 'OPENING' THEN t.amount WHEN 'WIN' THEN t.amount WHEN 'REFUND' THEN t.amount
			WHEN 'BET' THEN -t.amount
			WHEN 'ROLLBACK' THEN CASE WHEN r.kind = 'BET' THEN t.amount ELSE -t.amount END
			ELSE 0 END), 0)::bigint
		FROM wager_transactions t LEFT JOIN wager_transactions r ON r.id = t.resolved_reference_id
		WHERE t.wallet_id = $1 AND t.status = 'PROCESSED'`, w.id).Scan(&recomputed); err != nil {
		t.Fatal(err)
	}
	if recomputed != balance {
		t.Fatalf("carteira %s: saldo %d, recomputado pelas transações %d", w.id, balance, recomputed)
	}

	checks := map[string]string{
		"processada que movimenta saldo sem lançamento": `SELECT count(*) FROM wager_transactions t
			WHERE t.wallet_id = $1 AND t.status = 'PROCESSED' AND t.kind <> 'LOSS'
			AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l WHERE l.transaction_id = t.id)`,
		"lançamento de LOSS ou de transação não processada": `SELECT count(*) FROM wallet_ledger_entries l
			JOIN wager_transactions t ON t.id = l.transaction_id
			WHERE l.wallet_id = $1 AND (t.status <> 'PROCESSED' OR t.kind = 'LOSS')`,
		"direção do lançamento incompatível com o tipo": `SELECT count(*) FROM wallet_ledger_entries l
			JOIN wager_transactions t ON t.id = l.transaction_id
			LEFT JOIN wager_transactions r ON r.id = t.resolved_reference_id
			WHERE l.wallet_id = $1 AND NOT (
			   (t.kind = 'BET' AND l.direction = 'DEBIT')
			OR (t.kind IN ('WIN','REFUND','OPENING') AND l.direction = 'CREDIT')
			OR (t.kind = 'ROLLBACK' AND ((r.kind = 'BET' AND l.direction = 'CREDIT') OR (r.kind <> 'BET' AND l.direction = 'DEBIT'))))`,
		"referência com mais de uma reversão processada": `SELECT count(*) FROM (
			SELECT resolved_reference_id FROM wager_transactions
			WHERE wallet_id = $1 AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'
			GROUP BY 1 HAVING count(*) > 1) x`,
		"pendência esquecida": `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND status IN ('PENDING','PENDING_REFERENCE')`,
		"reversão processada sem referência resolvida": `SELECT count(*) FROM wager_transactions
			WHERE wallet_id = $1 AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED' AND resolved_reference_id IS NULL`,
		"rejeição ou falha sem código": `SELECT count(*) FROM wager_transactions
			WHERE wallet_id = $1 AND status IN ('REJECTED','FAILED') AND failure_code IS NULL`,
		"lançamento do diário desequilibrado": `SELECT count(*) FROM (
			SELECT e.id FROM journal_entries e JOIN journal_postings p ON p.entry_id = e.id
			JOIN wager_transactions t ON t.id = e.transaction_id WHERE t.wallet_id = $1
			GROUP BY e.id HAVING COALESCE(sum(p.amount) FILTER (WHERE p.direction = 'DEBIT'), 0)
			                  <> COALESCE(sum(p.amount) FILTER (WHERE p.direction = 'CREDIT'), 0)) x`,
		"movimento do ledger sem lançamento no diário": `SELECT count(*) FROM wallet_ledger_entries l
			WHERE l.wallet_id = $1 AND NOT EXISTS (SELECT 1 FROM journal_entries e WHERE e.transaction_id = l.transaction_id)`,
		"lançamento do diário sem movimento no ledger": `SELECT count(*) FROM journal_entries e
			JOIN wager_transactions t ON t.id = e.transaction_id
			WHERE t.wallet_id = $1 AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l WHERE l.transaction_id = e.transaction_id)`,
		"encadeamento do ledger quebrado": `SELECT count(*) FROM (
			SELECT balance_before, lag(balance_after) OVER (ORDER BY wallet_version) AS prev FROM wallet_ledger_entries WHERE wallet_id = $1) x
			WHERE x.prev IS NOT NULL AND x.prev <> x.balance_before`,
	}
	for name, query := range checks {
		if n := dbCount(t, query, w.id); n != 0 {
			t.Errorf("carteira %s: %s (%d)", w.id, name, n)
		}
	}
	// o saldo da conta da carteira no diário (créditos − débitos) é o saldo da carteira
	var journalBalance int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::bigint
		FROM journal_postings WHERE wallet_id = $1`, w.id).Scan(&journalBalance); err != nil {
		t.Fatal(err)
	}
	if journalBalance != balance {
		t.Errorf("carteira %s: saldo %d, mas a conta no diário soma %d", w.id, balance, journalBalance)
	}
	// versão = número de lançamentos (a abertura é a versão 1 e cada movimento avança uma)
	if entries, _, _ := dbLedger(t, w.id); int64(entries) != version {
		t.Errorf("carteira %s: versão %d, lançamentos %d", w.id, version, entries)
	}
	// reconciliação pela API
	_ = ctx
}

// checkBrokerOrdering lê a fila de eventos em sequência e confere eventId único
// e versões crescentes por carteira, com o encadeamento before/after correto.
// Em caso de falha, registra a ordem de enfileiramento do broker (SequenceNumber)
// e o histórico de entrega da outbox da carteira.
func checkBrokerOrdering(t *testing.T, q *queues, ws []testWallet) {
	t.Helper()
	type received struct {
		body, seq string
	}
	var got []received
	deadline := time.Now().Add(25 * time.Second)
	quiet := 0
	for time.Now().Before(deadline) && quiet < 3 {
		out, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(q.events), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 5,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(out.Messages) == 0 && len(got) > 0 {
			quiet++
		} else {
			quiet = 0
		}
		for _, m := range out.Messages {
			got = append(got, received{aws.ToString(m.Body), m.Attributes[string(types.MessageSystemAttributeNameSequenceNumber)]})
			_, _ = sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.events), ReceiptHandle: m.ReceiptHandle})
		}
	}

	type change struct {
		version       float64
		before, after string
	}
	seen := map[string]bool{}
	last := map[string]change{}
	trace := map[string][]string{}
	failedWallets := map[string]bool{}
	for idx, r := range got {
		var e struct {
			EventID   string `json:"eventId"`
			EventType string `json:"eventType"`
			Data      struct {
				WalletID      string                  `json:"walletId"`
				WalletVersion float64                 `json:"walletVersion"`
				Before        struct{ Amount string } `json:"balanceBefore"`
				After         struct{ Amount string } `json:"balanceAfter"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(r.body), &e); err != nil {
			t.Fatalf("evento inválido: %v", err)
		}
		dup := seen[e.EventID]
		seen[e.EventID] = true
		if e.EventType == "WalletBalanceChanged" {
			trace[e.Data.WalletID] = append(trace[e.Data.WalletID],
				fmt.Sprintf("recv#%d seq=%s v%.0f %s->%s dup=%v id=%s", idx, r.seq, e.Data.WalletVersion, e.Data.Before.Amount, e.Data.After.Amount, dup, e.EventID[:8]))
		}
		if dup || e.EventType != "WalletBalanceChanged" {
			continue // republicação com o mesmo eventId é permitida
		}
		prev, ok := last[e.Data.WalletID]
		if ok {
			if e.Data.WalletVersion <= prev.version {
				t.Errorf("carteira %s: evento v%.0f depois de v%.0f (fora de ordem)", e.Data.WalletID, e.Data.WalletVersion, prev.version)
				failedWallets[e.Data.WalletID] = true
			}
			if e.Data.Before.Amount != prev.after {
				t.Errorf("carteira %s: balanceBefore %s não continua balanceAfter %s", e.Data.WalletID, e.Data.Before.Amount, prev.after)
				failedWallets[e.Data.WalletID] = true
			}
		}
		last[e.Data.WalletID] = change{e.Data.WalletVersion, e.Data.Before.Amount, e.Data.After.Amount}
	}
	for _, w := range ws {
		var entries int
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id).Scan(&entries)
		if got, ok := last[w.id]; !ok || int(got.version) != entries {
			t.Errorf("carteira %s: último evento de saldo v%.0f, ledger com %d lançamentos", w.id, got.version, entries)
			failedWallets[w.id] = true
		}
	}
	for id := range failedWallets {
		t.Logf("--- diagnóstico da carteira %s: ordem recebida do broker", id)
		for _, line := range trace[id] {
			t.Log("   ", line)
		}
		rows, err := pool.Query(context.Background(), `SELECT seq, event_type, COALESCE(payload->'data'->>'walletVersion', '-'), attempts,
				COALESCE(last_error, ''), locked_by IS NOT NULL
			FROM outbox_events WHERE partition_key = $1 ORDER BY seq`, id)
		if err == nil {
			t.Log("    outbox (seq, tipo, versão, tentativas, erro):")
			for rows.Next() {
				var seq int64
				var typ, ver, lastErr string
				var attempts int
				var locked bool
				_ = rows.Scan(&seq, &typ, &ver, &attempts, &lastErr, &locked)
				t.Logf("      seq=%d %s v=%s attempts=%d err=%q", seq, typ, ver, attempts, lastErr)
			}
			rows.Close()
		}
	}
}
