//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// dockerAction chama a API do Docker Engine pelo socket Unix (sem precisar da CLI).
// O teste é ignorado quando o socket não está disponível.
func dockerAction(t *testing.T, action string) {
	t.Helper()
	sock := os.Getenv("TEST_DOCKER_SOCKET")
	name := os.Getenv("TEST_POSTGRES_CONTAINER")
	if sock == "" || name == "" {
		t.Skip("TEST_DOCKER_SOCKET/TEST_POSTGRES_CONTAINER não definidos")
	}
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}},
	}
	url := fmt.Sprintf("http://docker/containers/%s/%s", name, action)
	if action == "stop" {
		url += "?t=2"
	}
	resp, err := client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatalf("docker %s %s: %v", action, name, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		t.Fatalf("docker %s %s: status %d", action, name, resp.StatusCode)
	}
}

func waitPostgres(t *testing.T) {
	t.Helper()
	eventually(t, 90*time.Second, "PostgreSQL aceitar conexões", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, env.dbURL)
		if err != nil {
			return false
		}
		defer conn.Close(ctx)
		return conn.Ping(ctx) == nil
	})
}

// PostgreSQL totalmente parado e religado no meio do tráfego: a API responde 503
// (transitório, com Retry-After), o readiness acusa o banco, mensagens SQS ficam
// em retry, e depois da volta cada operação é aplicada exatamente uma vez.
func TestFullPostgreSQLOutageAndRecovery(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2, withEnv("CONSUMER_MAX_ATTEMPTS", "30", "CONSUMER_RETRY_BASE_DELAY", "1s", "CONSUMER_RETRY_MAX_DELAY", "3s"))
	w := openWallet(t, nodes[0], "100.00")

	ok := op{external: "outage-before-" + w.id, kind: "BET", amount: "10.00"}
	mustStatus(t, mustSubmit(t, nodes[0], w, ok), http.StatusOK)

	// restaura o banco mesmo se o teste falhar no meio
	restored := false
	restore := func() {
		if !restored {
			restored = true
			dockerAction(t, "start")
			waitPostgres(t)
		}
	}
	t.Cleanup(restore)

	dockerAction(t, "stop")

	// durante a queda
	during := op{external: "outage-during-" + w.id, kind: "BET", amount: "20.00"}
	r, during := submit(t, nodes[0], w, during)
	mustStatus(t, r, http.StatusServiceUnavailable)
	if r.header.Get("Retry-After") == "" || r.str("error", "code") != "SERVICE_UNAVAILABLE" {
		t.Fatalf("503 deve ser distinguível e trazer Retry-After: %d %s", r.status, r.raw)
	}
	ready := call(t, nodes[1].base, "GET", "/health/ready", "", "", nil)
	checks, _ := ready.body["checks"].(map[string]any)
	if ready.status != http.StatusServiceUnavailable || checks["postgres"] != "unavailable" {
		t.Fatalf("readiness deveria acusar o banco: %d %s", ready.status, ready.raw)
	}
	if call(t, nodes[1].base, "GET", "/health/live", "", "", nil).status != http.StatusOK {
		t.Fatal("o processo continua vivo: liveness deve ser 200")
	}
	if r := call(t, nodes[0].base, "GET", "/wallets/"+w.id, internalTok(t), "", nil); r.status != http.StatusServiceUnavailable {
		t.Fatalf("leitura durante a queda: %d %s", r.status, r.raw)
	}
	// mensagem SQS chegando com o banco fora: não é perdida nem descartada
	viaSQS := op{external: "outage-sqs-" + w.id, kind: "BET", amount: "5.00"}
	sendOp(t, q, w, viaSQS, "msg-outage-"+uuid.NewString())
	time.Sleep(6 * time.Second)
	if v, f := queueDepth(t, q.wager); v+f != 1 {
		t.Fatalf("a mensagem deve continuar na fila durante a queda (visíveis=%d, em voo=%d)", v, f)
	}
	if v, f := queueDepth(t, q.dlq); v+f != 0 {
		t.Fatal("queda do banco não é motivo para DLQ")
	}

	// o banco volta
	restore()
	eventually(t, 60*time.Second, "readiness voltar ao normal", func() bool {
		return call(t, nodes[0].base, "GET", "/health/ready", "", "", nil).status == http.StatusOK &&
			call(t, nodes[1].base, "GET", "/health/ready", "", "", nil).status == http.StatusOK
	})

	// o cliente repete com a mesma chave: aplica uma vez
	var again response
	eventually(t, 30*time.Second, "operação repetida ser aceita", func() bool {
		again, _ = submit(t, nodes[1], w, during)
		return again.status == http.StatusOK
	})
	if again.boolean("idempotentReplay") {
		t.Fatal("a operação não tinha sido aplicada durante a queda: não é replay")
	}
	eventually(t, 90*time.Second, "mensagem SQS ser processada depois da volta", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", viaSQS.external)
		return s == "PROCESSED"
	})
	waitQueueEmpty(t, q.wager, 30*time.Second)

	if bal := dbBalance(t, w.id); bal != 6500 {
		t.Fatalf("saldo = %d, quer 6500 (100 − 10 − 20 − 5)", bal)
	}
	if _, debits, _ := dbLedger(t, w.id); debits != 3 {
		t.Fatalf("débitos = %d", debits)
	}
	assertLedgerMatchesBalance(t, w.id)
	// a outbox continua publicando depois da recuperação
	eventually(t, 30*time.Second, "outbox publicar depois da volta", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND published_at IS NULL`, w.id) == 0
	})
}
