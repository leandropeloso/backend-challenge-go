//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/leandropeloso/wager-service/internal/config"
	"github.com/leandropeloso/wager-service/internal/platform"
)

func setAppEnv(t *testing.T, q *queues, addr string) {
	t.Helper()
	for k, v := range map[string]string{
		"HTTP_ADDR": addr, "DATABASE_URL": env.dbURL, "AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": env.awsEndpoint,
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
		"SQS_WAGER_QUEUE": q.wagerName, "SQS_WAGER_DLQ": q.dlqName, "SQS_EVENTS_QUEUE": q.eventsName,
		"CONSUMER_NAME": "it-fx-consumer", "AUTH_ISSUER": env.issuer, "AUTH_JWKS_URL": env.jwksURL,
		"CONSUMER_WAIT_TIME": "1s", "OUTBOX_POLL_INTERVAL": "100ms", "PENDING_POLL_INTERVAL": "200ms",
	} {
		t.Setenv(k, v)
	}
}

// A composição Fx sobe com infraestrutura real, atende requisições, processa
// mensagens e, no encerramento, para as entradas e libera todos os recursos.
func TestFxLifecycleStartsAndReleasesResources(t *testing.T) {
	q := newQueues(t)
	addr := freeAddr(t)
	setAppEnv(t, q, addr)

	var (
		cfg  *config.Config
		pgdb *pgxpool.Pool
	)
	app := platform.New(fx.Populate(&cfg, &pgdb))
	if err := app.Err(); err != nil {
		t.Fatalf("composição inválida: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = app.Stop(context.Background())
		}
	}()

	base := "http://" + addr
	if r := call(t, base, "GET", "/health/ready", "", "", nil); r.status != http.StatusOK {
		t.Fatalf("ready = %d: %s", r.status, r.raw)
	}
	if r := call(t, base, "GET", "/health/live", "", "", nil); r.status != http.StatusOK {
		t.Fatalf("live = %d", r.status)
	}

	// o consumidor está de pé: processa uma mensagem
	w := openWalletOn(t, base, "100.00")
	o := op{external: "fx-" + w.id, kind: "BET", amount: "10.00"}
	sendOp(t, q, w, o, "msg-fx-"+uuid.NewString())
	eventually(t, 20*time.Second, "consumidor do Fx processar a mensagem", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "PROCESSED"
	})
	eventually(t, 20*time.Second, "outbox do Fx publicar os eventos", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND published_at IS NULL`, w.id) == 0
	})

	stopCtx, cancelStop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped = true

	// recursos liberados
	if err := pgdb.Ping(context.Background()); err == nil {
		t.Fatal("o pool de conexões deveria ter sido fechado")
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Fatal("a porta HTTP deveria ter sido liberada")
	}
	// o consumidor parou de ler: uma nova mensagem permanece na fila
	late := op{external: "fx-late-" + w.id, kind: "BET", amount: "1.00"}
	sendOp(t, q, w, late, "msg-fx-late-"+uuid.NewString())
	time.Sleep(4 * time.Second)
	if v, f := queueDepth(t, q.wager); v+f != 1 {
		t.Fatalf("a mensagem deveria continuar na fila após o stop (visíveis=%d, em voo=%d)", v, f)
	}
	if _, _, found := dbTxStatus(t, "provider-a", late.external); found {
		t.Fatal("nada deveria ser processado depois do stop")
	}
}

// openWalletOn abre uma carteira em um servidor identificado só pela URL base.
func openWalletOn(t *testing.T, base, initial string) testWallet {
	t.Helper()
	player := uuid.NewString()
	r := call(t, base, "POST", "/wallets", internalTok(t), "", map[string]any{
		"playerId": player, "initialBalance": money(initial)})
	mustStatus(t, r, http.StatusCreated)
	w := testWallet{id: r.str("id"), player: player}
	retireOutbox(t, w.id)
	return w
}

// Configuração inválida impede a inicialização, sem abrir recursos.
func TestFxFailsFastOnInvalidConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("AUTH_ISSUER", "")
	t.Setenv("AUTH_JWKS_URL", "")
	var cfg *config.Config
	app := platform.New(fx.Populate(&cfg))
	if app.Err() == nil {
		t.Fatal("a composição deveria falhar com configuração inválida")
	}
}
