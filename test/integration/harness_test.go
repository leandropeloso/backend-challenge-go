//go:build integration

// Package integration exercita o serviço contra PostgreSQL, Keycloak e
// LocalStack reais, com instâncias do servidor rodando como processos
// independentes (cada uma com suas conexões, workers e memória).
//
// Variáveis esperadas (o docker-compose já as define no serviço "tests"):
//
//	TEST_DATABASE_URL, TEST_AWS_ENDPOINT_URL, TEST_AUTH_TOKEN_URL,
//	TEST_AUTH_ISSUER, TEST_AUTH_JWKS_URL
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var env struct {
	dbURL, awsEndpoint, tokenURL, issuer, jwksURL string
	serverBin                                     string
	coverDir                                      string
	otlpEndpoint, jaegerURL                       string
}

var (
	pool      *pgxpool.Pool
	sqsClient *sqs.Client
)

func TestMain(m *testing.M) {
	env.dbURL = os.Getenv("TEST_DATABASE_URL")
	env.awsEndpoint = os.Getenv("TEST_AWS_ENDPOINT_URL")
	env.tokenURL = os.Getenv("TEST_AUTH_TOKEN_URL")
	env.issuer = os.Getenv("TEST_AUTH_ISSUER")
	env.jwksURL = os.Getenv("TEST_AUTH_JWKS_URL")
	env.otlpEndpoint = os.Getenv("TEST_OTLP_ENDPOINT")
	env.jaegerURL = os.Getenv("TEST_JAEGER_URL")
	if env.dbURL == "" || env.awsEndpoint == "" || env.tokenURL == "" || env.issuer == "" || env.jwksURL == "" {
		fmt.Fprintln(os.Stderr, "integration: TEST_* variables not set; run `docker compose --profile test run --rm tests`")
		os.Exit(1)
	}

	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration:", err)
		os.Exit(1)
	}
	tmp, err := os.MkdirTemp("", "wager-it-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	// Binário com pontos de injeção de falha habilitados (build tag faultinject).
	env.serverBin = filepath.Join(tmp, "server")
	// -race também nos servidores: o -race do `go test` só instrumenta o processo dos
	// testes, não os binários separados que os cenários iniciam.
	buildArgs := []string{"build", "-race", "-tags", "faultinject", "-o", env.serverBin}
	if env.coverDir = os.Getenv("IT_COVERDIR"); env.coverDir != "" {
		// cobertura do código exercitado pelos processos do servidor
		buildArgs = append(buildArgs, "-cover", "-coverpkg=github.com/leandropeloso/wager-service/...")
	}
	build := exec.Command("go", append(buildArgs, "./cmd/server")...)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "integration: build failed: %v\n%s", err, out)
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err = pgxpool.New(ctx, env.dbURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration: database:", err)
		os.Exit(1)
	}
	defer pool.Close()

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration: aws config:", err)
		os.Exit(1)
	}
	sqsClient = sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(env.awsEndpoint) })

	os.Exit(m.Run())
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

// ---------------------------------------------------------------- filas

type queues struct {
	wagerName, dlqName, eventsName string
	wager, dlq, events             string
}

// newQueues cria filas exclusivas do teste (com redrive para a DLQ), isolando-o
// dos demais testes e das instâncias do docker compose.
func newQueues(t *testing.T) *queues { return newQueuesOn(t, sqsClient) }

// newQueuesOn cria as filas num broker específico (o padrão é o LocalStack).
func newQueuesOn(t *testing.T, client *sqs.Client) *queues {
	t.Helper()
	ctx := context.Background()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	q := &queues{
		wagerName:  "t-wager-" + suffix + ".fifo",
		dlqName:    "t-dlq-" + suffix + ".fifo",
		eventsName: "t-events-" + suffix + ".fifo",
	}
	create := func(name string, attrs map[string]string) string {
		out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue %s: %v", name, err)
		}
		return aws.ToString(out.QueueUrl)
	}
	fifo := map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}
	q.dlq = create(q.dlqName, fifo)
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": attrs.Attributes[string(types.QueueAttributeNameQueueArn)], "maxReceiveCount": "5"})
	q.wager = create(q.wagerName, map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "10", "RedrivePolicy": string(redrive)})
	q.events = create(q.eventsName, fifo)

	t.Cleanup(func() {
		for _, u := range []string{q.wager, q.dlq, q.events} {
			_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

// send publica uma mensagem na fila de entrada.
func (q *queues) send(t *testing.T, group, dedup string, body any) {
	t.Helper()
	raw, ok := body.(string)
	if !ok {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		raw = string(b)
	}
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(q.wager), MessageBody: aws.String(raw),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
}

// drain lê (e remove) mensagens da fila até o prazo; devolve os corpos recebidos.
func drain(t *testing.T, queueURL string, wait time.Duration, stop func(bodies []string) bool) []string {
	t.Helper()
	var bodies []string
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 5})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		for _, m := range out.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
			_, _ = sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
		if stop != nil && stop(bodies) {
			break
		}
	}
	return bodies
}

func queueDepth(t *testing.T, queueURL string) (visible, inFlight int) {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscan(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], &visible)
	fmt.Sscan(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], &inFlight)
	return
}

// ---------------------------------------------------------------- instâncias

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// instance é um processo do servidor.
type instance struct {
	t      *testing.T
	name   string
	cmd    *exec.Cmd
	base   string
	out    *syncBuffer
	exited chan struct{}
	state  *os.ProcessState
}

type startOpts struct {
	env    map[string]string
	noWait bool // não espera o /health/ready
}

func withEnv(kv ...string) func(*startOpts) {
	return func(o *startOpts) {
		for i := 0; i+1 < len(kv); i += 2 {
			o.env[kv[i]] = kv[i+1]
		}
	}
}

func noWait(o *startOpts) { o.noWait = true }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startInstance(t *testing.T, q *queues, opts ...func(*startOpts)) *instance {
	t.Helper()
	addr := freeAddr(t)
	o := &startOpts{env: map[string]string{
		"HTTP_ADDR":                   addr,
		"DATABASE_URL":                env.dbURL,
		"AWS_REGION":                  "us-east-1",
		"AWS_ENDPOINT_URL":            env.awsEndpoint,
		"AWS_ACCESS_KEY_ID":           "test",
		"AWS_SECRET_ACCESS_KEY":       "test",
		"SQS_WAGER_QUEUE":             q.wagerName,
		"SQS_WAGER_DLQ":               q.dlqName,
		"SQS_EVENTS_QUEUE":            q.eventsName,
		"CONSUMER_NAME":               "it-consumer",
		"AUTH_ISSUER":                 env.issuer,
		"AUTH_JWKS_URL":               env.jwksURL,
		"SHUTDOWN_TIMEOUT":            "15s",
		"CONSUMER_WAIT_TIME":          "1s",
		"CONSUMER_VISIBILITY_TIMEOUT": "10s",
		"CONSUMER_PROCESS_TIMEOUT":    "5s",
		"CONSUMER_RETRY_BASE_DELAY":   "1s",
		"OUTBOX_POLL_INTERVAL":        "100ms",
		"OUTBOX_LEASE":                "4s",
		"OUTBOX_PUBLISH_TIMEOUT":      "2s",
		"OUTBOX_BACKOFF_BASE":         "500ms",
		"PENDING_POLL_INTERVAL":       "200ms",
		"PENDING_BASE_BACKOFF":        "500ms",
		"PENDING_MAX_BACKOFF":         "2s",
		"PENDING_TTL":                 "20s",
		"PENDING_MAX_ATTEMPTS":        "50",
	}}
	for _, f := range opts {
		f(o)
	}

	cmd := exec.Command(env.serverBin)
	cmd.Env = os.Environ()
	for k, v := range o.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if env.coverDir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+env.coverDir)
	}
	if _, explicit := o.env["OTEL_EXPORTER_OTLP_ENDPOINT"]; env.otlpEndpoint != "" && !explicit {
		cmd.Env = append(cmd.Env, "OTEL_EXPORTER_OTLP_ENDPOINT="+env.otlpEndpoint, "OTEL_SERVICE_NAME=wager-it")
	}
	inst := &instance{t: t, name: filepath.Base(t.Name()) + "/" + addr, cmd: cmd, base: "http://" + addr,
		out: &syncBuffer{}, exited: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = inst.out, inst.out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	go func() {
		_ = cmd.Wait()
		inst.state = cmd.ProcessState
		close(inst.exited)
	}()
	t.Cleanup(func() {
		inst.shutdownAtEnd()
		if strings.Contains(inst.out.String(), "WARNING: DATA RACE") {
			t.Errorf("o detector de corridas acusou um problema no servidor %s:\n%s", inst.name, inst.out.String())
		}
		if t.Failed() {
			t.Logf("---- logs %s ----\n%s", inst.name, inst.out.String())
		}
	})
	if !o.noWait {
		inst.waitReady()
	}
	return inst
}

func (i *instance) waitReady() {
	i.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-i.exited:
			i.t.Fatalf("instance %s exited during startup:\n%s", i.name, i.out.String())
		default:
		}
		resp, err := http.Get(i.base + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	i.t.Fatalf("instance %s not ready in time:\n%s", i.name, i.out.String())
}

// kill encerra o processo abruptamente (SIGKILL), sem shutdown.
func (i *instance) kill() {
	select {
	case <-i.exited:
		return
	default:
	}
	_ = i.cmd.Process.Kill()
	<-i.exited
}

// shutdownAtEnd encerra a instância ao fim do teste com SIGTERM e exige saída
// limpa (código 0) dentro do prazo; instâncias já mortas (kill/falha injetada)
// são ignoradas. Assim todo teste também verifica o shutdown gracioso.
func (i *instance) shutdownAtEnd() {
	select {
	case <-i.exited:
		return
	default:
	}
	_ = i.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-i.exited:
		if code := i.state.ExitCode(); code != 0 {
			i.t.Errorf("instance %s exited with code %d on SIGTERM", i.name, code)
		}
	case <-time.After(25 * time.Second):
		i.t.Errorf("instance %s did not shut down within 25s of SIGTERM", i.name)
		_ = i.cmd.Process.Kill()
		<-i.exited
	}
}

// terminate envia SIGTERM e espera o shutdown gracioso; devolve o código de saída.
func (i *instance) terminate(wait time.Duration) int {
	i.t.Helper()
	_ = i.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-i.exited:
		return i.state.ExitCode()
	case <-time.After(wait):
		i.t.Fatalf("instance %s did not stop within %s:\n%s", i.name, wait, i.out.String())
		return -1
	}
}

func (i *instance) waitExit(wait time.Duration) (int, bool) {
	select {
	case <-i.exited:
		return i.state.ExitCode(), true
	case <-time.After(wait):
		return 0, false
	}
}

// ---------------------------------------------------------------- tokens e chamadas HTTP

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]cachedToken{}
)

// cachedToken guarda o token e quando foi obtido: o realm emite tokens de 300 s e
// a suíte dura mais que isso, então o cache renova antes de expirar.
type cachedToken struct {
	value string
	at    time.Time
}

func fetchToken(t *testing.T, client, secret string) string {
	t.Helper()
	resp, err := http.PostForm(env.tokenURL, url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secret}})
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || json.Unmarshal(raw, &body) != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d body %s", client, resp.StatusCode, raw)
	}
	return body.AccessToken
}

func token(t *testing.T, client, secret string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[client]; ok && time.Since(c.at) < 150*time.Second {
		return c.value
	}
	tok := fetchToken(t, client, secret)
	tokenCache[client] = cachedToken{value: tok, at: time.Now()}
	return tok
}

func providerA(t *testing.T) string { return token(t, "provider-a", "provider-a-secret") }
func providerB(t *testing.T) string { return token(t, "provider-b", "provider-b-secret") }
func internalTok(t *testing.T) string {
	return token(t, "internal-service", "internal-service-secret")
}

type response struct {
	status int
	header http.Header
	raw    []byte
	body   map[string]any
}

func (r response) str(path ...string) string {
	var cur any = r.body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (r response) boolean(key string) bool {
	b, _ := r.body[key].(bool)
	return b
}

func call(t *testing.T, base, method, path, tok, idemKey string, body any) response {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, base+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := response{status: resp.StatusCode, header: resp.Header, raw: raw}
	_ = json.Unmarshal(raw, &r.body)
	return r
}

// ---------------------------------------------------------------- domínio de teste

func money(amount string) map[string]string {
	return map[string]string{"amount": amount, "currency": "BRL"}
}

type testWallet struct {
	id, player string
}

// openWallet abre uma carteira (jogador novo) pela API.
func openWallet(t *testing.T, inst *instance, initial string) testWallet {
	t.Helper()
	player := uuid.NewString()
	r := call(t, inst.base, "POST", "/wallets", internalTok(t), "", map[string]any{
		"playerId": player, "initialBalance": money(initial)})
	if r.status != http.StatusCreated {
		t.Fatalf("open wallet: status %d body %s", r.status, r.raw)
	}
	w := testWallet{id: r.str("id"), player: player}
	retireOutbox(t, w.id)
	return w
}

// retireOutbox marca como publicados, ao fim do teste, os eventos que ele deixou
// pendentes (testes que desligam o relay). Sem isso, o relay de um teste
// posterior publicaria eventos órfãos na sua própria fila.
func retireOutbox(t *testing.T, walletID string) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`UPDATE outbox_events SET published_at = now(), locked_until = NULL, locked_by = NULL
			 WHERE partition_key = $1 AND published_at IS NULL`, walletID)
	})
}

type op struct {
	provider string
	external string
	key      string
	kind     string
	amount   string
	round    string
	ref      string
}

func (w testWallet) req(o op) map[string]any {
	if o.provider == "" {
		o.provider = "provider-a"
	}
	if o.external == "" {
		o.external = "ext-" + uuid.NewString()
	}
	if o.round == "" {
		o.round = "round-1"
	}
	m := map[string]any{
		"providerId": o.provider, "externalTransactionId": o.external, "playerId": w.player, "walletId": w.id,
		"roundId": o.round, "gameId": "game-1", "kind": o.kind, "money": money(o.amount),
	}
	if o.ref != "" {
		m["referenceExternalTransactionId"] = o.ref
	}
	return m
}

// submit envia a operação com o token do provedor A (ou B, se o.provider == "provider-b").
func submit(t *testing.T, inst *instance, w testWallet, o op) (response, op) {
	t.Helper()
	if o.provider == "" {
		o.provider = "provider-a"
	}
	if o.external == "" {
		o.external = "ext-" + uuid.NewString()
	}
	if o.key == "" {
		o.key = o.provider + ":" + o.external
	}
	tok := providerA(t)
	if o.provider == "provider-b" {
		tok = providerB(t)
	}
	return call(t, inst.base, "POST", "/wagering/transactions", tok, o.key, w.req(o)), o
}

func mustStatus(t *testing.T, r response, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("status = %d, quer %d; corpo: %s", r.status, want, r.raw)
	}
}

// ---------------------------------------------------------------- consultas ao banco

func dbBalance(t *testing.T, walletID string) int64 {
	t.Helper()
	var b int64
	if err := pool.QueryRow(context.Background(), `SELECT balance FROM wallets WHERE id = $1`, walletID).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func dbLedger(t *testing.T, walletID string) (entries, debits, credits int) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `SELECT count(*),
		count(*) FILTER (WHERE direction = 'DEBIT'), count(*) FILTER (WHERE direction = 'CREDIT')
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&entries, &debits, &credits)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func dbVersion(t *testing.T, walletID string) int64 {
	t.Helper()
	var v int64
	if err := pool.QueryRow(context.Background(), `SELECT version FROM wallets WHERE id = $1`, walletID).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func dbTxStatus(t *testing.T, provider, external string) (status, code string, found bool) {
	t.Helper()
	var c *string
	err := pool.QueryRow(context.Background(), `SELECT status, failure_code FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, provider, external).Scan(&status, &c)
	if err != nil {
		return "", "", false
	}
	if c != nil {
		code = *c
	}
	return status, code, true
}

func dbCount(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// assertLedgerMatchesBalance confere o saldo armazenado contra créditos - débitos do ledger.
func assertLedgerMatchesBalance(t *testing.T, walletID string) {
	t.Helper()
	var stored, calc int64
	err := pool.QueryRow(context.Background(), `SELECT w.balance,
		COALESCE((SELECT SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
		          FROM wallet_ledger_entries WHERE wallet_id = w.id), 0)
		FROM wallets w WHERE w.id = $1`, walletID).Scan(&stored, &calc)
	if err != nil {
		t.Fatal(err)
	}
	if stored != calc {
		t.Fatalf("saldo armazenado %d != créditos-débitos do ledger %d", stored, calc)
	}
}

// eventually repete check até ele passar ou o prazo acabar.
func eventually(t *testing.T, wait time.Duration, msg string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timeout esperando: %s", msg)
}

// parallel executa n funções simultaneamente e espera todas.
func parallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

func startCluster(t *testing.T, q *queues, n int, opts ...func(*startOpts)) []*instance {
	t.Helper()
	out := make([]*instance, n)
	for i := range out {
		out[i] = startInstance(t, q, opts...)
	}
	return out
}
