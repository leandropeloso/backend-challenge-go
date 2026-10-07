// Comando loadtest gera carga contra uma ou mais instâncias do serviço e
// reporta vazão, latências (p50/p95/p99), erros, conflitos e atraso da outbox.
// Ao final reconcilia todas as carteiras: qualquer divergência falha a execução.
//
//	go run ./cmd/loadtest -bases http://app1:8080,http://app2:8080,http://app3:8080 \
//	    -token-url http://keycloak:8080/realms/wager/protocol/openid-connect/token \
//	    -duration 30s -concurrency 32 -wallets 200
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type config struct {
	bases       []string
	tokenURL    string
	duration    time.Duration
	concurrency int
	wallets     int
	replayPct   int
}

type sample struct {
	d      time.Duration
	status int
}

func main() {
	var bases string
	cfg := config{}
	flag.StringVar(&bases, "bases", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "URLs das instâncias, separadas por vírgula")
	flag.StringVar(&cfg.tokenURL, "token-url", "http://localhost:8080/realms/wager/protocol/openid-connect/token", "endpoint de token do Keycloak")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "duração da carga")
	flag.IntVar(&cfg.concurrency, "concurrency", 32, "requisições simultâneas")
	flag.IntVar(&cfg.wallets, "wallets", 200, "carteiras usadas (mais carteiras, menos contenção)")
	flag.IntVar(&cfg.replayPct, "replay-pct", 10, "percentual de reenvios idênticos (idempotência sob carga)")
	flag.Parse()
	cfg.bases = strings.Split(bases, ",")

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

var client = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}

func run(cfg config) error {
	internal, err := token(cfg.tokenURL, "internal-service", "internal-service-secret")
	if err != nil {
		return err
	}
	provider, err := token(cfg.tokenURL, "provider-a", "provider-a-secret")
	if err != nil {
		return err
	}

	type wallet struct{ id, player string }
	ws := make([]wallet, cfg.wallets)
	for i := range ws {
		player := uuid.NewString()
		body, _ := json.Marshal(map[string]any{"playerId": player, "initialBalance": map[string]string{"amount": "1000000.00", "currency": "BRL"}})
		var resp struct{ ID string }
		if err := do(cfg.bases[i%len(cfg.bases)], "POST", "/wallets", internal, "", body, &resp); err != nil {
			return fmt.Errorf("open wallet: %w", err)
		}
		ws[i] = wallet{resp.ID, player}
	}

	run := uuid.NewString()[:8]
	var (
		counter   atomic.Int64
		mu        sync.Mutex
		samples   []sample
		wg        sync.WaitGroup
		stopAt    = time.Now().Add(cfg.duration)
		statuses  = map[int]int{}
		replays   atomic.Int64
		replayBad atomic.Int64
	)
	started := time.Now()
	for g := 0; g < cfg.concurrency; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var local []sample
			for time.Now().Before(stopAt) {
				n := counter.Add(1)
				w := ws[int(n)%len(ws)]
				kind, amount := "BET", "1.00"
				if n%4 == 0 {
					kind, amount = "WIN", "1.50"
				}
				ext := fmt.Sprintf("lt-%s-%d", run, n)
				payload, _ := json.Marshal(map[string]any{
					"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
					"roundId": "round-1", "gameId": "load", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"},
				})
				base := cfg.bases[int(n)%len(cfg.bases)]
				t0 := time.Now()
				status, firstBody, _ := raw(base, "POST", "/wagering/transactions", provider, "provider-a:"+ext, payload)
				local = append(local, sample{time.Since(t0), status})

				// reenvio idêntico por outra instância: deve devolver o mesmo resultado
				if int(n)%100 < cfg.replayPct && status == http.StatusOK {
					replays.Add(1)
					rs, replayBody, _ := raw(cfg.bases[(int(n)+1)%len(cfg.bases)], "POST", "/wagering/transactions", provider, "provider-a:"+ext, payload)
					if rs != http.StatusOK || !sameResult(firstBody, replayBody) {
						replayBad.Add(1)
					}
				}
			}
			mu.Lock()
			samples = append(samples, local...)
			for _, s := range local {
				statuses[s.status]++
			}
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(started)

	// mede o backlog da outbox ao fim da carga e o tempo até esvaziar
	lag, pending := outboxState(cfg.bases[0])
	backlog, drainStart := pending, time.Now()
	deadline := time.Now().Add(5 * time.Minute)
	for pending > 0 && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		_, pending = outboxState(cfg.bases[0])
	}
	drain := time.Since(drainStart)
	// reconciliação de todas as carteiras
	diverged := 0
	for i, w := range ws {
		var rec struct{ Consistent bool }
		if err := do(cfg.bases[i%len(cfg.bases)], "POST", "/wallets/"+w.id+"/reconciliation", internal, "", nil, &rec); err != nil || !rec.Consistent {
			diverged++
		}
	}

	report(cfg, samples, statuses, elapsed, replays.Load(), replayBad.Load(), lag, backlog, pending, drain, diverged)
	if diverged > 0 || replayBad.Load() > 0 {
		return fmt.Errorf("integridade violada: %d carteiras divergentes, %d replays divergentes", diverged, replayBad.Load())
	}
	return nil
}

func report(cfg config, samples []sample, statuses map[int]int, elapsed time.Duration, replays, replayBad int64, lag float64, backlog, pending int, drain time.Duration, diverged int) {
	lat := make([]time.Duration, 0, len(samples))
	for _, s := range samples {
		lat = append(lat, s.d)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(float64(len(lat))*p))]
	}
	ok := statuses[http.StatusOK]
	fmt.Println("=== Resultado do teste de carga ===")
	fmt.Printf("instâncias=%d concorrência=%d carteiras=%d duração=%s\n", len(cfg.bases), cfg.concurrency, cfg.wallets, elapsed.Round(time.Millisecond))
	fmt.Printf("requisições=%d  sucesso(200)=%d  vazão=%.1f req/s\n", len(samples), ok, float64(len(samples))/elapsed.Seconds())
	fmt.Printf("latência p50=%s p95=%s p99=%s máx=%s\n", pct(.50).Round(time.Microsecond), pct(.95).Round(time.Microsecond), pct(.99).Round(time.Microsecond), pct(1).Round(time.Microsecond))
	codes := make([]int, 0, len(statuses))
	for c := range statuses {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	fmt.Print("status HTTP:")
	for _, c := range codes {
		fmt.Printf(" %d×%d", c, statuses[c])
	}
	fmt.Println()
	errs := 0
	for c, n := range statuses {
		if c >= 500 || c == 0 {
			errs += n
		}
	}
	fmt.Printf("erros(5xx/rede)=%d  conflitos(409)=%d  rejeições(422)=%d\n", errs, statuses[409], statuses[422])
	fmt.Printf("replays idempotentes=%d divergentes=%d\n", replays, replayBad)
	fmt.Printf("outbox: backlog ao fim da carga=%d (atraso do mais antigo %.1fs); esvaziou em %s (%.0f eventos/s); pendentes finais=%d\n",
		backlog, lag, drain.Round(time.Second), float64(backlog)/max(drain.Seconds(), 0.001), pending)
	fmt.Printf("reconciliação: carteiras divergentes=%d de %d\n", diverged, cfg.wallets)
}

var gauge = regexp.MustCompile(`(?m)^(wager_outbox_lag_seconds|wager_outbox_pending) ([0-9.eE+-]+)$`)

func outboxState(base string) (lag float64, pending int) {
	resp, err := client.Get(base + "/metrics")
	if err != nil {
		return 0, 0
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, m := range gauge.FindAllStringSubmatch(string(raw), -1) {
		var v float64
		fmt.Sscan(m[2], &v)
		if m[1] == "wager_outbox_lag_seconds" {
			lag = v
		} else {
			pending = int(v)
		}
	}
	return
}

func sameResult(a, b []byte) bool {
	var x, y struct {
		TransactionID string                  `json:"transactionId"`
		Status        string                  `json:"status"`
		Balance       struct{ Amount string } `json:"balance"`
	}
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return x.TransactionID == y.TransactionID && x.Status == y.Status && x.Balance.Amount == y.Balance.Amount
}

func token(tokenURL, id, secret string) (string, error) {
	resp, err := client.PostForm(tokenURL, url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.AccessToken == "" {
		return "", fmt.Errorf("token for %s: status %d", id, resp.StatusCode)
	}
	return out.AccessToken, nil
}

func raw(base, method, path, tok, idem string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, nil
}

func do(base, method, path, tok, idem string, body []byte, out any) error {
	status, b, err := raw(base, method, path, tok, idem, body)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("%s %s: status %d: %s", method, path, status, b)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}
