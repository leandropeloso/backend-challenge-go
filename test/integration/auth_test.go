//go:build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestUnauthenticatedAccessHasNoEffects(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")
	before := dbCount(t, `SELECT count(*) FROM wager_transactions`)
	walletsBefore := dbCount(t, `SELECT count(*) FROM wallets`)

	forged, _ := rsa.GenerateKey(rand.Reader, 2048)
	forgedTok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": env.issuer, "aud": "wager-api", "exp": time.Now().Add(time.Hour).Unix(), "providerId": "provider-a",
		"realm_access": map[string]any{"roles": []string{"provider", "internal"}},
	})
	forgedTok.Header["kid"] = "forged"
	forgedStr, err := forgedTok.SignedString(forged)
	if err != nil {
		t.Fatal(err)
	}
	noRole := token(t, "no-role-client", "no-role-secret")

	tokens := map[string]string{
		"sem token":          "",
		"lixo":               "not-a-token",
		"assinatura forjada": forgedStr,
		"sem papéis":         noRole,
	}
	body := w.req(op{kind: "BET", amount: "10.00"})
	for name, tok := range tokens {
		for _, rt := range []struct {
			method, path string
			body         any
			key          string
		}{
			{"POST", "/wagering/transactions", body, "k-unauth"},
			{"POST", "/wallets", map[string]any{"playerId": uuid.NewString(), "initialBalance": money("1.00")}, ""},
			{"GET", "/wallets/" + w.id, nil, ""},
			{"GET", "/wallets/" + w.id + "/ledger", nil, ""},
			{"POST", "/wallets/" + w.id + "/reconciliation", nil, ""},
			{"GET", "/wagering/transactions/" + uuid.NewString(), nil, ""},
			{"GET", "/providers/provider-a/wagering/transactions/x", nil, ""},
		} {
			r := call(t, inst.base, rt.method, rt.path, tok, rt.key, rt.body)
			if r.status != http.StatusUnauthorized {
				t.Errorf("%s %s com %s: status %d", rt.method, rt.path, name, r.status)
			}
			if r.header.Get("WWW-Authenticate") == "" {
				t.Errorf("%s %s com %s: faltou WWW-Authenticate", rt.method, rt.path, name)
			}
		}
	}
	if after := dbCount(t, `SELECT count(*) FROM wager_transactions`); after != before {
		t.Fatalf("acessos negados criaram %d transações", after-before)
	}
	if after := dbCount(t, `SELECT count(*) FROM wallets`); after != walletsBefore {
		t.Fatalf("acessos negados criaram %d carteiras", after-walletsBefore)
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
}

func TestExpiredTokenIsRejected(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	short := fetchToken(t, "provider-a-short-lived", "provider-a-short-secret") // vive 2s
	o := op{external: "exp-" + w.id, kind: "BET", amount: "1.00"}
	r := call(t, inst.base, "POST", "/wagering/transactions", short, "provider-a:"+o.external, w.req(o))
	mustStatus(t, r, http.StatusOK) // ainda válido

	time.Sleep(6 * time.Second)
	o2 := op{external: "exp2-" + w.id, kind: "BET", amount: "1.00"}
	r = call(t, inst.base, "POST", "/wagering/transactions", short, "provider-a:"+o2.external, w.req(o2))
	mustStatus(t, r, http.StatusUnauthorized)
	if _, _, found := dbTxStatus(t, "provider-a", o2.external); found {
		t.Fatal("token expirado não pode gerar transação")
	}
}

func TestRolesAreEnforced(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	// provedores não operam carteiras
	for _, rt := range []struct{ method, path string }{
		{"POST", "/wallets"}, {"GET", "/wallets/" + w.id}, {"GET", "/wallets/" + w.id + "/ledger"},
		{"POST", "/wallets/" + w.id + "/reconciliation"},
	} {
		r := call(t, inst.base, rt.method, rt.path, providerA(t), "", map[string]any{"playerId": uuid.NewString(), "initialBalance": money("1.00")})
		if r.status != http.StatusForbidden {
			t.Errorf("%s %s com provedor: status %d", rt.method, rt.path, r.status)
		}
	}
	// o serviço interno não envia operações como provedor
	r := call(t, inst.base, "POST", "/wagering/transactions", internalTok(t), "provider-a:x", w.req(op{external: "x", kind: "BET", amount: "1.00"}))
	mustStatus(t, r, http.StatusForbidden)
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
}

// Um provedor nunca acessa dados de outro: consultas, replays e envios.
func TestProviderIsolation(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 2, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "100.00")

	a := op{external: "iso-a-" + w.id, kind: "BET", amount: "10.00"}
	ra, a := submit(t, nodes[0], w, a)
	mustStatus(t, ra, http.StatusOK)
	txID := ra.str("transactionId")

	// B não lê a transação de A, nem por id nem pelo caminho do provedor
	r := call(t, nodes[1].base, "GET", "/wagering/transactions/"+txID, providerB(t), "", nil)
	mustStatus(t, r, http.StatusNotFound)
	r = call(t, nodes[1].base, "GET", "/providers/provider-a/wagering/transactions/"+a.external, providerB(t), "", nil)
	mustStatus(t, r, http.StatusForbidden)
	// nem conhece a existência: o próprio caminho de B não encontra a operação de A
	r = call(t, nodes[1].base, "GET", "/providers/provider-b/wagering/transactions/"+a.external, providerB(t), "", nil)
	mustStatus(t, r, http.StatusNotFound)

	// B não reenvia (replay) a operação de A se declarando A
	replay := call(t, nodes[1].base, "POST", "/wagering/transactions", providerB(t), a.key, w.req(a))
	mustStatus(t, replay, http.StatusForbidden)
	if replay.str("balance", "amount") != "" || replay.str("transactionId") != "" {
		t.Fatalf("a resposta de erro não pode vazar dados: %s", replay.raw)
	}

	// o dono lê normalmente; o serviço interno também
	mustStatus(t, call(t, nodes[1].base, "GET", "/wagering/transactions/"+txID, providerA(t), "", nil), http.StatusOK)
	mustStatus(t, call(t, nodes[1].base, "GET", "/wagering/transactions/"+txID, internalTok(t), "", nil), http.StatusOK)
	mustStatus(t, call(t, nodes[1].base, "GET", "/providers/provider-a/wagering/transactions/"+a.external, providerA(t), "", nil), http.StatusOK)

	// a abertura de carteira (OPENING) é interna: nenhum provedor a enxerga
	var openingID string
	if err := pool.QueryRow(t.Context(), `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.id).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	mustStatus(t, call(t, nodes[0].base, "GET", "/wagering/transactions/"+openingID, providerA(t), "", nil), http.StatusNotFound)
	mustStatus(t, call(t, nodes[0].base, "GET", "/wagering/transactions/"+openingID, internalTok(t), "", nil), http.StatusOK)

	if bal := dbBalance(t, w.id); bal != 9000 {
		t.Fatalf("saldo = %d: tentativas cruzadas alteraram o saldo", bal)
	}
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND provider_id = 'provider-b'`, w.id); n != 0 {
		t.Fatalf("provedor B tem %d transações indevidas", n)
	}
}
