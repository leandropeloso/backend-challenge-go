//go:build integration

package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestOpenWalletRules(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false"))

	player := uuid.NewString()
	body := map[string]any{"playerId": player, "initialBalance": money("1000.00")}
	r := call(t, inst.base, "POST", "/wallets", internalTok(t), "", body)
	mustStatus(t, r, http.StatusCreated)
	if r.str("balance", "amount") != "1000.00" || r.body["version"].(float64) != 1 {
		t.Fatalf("resposta: %s", r.raw)
	}
	walletID := r.str("id")

	// OPENING + ledger + dois eventos de outbox, no mesmo commit
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL'`, walletID); n != 1 {
		t.Fatalf("transações OPENING = %d", n)
	}
	if entries, _, credits := dbLedger(t, walletID); entries != 1 || credits != 1 {
		t.Fatalf("ledger: %d lançamentos / %d créditos", entries, credits)
	}
	if n := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, walletID); n != 2 {
		t.Fatalf("eventos de outbox = %d, quer 2 (WagerTransactionProcessed + WalletBalanceChanged)", n)
	}

	// mesmo jogador e moeda: conflito
	dup := call(t, inst.base, "POST", "/wallets", internalTok(t), "", body)
	mustStatus(t, dup, http.StatusConflict)

	// saldo zero: sem OPENING, ledger nem eventos financeiros
	zero := call(t, inst.base, "POST", "/wallets", internalTok(t), "", map[string]any{
		"playerId": uuid.NewString(), "initialBalance": money("0.00")})
	mustStatus(t, zero, http.StatusCreated)
	zid := zero.str("id")
	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zid); n != 0 {
		t.Fatalf("saldo zero não deve criar OPENING (%d)", n)
	}
	if entries, _, _ := dbLedger(t, zid); entries != 0 {
		t.Fatalf("ledger = %d", entries)
	}
	if n := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, zid); n != 0 {
		t.Fatalf("eventos = %d", n)
	}

	// entradas inválidas
	for name, b := range map[string]any{
		"negativo":     map[string]any{"playerId": uuid.NewString(), "initialBalance": money("-1.00")},
		"escala":       map[string]any{"playerId": uuid.NewString(), "initialBalance": money("1.005")},
		"jogador":      map[string]any{"playerId": "nao-uuid", "initialBalance": money("1.00")},
		"moeda":        map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "XYZ"}},
		"valor número": `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":10.5,"currency":"BRL"}}`,
	} {
		if r := call(t, inst.base, "POST", "/wallets", internalTok(t), "", b); r.status != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s)", name, r.status, r.raw)
		}
	}

	get := call(t, inst.base, "GET", "/wallets/"+walletID, internalTok(t), "", nil)
	mustStatus(t, get, http.StatusOK)
	if call(t, inst.base, "GET", "/wallets/"+uuid.NewString(), internalTok(t), "", nil).status != http.StatusNotFound {
		t.Error("carteira inexistente deve ser 404")
	}
}

func TestInvalidInputsAreRejectedWithoutEffects(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	cases := map[string]op{
		"OPENING por HTTP":      {kind: "OPENING", amount: "10.00"},
		"tipo desconhecido":     {kind: "JACKPOT", amount: "10.00"},
		"BET zero":              {kind: "BET", amount: "0.00"},
		"WIN zero":              {kind: "WIN", amount: "0.00"},
		"LOSS com valor":        {kind: "LOSS", amount: "1.00"},
		"valor negativo":        {kind: "BET", amount: "-5.00"},
		"uma casa":              {kind: "BET", amount: "5.5"},
		"sem casas":             {kind: "BET", amount: "5"},
		"três casas":            {kind: "BET", amount: "5.001"},
		"notação científica":    {kind: "BET", amount: "1e2"},
		"NaN":                   {kind: "BET", amount: "NaN"},
		"Infinity":              {kind: "BET", amount: "Infinity"},
		"vazio":                 {kind: "BET", amount: ""},
		"overflow":              {kind: "BET", amount: "99999999999999999999.00"},
		"REFUND sem referência": {kind: "REFUND", amount: "5.00"},
		"BET com referência":    {kind: "BET", amount: "5.00", ref: "x"},
	}
	for name, o := range cases {
		r, _ := submit(t, inst, w, o)
		if r.status != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s)", name, r.status, r.raw)
		}
	}
	// número JSON no lugar da string decimal
	raw := `{"providerId":"provider-a","externalTransactionId":"n1","playerId":"` + w.player + `","walletId":"` + w.id +
		`","roundId":"r","gameId":"g","kind":"BET","money":{"amount":25.00,"currency":"BRL"}}`
	if r := call(t, inst.base, "POST", "/wagering/transactions", providerA(t), "k-num", raw); r.status != http.StatusBadRequest {
		t.Errorf("número JSON: status %d", r.status)
	}
	// header obrigatório
	r := call(t, inst.base, "POST", "/wagering/transactions", providerA(t), "", w.req(op{kind: "BET", amount: "1.00"}))
	mustStatus(t, r, http.StatusBadRequest)

	if n := dbCount(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind <> 'OPENING'`, w.id); n != 0 {
		t.Fatalf("entradas inválidas deixaram %d transações", n)
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
}

func TestWinAndLossPolicy(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	bet, _ := submit(t, inst, w, op{external: "b-" + w.id, kind: "BET", amount: "10.00"})
	mustStatus(t, bet, http.StatusOK)
	versionAfterBet := dbVersion(t, w.id)

	loss, _ := submit(t, inst, w, op{kind: "LOSS", amount: "0.00", ref: "b-" + w.id})
	mustStatus(t, loss, http.StatusOK)
	if loss.str("balance", "amount") != "90.00" {
		t.Fatalf("saldo da LOSS = %s", loss.str("balance", "amount"))
	}
	if dbVersion(t, w.id) != versionAfterBet {
		t.Fatal("LOSS não pode alterar a versão da carteira")
	}
	if entries, _, _ := dbLedger(t, w.id); entries != 2 {
		t.Fatalf("LOSS não cria ledger (entradas = %d)", entries)
	}

	win, _ := submit(t, inst, w, op{kind: "WIN", amount: "35.00", ref: "b-" + w.id})
	mustStatus(t, win, http.StatusOK)
	if win.str("balance", "amount") != "125.00" {
		t.Fatalf("saldo da WIN = %s", win.str("balance", "amount"))
	}
	// WIN apontando para algo que não é aposta
	badRef, _ := submit(t, inst, w, op{kind: "WIN", amount: "1.00", ref: "b-" + w.id + "-inexistente"})
	mustStatus(t, badRef, http.StatusAccepted) // referência ainda não chegou: fica em espera
	assertLedgerMatchesBalance(t, w.id)
}

func TestRefundAndRollbackRules(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	bet := op{external: "bet-" + w.id, kind: "BET", amount: "40.00"}
	r, _ := submit(t, inst, w, bet)
	mustStatus(t, r, http.StatusOK)

	// reembolso integral
	refund, _ := submit(t, inst, w, op{kind: "REFUND", amount: "40.00", ref: bet.external})
	mustStatus(t, refund, http.StatusOK)
	if refund.str("balance", "amount") != "100.00" {
		t.Fatalf("saldo após refund = %s", refund.str("balance", "amount"))
	}
	// segundo reembolso da mesma aposta
	again, _ := submit(t, inst, w, op{kind: "REFUND", amount: "40.00", ref: bet.external})
	mustStatus(t, again, http.StatusUnprocessableEntity)
	if again.str("failureCode") != "ALREADY_REVERSED" {
		t.Fatalf("code = %s", again.str("failureCode"))
	}
	// ROLLBACK da aposta já reembolsada: REFUND e ROLLBACK são mutuamente exclusivos
	rb, _ := submit(t, inst, w, op{kind: "ROLLBACK", amount: "40.00", ref: bet.external})
	mustStatus(t, rb, http.StatusUnprocessableEntity)
	if rb.str("failureCode") != "ALREADY_REVERSED" {
		t.Fatalf("code = %s", rb.str("failureCode"))
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d: devolução duplicada", bal)
	}

	// ROLLBACK de WIN debita
	win := op{external: "win-" + w.id, kind: "WIN", amount: "25.00"}
	if r, _ := submit(t, inst, w, win); r.status != http.StatusOK {
		t.Fatalf("win: %s", r.raw)
	}
	rbWin, _ := submit(t, inst, w, op{kind: "ROLLBACK", amount: "25.00", ref: win.external})
	mustStatus(t, rbWin, http.StatusOK)
	if rbWin.str("balance", "amount") != "100.00" {
		t.Fatalf("saldo = %s", rbWin.str("balance", "amount"))
	}

	// ROLLBACK de REFUND debita de novo; depois a aposta não pode ser reembolsada outra vez
	refundExt := externalOf(t, refund.str("transactionId"))
	rbOfRefund, _ := submit(t, inst, w, op{kind: "ROLLBACK", amount: "40.00", ref: refundExt})
	mustStatus(t, rbOfRefund, http.StatusOK)
	if rbOfRefund.str("balance", "amount") != "60.00" {
		t.Fatalf("saldo = %s", rbOfRefund.str("balance", "amount"))
	}
	refundAgain, _ := submit(t, inst, w, op{kind: "REFUND", amount: "40.00", ref: bet.external})
	mustStatus(t, refundAgain, http.StatusUnprocessableEntity)

	// reversão que deixaria o saldo negativo usa código próprio
	w2 := openWallet(t, inst, "0.00")
	winOnly := op{external: "w2-win-" + w2.id, kind: "WIN", amount: "50.00"}
	mustStatus(t, mustSubmit(t, inst, w2, winOnly), http.StatusOK)
	spend := op{kind: "BET", amount: "50.00"}
	mustStatus(t, mustSubmit(t, inst, w2, spend), http.StatusOK)
	rbNoFunds, _ := submit(t, inst, w2, op{kind: "ROLLBACK", amount: "50.00", ref: winOnly.external})
	mustStatus(t, rbNoFunds, http.StatusUnprocessableEntity)
	if rbNoFunds.str("failureCode") != "INSUFFICIENT_FUNDS_FOR_REVERSAL" {
		t.Fatalf("code = %s", rbNoFunds.str("failureCode"))
	}
	betNoFunds, _ := submit(t, inst, w2, op{kind: "BET", amount: "10.00"})
	if betNoFunds.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("code da aposta = %s", betNoFunds.str("failureCode"))
	}

	assertLedgerMatchesBalance(t, w.id)
	assertLedgerMatchesBalance(t, w2.id)
}

func mustSubmit(t *testing.T, inst *instance, w testWallet, o op) response {
	t.Helper()
	r, _ := submit(t, inst, w, o)
	return r
}

// externalOf devolve o externalTransactionId de uma transação (para referenciá-la).
func externalOf(t *testing.T, transactionID string) string {
	t.Helper()
	var ext string
	if err := pool.QueryRow(context.Background(), `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, transactionID).Scan(&ext); err != nil {
		t.Fatal(err)
	}
	return ext
}

func TestReferenceMismatchesAreRejected(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")
	other := openWallet(t, inst, "100.00")

	bet := op{external: "bet-" + w.id, kind: "BET", amount: "20.00", round: "round-X"}
	mustStatus(t, mustSubmit(t, inst, w, bet), http.StatusOK)

	cases := []struct {
		name string
		w    testWallet
		o    op
		code string
	}{
		{"valor diferente", w, op{kind: "REFUND", amount: "10.00", ref: bet.external, round: "round-X"}, "REFERENCE_AMOUNT_MISMATCH"},
		{"rodada diferente", w, op{kind: "REFUND", amount: "20.00", ref: bet.external, round: "round-Y"}, "REFERENCE_MISMATCH"},
		{"outra carteira", other, op{kind: "REFUND", amount: "20.00", ref: bet.external, round: "round-X"}, "REFERENCE_MISMATCH"},
		{"REFUND de WIN", w, op{kind: "REFUND", amount: "5.00", ref: "win-" + w.id, round: "round-X"}, ""},
	}
	win := op{external: "win-" + w.id, kind: "WIN", amount: "5.00", round: "round-X"}
	mustStatus(t, mustSubmit(t, inst, w, win), http.StatusOK)
	cases[3].code = "REFERENCE_KIND_NOT_ALLOWED"

	for _, c := range cases {
		r, _ := submit(t, inst, c.w, c.o)
		if r.status != http.StatusUnprocessableEntity || r.str("failureCode") != c.code {
			t.Errorf("%s: status %d code %s (%s)", c.name, r.status, r.str("failureCode"), r.raw)
		}
		if r.body["correctable"] != true {
			t.Errorf("%s: deveria ser marcada como corrigível", c.name)
		}
	}

	// jogador diferente do dono da carteira
	imposter := testWallet{id: w.id, player: uuid.NewString()}
	r, _ := submit(t, inst, imposter, op{kind: "BET", amount: "1.00"})
	if r.status != http.StatusUnprocessableEntity || r.str("failureCode") != "PLAYER_MISMATCH" {
		t.Errorf("jogador: %d %s", r.status, r.raw)
	}
	// carteira inexistente
	ghost := testWallet{id: uuid.NewString(), player: uuid.NewString()}
	r, _ = submit(t, inst, ghost, op{kind: "BET", amount: "1.00"})
	if r.status != http.StatusUnprocessableEntity || r.str("failureCode") != "WALLET_NOT_FOUND" {
		t.Errorf("carteira: %d %s", r.status, r.raw)
	}
	// moeda diferente da carteira
	usd := w.req(op{kind: "BET", amount: "1.00"})
	usd["money"] = map[string]string{"amount": "1.00", "currency": "USD"}
	r = call(t, inst.base, "POST", "/wagering/transactions", providerA(t), "k-usd-"+w.id, usd)
	if r.status != http.StatusUnprocessableEntity || r.str("failureCode") != "CURRENCY_MISMATCH" {
		t.Errorf("moeda: %d %s", r.status, r.raw)
	}
}

// Reversão que chega antes da referência fica pendente e é resolvida depois.
func TestReversalBeforeReferenceIsResolvedLater(t *testing.T) {
	q := newQueues(t)
	nodes := startCluster(t, q, 3, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, nodes[0], "100.00")

	betExt := "late-bet-" + w.id
	refund, refundOp := submit(t, nodes[0], w, op{kind: "REFUND", amount: "30.00", ref: betExt})
	mustStatus(t, refund, http.StatusAccepted)
	if refund.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("status = %s", refund.str("status"))
	}
	if refund.header.Get("Location") == "" {
		t.Error("202 deve apontar para o recurso acompanhável")
	}
	txID := refund.str("transactionId")

	// consulta de acompanhamento
	got := call(t, nodes[1].base, "GET", "/wagering/transactions/"+txID, providerA(t), "", nil)
	mustStatus(t, got, http.StatusOK)
	if got.str("status") != "PENDING_REFERENCE" || got.str("nextAttemptAt") == "" || got.str("expiresAt") == "" {
		t.Fatalf("pendência: %s", got.raw)
	}
	// replay enquanto pendente
	replay, _ := submit(t, nodes[2], w, refundOp)
	mustStatus(t, replay, http.StatusAccepted)
	if !replay.boolean("idempotentReplay") || replay.str("transactionId") != txID {
		t.Fatalf("replay pendente: %s", replay.raw)
	}

	// a aposta chega depois
	mustStatus(t, mustSubmit(t, nodes[1], w, op{external: betExt, kind: "BET", amount: "30.00"}), http.StatusOK)

	eventually(t, 30*time.Second, "refund pendente ser processado", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", refundOp.external)
		return s == "PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d, quer 10000", bal)
	}
	done := call(t, nodes[2].base, "GET", "/providers/provider-a/wagering/transactions/"+refundOp.external, providerA(t), "", nil)
	mustStatus(t, done, http.StatusOK)
	if done.str("status") != "PROCESSED" || done.str("balance", "amount") != "100.00" {
		t.Fatalf("resultado: %s", done.raw)
	}
	// o replay agora devolve o resultado final
	final, _ := submit(t, nodes[0], w, refundOp)
	mustStatus(t, final, http.StatusOK)
	if !final.boolean("idempotentReplay") {
		t.Fatal("esperava replay")
	}
	if _, debits, credits := dbLedger(t, w.id); debits != 1 || credits != 2 {
		t.Fatalf("ledger: %d débitos / %d créditos", debits, credits)
	}
	assertLedgerMatchesBalance(t, w.id)
}

// Sem a referência, a pendência expira e vira REJECTED (REFERENCE_NOT_FOUND).
func TestPendingReferenceExpires(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "PENDING_TTL", "4s"))
	w := openWallet(t, inst, "100.00")

	r, o := submit(t, inst, w, op{kind: "ROLLBACK", amount: "10.00", ref: "never-" + w.id})
	mustStatus(t, r, http.StatusAccepted)

	eventually(t, 30*time.Second, "pendência expirar", func() bool {
		s, code, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "REJECTED" && code == "REFERENCE_NOT_FOUND"
	})
	final, _ := submit(t, inst, w, o)
	mustStatus(t, final, http.StatusUnprocessableEntity)
	if !final.boolean("idempotentReplay") || final.str("failureCode") != "REFERENCE_NOT_FOUND" {
		t.Fatalf("replay: %s", final.raw)
	}
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
	if _, _, found := dbTxStatus(t, "provider-a", o.external); !found {
		t.Fatal("a rejeição deve ficar auditável")
	}
	events := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND event_type IN ('WagerTransactionPendingReference', 'WagerTransactionRejected')`, w.id)
	if events != 2 {
		t.Fatalf("eventos de pendência/rejeição = %d, quer 2", events)
	}
}

// O número máximo de tentativas também encerra a pendência (sem esperar o TTL).
func TestPendingReferenceExpiresByAttemptLimit(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "PENDING_TTL", "10m", "PENDING_MAX_ATTEMPTS", "3"))
	w := openWallet(t, inst, "100.00")

	start := time.Now()
	r, o := submit(t, inst, w, op{kind: "REFUND", amount: "10.00", ref: "ghost-" + w.id})
	mustStatus(t, r, http.StatusAccepted)
	eventually(t, 30*time.Second, "limite de tentativas", func() bool {
		s, code, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "REJECTED" && code == "REFERENCE_NOT_FOUND"
	})
	if time.Since(start) > 20*time.Second {
		t.Fatalf("o limite de tentativas deveria encerrar bem antes do TTL de 10m (%s)", time.Since(start))
	}
	var attempts int
	if err := pool.QueryRow(t.Context(), `SELECT attempts FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1`, o.external).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts < 2 {
		t.Fatalf("tentativas registradas = %d", attempts)
	}
}

// Uma reversão que aponta para outra reversão ainda pendente espera; quando a
// referência é rejeitada, ela também é, com o código REFERENCE_NOT_PROCESSED.
func TestReversalOfAPendingReversalIsRejectedWhenItFails(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "PENDING_TTL", "4s"))
	w := openWallet(t, inst, "100.00")

	first, firstOp := submit(t, inst, w, op{kind: "REFUND", amount: "10.00", ref: "ghost-" + w.id})
	mustStatus(t, first, http.StatusAccepted)
	second, secondOp := submit(t, inst, w, op{kind: "ROLLBACK", amount: "10.00", ref: firstOp.external})
	mustStatus(t, second, http.StatusAccepted)

	eventually(t, 40*time.Second, "as duas pendências terminarem", func() bool {
		s1, c1, _ := dbTxStatus(t, "provider-a", firstOp.external)
		s2, c2, _ := dbTxStatus(t, "provider-a", secondOp.external)
		return s1 == "REJECTED" && c1 == "REFERENCE_NOT_FOUND" && s2 == "REJECTED" && c2 == "REFERENCE_NOT_PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 10000 {
		t.Fatalf("saldo = %d", bal)
	}
}

// Referência que existe mas terminou sem sucesso: rejeição imediata e distinta.
func TestReferenceThatWasRejectedRejectsTheReversal(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "10.00")

	bet := op{external: "broke-" + w.id, kind: "BET", amount: "50.00"}
	r, _ := submit(t, inst, w, bet)
	mustStatus(t, r, http.StatusUnprocessableEntity)

	refund, _ := submit(t, inst, w, op{kind: "REFUND", amount: "50.00", ref: bet.external})
	mustStatus(t, refund, http.StatusUnprocessableEntity)
	if refund.str("failureCode") != "REFERENCE_NOT_PROCESSED" {
		t.Fatalf("code = %s", refund.str("failureCode"))
	}
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "1000.00")
	for i := 0; i < 11; i++ {
		mustStatus(t, mustSubmit(t, inst, w, op{kind: "BET", amount: "1.00"}), http.StatusOK)
	}

	var seen []int
	cursor := ""
	pages := 0
	for {
		path := "/wallets/" + w.id + "/ledger?limit=4"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := call(t, inst.base, "GET", path, internalTok(t), "", nil)
		mustStatus(t, r, http.StatusOK)
		items := r.body["items"].([]any)
		for _, it := range items {
			seen = append(seen, int(it.(map[string]any)["walletVersion"].(float64)))
		}
		pages++
		next, _ := r.body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatal("paginação não terminou")
		}
	}
	if len(seen) != 12 || pages != 3 {
		t.Fatalf("itens = %d em %d páginas (%v)", len(seen), pages, seen)
	}
	for i, v := range seen {
		if v != i+1 {
			t.Fatalf("ordem instável: %v", seen)
		}
	}

	if r := call(t, inst.base, "GET", "/wallets/"+w.id+"/ledger?cursor=%21%21", internalTok(t), "", nil); r.status != http.StatusBadRequest {
		t.Errorf("cursor inválido: %d", r.status)
	}
	if r := call(t, inst.base, "GET", "/wallets/"+w.id+"/ledger?limit=0", internalTok(t), "", nil); r.status != http.StatusBadRequest {
		t.Errorf("limit=0: %d", r.status)
	}

	rec := call(t, inst.base, "POST", "/wallets/"+w.id+"/reconciliation", internalTok(t), "", nil)
	mustStatus(t, rec, http.StatusOK)
	if !rec.boolean("consistent") || rec.str("storedBalance", "amount") != "989.00" ||
		rec.str("calculatedBalance", "amount") != "989.00" || rec.str("difference", "amount") != "0.00" ||
		rec.body["checkedEntries"].(float64) != 12 {
		t.Fatalf("reconciliação: %s", rec.raw)
	}
	if call(t, inst.base, "POST", "/wallets/"+uuid.NewString()+"/reconciliation", internalTok(t), "", nil).status != http.StatusNotFound {
		t.Error("reconciliação de carteira inexistente deve ser 404")
	}
	if !strings.Contains(string(rec.raw), `"walletId"`) {
		t.Error("resposta sem walletId")
	}
}
