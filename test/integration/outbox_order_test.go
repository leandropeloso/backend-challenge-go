//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

// Vários relays com lotes pequenos e polling agressivo disputam a mesma outbox
// enquanto novos eventos chegam. Regressão da corrida de reserva parcial: um
// relay não pode reservar eventos posteriores de uma carteira enquanto outro
// ainda segura os anteriores. A ordem é conferida pelo SequenceNumber do broker.
func TestOutboxKeepsPerWalletOrderUnderConcurrentRelays(t *testing.T) {
	q := newQueues(t)
	writer := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "RUN_OUTBOX", "false", "RUN_RESOLVER", "false"))

	const wallets, betsPerWallet = 30, 20
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, writer, "1000.00")
	}

	// cinco relays, lotes de 8 e polling a cada 1 ms: cortes de LIMIT diferentes a cada ciclo
	startCluster(t, q, 5, withEnv("RUN_CONSUMER", "false", "RUN_RESOLVER", "false",
		"OUTBOX_BATCH_SIZE", "8", "OUTBOX_POLL_INTERVAL", "1ms", "OUTBOX_CONCURRENCY", "4"))

	// eventos continuam chegando enquanto os relays trabalham
	parallel(wallets, func(i int) {
		for k := 0; k < betsPerWallet; k++ {
			r, _ := submit(t, writer, ws[i], op{kind: "BET", amount: "1.00"})
			if r.status != http.StatusOK {
				t.Errorf("status %d: %s", r.status, r.raw)
				return
			}
		}
	})

	eventually(t, 90*time.Second, "todos os eventos publicados", func() bool {
		return dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = ANY($1) AND published_at IS NULL`, partitions(ws)) == 0
	})
	if n := dbCount(t, `SELECT count(*) FROM outbox_events WHERE partition_key = ANY($1) AND attempts <> 1`, partitions(ws)); n != 0 {
		t.Fatalf("%d eventos foram reservados mais de uma vez", n)
	}
	checkBrokerOrdering(t, q, ws)
}
