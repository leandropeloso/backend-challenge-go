package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/event"
	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/infra/eventjson"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

type ledgerRepo struct{ q querier }

func (r *ledgerRepo) Append(ctx context.Context, e *ledger.Entry, walletVersion int64) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, currency, wallet_version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), string(e.Amount().Currency()), walletVersion, e.CreatedAt())
	if err != nil {
		return fmt.Errorf("append ledger entry: %w", err)
	}
	return nil
}

type inboxRepo struct{ q querier }

func (r *inboxRepo) Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (port.InboxResult, error) {
	tag, err := r.q.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, hash, now)
	if err != nil {
		return 0, fmt.Errorf("insert inbox message: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return port.InboxNew, nil
	}
	var stored string
	err = r.q.QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&stored)
	if err != nil {
		return 0, fmt.Errorf("read inbox message: %w", err)
	}
	if stored != hash {
		return port.InboxHashMismatch, nil
	}
	return port.InboxDuplicate, nil
}

func (r *inboxRepo) Complete(ctx context.Context, consumer, messageID string, now time.Time) error {
	tag, err := r.q.Exec(ctx, `UPDATE inbox_messages SET completed_at = $3
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, now)
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return port.ErrNotFound
	}
	return nil
}

type outboxRepo struct{ q querier }

func (r *outboxRepo) Add(ctx context.Context, e event.Event) error {
	payload, err := eventjson.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	_, err = r.q.Exec(ctx, `INSERT INTO outbox_events
		(id, aggregate_id, partition_key, event_type, event_version, correlation_id, payload, occurred_at, trace_context)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.AggregateID(), e.PartitionKey().String(), string(e.Type()), e.Version(),
		e.CorrelationID(), payload, e.OccurredAt(), nullable(telemetry.Inject(ctx)))
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// claimWindow limita quantos eventos pendentes (os mais antigos) a reserva
// examina por ciclo. Predecessores de qualquer evento têm seq menor, logo estão
// sempre dentro da janela, e a ordem por carteira é preservada. O custo da
// reserva passa a depender da janela, não do tamanho do backlog.
const claimWindow = 3000

// outboxClaimLock é a chave do lock consultivo que serializa as reservas da outbox.
const outboxClaimLock = 727275

// OutboxStore atende o publisher, usando o pool diretamente.
type OutboxStore struct{ s *Store }

func NewOutboxStore(s *Store) *OutboxStore { return &OutboxStore{s: s} }

// Claim reserva eventos prontos por lease. Só entram eventos que formam o
// prefixo pronto da sua partição (carteira), para manter a ordem por carteira;
// o UPDATE refaz a checagem de lease depois de qualquer espera por lock, então
// dois publishers nunca recebem o mesmo evento.
func (o *OutboxStore) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]port.OutboxMessage, error) {
	// A seleção e o UPDATE rodam sob um lock consultivo curto: sem ele, dois relays
	// calculam a seleção em snapshots diferentes (cortes distintos do LIMIT, ou
	// eventos de seq menor confirmados tarde) e o segundo acaba reservando
	// eventos POSTERIORES de uma carteira enquanto o primeiro segura os anteriores,
	// publicando fora de ordem. O lock dura só o tempo da reserva (milissegundos);
	// a publicação e o processamento das carteiras não são afetados.
	tx, err := o.s.pool.Begin(ctx)
	if err != nil {
		return nil, translate(transient(fmt.Errorf("claim outbox: %w", err)))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, outboxClaimLock); err != nil {
		return nil, translate(transient(fmt.Errorf("claim outbox lock: %w", err)))
	}
	rows, err := tx.Query(ctx, `
		WITH head AS (
			SELECT id, seq, partition_key, next_attempt_at, locked_until
			FROM outbox_events
			WHERE published_at IS NULL
			ORDER BY seq
			LIMIT $4
		), ranked AS (
			SELECT id, seq,
			       bool_and(next_attempt_at <= now() AND (locked_until IS NULL OR locked_until < now()))
			           OVER (PARTITION BY partition_key ORDER BY seq) AS ready
			FROM head
		), picked AS (
			SELECT id FROM ranked WHERE ready ORDER BY seq LIMIT $3
		)
		UPDATE outbox_events o
		   SET locked_until = now() + $2::bigint * interval '1 millisecond',
		       locked_by = $1,
		       attempts = o.attempts + 1
		  FROM picked
		 WHERE o.id = picked.id
		   AND o.published_at IS NULL
		   AND (o.locked_until IS NULL OR o.locked_until < now())
		RETURNING o.id, o.seq, o.partition_key, o.event_type, o.payload, o.attempts, o.occurred_at, COALESCE(o.trace_context, '')`,
		owner, lease.Milliseconds(), limit, claimWindow)
	if err != nil {
		return nil, translate(transient(fmt.Errorf("claim outbox: %w", err)))
	}
	defer rows.Close()

	type claimed struct {
		seq int64
		msg port.OutboxMessage
	}
	var out []claimed
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.msg.ID, &c.seq, &c.msg.PartitionKey, &c.msg.EventType, &c.msg.Payload,
			&c.msg.Attempts, &c.msg.OccurredAt, &c.msg.TraceContext); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, translate(transient(fmt.Errorf("claim outbox: %w", err)))
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, translate(transient(fmt.Errorf("claim outbox commit: %w", err)))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	msgs := make([]port.OutboxMessage, len(out))
	for i, c := range out {
		msgs[i] = c.msg
	}
	return msgs, nil
}

func (o *OutboxStore) MarkPublishedMany(ctx context.Context, ids []uuid.UUID, owner string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := o.s.pool.Exec(ctx, `UPDATE outbox_events
		SET published_at = now(), locked_until = NULL, locked_by = NULL, last_error = NULL
		WHERE id = ANY($1) AND locked_by = $2 AND published_at IS NULL`, ids, owner)
	if err != nil {
		return translate(transient(fmt.Errorf("mark published: %w", err)))
	}
	return nil
}

func (o *OutboxStore) Release(ctx context.Context, id uuid.UUID, owner, cause string, retryAt time.Time) error {
	_, err := o.s.pool.Exec(ctx, `UPDATE outbox_events
		SET locked_until = NULL, locked_by = NULL, last_error = $3, next_attempt_at = $4
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, truncate(cause, 500), retryAt)
	if err != nil {
		return translate(transient(fmt.Errorf("release outbox event: %w", err)))
	}
	return nil
}

func (o *OutboxStore) Stats(ctx context.Context) (port.OutboxStats, error) {
	var (
		st port.OutboxStats
		ms int64
	)
	// A idade vem do evento mais antigo (menor seq), lido pelo índice parcial.
	err := o.s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
		COALESCE((SELECT (EXTRACT(EPOCH FROM now() - created_at) * 1000)::bigint
		          FROM outbox_events WHERE published_at IS NULL ORDER BY seq LIMIT 1), 0)`).Scan(&st.Pending, &ms)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return st, nil
		}
		return st, translate(transient(fmt.Errorf("outbox stats: %w", err)))
	}
	st.OldestAge = time.Duration(ms) * time.Millisecond
	return st, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
