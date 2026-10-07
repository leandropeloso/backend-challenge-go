-- A reserva de eventos olha só a "cabeça" da fila (os mais antigos não publicados).
-- Este índice parcial a torna proporcional ao tamanho da janela, não ao backlog.
CREATE INDEX outbox_events_unpublished_seq ON outbox_events (seq) WHERE published_at IS NULL;
