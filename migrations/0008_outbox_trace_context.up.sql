-- Contexto W3C (traceparent) do trace que originou o evento, para que a publicação
-- continue o mesmo trace mesmo feita por outro processo, depois do commit.
ALTER TABLE outbox_events ADD COLUMN trace_context text;
