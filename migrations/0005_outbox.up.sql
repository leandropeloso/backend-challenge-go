CREATE TABLE outbox_events (
    id             uuid        PRIMARY KEY,
    seq            bigint      GENERATED ALWAYS AS IDENTITY,
    aggregate_id   uuid        NOT NULL,
    partition_key  text        NOT NULL,
    event_type     text        NOT NULL,
    event_version  integer     NOT NULL,
    correlation_id text        NOT NULL,
    payload        jsonb       NOT NULL,
    occurred_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    attempts       integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until   timestamptz,
    locked_by      text,
    last_error     text,
    published_at   timestamptz
);

CREATE UNIQUE INDEX outbox_events_seq_key ON outbox_events (seq);

CREATE INDEX outbox_events_unpublished ON outbox_events (partition_key, seq)
    WHERE published_at IS NULL;

-- O payload é um snapshot imutável: só os campos de controle de entrega mudam.
CREATE FUNCTION outbox_events_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox events cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.payload <> OLD.payload OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version OR NEW.aggregate_id <> OLD.aggregate_id
       OR NEW.partition_key <> OLD.partition_key OR NEW.occurred_at <> OLD.occurred_at
       OR NEW.correlation_id <> OLD.correlation_id OR NEW.seq <> OLD.seq THEN
        RAISE EXCEPTION 'outbox event content is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'published outbox events cannot change' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_events_guard BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();
