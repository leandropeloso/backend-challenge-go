-- wallet_id não tem FK de propósito: operações para carteiras inexistentes
-- precisam ficar registradas como REJECTED (WALLET_NOT_FOUND) para auditoria
-- e para que o replay devolva o mesmo resultado.
CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    wallet_id                         uuid        NOT NULL,
    player_id                         uuid        NOT NULL,
    round_id                          text,
    game_id                           text,
    amount                            bigint      NOT NULL CHECK (amount >= 0),
    currency                          char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id text,
    resolved_reference_id             uuid        REFERENCES wager_transactions (id),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code                      text,
    result_balance                    bigint      CHECK (result_balance >= 0),
    attempts                          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    expires_at                        timestamptz,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    completed_at                      timestamptz,

    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND amount > 0)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)
    ),
    CONSTRAINT wager_transactions_reference_policy CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
        OR (kind = 'BET' AND reference_external_transaction_id IS NULL)
        OR kind IN ('WIN', 'LOSS', 'OPENING')
    ),
    CONSTRAINT wager_transactions_failure_code CHECK (
        (status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_processed_balance CHECK (
        status <> 'PROCESSED' OR result_balance IS NOT NULL
    ),
    CONSTRAINT wager_transactions_completion CHECK (
        (status IN ('PROCESSED', 'REJECTED', 'FAILED')) = (completed_at IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_provider_external_key UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT wager_transactions_provider_idempotency_key UNIQUE (provider_id, idempotency_key)
);

-- Uma carteira só pode ter uma abertura (crédito inicial duplicado é impossível).
CREATE UNIQUE INDEX wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- Uma referência não recebe duas reversões bem-sucedidas, de nenhum tipo:
-- REFUND e ROLLBACK sobre a mesma aposta são mutuamente exclusivos.
CREATE UNIQUE INDEX wager_transactions_one_reversal_per_reference
    ON wager_transactions (resolved_reference_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

CREATE INDEX wager_transactions_due
    ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');

CREATE INDEX wager_transactions_provider_reference
    ON wager_transactions (provider_id, external_transaction_id);

-- Estados terminais são imutáveis e nenhuma transação é apagada.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager transactions cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is in a terminal state and cannot change', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.kind <> OLD.kind OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.player_id <> OLD.player_id OR NEW.amount <> OLD.amount OR NEW.currency <> OLD.currency
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id THEN
        RAISE EXCEPTION 'immutable transaction columns cannot change' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wager_transactions_guard BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();
