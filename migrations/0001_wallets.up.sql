CREATE TABLE wallets (
    id          uuid        PRIMARY KEY,
    player_id   uuid        NOT NULL,
    currency    char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance     bigint      NOT NULL CHECK (balance >= 0),
    version     bigint      NOT NULL CHECK (version >= 1),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- A versão só avança quando o saldo muda; moeda, jogador e criação são imutáveis.
CREATE FUNCTION wallets_guard_update() RETURNS trigger AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id
       OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity columns are immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet version must advance by one when the balance changes' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance = OLD.balance AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet version cannot change without a balance change' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallets_guard_update BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();

CREATE FUNCTION wallets_forbid_delete() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallets_forbid_delete BEFORE DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_forbid_delete();
