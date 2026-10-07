CREATE TABLE wallet_ledger_entries (
    id             uuid        PRIMARY KEY,
    wallet_id      uuid        NOT NULL REFERENCES wallets (id),
    transaction_id uuid        NOT NULL REFERENCES wager_transactions (id),
    direction      text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount         bigint      NOT NULL CHECK (amount > 0),
    balance_before bigint      NOT NULL CHECK (balance_before >= 0),
    balance_after  bigint      NOT NULL CHECK (balance_after >= 0),
    currency       char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    wallet_version bigint      NOT NULL CHECK (wallet_version >= 1),
    created_at     timestamptz NOT NULL,
    CONSTRAINT wallet_ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_balance_invariant CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount)
        OR (direction = 'DEBIT' AND balance_after = balance_before - amount)
    )
);

-- Append-only: nenhum UPDATE, DELETE ou TRUNCATE passa, nem para o dono da tabela.
CREATE FUNCTION wallet_ledger_forbid_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% rejected)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_forbid_change();

CREATE TRIGGER wallet_ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION wallet_ledger_forbid_change();

-- Cada lançamento continua exatamente o anterior da mesma carteira. Os
-- escritores de uma carteira são serializados pelo lock da linha em wallets,
-- então ler o último lançamento aqui é seguro.
CREATE FUNCTION wallet_ledger_check_chain() RETURNS trigger AS $$
DECLARE
    last_after bigint;
    wallet_currency char(3);
BEGIN
    SELECT currency INTO wallet_currency FROM wallets WHERE id = NEW.wallet_id;
    IF wallet_currency IS DISTINCT FROM NEW.currency THEN
        RAISE EXCEPTION 'ledger currency differs from wallet currency' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    SELECT balance_after INTO last_after FROM wallet_ledger_entries
        WHERE wallet_id = NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
    IF NOT FOUND THEN
        last_after := 0;
    END IF;
    IF NEW.balance_before <> last_after THEN
        RAISE EXCEPTION 'ledger entry does not continue the previous balance (expected %, got %)', last_after, NEW.balance_before
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_check_chain BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_check_chain();

-- No commit, o saldo da carteira tem de ser igual ao último lançamento
-- (ou zero, enquanto não houver lançamentos).
CREATE FUNCTION wallets_check_against_ledger() RETURNS trigger AS $$
DECLARE
    last_after bigint;
BEGIN
    SELECT balance_after INTO last_after FROM wallet_ledger_entries
        WHERE wallet_id = NEW.id ORDER BY wallet_version DESC LIMIT 1;
    IF NOT FOUND THEN
        last_after := 0;
    END IF;
    IF NEW.balance <> last_after THEN
        RAISE EXCEPTION 'wallet % balance % does not match its ledger (%)', NEW.id, NEW.balance, last_after
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER wallets_check_against_ledger AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_check_against_ledger();
