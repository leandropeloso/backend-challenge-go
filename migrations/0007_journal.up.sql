-- Livro-diário de partidas dobradas. Cada lançamento do ledger da carteira tem um
-- lançamento correspondente aqui, com débitos = créditos, imposto pelo banco.

CREATE TABLE journal_entries (
    id             uuid        PRIMARY KEY,
    transaction_id uuid        NOT NULL UNIQUE REFERENCES wager_transactions (id),
    created_at     timestamptz NOT NULL
);

CREATE TABLE journal_postings (
    id        bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id  uuid    NOT NULL REFERENCES journal_entries (id),
    account   text    NOT NULL CHECK (account <> ''),
    wallet_id uuid    REFERENCES wallets (id),
    direction text    NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount    bigint  NOT NULL CHECK (amount > 0),
    currency  char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- contas de carteira apontam para a carteira; as demais, não
    CONSTRAINT journal_postings_wallet_account CHECK ((account LIKE 'wallet:%') = (wallet_id IS NOT NULL))
);

CREATE INDEX journal_postings_entry ON journal_postings (entry_id);
CREATE INDEX journal_postings_wallet ON journal_postings (wallet_id) WHERE wallet_id IS NOT NULL;

-- Append-only.
CREATE FUNCTION journal_forbid_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'the journal is append-only (% rejected)', TG_OP USING ERRCODE = 'integrity_constraint_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_entries_append_only BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION journal_forbid_change();
CREATE TRIGGER journal_postings_append_only BEFORE UPDATE OR DELETE ON journal_postings
    FOR EACH ROW EXECUTE FUNCTION journal_forbid_change();
CREATE TRIGGER journal_entries_no_truncate BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION journal_forbid_change();
CREATE TRIGGER journal_postings_no_truncate BEFORE TRUNCATE ON journal_postings
    FOR EACH STATEMENT EXECUTE FUNCTION journal_forbid_change();

-- No commit: cada lançamento tem >= 2 postings, uma moeda e débitos = créditos.
CREATE FUNCTION journal_check_entry_balanced() RETURNS trigger AS $$
DECLARE
    eid uuid;
    n integer;
    currencies integer;
    debits bigint;
    credits bigint;
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN eid := NEW.id; ELSE eid := NEW.entry_id; END IF;
    SELECT count(*), count(DISTINCT currency),
           COALESCE(sum(amount) FILTER (WHERE direction = 'DEBIT'), 0),
           COALESCE(sum(amount) FILTER (WHERE direction = 'CREDIT'), 0)
      INTO n, currencies, debits, credits
      FROM journal_postings WHERE entry_id = eid;
    IF n < 2 OR currencies <> 1 OR debits <> credits THEN
        RAISE EXCEPTION 'journal entry % is not balanced (postings=%, currencies=%, debits=%, credits=%)',
            eid, n, currencies, debits, credits USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER journal_entries_balanced AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION journal_check_entry_balanced();
CREATE CONSTRAINT TRIGGER journal_postings_balanced AFTER INSERT ON journal_postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION journal_check_entry_balanced();

-- Histórico: um lançamento para cada movimento já existente no ledger da carteira.
INSERT INTO journal_entries (id, transaction_id, created_at)
SELECT gen_random_uuid(), l.transaction_id, l.created_at FROM wallet_ledger_entries l;

INSERT INTO journal_postings (entry_id, account, wallet_id, direction, amount, currency)
SELECT j.id, 'wallet:' || l.wallet_id::text, l.wallet_id, l.direction, l.amount, l.currency
  FROM wallet_ledger_entries l JOIN journal_entries j ON j.transaction_id = l.transaction_id;

INSERT INTO journal_postings (entry_id, account, wallet_id, direction, amount, currency)
SELECT j.id,
       CASE WHEN t.kind = 'OPENING' THEN 'funding:opening' ELSE 'house:' || t.provider_id END,
       NULL,
       CASE WHEN l.direction = 'CREDIT' THEN 'DEBIT' ELSE 'CREDIT' END,
       l.amount, l.currency
  FROM wallet_ledger_entries l
  JOIN journal_entries j ON j.transaction_id = l.transaction_id
  JOIN wager_transactions t ON t.id = l.transaction_id;

-- Cada lançamento do ledger da carteira exige, no commit, o posting espelho da
-- carteira no diário (mesma transação, direção e valor).
CREATE FUNCTION wallet_ledger_requires_journal() RETURNS trigger AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM journal_entries e JOIN journal_postings p ON p.entry_id = e.id
         WHERE e.transaction_id = NEW.transaction_id AND p.wallet_id = NEW.wallet_id
           AND p.direction = NEW.direction AND p.amount = NEW.amount AND p.currency = NEW.currency) THEN
        RAISE EXCEPTION 'ledger entry % has no matching journal posting', NEW.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER wallet_ledger_requires_journal AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wallet_ledger_requires_journal();
