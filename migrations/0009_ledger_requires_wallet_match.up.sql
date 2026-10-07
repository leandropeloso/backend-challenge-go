-- O saldo da carteira deve coincidir com o ledger quando o commit chega, qualquer que
-- seja a tabela escrita. Antes, a conferência só disparava em escritas na tabela
-- wallets; um lançamento de ledger sem atualizar a carteira passava despercebido.
CREATE FUNCTION wallet_ledger_matches_wallet() RETURNS trigger AS $$
DECLARE
    last_after   bigint;
    last_version bigint;
    w_balance    bigint;
    w_version    bigint;
BEGIN
    SELECT balance_after, wallet_version INTO last_after, last_version
      FROM wallet_ledger_entries WHERE wallet_id = NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
    SELECT balance, version INTO w_balance, w_version FROM wallets WHERE id = NEW.wallet_id;
    IF w_balance IS DISTINCT FROM last_after OR w_version IS DISTINCT FROM last_version THEN
        RAISE EXCEPTION 'wallet % (balance %, version %) does not match its latest ledger entry (balance %, version %)',
            NEW.wallet_id, w_balance, w_version, last_after, last_version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER wallet_ledger_matches_wallet AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_matches_wallet();
