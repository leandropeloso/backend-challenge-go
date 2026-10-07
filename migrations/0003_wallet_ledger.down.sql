DROP TRIGGER IF EXISTS wallets_check_against_ledger ON wallets;
DROP FUNCTION IF EXISTS wallets_check_against_ledger();
DROP TRIGGER IF EXISTS wallet_ledger_check_chain ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_check_chain();
DROP TRIGGER IF EXISTS wallet_ledger_no_truncate ON wallet_ledger_entries;
DROP TRIGGER IF EXISTS wallet_ledger_no_update_delete ON wallet_ledger_entries;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_forbid_change();
