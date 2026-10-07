DROP TRIGGER IF EXISTS wallets_forbid_delete ON wallets;
DROP TRIGGER IF EXISTS wallets_guard_update ON wallets;
DROP FUNCTION IF EXISTS wallets_forbid_delete();
DROP FUNCTION IF EXISTS wallets_guard_update();
DROP TABLE IF EXISTS wallets;
