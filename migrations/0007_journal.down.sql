DROP TRIGGER IF EXISTS wallet_ledger_requires_journal ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_requires_journal();
DROP TRIGGER IF EXISTS journal_postings_balanced ON journal_postings;
DROP TRIGGER IF EXISTS journal_entries_balanced ON journal_entries;
DROP FUNCTION IF EXISTS journal_check_entry_balanced();
DROP TABLE IF EXISTS journal_postings;
DROP TABLE IF EXISTS journal_entries;
DROP FUNCTION IF EXISTS journal_forbid_change();
