-- Consultas de leitura para inspecionar o que foi gravado no banco.
-- Somente SELECT: nada aqui altera dados. Valores monetários ficam em centavos
-- no banco; as colunas "valor" abaixo já mostram o número dividido por 100.
--
-- No DBeaver: abra este arquivo (Arquivo > Abrir), escolha a conexão "wager"
-- e rode um bloco por vez com Ctrl+Enter (ou tudo com Alt+X).
-- No terminal:  .\scripts\db.ps1 -Consulta resumo

-- name: resumo
-- Visão geral: quantas linhas existem em cada tabela.
SELECT 'wallets' AS tabela, count(*) AS linhas FROM wallets
UNION ALL SELECT 'wager_transactions', count(*) FROM wager_transactions
UNION ALL SELECT 'wallet_ledger_entries', count(*) FROM wallet_ledger_entries
UNION ALL SELECT 'journal_entries', count(*) FROM journal_entries
UNION ALL SELECT 'journal_postings', count(*) FROM journal_postings
UNION ALL SELECT 'inbox_messages', count(*) FROM inbox_messages
UNION ALL SELECT 'outbox_events', count(*) FROM outbox_events;

-- name: carteiras
-- Carteiras com saldo atual e quantas movimentações já tiveram.
SELECT w.id, w.player_id, w.currency,
       round(w.balance / 100.0, 2) AS saldo,
       w.version, w.created_at, w.updated_at
FROM wallets w
ORDER BY w.updated_at DESC
LIMIT 50;

-- name: transacoes
-- Últimas transações (apostas, ganhos, perdas, estornos...) e o resultado de cada uma.
SELECT t.created_at, t.kind, t.status, t.provider_id,
       t.external_transaction_id, t.idempotency_key,
       round(t.amount / 100.0, 2) AS valor,
       round(t.result_balance / 100.0, 2) AS saldo_apos,
       t.failure_code, t.attempts, t.wallet_id
FROM wager_transactions t
ORDER BY t.created_at DESC
LIMIT 50;

-- name: pendentes
-- Transações que ainda não terminaram (aguardando referência, em reprocesso, falhas).
SELECT t.created_at, t.kind, t.status, t.provider_id, t.external_transaction_id,
       t.reference_external_transaction_id, t.attempts, t.next_attempt_at,
       t.expires_at, t.failure_code
FROM wager_transactions t
WHERE t.status IN ('PENDING', 'PENDING_REFERENCE', 'FAILED')
ORDER BY t.created_at;

-- name: extrato
-- Extrato (ledger) por carteira: cada linha é um movimento com saldo antes/depois.
-- Troque o UUID abaixo pela carteira que quer ver (ou apague o WHERE).
SELECT l.wallet_version AS versao, l.created_at, l.direction,
       round(l.amount / 100.0, 2) AS valor,
       round(l.balance_before / 100.0, 2) AS saldo_antes,
       round(l.balance_after / 100.0, 2) AS saldo_depois,
       t.kind, t.status, t.external_transaction_id
FROM wallet_ledger_entries l
JOIN wager_transactions t ON t.id = l.transaction_id
-- WHERE l.wallet_id = '00000000-0000-0000-0000-000000000000'
ORDER BY l.wallet_id, l.wallet_version DESC
LIMIT 100;

-- name: conferencia
-- Conferência: o saldo da carteira tem de bater com o último lançamento do ledger.
-- Qualquer linha retornada aqui indica divergência (o esperado é zero linhas).
-- Carteira sem nenhum lançamento é considerada saldo 0 na versão 1.
SELECT w.id, round(w.balance / 100.0, 2) AS saldo_carteira,
       round(l.balance_after / 100.0, 2) AS saldo_ledger, w.version, l.wallet_version
FROM wallets w
LEFT JOIN LATERAL (
    SELECT balance_after, wallet_version FROM wallet_ledger_entries
    WHERE wallet_id = w.id ORDER BY wallet_version DESC LIMIT 1
) l ON true
WHERE coalesce(l.balance_after, 0) <> w.balance OR coalesce(l.wallet_version, 1) <> w.version;

-- name: partidas
-- Partidas dobradas: cada lançamento tem débitos e créditos que se equilibram.
SELECT e.created_at, t.kind, t.external_transaction_id,
       p.account, p.direction, round(p.amount / 100.0, 2) AS valor, p.currency
FROM journal_entries e
JOIN wager_transactions t ON t.id = e.transaction_id
JOIN journal_postings p ON p.entry_id = e.id
ORDER BY e.created_at DESC, e.id, p.direction DESC
LIMIT 100;

-- name: balancete
-- Balancete por moeda: total de débitos e créditos (devem ser iguais).
SELECT currency,
       round(sum(amount) FILTER (WHERE direction = 'DEBIT') / 100.0, 2) AS debitos,
       round(sum(amount) FILTER (WHERE direction = 'CREDIT') / 100.0, 2) AS creditos
FROM journal_postings
GROUP BY currency;

-- name: outbox
-- Eventos a publicar na fila: published_at vazio = ainda não enviado.
SELECT seq, event_type, aggregate_id AS carteira, partition_key,
       occurred_at, published_at, attempts, last_error, left(payload::text, 100) AS payload
FROM outbox_events
ORDER BY seq DESC
LIMIT 50;

-- name: outbox_atrasado
-- Eventos que ainda não foram publicados e há quanto tempo estão esperando.
SELECT seq, event_type, aggregate_id AS carteira, now() - created_at AS esperando,
       attempts, last_error
FROM outbox_events
WHERE published_at IS NULL
ORDER BY seq;

-- name: inbox
-- Mensagens já consumidas da fila (garante que cada uma é processada uma vez).
SELECT consumer_name, message_id, received_at, completed_at
FROM inbox_messages
ORDER BY received_at DESC
LIMIT 50;

-- name: por_provedor
-- Resumo por provedor e status.
SELECT provider_id, kind, status, count(*) AS qtd, round(sum(amount) / 100.0, 2) AS valor_total
FROM wager_transactions
WHERE origin = 'EXTERNAL'
GROUP BY provider_id, kind, status
ORDER BY provider_id, kind, status;
