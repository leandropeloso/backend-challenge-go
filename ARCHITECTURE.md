# Arquitetura e decisões

Aqui estão as decisões técnicas que tomei, como interpretei os pontos em aberto do enunciado e o que
ficou de fora. O enunciado está em [DESAFIO.md](DESAFIO.md); como executar, no [README.md](README.md).

## Visão geral

```
                 ┌────────────┐   JWT (JWKS)   ┌──────────┐
 provedores ───▶ │  HTTP API  │ ◀───────────── │ Keycloak │
                 └─────┬──────┘                └──────────┘
 SQS FIFO ──▶ consumer │          mesmo caso de uso: processwager.Service
                       ▼
                ┌───────────────┐   uma transação SQL por operação
                │  PostgreSQL   │   wallets · wager_transactions · ledger
                │               │   inbox_messages · outbox_events
                └──────┬────────┘
   outbox relay ◀──────┘  publica após o commit ──▶ wager-events.fifo
   pending resolver ◀──── retoma PENDING_REFERENCE
```

Camadas (dependências apontam para dentro):

- `internal/domain` — modelo puro (`money`, `wallet`, `wager`, `ledger`, `journal`, `event`). Só depende da
  biblioteca padrão e de `google/uuid`. Sem tags de JSON/banco, sem Fx, HTTP, SQS ou pgx.
  Entidades têm estado encapsulado, construtores validados (`Open`, `NewExternal`, `NewOpening`,
  `ledger.New`) e funções de reidratação separadas (`Rehydrate`) que só validam, sem reaplicar
  movimentações, transições nem eventos.
- `internal/app` — casos de uso e *ports* (`UnitOfWork`, repositórios, `Reader`, `OutboxStore`,
  `Metrics`). `processwager.Service` é o único caminho de processamento, usado por HTTP, SQS e pelo
  worker de pendências.
- `internal/infra` — adaptadores: `postgres` (pgx, SQL explícito), `sqsx`, `httpapi`, `auth`,
  `eventjson`, `observability`, `migrate`, `ids`.
- `internal/worker` — loops da outbox e das referências pendentes; `internal/telemetry` —
  OpenTelemetry; `internal/config` — variáveis de ambiente; `internal/fault` — pontos de falha dos
  testes (só com a tag `faultinject`).
- `internal/platform` — composição Fx.

Erros de domínio são sentinelas ou tipos (`errors.Is`/`errors.As`): `money.ErrOverflow`,
`wallet.ErrInsufficientFunds`, `wager.ErrTerminalState`, `processwager.ErrIdempotencyConflict` etc.
Nenhuma regra de negócio usa `panic`; o único `recover` é o do middleware HTTP, que devolve 500.
Toda operação de I/O recebe `context.Context` e o respeita (rollback com contexto desacoplado para
que a transação seja realmente desfeita quando o contexto cancela).

## Dinheiro

- `Money` é um value object imutável: `int64` em **centavos** (escala fixa de duas casas) mais
  moeda ISO 4217 (lista de códigos ativos embutida). O zero value (sem moeda) é inválido e
  rejeitado por toda operação.
- Faixa: ±92.233.720.368.547.758,07. Há checagem explícita de overflow no parsing (`ParseInt` e
  `units*100+cents`), na soma, na subtração e na negação (`-MinInt64`).
- Parsing estrito, sem arredondar nem normalizar: só `^\d+\.\d{2}$`, sem sinal, sem zeros à
  esquerda (exceto `0`), sem expoente, `NaN`, `Infinity`, vazio ou separadores. `"25"`, `"25.5"`,
  `"25.001"` e `"1e3"` são **rejeitados** em vez de convertidos; assim o hash de idempotência é
  determinístico e não existe forma "equivalente" para normalizar. Valores negativos só existem em
  cálculos internos.
- Nenhum `float32`/`float64` toca dinheiro: no JSON o valor é `string` (um número JSON no lugar é
  rejeitado pelo decoder), no Go é `int64`, no banco é `BIGINT` + `CHAR(3)`. Durações do lease e da
  idade da outbox também usam inteiros (milissegundos). O único `float64` do código de produção
  está em `observability` (buckets e gauges do Prometheus), fora de qualquer caminho monetário.
- Operações entre moedas diferentes retornam `ErrCurrencyMismatch`. Os cenários principais usam
  BRL, mas o tipo, a carteira `(playerId, currency)` e o schema carregam a moeda.

## Carteira e concorrência

A carteira é a raiz do agregado e guarda `id`, `playerId`, `balance`, `version` (começa em 1 e só avança
quando o saldo muda), timestamps. `Debit`/`Credit` exigem valor positivo e da mesma moeda; o débito
recusa saldo negativo. A carteira devolve um `Movement` que a aplicação transforma em lançamento de
ledger e evento.

**Estratégia: pessimista por carteira, com guarda otimista e restrições no banco.**

1. Cada operação roda numa transação `READ COMMITTED` e trava a linha da carteira com
   `SELECT ... FOR NO KEY UPDATE`. Escritores da mesma carteira se serializam; carteiras
   diferentes seguem em paralelo. `NO KEY UPDATE` evita bloquear inserts que só referenciam a linha
   por chave estrangeira. Não há locks em memória nem globais.
2. O `UPDATE` do saldo ainda exige `WHERE version = $esperada` (`ErrConcurrentUpdate`), de modo que
   mesmo um caminho que esquecesse o lock não produziria *lost update*.
3. O banco impõe por conta própria: `CHECK (balance >= 0)`; trigger que só deixa a versão avançar
   `+1` junto com uma mudança de saldo; e *constraint triggers* diferidos que, no commit, exigem
   que `wallets.balance` seja igual ao `balance_after` do último lançamento (ou zero sem
   lançamentos) e que a versão da carteira seja a do último lançamento. A conferência é feita por
   um trigger em `wallets` **e** um em `wallet_ledger_entries` (migration 0009): uma carteira
   alterada sem lançamento, ou um lançamento sem a atualização da carteira, não commita.
4. Deadlock/serialization failure (`40P01`/`40001`) vira `ErrRetryable` e a operação inteira é
   repetida até 4 vezes (métrica `wager_concurrency_conflicts_total`). A ordem de locks é sempre
   linha da transação → linha da carteira, sem ciclos possíveis.

## Ledger

`wallet_ledger_entries` é append-only e auditável:

- `CHECK` de que `balance_after = balance_before ± amount` conforme a direção, `amount > 0`,
  saldos não negativos;
- `UNIQUE (wallet_id, transaction_id)` e `UNIQUE (wallet_id, wallet_version)`; FKs para `wallets`
  e `wager_transactions`;
- triggers que rejeitam `UPDATE`, `DELETE` e `TRUNCATE` (inclusive do dono da tabela) e uma
  trigger que exige que cada lançamento continue exatamente o anterior da mesma carteira.

`LOSS` e operações rejeitadas não geram lançamento nem mudam a versão. Correções financeiras são
novos lançamentos (`REFUND`/`ROLLBACK`).

### Partidas dobradas (livro-diário)

Além do ledger da carteira, cada movimento gera, **na mesma transação**, um lançamento no
livro-diário (`journal_entries` + `journal_postings`), modelado no domínio (`internal/domain/journal`):

- a carteira do jogador é uma conta de **passivo** (credit-normal): `wallet:<id>`; um crédito na
  carteira aumenta o saldo. A contrapartida é `house:<providerId>` nas operações externas e
  `funding:opening` na abertura. `BET` debita a carteira e credita a casa; `WIN`/`REFUND`
  creditam a carteira; `ROLLBACK` faz o oposto do original. Logo, o saldo da conta da carteira
  (créditos − débitos) é sempre o saldo da carteira.
- o **banco** impõe: *constraint trigger* diferido que, no commit, exige ≥ 2 postings, uma só
  moeda e débitos = créditos por lançamento; `CHECK` de que contas de carteira trazem `wallet_id`
  (e as demais, não); imutabilidade (`UPDATE`/`DELETE`/`TRUNCATE` recusados); e um trigger no
  ledger da carteira que exige o posting espelho (mesma transação, direção e valor) — um movimento
  sem lançamento no diário não commita. A migration 0007 faz o *backfill* do histórico já existente
  no ledger.
- `GET /accounting/trial-balance` totaliza por moeda em um snapshot `REPEATABLE READ`. Os totais
  agregados podem exceder `int64` mesmo com cada valor válido, por isso são decimais exatos
  calculados em `NUMERIC` (nunca `Money`, nunca ponto flutuante).

## Transações (WagerTransaction) e máquina de estados

Cada operação externa vira uma linha em `wager_transactions` com identificadores interno e externo,
provedor, chave e hash de idempotência, carteira, jogador, rodada, jogo, tipo, valor, referência
externa e resolvida, estado, `failure_code`, saldo resultante e timestamps. `OPENING` é a origem
interna: o schema (`CHECK` de forma por origem) exige que ela **não** carregue provedor, ID externo,
chave, hash, rodada, jogo ou referência, e um índice único parcial impede um segundo `OPENING` para
a mesma carteira.

```
            ┌─────────────── Process ───────────────▶ PROCESSED
 PENDING ───┼─────────────── Reject  ───────────────▶ REJECTED
            ├─────────────── Fail    ───────────────▶ FAILED
            └─ AwaitReference ─▶ PENDING_REFERENCE ──(Process / Reject / Fail)──▶ terminais
                                   └─ ScheduleRetry (permanece em PENDING_REFERENCE, attempts+1)
```

Estados terminais não aceitam novas transições (`ErrTerminalState` no domínio e trigger no banco).

- **Falha transitória** (banco/SQS indisponível, deadlock, timeout): nenhuma linha é gravada — a
  transação SQL desfaz tudo; HTTP responde `503` com `Retry-After`, SQS aplica backoff.
- **Rejeição de negócio** (`REJECTED`): é *commitada* e auditável, com `failureCode` estável. O
  replay devolve a mesma rejeição.
- **Falha permanente** (`FAILED`, `INTERNAL_ERROR`): reservada a erros que nenhuma repetição
  resolve (hoje, overflow aritmético); registrada para auditoria.
- **Processamento síncrono**: as operações são concluídas na mesma transação do registro — não
  existe commit intermediário de aceite. `PENDING` nunca é commitado hoje; mesmo assim,
  `ClaimDue` (`FOR UPDATE SKIP LOCKED`) retoma qualquer `PENDING`/`PENDING_REFERENCE` vencido,
  de qualquer instância, para o caso de uma evolução para aceite assíncrono.

### Códigos de falha

Resultados definitivos (reenviar não muda o desfecho): `INSUFFICIENT_FUNDS`,
`INSUFFICIENT_FUNDS_FOR_REVERSAL` (reversão que deixaria saldo negativo; distinto do anterior),
`REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `ALREADY_REVERSED`.

Entradas corrigíveis (`correctable: true`; o provedor deve enviar uma operação nova e corrigida):
`WALLET_NOT_FOUND`, `PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `REFERENCE_MISMATCH`,
`REFERENCE_KIND_NOT_ALLOWED`, `REFERENCE_AMOUNT_MISMATCH`.

Operações para carteira inexistente ficam registradas como `REJECTED/WALLET_NOT_FOUND`; por isso
`wager_transactions.wallet_id` não tem FK (o ledger, sim, tem todas as FKs).

## Regras das operações

| Tipo | Efeito | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0, saldo suficiente; não aceita referência |
| `WIN` | crédito | valor > 0; referência opcional (deve ser uma `BET` processada) |
| `LOSS` | nenhum | valor exatamente `0.00`; sem ledger, sem mudar a versão; evento `WagerTransactionProcessed` sem `WalletBalanceChanged`; referência opcional como na `WIN` |
| `REFUND` | crédito | referência obrigatória, deve ser `BET` processada, valor igual |
| `ROLLBACK` | oposto do original | referência obrigatória: `BET`→crédito, `WIN`→débito, `REFUND`→débito; valor igual |
| `OPENING` | crédito inicial | só interno; rejeitado por HTTP e SQS |

A referência é resolvida por `(providerId, referenceExternalTransactionId)` — nunca cruza
provedores — e precisa concordar em jogador, carteira, moeda e rodada (`REFERENCE_MISMATCH`).
Reversões parciais não existem.

**REFUND × ROLLBACK.** Uma transação só pode ter **uma** reversão bem-sucedida, de qualquer tipo,
imposto por checagem na aplicação (`HasProcessedReversal` → `ALREADY_REVERSED`) e por índice único
parcial em `resolved_reference_id` para `REFUND`/`ROLLBACK` processados. Logo, `REFUND` e `ROLLBACK`
sobre a mesma aposta são mutuamente exclusivos (não há devolução em dobro). Um `ROLLBACK` de um
`REFUND` é uma reversão do próprio `REFUND` (debita de volta) e, depois dele, a aposta original
continua marcada como já revertida: não pode ser reembolsada de novo.

### Referências ainda indisponíveis

Se a referência não existe, ou existe mas ainda está `PENDING`/`PENDING_REFERENCE`, a operação é
gravada como `PENDING_REFERENCE` (HTTP `202`; evento `WagerTransactionPendingReference`) com
`next_attempt_at` e `expires_at`. O worker (`pending-resolver`) reprocessa com **backoff
exponencial** (`PENDING_BASE_BACKOFF` × 2ⁿ até `PENDING_MAX_BACKOFF`), em qualquer instância e após
reinícios, pois o agendamento é durável no banco. A pendência termina quando:

- a referência é processada → a operação é aplicada normalmente;
- a referência terminou em `REJECTED`/`FAILED` → rejeição imediata `REFERENCE_NOT_PROCESSED`;
- o prazo (`PENDING_TTL`) ou o número máximo de tentativas (`PENDING_MAX_ATTEMPTS`) se esgota →
  `REJECTED`, com `REFERENCE_NOT_FOUND` (a referência nunca apareceu) ou `REFERENCE_NOT_PROCESSED`
  (existia, mas continuou pendente), e evento `WagerTransactionRejected`.

## Idempotência

Fica toda no PostgreSQL; não depende de memória, de locks locais nem da deduplicação do SQS FIFO.

- `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)`. O
  escopo é por provedor: a chave de um provedor nunca produz replay para outro.
- A primeira escrita da operação é `INSERT ... ON CONFLICT DO NOTHING`. Se não inseriu, a
  transação concorrente (ou anterior) venceu; a aplicação compara os hashes:
  - mesma chave e mesmo hash → **replay**: devolve o resultado persistido
    (`idempotentReplay: true`) com o saldo observado **no processamento original**
    (`result_balance`), nunca o saldo atual;
  - mesma chave, hash diferente → `409 IDEMPOTENCY_KEY_CONFLICT`;
  - mesma operação externa com outra chave → `409 EXTERNAL_TRANSACTION_CONFLICT`.
- A chave informada pelo cliente nunca é substituída por outra calculada pelo servidor.
- **Hash**: SHA-256 (hex) do JSON canônico (chaves ordenadas por `encoding/json` sobre um mapa)
  dos campos de negócio: `providerId`, `externalTransactionId`, `playerId`, `walletId` (UUIDs na
  forma canônica minúscula), `roundId`, `gameId`, `kind`, `money.amount`, `money.currency` e, só
  quando informado, `referenceExternalTransactionId`. Ficam de fora a chave de idempotência e
  qualquer metadado de transporte (headers, `messageId`, `occurredAt`). HTTP e SQS constroem a
  transação pelo mesmo `wager.NewExternal`, então produzem o mesmo hash para o mesmo conteúdo.
- Espaços nas pontas e caracteres de controle são rejeitados em vez de normalizados.

## Inbox e outbox

**Inbox** (`inbox_messages`, `PRIMARY KEY (consumer_name, message_id)`): o registro da mensagem, a
transação de negócio, o ledger, a outbox e a marca de conclusão (`completed_at`) compartilham a
**mesma transação SQL**. O hash da inbox é `sha256(payloadHash | idempotencyKey)`. Uma reentrega
com o mesmo hash é tratada como duplicata e devolve o resultado original; com hash diferente é
conflito (vai para a DLQ). Se o tratamento falha, a transação desfaz também a linha da inbox.

**Outbox** (`outbox_events`): eventos nascem na mesma transação que o saldo, o ledger e o estado
da operação, e só são enviados ao SQS depois do commit, por um relay separado.

- Reserva por **lease**: um `UPDATE ... RETURNING` condicionado a `published_at IS NULL` e
  `locked_until` vencido (reavaliado após qualquer espera por lock) garante que dois relays não
  recebam o mesmo evento. Se o relay morre, o lease expira e outro assume.
- Ordem por carteira: o relay só reserva o *prefixo pronto* de cada partição (carteira), então um
  evento com backoff ou em lease de outro relay segura os posteriores da mesma carteira.
- Escalabilidade: a reserva examina apenas a **cabeça** da fila (os 3.000 eventos pendentes mais
  antigos, por índice parcial em `seq`); os predecessores de qualquer evento têm `seq` menor e estão
  sempre dentro da janela, então a ordem é preservada e o custo não cresce com o backlog (medido:
  432 ms → 9,7 ms com 83 mil pendentes). Limitação: se mais de 3.000 eventos *mais antigos* estiverem
  todos bloqueados (backoff/lease), eventos posteriores só andam quando eles liberarem.
- Publicação em lote: cada chamada de `SendMessageBatch` leva até 10 eventos e **nunca dois da
  mesma carteira**; os eventos de uma carteira saem em rodadas sequenciais, e uma falha parcial
  libera o restante da carteira em vez de furar a ordem. Os lotes de uma rodada correm em paralelo
  (`OUTBOX_CONCURRENCY`) e a confirmação na outbox é um único `UPDATE ... WHERE id = ANY(...)`.
- Falha de publicação: `attempts` cresce, `last_error` é gravado e `next_attempt_at` recebe
  backoff exponencial (`OUTBOX_BACKOFF_BASE`…`MAX`). Eventos **nunca** são descartados.
- Publicação repetida: o corpo é o envelope gravado, com o mesmo `eventId`. No SQS FIFO,
  `MessageDeduplicationId = eventId` e `MessageGroupId = walletId`; consumidores devem ser
  idempotentes por `eventId` e podem ordenar por `walletVersion` (fora da janela de 5 min de
  deduplicação do broker, uma republicação é entregue de novo).
- O payload é um snapshot imutável (trigger bloqueia alterar conteúdo e apagar).

### Contrato dos eventos

Envelope: `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId` (opcional),
`occurredAt` (RFC 3339 UTC), `version` (inteiro, hoje 1) e `data`. Tipo e versão vêm do construtor
do evento, não do chamador. `aggregateId` é o ID da transação (eventos de transação) ou da
carteira (`WalletBalanceChanged`).

| Evento | Quando | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | operação concluída (inclui `LOSS` e `OPENING`) | dados da transação + `balanceAfter` |
| `WagerTransactionRejected` | rejeição definitiva | dados da transação + `failureCode`, `correctable` |
| `WalletBalanceChanged` | saldo alterado | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |
| `WagerTransactionPendingReference` | registro da espera | dados da transação + `referenceExternalTransactionId`, `nextAttemptAt`, `expiresAt` |

Eventos da abertura de carteira omitem os campos externos (`providerId`, `externalTransactionId`,
`roundId`, `gameId`). Valores monetários são strings decimais.

## Consumidor SQS

- Mensagem: `WagerTransactionRequested`, com `messageId` como identidade durável e
  `data.idempotencyKey` como chave. Decodificação estrita (campo desconhecido, número no lugar de
  string, tipo diferente ou `messageId` ausente → mensagem inválida).
- `MessageGroupId = walletId` (ordem por carteira; carteiras diferentes em paralelo);
  `MessageDeduplicationId = messageId` — a deduplicação do broker é só uma otimização, a garantia
  é a inbox. A mensagem só é removida **depois do commit**.
- Resultado → ação: processada, rejeitada (terminal) ou pendente por referência → **remove**.
  Entrada inválida, conflito de idempotência/`messageId` → **DLQ explícita** (com `failureReason`)
  e remove da origem. Falha transitória → backoff via `ChangeMessageVisibility`
  (`CONSUMER_RETRY_BASE_DELAY` × 2ⁿ⁻¹, máx. `CONSUMER_RETRY_MAX_DELAY`); depois de
  `CONSUMER_MAX_ATTEMPTS` (5) recebimentos → DLQ. O *redrive* da fila (`maxReceiveCount=5`) é a
  rede de segurança caso a própria DLQ falhe.
- Visibility timeout: 30 s na fila e por recebimento (`CONSUMER_VISIBILITY_TIMEOUT`); o
  processamento tem timeout menor (`CONSUMER_PROCESS_TIMEOUT=20s`) para não estourar a visibilidade.
- Ordem: o lote é processado em sequência; se uma mensagem de um grupo não conclui, as demais do
  mesmo grupo no lote são devolvidas (`visibility=0`) para não furar a ordem.
- Se a remoção falha (ou o processo morre) depois do commit, a reentrega cai na inbox e vira
  duplicata inofensiva.
- Concorrência HTTP × SQS: ambos usam a mesma `processwager.Service`, os mesmos índices únicos e o
  mesmo lock por carteira; o teste cruza os dois caminhos nos dois sentidos.
- **Controle de acesso do broker** (credenciais e políticas, conforme o enunciado): a identidade
  do provedor nas mensagens é confiada ao broker, que só aceita envios de quem tem
  `sqs:SendMessage` na fila de entrada; as validações de domínio continuam no consumidor. Três
  identidades de menor privilégio estão em `deploy/iam/` (`gateway-sender`, `wager-app`,
  `events-consumer`). A aplicação aceita as URLs das filas por configuração, dispensando
  `sqs:GetQueueUrl`. Como o LocalStack comunitário não impõe IAM, o teste
  `TestBrokerAccessIsLeastPrivilege` usa o **Moto**, que impõe as políticas: confere que cada
  identidade só faz o seu papel e que a aplicação funciona (consome, envia à DLQ, publica
  eventos) apenas com as permissões de `wager-app`. Diferença do emulador: o Moto trata
  `SendMessageBatch` como ação própria, e o teste a acrescenta à política; na AWS real o
  `sqs:SendMessage` já a cobre. Política de recurso nas filas, SSE e TLS ficam para o ambiente
  de produção (ver `deploy/iam/README.md`).

## Autenticação e autorização

- **IdP**: Keycloak (recomendado no enunciado), fluxo `client_credentials` entre serviços. A
  aplicação não cadastra senhas nem emite tokens.
- **Validação**: JWT `RS256`/`ES256` (nunca `none`/HMAC) com chaves do **JWKS** do IdP (cache e
  atualização em segundo plano); confere `iss`, `aud` (`wager-api`), `exp` obrigatório (tolerância
  de 2 s). No início a aplicação sonda o JWKS até responder; `/health/ready` reflete se as chaves
  estão carregadas.
- **Modelo de permissões**: papéis de realm em `realm_access.roles`.
  - `internal`: operações de carteira (`POST /wallets`, leitura, ledger, reconciliação) e leitura
    de qualquer transação.
  - `provider`: `POST /wagering/transactions` e leitura das **próprias** transações. O `providerId`
    vem de um claim fixado por client no Keycloak (mapper `oidc-hardcoded-claim-mapper`), nunca do
    corpo da requisição: o corpo precisa coincidir (`403 FORBIDDEN_PROVIDER` se diferir).
  - Token sem papel útil → `401`; papel errado → `403`. Provedores não acessam carteiras.
- **Isolamento**: consultas e replays são escopados pelo provedor autenticado. Ler a transação de
  outro provedor por ID responde `404` (não revela existência); pelo caminho
  `/providers/{outro}/…` responde `403`; `OPENING` é invisível para provedores. Chaves de
  idempotência são por provedor.
- Acessos negados não produzem efeito financeiro nem expõem dados (teste de integração conta
  linhas antes e depois).

## Composição Fx e ciclo de vida

`internal/platform` organiza `fx.Module`s (`config`, `observability` — logs, métricas e tracing —,
`postgres`, `auth`, `sqs`, `app`, `workers`, `http`) com `fx.Provide` por construtores e `fx.Invoke` para registrar os
componentes. O grafo é validado por `fx.ValidateApp` (teste unitário) e exercitado de ponta a ponta
no teste de integração do ciclo de vida.

- **Início** (hooks `OnStart` na ordem de construção das dependências): a configuração é validada
  em `config.Load` (falha rápida); o pool valida a conexão (`Ping` com retentativas); as filas são
  resolvidas; sobem o relay da outbox, o worker de pendências e o consumidor; o JWKS é carregado
  e, só então, o servidor HTTP começa a aceitar requisições.
- **Parada** (hooks `OnStop` em ordem inversa): servidor HTTP (`Shutdown`, para de aceitar e
  conclui as requisições em andamento) → validador JWT → consumidor SQS → worker de pendências →
  relay da outbox → **pool do PostgreSQL por último**, de modo que nada use uma conexão já fechada.
- **Workers em duas fases** (`worker.Loop`, `sqsx.Consumer`): ao parar, impedem novas iterações /
  `ReceiveMessage` e esperam o trabalho em andamento até o prazo de `SHUTDOWN_TIMEOUT`; só se o
  prazo estoura o trabalho é cancelado — a transação sofre rollback e a mensagem é liberada
  (`visibility=0`) para reentrega segura.
- `main` trata `SIGTERM`/`SIGINT` desde o início: um sinal **durante a inicialização** cancela o
  `Start` e o Fx desfaz em ordem o que já subiu; depois da inicialização, a parada tem prazo
  `SHUTDOWN_TIMEOUT`. O processo sai com código 0 quando tudo termina a tempo (todo teste de
  integração verifica isso no encerramento de cada instância).

## Acesso ao banco

`pgx/v5` com SQL explícito (sem ORM). Biblioteca e mapeamento: `Money` ⇄ `BIGINT` (centavos) +
`CHAR(3)`. **Delimitação da transação**: `postgres.Store.Do(ctx, fn)` abre uma `pgx.Tx` e entrega
aos repositórios (carteiras, transações, ledger, inbox, outbox) a *mesma* transação via
`port.Repositories`; só `Do` faz `Begin`/`Commit`/`Rollback`. Leituras (`Reader`) usam o pool, e a
reconciliação lê saldo e ledger numa transação `REPEATABLE READ` somente leitura, para comparar os
dois no mesmo snapshot. Erros de banco são classificados em retentáveis (`40001`, `40P01`) e
transitórios (classes `08`, `53`, `57`, `58`, erros de rede).

## Contrato HTTP

| Situação | Status | Corpo |
| --- | --- | --- |
| Processada (ou replay de processada) | `200` | `{transactionId, status:"PROCESSED", balance, idempotentReplay}` |
| Aguardando referência (ou replay) | `202` + `Location` | `{transactionId, status:"PENDING_REFERENCE", idempotentReplay}` |
| Rejeitada por regra de negócio (ou replay) | `422` | `{transactionId, status:"REJECTED", failureCode, correctable, idempotentReplay}` |
| Entrada inválida / sem `Idempotency-Key` | `400` | `{error:{code, message, field}}` |
| Sem token ou token inválido/expirado | `401` + `WWW-Authenticate` | `{error:{code:"UNAUTHENTICATED"}}` |
| Papel insuficiente / provedor diferente | `403` | `FORBIDDEN` / `FORBIDDEN_PROVIDER` |
| Inexistente (ou de outro provedor) | `404` | `NOT_FOUND` |
| Conflito de idempotência / operação externa / carteira | `409` | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| Dependência indisponível (banco, cancelamento) | `503` + `Retry-After: 1` | `SERVICE_UNAVAILABLE` — repetir com a mesma chave |
| Falha permanente registrada / erro inesperado | `500` | `FAILED` com `failureCode`, ou `INTERNAL_ERROR` |

Corpo JSON estrito (campos desconhecidos e conteúdo extra são rejeitados; limite de 64 KiB).
O ledger usa cursor opaco (`base64url({"v":<wallet_version>})`) e ordena por `wallet_version`,
único por carteira; `limit` entre 1 e 200 (padrão 50).

## Observabilidade

Logs JSON (`slog`) com `correlationId` (header `X-Correlation-Id` ou `messageId`), `messageId`,
`transactionId`, `walletId` e `providerId`; não registram credenciais nem corpos financeiros.
Métricas Prometheus por resultado/status/código, duplicatas, retries, DLQ, conflitos de
concorrência, atraso e fila da outbox, latência e divergências de reconciliação (nomes `wager_*` em `internal/infra/observability/metrics.go`).
A reconciliação reconstrói o saldo a partir do ledger (inclui a abertura), devolve
`difference = armazenado − reconstruído`, e divergências viram resposta, log e métrica sem alterar
o saldo. Health: `/health/live` (processo) e `/health/ready` (PostgreSQL, SQS, chaves do IdP).

**Tracing (OpenTelemetry).** `internal/telemetry` configura o tracer (OTLP/HTTP; no-op sem
endpoint) e propaga o contexto W3C. Spans: HTTP (`otelhttp`, nomeado pelo padrão da rota, nunca
pelo caminho com ids), `wager.execute`, `wager.resume`, `wallet.open`, `db.transaction`,
`sqs.process` (consumidor) e `outbox.publish` (produtor). O `traceparent` do trace que gravou o
evento é guardado em `outbox_events.trace_context` (mesma transação) e o relay o continua ao
publicar, inclusive em outro processo e depois do commit; a mensagem SQS de saída leva o
atributo `traceparent`, e o consumidor de entrada o extrai do atributo da mensagem. O Fx
encerra o provider por último (flush dos spans pendentes). Os logs trazem o `traceId`.

## Estratégia de testes

- **Unitários** (`go test ./...`): `Money` (parsing, escala, limites, moedas), carteira, ledger,
  máquina de estados, regras dos cinco tipos, abertura interna, eventos, hash/conflito de payload,
  configuração, codec JSON, cursor, validação JWT com JWKS real gerado no teste e middleware.
- **Integração** (`test/integration`, tag `integration`): PostgreSQL, Keycloak e LocalStack
  reais; processos independentes do servidor, compilados com `-race`; sem substituir
  infraestrutura por mocks. Os cenários estão em `test/integration`.
- **Ponta a ponta** (`scripts/e2e.ps1`): a jornada completa pelas três instâncias do Compose.
- **Fuzz** do parser monetário (`FuzzParse`) e **carga** reproduzível (`cmd/loadtest`), que ao final
  reconcilia todas as carteiras.
- `-race` roda no container Go (o runner dos testes do Compose também usa `-race`).

## Interpretações, limitações e trabalho não concluído

- **Aceite síncrono**: sem commit intermediário de `PENDING`. A retomada durável de `PENDING` está
  implementada (`ClaimDue`) e é exercitada para `PENDING_REFERENCE`, mas nenhum caminho atual deixa
  um `PENDING` commitado.
- **Carteiras não pertencem a provedores**: o enunciado só restringe provedores às *suas
  transações*. Qualquer provedor autenticado pode movimentar uma carteira conhecida; o controle é
  pelo isolamento das transações, não pela posse da carteira.
- **Operação para carteira inexistente** é registrada como rejeição (`WALLET_NOT_FOUND`), o que
  motivou não ter FK em `wager_transactions.wallet_id`.
- **`WIN`/`LOSS` com referência** (opcional): seguem o mesmo mecanismo de espera das reversões
  (ficam pendentes se a aposta ainda não chegou) e a referência precisa ser uma `BET`.
- **Ordem da outbox**: garantida por carteira. As reservas dos relays são serializadas por um
  `pg_advisory_xact_lock` curto (a seleção e o `UPDATE` rodam numa transação de milissegundos). Sem
  ele, dois relays com snapshots diferentes podiam reservar eventos posteriores de uma carteira
  enquanto outro segurava os anteriores; isso apareceu no teste de carga aleatória. O lock não afeta
  o processamento das carteiras nem a publicação. O payload também traz `walletVersion`, e os
  consumidores devem ser idempotentes por `eventId` (uma republicação fora da janela de 5 min do
  broker é entregue de novo). Um evento "venenoso" nunca é descartado e segura os posteriores da
  mesma carteira.
- **Transação pendente que falha sempre** (ex.: dado inconsistente): ao retomá-la, um erro não
  transitório gera backoff nela e, no limite de tentativas, `FAILED/INTERNAL_ERROR` auditável, em
  vez de monopolizar o worker (ela seria sempre a mais antiga da fila).
- Não há limpeza/arquivamento de eventos publicados na outbox (a tabela só cresce).
- `/metrics` e `/health/*` são públicos (esperado atrás da rede interna); não há TLS na aplicação.
- Os secrets do realm de teste estão em texto no JSON; servem só para o ambiente local.
- **Não feito**: dashboards (opcionais no enunciado). Partidas dobradas, tracing OpenTelemetry e
  teste de carga existem. Várias moedas foram modeladas, mas só BRL é exercitado nos
  cenários principais; o balancete e o diário já são por moeda.
- **Broker em produção**: as políticas IAM de `deploy/iam` foram verificadas contra um emulador que
  as impõe (Moto), não contra a AWS real; política de recurso nas filas, SSE e credenciais
  temporárias por papel ficam por conta do ambiente.
- **Tracing**: apenas spans de traces (sem métricas/logs via OTLP); a amostragem é a padrão do SDK
  (sempre amostrado) — em produção convém configurar uma amostragem proporcional.
- Em máquinas Windows com política de controle de aplicativos, executáveis de teste `.exe`
  recém-gerados e o `gcc` podem ser bloqueados; use `make test-docker` e o serviço `tests` do
  Compose, que rodam tudo em containers Linux.
- LocalStack fixado em `3.8` (versões recentes exigem token de conta).
