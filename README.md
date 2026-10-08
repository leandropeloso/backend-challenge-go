# Wager Service

Serviço em Go que processa operações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) sobre
carteiras de jogadores, recebidas por HTTP ou por uma fila SQS FIFO. Resolução do desafio descrito em
[DESAFIO.md](DESAFIO.md). As decisões de projeto estão em [ARCHITECTURE.md](ARCHITECTURE.md).

Stack: Go, PostgreSQL, SQS (LocalStack), Keycloak, Uber Fx e Docker Compose.

## Pré-requisitos

- Docker com Compose v2
- Go 1.27.1 (só para rodar testes ou a aplicação fora do Docker)
- `make` (opcional)

## Executando

```sh
docker compose up --build
```

Sobem PostgreSQL, Keycloak, LocalStack (as filas são criadas por
[deploy/localstack/init-queues.sh](deploy/localstack/init-queues.sh)), Jaeger, as migrations e três
instâncias da aplicação em `localhost:8081`, `:8082` e `:8083`. O Keycloak fica em `localhost:8080`.

Para checar: `curl localhost:8081/health/ready`.

Para rodar a aplicação fora do Docker, suba só a infraestrutura e use o `.env.example`:

```sh
docker compose up -d postgres keycloak localstack migrate
cp .env.example .env   # e exporte as variáveis no shell
go run ./cmd/server
```

## Variáveis de ambiente

Os valores de exemplo para uso local estão em [.env.example](.env.example). As obrigatórias são
`DATABASE_URL`, `AUTH_ISSUER` e `AUTH_JWKS_URL`. As demais têm padrão e estão em
[internal/config/config.go](internal/config/config.go).

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | endereço do servidor HTTP |
| `SHUTDOWN_TIMEOUT` | `30s` | prazo do shutdown |
| `DATABASE_URL` | — | URL do PostgreSQL |
| `DATABASE_MAX_CONNS` | `20` | tamanho do pool |
| `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `us-east-1`, vazio | cliente SQS (o endpoint é só para o LocalStack) |
| `SQS_WAGER_QUEUE`, `SQS_WAGER_DLQ`, `SQS_EVENTS_QUEUE` | `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wager-events.fifo` | nomes das filas (FIFO) |
| `SQS_WAGER_QUEUE_URL`, `SQS_WAGER_DLQ_URL`, `SQS_EVENTS_QUEUE_URL` | vazio | URLs das filas, se já forem conhecidas |
| `AUTH_ISSUER` | — | valor esperado do claim `iss` |
| `AUTH_JWKS_URL` | — | endpoint JWKS do IdP |
| `AUTH_AUDIENCE` | `wager-api` | claim `aud` esperado |
| `AUTH_PROVIDER_ROLE`, `AUTH_INTERNAL_ROLE` | `provider`, `internal` | papéis em `realm_access.roles` |
| `AUTH_PROVIDER_ID_CLAIM` | `providerId` | claim que identifica o provedor |
| `RUN_HTTP`, `RUN_CONSUMER`, `RUN_OUTBOX`, `RUN_RESOLVER` | `true` | quais componentes a instância executa |
| `CONSUMER_CONCURRENCY` | `4` | loops de leitura por instância |
| `CONSUMER_MAX_ATTEMPTS` | `5` | recebimentos antes de ir para a DLQ |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE` | `500ms`, `100` | relay da outbox |
| `PENDING_TTL`, `PENDING_MAX_ATTEMPTS` | `15m`, `12` | limite das referências pendentes |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | vazio | tracing OpenTelemetry (desligado se vazio) |

## Migrations

As migrations ficam em [migrations/](migrations) (`.up.sql` e `.down.sql`). O `docker compose up`
aplica as pendentes antes de subir a aplicação. Também dá para rodar à mão:

```sh
docker compose run --rm migrate up        # aplica
docker compose run --rm migrate status    # lista
docker compose run --rm migrate down      # reverte a última
docker compose run --rm migrate down 0    # reverte todas
```

Sem Docker: `DATABASE_URL=... go run ./cmd/migrate up`.

## Autenticação

O Keycloak importa o realm `wager` ([deploy/keycloak/realm-wager.json](deploy/keycloak/realm-wager.json)).
Clients de teste, com `client_credentials`:

| client_id | client_secret | Papel |
| --- | --- | --- |
| `provider-a` | `provider-a-secret` | `provider` |
| `provider-b` | `provider-b-secret` | `provider` |
| `internal-service` | `internal-service-secret` | `internal` |

Esses valores são só do ambiente local.

## Exemplos de chamadas

```sh
token() { curl -s -X POST http://localhost:8080/realms/wager/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" | jq -r .access_token; }

INTERNAL=$(token internal-service internal-service-secret)
PROVIDER=$(token provider-a provider-a-secret)

# abrir carteira (só serviço interno)
PLAYER=$(uuidgen)
WALLET=$(curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $INTERNAL" \
  -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}" | jq -r .id)

# apostar (repetir o comando devolve idempotentReplay=true)
curl -s -X POST localhost:8082/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Idempotency-Key: provider-a:transaction-123' \
  -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# reembolso
curl -s -X POST localhost:8083/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Idempotency-Key: provider-a:refund-1' \
  -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"refund-1\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"REFUND\",
       \"referenceExternalTransactionId\":\"transaction-123\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# consultas
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
```

Enviando uma operação pela fila (o `MessageGroupId` é o `walletId`):

```sh
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-456","idempotencyKey":"provider-a:transaction-456",
    "playerId":"'$PLAYER'","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
    "money":{"amount":"5.00","currency":"BRL"}}}'
```

## Rotas

| Rota | Acesso |
| --- | --- |
| `POST /wallets` | interno |
| `GET /wallets/{id}`, `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` | interno |
| `GET /accounting/trial-balance` | interno |
| `POST /wagering/transactions` | provedor |
| `GET /wagering/transactions/{id}` | provedor (só as suas) ou interno |
| `GET /providers/{providerId}/wagering/transactions/{externalId}` | provedor dono ou interno |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | público |

Os códigos de resposta e de erro estão em [ARCHITECTURE.md](ARCHITECTURE.md#contrato-http).

## Testes

| O quê | Comando |
| --- | --- |
| Unitários | `go test ./...` |
| Formatação e vet | `gofmt -l .` e `go vet ./...` |
| Unitários com `-race` | `make test-docker` (ou `go test -race ./...`, que precisa de gcc) |
| Integração | `docker compose --profile test run --rm tests` (ou `make test-integration`) |
| Ponta a ponta | `pwsh scripts/e2e.ps1` (com o Compose no ar) |
| Fuzz do parser monetário | `make test-fuzz` |
| Carga | `make load-test` |

Os testes de integração ficam em [test/integration](test/integration), usam PostgreSQL, Keycloak e
LocalStack reais e sobem processos separados do servidor. Cobrem concorrência, idempotência, inbox,
outbox, reentrega, DLQ, referências pendentes, autenticação, queda do banco e shutdown. Para rodar um
teste só:

```sh
docker compose --profile test run --rm tests \
  go test -race -count=1 -tags integration -run TestSameBetFiftyTimesInParallel -v ./test/integration/...
```

O teste de queda do PostgreSQL usa o socket do Docker, montado só no serviço `tests`.

O teste de carga gera operações concorrentes nas três instâncias e, no fim, reconcilia todas as
carteiras. Na minha máquina (Docker Desktop no Windows 11, tudo no mesmo host) deu cerca de 650
req/s, sem divergências. O número depende da máquina.

## Estrutura

```
cmd/server         ponto de entrada
cmd/migrate        aplica/reverte migrations
cmd/loadtest       teste de carga
internal/domain    modelo de domínio
internal/app       casos de uso e ports
internal/infra     postgres, sqs, http, auth
internal/worker    outbox e referências pendentes
internal/platform  composição com Fx
migrations         SQL versionado
deploy             Keycloak, filas SQS e políticas IAM
test/integration   testes com infraestrutura real
```
