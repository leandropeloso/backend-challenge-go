GO_IMAGE ?= golang:1.27.1
DOCKER_GO = docker run --rm -v "$(CURDIR):/src" -v wager_gomodcache:/go/pkg/mod -w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE)

.PHONY: up down build fmt vet test test-race test-docker test-fuzz test-integration test-integration-cover load-test migrate-up migrate-down migrate-status

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# Testes unitários no host.
test:
	go test ./...

# Com o detector de corridas (exige cgo/gcc no host).
test-race:
	go test -race ./...

# Unitários + vet + race dentro de um container Go, sem depender do toolchain local.
test-docker:
	$(DOCKER_GO) sh -c "gofmt -l . && go vet ./... && go test -race -count=1 ./..."

# Integração: PostgreSQL, Keycloak e LocalStack reais, várias instâncias do servidor
# como processos independentes e simulações de falha (build tag faultinject).
test-integration:
	docker compose stop app1 app2 app3
	docker compose --profile test run --rm tests

# Fuzz do parser monetário (use FUZZTIME=2m para sessões mais longas).
FUZZTIME ?= 30s
test-fuzz:
	$(DOCKER_GO) go test -run '^$$' -fuzz FuzzParse -fuzztime $(FUZZTIME) ./internal/domain/money/

# Integração com cobertura do binário do servidor (relatório por pacote ao final).
test-integration-cover:
	docker compose stop app1 app2 app3
	docker compose --profile test run --rm tests sh -c 'mkdir -p /tmp/c && IT_COVERDIR=/tmp/c go test -race -count=1 -tags integration -timeout 25m ./test/integration/... ; go tool covdata percent -i=/tmp/c'

# Teste de carga contra as 3 instâncias do compose (make up -d antes). Ajuste DURATION/CONCURRENCY/WALLETS.
DURATION ?= 30s
CONCURRENCY ?= 32
WALLETS ?= 200
load-test:
	docker run --rm --network wager_default -v "$(CURDIR):/src" -v wager_gomodcache:/go/pkg/mod -w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE) \
		go run ./cmd/loadtest -bases http://app1:8080,http://app2:8080,http://app3:8080 \
		-token-url http://keycloak:8080/realms/wager/protocol/openid-connect/token \
		-duration $(DURATION) -concurrency $(CONCURRENCY) -wallets $(WALLETS)

migrate-up:
	docker compose run --rm migrate up

# Reverte a última migration (use `make migrate-down STEPS=0` para todas).
STEPS ?= 1
migrate-down:
	docker compose run --rm migrate down $(STEPS)

migrate-status:
	docker compose run --rm migrate status
