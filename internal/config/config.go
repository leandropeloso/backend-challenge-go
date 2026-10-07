// Package config carrega e valida a configuração a partir de variáveis de ambiente.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration

	DatabaseURL      string
	DatabaseMaxConns int32

	AWSRegion          string
	AWSEndpoint        string
	AWSAccessKeyID     string
	AWSSecretAccessKey string

	WagerQueueName  string
	WagerDLQName    string
	EventsQueueName string
	ConsumerName    string

	// URLs das filas (opcionais). Quando informadas, a aplicação não precisa da
	// permissão sqs:GetQueueUrl: o IaC injeta as URLs e o papel IAM fica mínimo.
	WagerQueueURL  string
	WagerDLQURL    string
	EventsQueueURL string

	Auth Auth

	// Rastreamento OpenTelemetry (desligado se o endpoint estiver vazio).
	Telemetry Telemetry

	// Workers que esta instância executa.
	RunHTTP     bool
	RunConsumer bool
	RunOutbox   bool
	RunResolver bool

	Consumer Consumer
	Outbox   Outbox
	Pending  Pending
}

// Telemetry usa os nomes padrão do OpenTelemetry.
type Telemetry struct {
	Endpoint    string // OTEL_EXPORTER_OTLP_ENDPOINT, ex.: http://jaeger:4318
	ServiceName string // OTEL_SERVICE_NAME
}

type Auth struct {
	Issuer          string
	JWKSURL         string
	Audience        string
	ProviderRole    string
	InternalRole    string
	ProviderIDClaim string
	RefreshInterval time.Duration
}

type Consumer struct {
	Concurrency       int
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	MaxAttempts       int
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	ProcessTimeout    time.Duration
}

type Outbox struct {
	PollInterval   time.Duration
	BatchSize      int
	Concurrency    int
	Lease          time.Duration
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	PublishTimeout time.Duration
}

type Pending struct {
	PollInterval time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	TTL          time.Duration
	MaxAttempts  int
}

// Load lê o ambiente e valida o resultado; erros de configuração impedem a inicialização.
func Load() (*Config, error) {
	e := &envReader{}
	c := &Config{
		HTTPAddr:         e.str("HTTP_ADDR", ":8080"),
		ShutdownTimeout:  e.dur("SHUTDOWN_TIMEOUT", 30*time.Second),
		DatabaseURL:      e.required("DATABASE_URL"),
		DatabaseMaxConns: int32(e.integer("DATABASE_MAX_CONNS", 20)),

		AWSRegion:          e.str("AWS_REGION", "us-east-1"),
		AWSEndpoint:        e.str("AWS_ENDPOINT_URL", ""),
		AWSAccessKeyID:     e.str("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: e.str("AWS_SECRET_ACCESS_KEY", ""),

		WagerQueueName:  e.str("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		WagerDLQName:    e.str("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		EventsQueueName: e.str("SQS_EVENTS_QUEUE", "wager-events.fifo"),
		ConsumerName:    e.str("CONSUMER_NAME", "wager-transactions-consumer"),
		WagerQueueURL:   e.str("SQS_WAGER_QUEUE_URL", ""),
		WagerDLQURL:     e.str("SQS_WAGER_DLQ_URL", ""),
		EventsQueueURL:  e.str("SQS_EVENTS_QUEUE_URL", ""),

		Auth: Auth{
			Issuer:          e.required("AUTH_ISSUER"),
			JWKSURL:         e.required("AUTH_JWKS_URL"),
			Audience:        e.str("AUTH_AUDIENCE", "wager-api"),
			ProviderRole:    e.str("AUTH_PROVIDER_ROLE", "provider"),
			InternalRole:    e.str("AUTH_INTERNAL_ROLE", "internal"),
			ProviderIDClaim: e.str("AUTH_PROVIDER_ID_CLAIM", "providerId"),
			RefreshInterval: e.dur("AUTH_JWKS_REFRESH", 5*time.Minute),
		},

		Telemetry: Telemetry{
			Endpoint:    e.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
			ServiceName: e.str("OTEL_SERVICE_NAME", "wager-service"),
		},

		RunHTTP:     e.boolean("RUN_HTTP", true),
		RunConsumer: e.boolean("RUN_CONSUMER", true),
		RunOutbox:   e.boolean("RUN_OUTBOX", true),
		RunResolver: e.boolean("RUN_RESOLVER", true),

		Consumer: Consumer{
			Concurrency:       e.integer("CONSUMER_CONCURRENCY", 4),
			WaitTime:          e.dur("CONSUMER_WAIT_TIME", 5*time.Second),
			VisibilityTimeout: e.dur("CONSUMER_VISIBILITY_TIMEOUT", 30*time.Second),
			MaxAttempts:       e.integer("CONSUMER_MAX_ATTEMPTS", 5),
			RetryBaseDelay:    e.dur("CONSUMER_RETRY_BASE_DELAY", 2*time.Second),
			RetryMaxDelay:     e.dur("CONSUMER_RETRY_MAX_DELAY", 2*time.Minute),
			ProcessTimeout:    e.dur("CONSUMER_PROCESS_TIMEOUT", 20*time.Second),
		},
		Outbox: Outbox{
			PollInterval:   e.dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:      e.integer("OUTBOX_BATCH_SIZE", 100),
			Concurrency:    e.integer("OUTBOX_CONCURRENCY", 16),
			Lease:          e.dur("OUTBOX_LEASE", 30*time.Second),
			BackoffBase:    e.dur("OUTBOX_BACKOFF_BASE", time.Second),
			BackoffMax:     e.dur("OUTBOX_BACKOFF_MAX", time.Minute),
			PublishTimeout: e.dur("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second),
		},
		Pending: Pending{
			PollInterval: e.dur("PENDING_POLL_INTERVAL", time.Second),
			BaseBackoff:  e.dur("PENDING_BASE_BACKOFF", 2*time.Second),
			MaxBackoff:   e.dur("PENDING_MAX_BACKOFF", 2*time.Minute),
			TTL:          e.dur("PENDING_TTL", 15*time.Minute),
			MaxAttempts:  e.integer("PENDING_MAX_ATTEMPTS", 12),
		},
	}
	if err := errors.Join(e.err, c.validate()); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return c, nil
}

func (c *Config) validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	for _, u := range []struct{ name, v string }{{"DATABASE_URL", c.DatabaseURL}, {"AUTH_ISSUER", c.Auth.Issuer}, {"AUTH_JWKS_URL", c.Auth.JWKSURL}} {
		parsed, err := url.Parse(u.v)
		check(err == nil && parsed.Scheme != "" && parsed.Host != "", u.name+" must be an absolute URL")
	}
	if c.AWSEndpoint != "" {
		parsed, err := url.Parse(c.AWSEndpoint)
		check(err == nil && parsed.Host != "", "AWS_ENDPOINT_URL must be an absolute URL")
	}
	check(strings.HasSuffix(c.WagerQueueName, ".fifo"), "SQS_WAGER_QUEUE must be a FIFO queue")
	check(strings.HasSuffix(c.WagerDLQName, ".fifo"), "SQS_WAGER_DLQ must be a FIFO queue")
	check(strings.HasSuffix(c.EventsQueueName, ".fifo"), "SQS_EVENTS_QUEUE must be a FIFO queue")
	check(c.Consumer.Concurrency > 0, "CONSUMER_CONCURRENCY must be positive")
	check(c.Consumer.MaxAttempts > 0, "CONSUMER_MAX_ATTEMPTS must be positive")
	check(c.Consumer.ProcessTimeout < c.Consumer.VisibilityTimeout, "CONSUMER_PROCESS_TIMEOUT must be shorter than CONSUMER_VISIBILITY_TIMEOUT")
	check(c.Outbox.BatchSize > 0, "OUTBOX_BATCH_SIZE must be positive")
	check(c.Outbox.Concurrency > 0, "OUTBOX_CONCURRENCY must be positive")
	check(c.Outbox.Lease > c.Outbox.PublishTimeout, "OUTBOX_LEASE must be longer than OUTBOX_PUBLISH_TIMEOUT")
	check(c.Pending.MaxAttempts > 0, "PENDING_MAX_ATTEMPTS must be positive")
	check(c.Pending.BaseBackoff > 0 && c.Pending.MaxBackoff >= c.Pending.BaseBackoff, "PENDING backoff bounds are invalid")
	check(c.ShutdownTimeout > 0, "SHUTDOWN_TIMEOUT must be positive")
	check((c.AWSAccessKeyID == "") == (c.AWSSecretAccessKey == ""), "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together")
	return errors.Join(errs...)
}

type envReader struct{ err error }

func (e *envReader) fail(err error) { e.err = errors.Join(e.err, err) }

func (e *envReader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *envReader) required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		e.fail(fmt.Errorf("%s is required", key))
	}
	return v
}

func (e *envReader) integer(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.fail(fmt.Errorf("%s must be an integer", key))
		return def
	}
	return n
}

func (e *envReader) boolean(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.fail(fmt.Errorf("%s must be a boolean", key))
		return def
	}
	return b
}

func (e *envReader) dur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		e.fail(fmt.Errorf("%s must be a positive duration (e.g. 5s)", key))
		return def
	}
	return d
}
