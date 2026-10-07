package sqsx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/config"
	"github.com/leandropeloso/wager-service/internal/fault"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

const requestedType = "WagerTransactionRequested"

type requestEnvelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurredAt"`
	Data       requestData `json:"data"`
}

type requestData struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	IdempotencyKey        string `json:"idempotencyKey"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

// Consumer lê WagerTransactionRequested da fila FIFO e delega ao mesmo caso de
// uso da API HTTP. A mensagem só é removida depois do commit do tratamento.
type Consumer struct {
	api     API
	queues  *Queues
	service *processwager.Service
	metrics port.Metrics
	log     *slog.Logger
	cfg     *config.Config

	pollCtx   context.Context
	stopPoll  context.CancelFunc
	workCtx   context.Context
	stopWork  context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
}

func NewConsumer(api API, queues *Queues, service *processwager.Service, metrics port.Metrics, log *slog.Logger, cfg *config.Config) *Consumer {
	c := &Consumer{api: api, queues: queues, service: service, metrics: metrics, log: log, cfg: cfg}
	c.pollCtx, c.stopPoll = context.WithCancel(context.Background())
	c.workCtx, c.stopWork = context.WithCancel(context.Background())
	return c
}

// Start abre os loops de leitura e retorna imediatamente.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		for i := 0; i < c.cfg.Consumer.Concurrency; i++ {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.loop()
			}()
		}
	})
}

// Stop interrompe a busca de novas mensagens e espera as em andamento até o
// prazo de ctx. Se o prazo estourar, cancela o trabalho: as transações abertas
// sofrem rollback e as mensagens voltam à fila (visibilidade liberada).
func (c *Consumer) Stop(ctx context.Context) error {
	c.stopPoll()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		c.stopWork()
		return nil
	case <-ctx.Done():
		c.stopWork()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		return fmt.Errorf("consumer stopped before in-flight messages finished: %w", ctx.Err())
	}
}

func (c *Consumer) loop() {
	wait := int32(c.cfg.Consumer.WaitTime / time.Second)
	if wait < 1 {
		wait = 1
	}
	if wait > 20 {
		wait = 20
	}
	for c.pollCtx.Err() == nil {
		out, err := c.api.ReceiveMessage(c.pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.queues.Wager),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             wait,
			VisibilityTimeout:           int32(c.cfg.Consumer.VisibilityTimeout / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"}, // inclui o traceparent do produtor
		})
		if err != nil {
			if c.pollCtx.Err() != nil {
				return
			}
			c.log.Warn("sqs receive failed", "error", err)
			c.sleep(2 * time.Second)
			continue
		}
		c.processBatch(out.Messages)
	}
}

func (c *Consumer) sleep(d time.Duration) {
	select {
	case <-c.pollCtx.Done():
	case <-time.After(d):
	}
}

// processBatch trata as mensagens em ordem. Uma falha em um grupo libera as
// demais mensagens do mesmo grupo, preservando a ordem FIFO por carteira.
func (c *Consumer) processBatch(msgs []types.Message) {
	blocked := map[string]bool{}
	for _, m := range msgs {
		group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if c.pollCtx.Err() != nil || blocked[group] {
			c.release(m)
			continue
		}
		if !c.handle(m, group) {
			blocked[group] = true
		}
	}
}

// handle devolve false quando a mensagem não foi concluída (o grupo deve parar).
func (c *Consumer) handle(m types.Message, group string) bool {
	body := aws.ToString(m.Body)
	receives := receiveCount(m)
	log := c.log.With("sqsMessageId", aws.ToString(m.MessageId), "receiveCount", receives)

	env, err := parseRequest(body)
	if err != nil {
		log.Warn("invalid message sent to dlq", "reason", err.Error())
		return c.toDLQ(m, group, "invalid_message", err.Error())
	}
	log = log.With("messageId", env.MessageID, "providerId", env.Data.ProviderID,
		"walletId", env.Data.WalletID, "correlationId", env.MessageID)

	ctx, cancel := context.WithTimeout(c.workCtx, c.cfg.Consumer.ProcessTimeout)
	defer cancel()
	// Continua o trace do produtor (traceparent no atributo da mensagem), se houver.
	ctx = telemetry.Extract(ctx, attributeValue(m, TraceparentAttribute))
	ctx, span := telemetry.Tracer().Start(ctx, "sqs.process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", "aws_sqs"), attribute.String("messaging.message_id", env.MessageID),
			attribute.String("messaging.message_group_id", group), attribute.Int("messaging.receive_count", receives)))
	defer span.End()
	if id := telemetry.TraceID(ctx); id != "" {
		log = log.With("traceId", id)
	}
	out, err := c.service.Execute(ctx, processwager.Command{
		ProviderID: env.Data.ProviderID, ExternalTransactionID: env.Data.ExternalTransactionID,
		IdempotencyKey: env.Data.IdempotencyKey, PlayerID: env.Data.PlayerID, WalletID: env.Data.WalletID,
		RoundID: env.Data.RoundID, GameID: env.Data.GameID, Kind: env.Data.Kind,
		Amount: env.Data.Money.Amount, Currency: env.Data.Money.Currency,
		ReferenceExternalTransactionID: env.Data.ReferenceExternalTransactionID,
		CorrelationID:                  env.MessageID, Source: "sqs",
		Inbox: &processwager.Inbox{Consumer: c.cfg.ConsumerName, MessageID: env.MessageID},
	})

	var invalid *processwager.InvalidInputError
	switch {
	case err == nil:
		// Ponto de falha dos testes: o commit já aconteceu, a mensagem ainda não foi removida.
		fault.Hit("consumer.after_commit")
		log.Info("message handled", "transactionId", out.TransactionID, "status", out.Status, "replay", out.Replay)
		return c.ack(m)
	case errors.As(err, &invalid):
		log.Warn("invalid message sent to dlq", "reason", err.Error())
		return c.toDLQ(m, group, "invalid_input", err.Error())
	case errors.Is(err, processwager.ErrIdempotencyConflict),
		errors.Is(err, processwager.ErrExternalIDConflict),
		errors.Is(err, processwager.ErrInboxConflict):
		log.Warn("conflicting message sent to dlq", "reason", err.Error())
		return c.toDLQ(m, group, "conflict", err.Error())
	case c.workCtx.Err() != nil:
		// Encerramento forçado: devolve a mensagem para outra instância.
		c.release(m)
		return false
	}

	if receives >= c.cfg.Consumer.MaxAttempts {
		log.Error("retries exhausted, sending to dlq", "error", err)
		return c.toDLQ(m, group, "retries_exhausted", err.Error())
	}
	delay := c.retryDelay(receives)
	log.Warn("transient failure, will retry", "error", err, "delay", delay.String())
	c.metrics.Retry("sqs")
	c.changeVisibility(m, int32(delay/time.Second))
	return false
}

func attributeValue(m types.Message, name string) string {
	if v, ok := m.MessageAttributes[name]; ok {
		return aws.ToString(v.StringValue)
	}
	return ""
}

func parseRequest(body string) (requestEnvelope, error) {
	var env requestEnvelope
	dec := json.NewDecoder(bytes.NewReader([]byte(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return env, fmt.Errorf("malformed message: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return env, errors.New("unexpected content after the message")
	}
	if env.Type != requestedType {
		return env, fmt.Errorf("unsupported message type %q", env.Type)
	}
	if env.MessageID == "" || len(env.MessageID) > 255 {
		return env, errors.New("messageId is required (up to 255 characters)")
	}
	if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
		return env, errors.New("occurredAt must be an RFC 3339 timestamp")
	}
	return env, nil
}

func receiveCount(m types.Message) int {
	n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// retryDelay: base * 2^(tentativa-1), limitado ao máximo configurado.
func (c *Consumer) retryDelay(receives int) time.Duration {
	d := c.cfg.Consumer.RetryBaseDelay
	for i := 1; i < receives && d < c.cfg.Consumer.RetryMaxDelay; i++ {
		d *= 2
	}
	if d > c.cfg.Consumer.RetryMaxDelay {
		d = c.cfg.Consumer.RetryMaxDelay
	}
	if d < time.Second {
		d = time.Second
	}
	return d
}

// background devolve um contexto curto que sobrevive ao encerramento do consumidor,
// usado para confirmar ou liberar mensagens já tratadas.
func background() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (c *Consumer) ack(m types.Message) bool {
	ctx, cancel := background()
	defer cancel()
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.queues.Wager), ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		// O resultado já está no banco; a reentrega será tratada como duplicata pela inbox.
		c.log.Warn("delete message failed; redelivery will be deduplicated", "error", err)
		return false
	}
	return true
}

func (c *Consumer) toDLQ(m types.Message, group, reason, detail string) bool {
	ctx, cancel := background()
	defer cancel()
	if group == "" {
		group = "unknown"
	}
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DLQ),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"failureDetail": {DataType: aws.String("String"), StringValue: aws.String(truncate(detail, 500))},
		},
	})
	if err != nil {
		c.log.Error("could not send message to dlq; leaving it for redrive", "error", err)
		return false
	}
	c.metrics.DLQ(reason)
	return c.ack(m)
}

func (c *Consumer) release(m types.Message) { c.changeVisibility(m, 0) }

func (c *Consumer) changeVisibility(m types.Message, seconds int32) {
	ctx, cancel := background()
	defer cancel()
	_, err := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Wager), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: seconds,
	})
	if err != nil {
		c.log.Warn("change message visibility failed", "error", err)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
