package sqsx

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

// MaxBatch é o limite de entradas por SendMessageBatch do SQS.
const MaxBatch = 10

// TraceparentAttribute é o atributo de mensagem que carrega o contexto W3C.
const TraceparentAttribute = "traceparent"

// EventPublisher envia os eventos da outbox para a fila FIFO de eventos.
//
// Contrato de roteamento: MessageGroupId = id da carteira (ordena os eventos
// de uma carteira); MessageDeduplicationId = eventId (republicações dentro da
// janela de 5 minutos do SQS FIFO são descartadas pelo broker); o corpo é o
// envelope JSON guardado na outbox, sem alterações. O atributo `traceparent`
// continua o trace que originou o evento.
type EventPublisher struct {
	api    API
	queues *Queues
}

func NewEventPublisher(api API, queues *Queues) *EventPublisher {
	return &EventPublisher{api: api, queues: queues}
}

func entryOf(i int, m port.OutboxMessage, traceparent string) types.SendMessageBatchRequestEntry {
	attrs := map[string]types.MessageAttributeValue{
		"eventType": {DataType: aws.String("String"), StringValue: aws.String(m.EventType)},
		"eventId":   {DataType: aws.String("String"), StringValue: aws.String(m.ID.String())},
	}
	if traceparent != "" {
		attrs[TraceparentAttribute] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(traceparent)}
	}
	return types.SendMessageBatchRequestEntry{
		Id:                     aws.String(strconv.Itoa(i)),
		MessageBody:            aws.String(string(m.Payload)),
		MessageGroupId:         aws.String(m.PartitionKey),
		MessageDeduplicationId: aws.String(m.ID.String()),
		MessageAttributes:      attrs,
	}
}

// PublishBatch envia até MaxBatch mensagens numa única chamada e devolve um
// resultado por mensagem (nil = aceita pelo broker). Quem chama garante que o
// lote não traz duas mensagens da mesma carteira, de modo que uma falha parcial
// nunca inverte a ordem de uma carteira.
func (p *EventPublisher) PublishBatch(ctx context.Context, msgs []port.OutboxMessage) []error {
	errs := make([]error, len(msgs))
	if len(msgs) == 0 {
		return errs
	}
	if len(msgs) > MaxBatch {
		err := fmt.Errorf("batch of %d exceeds the SQS limit of %d", len(msgs), MaxBatch)
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	// Um span "outbox.publish" por evento, filho do trace que o originou
	// (restaurado do traceparent guardado na outbox, mesmo vindo de outro processo).
	spans := make([]trace.Span, len(msgs))
	entries := make([]types.SendMessageBatchRequestEntry, len(msgs))
	for i, m := range msgs {
		spanCtx, span := telemetry.Tracer().Start(telemetry.Extract(ctx, m.TraceContext), "outbox.publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("messaging.system", "aws_sqs"), attribute.String("messaging.destination", "events"),
				attribute.String("event.id", m.ID.String()), attribute.String("event.type", m.EventType),
				attribute.String("wallet.id", m.PartitionKey), attribute.Int("outbox.attempts", m.Attempts)))
		spans[i] = span
		entries[i] = entryOf(i, m, telemetry.Inject(spanCtx))
	}
	defer func() {
		for i, span := range spans {
			telemetry.Fail(span, errs[i])
			span.End()
		}
	}()

	out, err := p.api.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(p.queues.Events), Entries: entries})
	if err != nil {
		for i, m := range msgs {
			errs[i] = fmt.Errorf("send event %s: %w", m.ID, err)
		}
		return errs
	}
	failed := map[string]string{}
	for _, f := range out.Failed {
		failed[aws.ToString(f.Id)] = aws.ToString(f.Code) + ": " + aws.ToString(f.Message)
	}
	for i, m := range msgs {
		if reason, bad := failed[strconv.Itoa(i)]; bad {
			errs[i] = fmt.Errorf("send event %s: %s", m.ID, reason)
		}
	}
	return errs
}
