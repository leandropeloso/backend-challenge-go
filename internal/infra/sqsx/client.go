// Package sqsx reúne o cliente SQS, o consumidor de operações e o publisher de eventos.
package sqsx

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/leandropeloso/wager-service/internal/config"
)

// API é o subconjunto do cliente SQS usado pela aplicação.
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

func NewClient(ctx context.Context, cfg *config.Config) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.AWSRegion)}
	if cfg.AWSAccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	}), nil
}

// Queues guarda as URLs resolvidas das filas usadas pela aplicação.
type Queues struct {
	Wager  string
	DLQ    string
	Events string
}

// ResolveQueues descobre as URLs pelos nomes, aguardando até wait pelo
// provisionamento das filas (LocalStack pode subir depois da aplicação).
func ResolveQueues(ctx context.Context, api API, cfg *config.Config, wait time.Duration) (Queues, error) {
	var q Queues
	targets := []struct {
		name, url string
		dst       *string
	}{
		{cfg.WagerQueueName, cfg.WagerQueueURL, &q.Wager},
		{cfg.WagerDLQName, cfg.WagerDLQURL, &q.DLQ},
		{cfg.EventsQueueName, cfg.EventsQueueURL, &q.Events},
	}

	deadline := time.Now().Add(wait)
	for _, t := range targets {
		if t.url != "" { // URL injetada: dispensa a permissão sqs:GetQueueUrl
			*t.dst = t.url
			continue
		}
		for {
			out, err := api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(t.name)})
			if err == nil {
				*t.dst = aws.ToString(out.QueueUrl)
				break
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				return Queues{}, fmt.Errorf("resolve queue %s: %w", t.name, err)
			}
			select {
			case <-ctx.Done():
				return Queues{}, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	return q, nil
}

// Ping verifica se a fila principal está acessível (readiness).
func Ping(ctx context.Context, api API, queueURL string) error {
	_, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}
