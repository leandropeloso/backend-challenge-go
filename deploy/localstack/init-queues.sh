#!/bin/bash
# Provisiona as filas FIFO e a política de redrive. Executado pelo LocalStack
# quando fica pronto (/etc/localstack/init/ready.d). É idempotente.
set -euo pipefail

create() { awslocal sqs create-queue --queue-name "$1" --attributes "$2" --query QueueUrl --output text; }

DLQ_URL=$(create wager-transactions-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}')
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)

# Visibility timeout de 30s; depois de 5 recebimentos sem sucesso a mensagem vai para a DLQ.
REDRIVE=$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "$DLQ_ARN" | sed 's/"/\\"/g')
create wager-transactions.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"${REDRIVE}\"}" >/dev/null

# Destino dos eventos de integração publicados pela outbox.
create wager-events.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}' >/dev/null

touch /tmp/queues-ready
echo "wager queues provisioned"
