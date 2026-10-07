//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const motoAccount = "123456789012"

type identity struct {
	name, accessKey, secret string
	client                  *sqs.Client
}

func sqsFor(t *testing.T, endpoint, key, secret string) *sqs.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")))
	if err != nil {
		t.Fatal(err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func denied(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{"AccessDenied", "not authorized", "InvalidClientTokenId", "SignatureDoesNotMatch", "AuthFailure", "403"} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// renderPolicy lê o template de deploy/iam e substitui os ARNs das filas do teste.
func renderPolicy(t *testing.T, file string, q *queues) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "iam", file))
	if err != nil {
		t.Fatal(err)
	}
	arn := func(name string) string { return "arn:aws:sqs:us-east-1:" + motoAccount + ":" + name }
	r := strings.NewReplacer("${WAGER_QUEUE_ARN}", arn(q.wagerName), "${DLQ_ARN}", arn(q.dlqName), "${EVENTS_QUEUE_ARN}", arn(q.eventsName))
	doc := r.Replace(string(raw))
	if !json.Valid([]byte(doc)) {
		t.Fatalf("política inválida após a substituição: %s", file)
	}
	return doc
}

// withBatchActionForMoto acrescenta "sqs:SendMessageBatch" aos statements que permitem
// "sqs:SendMessage". Na AWS real o envio em lote é autorizado por sqs:SendMessage (a política
// de deploy/iam vale como está); o Moto, porém, trata SendMessageBatch como ação própria.
func withBatchActionForMoto(t *testing.T, doc string) string {
	t.Helper()
	var p struct {
		Version   string
		Statement []map[string]any
	}
	if err := json.Unmarshal([]byte(doc), &p); err != nil {
		t.Fatal(err)
	}
	for _, st := range p.Statement {
		actions, _ := st["Action"].([]any)
		for _, a := range actions {
			if a == "sqs:SendMessage" {
				st["Action"] = append(actions, "sqs:SendMessageBatch")
				break
			}
		}
	}
	out, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// Menor privilégio no broker: com IAM imposto, cada identidade só faz o seu papel, e a
// aplicação funciona (consome, envia à DLQ, publica eventos) com apenas as permissões
// de deploy/iam/wager-app.policy.json.tmpl.
func TestBrokerAccessIsLeastPrivilege(t *testing.T) {
	endpoint := os.Getenv("TEST_IAM_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_IAM_ENDPOINT não definido (emulador com IAM imposto)")
	}
	ctx := context.Background()

	// 1) provisionamento sem autenticação (antes de ligar a imposição)
	root := sqsFor(t, endpoint, "root", "root")
	q := newQueuesOn(t, root)

	iamCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("root", "root", "")))
	if err != nil {
		t.Fatal(err)
	}
	iamClient := iam.NewFromConfig(iamCfg, func(o *iam.Options) { o.BaseEndpoint = aws.String(endpoint) })

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	auditorDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:GetQueueAttributes","sqs:GetQueueUrl"],"Resource":"*"}]}`
	specs := []struct {
		name, policyFile, inline string
	}{
		{"gateway-sender", "gateway-sender.policy.json.tmpl", ""},
		{"wager-app", "wager-app.policy.json.tmpl", ""},
		{"events-consumer", "events-consumer.policy.json.tmpl", ""},
		{"auditor", "", auditorDoc}, // só inspeciona contadores das filas, para o teste
	}
	ids := map[string]*identity{}
	for _, s := range specs {
		user := s.name + "-" + suffix
		if _, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(user)}); err != nil {
			t.Fatalf("create user: %v", err)
		}
		doc := s.inline
		if doc == "" {
			doc = renderPolicy(t, s.policyFile, q)
			if s.name == "wager-app" {
				doc = withBatchActionForMoto(t, doc)
			}
		}
		pol, err := iamClient.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String(user), PolicyDocument: aws.String(doc)})
		if err != nil {
			t.Fatalf("create policy %s: %v", s.name, err)
		}
		if _, err := iamClient.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{UserName: aws.String(user), PolicyArn: pol.Policy.Arn}); err != nil {
			t.Fatalf("attach policy: %v", err)
		}
		key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(user)})
		if err != nil {
			t.Fatalf("create access key: %v", err)
		}
		id := &identity{name: s.name, accessKey: aws.ToString(key.AccessKey.AccessKeyId), secret: aws.ToString(key.AccessKey.SecretAccessKey)}
		id.client = sqsFor(t, endpoint, id.accessKey, id.secret)
		ids[s.name] = id
	}

	// 2) a partir daqui toda chamada exige credenciais válidas e permissão
	resp, err := http.Post(endpoint+"/moto-api/reset-auth", "application/json", strings.NewReader("0"))
	if err != nil || resp.StatusCode >= 300 {
		t.Skipf("o emulador não permite ligar a imposição de IAM (%v)", err)
	}
	resp.Body.Close()
	t.Cleanup(func() {
		r, err := http.Post(endpoint+"/moto-api/reset-auth", "application/json", strings.NewReader("100000"))
		if err == nil {
			r.Body.Close()
		}
	})

	gateway, app, consumer, auditor := ids["gateway-sender"], ids["wager-app"], ids["events-consumer"], ids["auditor"]
	send := func(c *sqs.Client, queueURL, group string) error {
		_, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("{}"),
			MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString())})
		return err
	}
	receive := func(c *sqs.Client, queueURL string) error {
		_, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), WaitTimeSeconds: 0})
		return err
	}

	t.Run("credencial inexistente é barrada", func(t *testing.T) {
		stranger := sqsFor(t, endpoint, "AKIAUNKNOWNKEY", "nope")
		if err := send(stranger, q.wager, "g"); !denied(err) {
			t.Fatalf("chave desconhecida deveria ser negada: %v", err)
		}
	})

	t.Run("gateway só enfileira operações", func(t *testing.T) {
		if err := send(gateway.client, q.wager, "g"); err != nil {
			t.Fatalf("gateway deveria poder enviar à fila de entrada: %v", err)
		}
		// cuidado: essa mensagem "{}" é lixo; a aplicação a levará à DLQ mais abaixo
		if err := receive(gateway.client, q.wager); !denied(err) {
			t.Errorf("gateway não pode ler a fila de entrada: %v", err)
		}
		if err := send(gateway.client, q.events, "g"); !denied(err) {
			t.Errorf("gateway não pode escrever nos eventos: %v", err)
		}
		if err := send(gateway.client, q.dlq, "g"); !denied(err) {
			t.Errorf("gateway não pode escrever na DLQ: %v", err)
		}
		if _, err := gateway.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(q.wager)}); !denied(err) {
			t.Errorf("gateway não pode inspecionar a fila: %v", err)
		}
	})

	t.Run("aplicação não escreve na própria entrada nem lê os eventos", func(t *testing.T) {
		if err := send(app.client, q.wager, "g"); !denied(err) {
			t.Errorf("a aplicação não pode enviar à fila de entrada: %v", err)
		}
		if err := receive(app.client, q.events); !denied(err) {
			t.Errorf("a aplicação não pode ler os eventos que publica: %v", err)
		}
		if err := receive(app.client, q.dlq); !denied(err) {
			t.Errorf("a aplicação não precisa ler a DLQ: %v", err)
		}
	})

	t.Run("consumidor de eventos só lê eventos", func(t *testing.T) {
		if err := receive(consumer.client, q.events); err != nil {
			t.Errorf("deveria poder ler os eventos: %v", err)
		}
		for name, url := range map[string]string{"entrada": q.wager, "eventos": q.events, "DLQ": q.dlq} {
			if err := send(consumer.client, url, "g"); !denied(err) {
				t.Errorf("não pode enviar à fila de %s: %v", name, err)
			}
		}
		if err := receive(consumer.client, q.wager); !denied(err) {
			t.Errorf("não pode ler a entrada: %v", err)
		}
	})

	// 3) a aplicação roda com as credenciais de menor privilégio
	// As URLs das filas são injetadas (como faria o IaC): a aplicação não precisa de sqs:GetQueueUrl.
	inst := startInstance(t, q, withEnv(
		"AWS_ENDPOINT_URL", endpoint, "AWS_ACCESS_KEY_ID", app.accessKey, "AWS_SECRET_ACCESS_KEY", app.secret,
		"SQS_WAGER_QUEUE_URL", q.wager, "SQS_WAGER_DLQ_URL", q.dlq, "SQS_EVENTS_QUEUE_URL", q.events))
	w := openWallet(t, inst, "100.00")

	// operação válida enviada pelo gateway é consumida e processada
	o := op{external: "iam-" + w.id, kind: "BET", amount: "10.00"}
	msgID := "msg-iam-" + uuid.NewString()
	body, _ := json.Marshal(requestMsg(w, o, msgID))
	if _, err := gateway.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(q.wager), MessageBody: aws.String(string(body)),
		MessageGroupId: aws.String(w.id), MessageDeduplicationId: aws.String(msgID)}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 40*time.Second, "operação consumida com as credenciais de menor privilégio", func() bool {
		s, _, _ := dbTxStatus(t, "provider-a", o.external)
		return s == "PROCESSED"
	})
	if bal := dbBalance(t, w.id); bal != 9000 {
		t.Fatalf("saldo = %d", bal)
	}

	// a mensagem inválida (o "{}" do gateway) vai para a DLQ, o que exige sqs:SendMessage na DLQ
	attr := func(url, name string) string {
		out, err := auditor.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeName(name)}})
		if err != nil {
			t.Fatalf("auditor: %v", err)
		}
		return out.Attributes[name]
	}
	eventually(t, 40*time.Second, "mensagem inválida enviada à DLQ", func() bool {
		return attr(q.dlq, "ApproximateNumberOfMessages") != "0"
	})

	// os eventos são publicados com o papel da aplicação e lidos pelo consumidor de eventos
	var events int
	eventually(t, 40*time.Second, "eventos lidos pelo consumidor de eventos", func() bool {
		out, err := consumer.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q.events), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			t.Fatalf("consumidor de eventos: %v", err)
		}
		events += len(out.Messages)
		for _, m := range out.Messages {
			_, _ = consumer.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(q.events), ReceiptHandle: m.ReceiptHandle})
		}
		return events >= 2
	})

	// nada disso gerou erro de autorização nos logs da aplicação
	if strings.Contains(inst.out.String(), "AccessDenied") {
		t.Errorf("a aplicação foi barrada pelo broker:\n%s", inst.out.String())
	}
}
