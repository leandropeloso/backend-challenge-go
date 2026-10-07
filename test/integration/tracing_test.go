//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type jaegerSpan struct {
	SpanID        string `json:"spanID"`
	TraceID       string `json:"traceID"`
	OperationName string `json:"operationName"`
	References    []struct {
		RefType string `json:"refType"`
		SpanID  string `json:"spanID"`
	} `json:"references"`
}

func (s jaegerSpan) parent() string {
	for _, r := range s.References {
		if r.RefType == "CHILD_OF" {
			return r.SpanID
		}
	}
	return ""
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// fetchTrace consulta a API do Jaeger até que o trace tenha todas as operações esperadas.
func fetchTrace(t *testing.T, traceID string, want ...string) []jaegerSpan {
	t.Helper()
	if env.jaegerURL == "" {
		t.Skip("TEST_JAEGER_URL não definido")
	}
	var spans []jaegerSpan
	eventually(t, 45*time.Second, "trace completo no Jaeger ("+strings.Join(want, ", ")+")", func() bool {
		resp, err := http.Get(env.jaegerURL + "/api/traces/" + traceID)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var body struct {
			Data []struct {
				Spans []jaegerSpan `json:"spans"`
			} `json:"data"`
		}
		if json.NewDecoder(resp.Body).Decode(&body) != nil || len(body.Data) == 0 {
			return false
		}
		spans = body.Data[0].Spans
		have := map[string]bool{}
		for _, s := range spans {
			have[s.OperationName] = true
		}
		for _, w := range want {
			if !have[w] {
				return false
			}
		}
		return true
	})
	return spans
}

func spanNamed(t *testing.T, spans []jaegerSpan, name string) jaegerSpan {
	t.Helper()
	for _, s := range spans {
		if s.OperationName == name {
			return s
		}
	}
	t.Fatalf("span %q não encontrado", name)
	return jaegerSpan{}
}

// O trace começa no cliente (traceparent), atravessa HTTP → caso de uso → transação
// no banco e continua, depois do commit e em outro componente, na publicação da outbox.
func TestTraceFollowsHTTPRequestThroughTheOutbox(t *testing.T) {
	if env.otlpEndpoint == "" {
		t.Skip("TEST_OTLP_ENDPOINT não definido")
	}
	q := newQueues(t)
	inst := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	w := openWallet(t, inst, "100.00")

	traceID, clientSpan := randHex(16), randHex(8)
	o := op{external: "trace-http-" + w.id, kind: "BET", amount: "10.00"}
	o.key = "provider-a:" + o.external
	req, _ := http.NewRequest("POST", inst.base+"/wagering/transactions", strings.NewReader(mustJSON(t, w.req(o))))
	req.Header.Set("Authorization", "Bearer "+providerA(t))
	req.Header.Set("Idempotency-Key", o.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", fmt.Sprintf("00-%s-%s-01", traceID, clientSpan))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	spans := fetchTrace(t, traceID, "POST /wagering/transactions", "wager.execute", "db.transaction", "outbox.publish")
	httpSpan := spanNamed(t, spans, "POST /wagering/transactions")
	exec := spanNamed(t, spans, "wager.execute")
	db := spanNamed(t, spans, "db.transaction")
	pub := spanNamed(t, spans, "outbox.publish")

	for _, s := range spans {
		if s.TraceID != traceID {
			t.Fatalf("span %s fora do trace do cliente", s.OperationName)
		}
	}
	if httpSpan.parent() != clientSpan {
		t.Errorf("o span HTTP deve ser filho do span do cliente (pai=%s, quer %s)", httpSpan.parent(), clientSpan)
	}
	if exec.parent() != httpSpan.SpanID {
		t.Errorf("wager.execute deve ser filho do span HTTP")
	}
	if db.parent() != exec.SpanID {
		t.Errorf("db.transaction deve ser filho de wager.execute")
	}
	// a publicação da outbox acontece depois do commit, mas continua o mesmo trace
	if pub.parent() != db.SpanID {
		t.Errorf("outbox.publish deve ser filho do span da transação que gravou o evento (pai=%s)", pub.parent())
	}

	// o log da requisição carrega o mesmo traceId
	if !strings.Contains(inst.out.String(), `"traceId":"`+traceID+`"`) {
		t.Error("o log da requisição deveria trazer o traceId do cliente")
	}
	// a rota fica no nome do span, não o caminho com ids
	if strings.Contains(httpSpan.OperationName, w.id) {
		t.Error("o nome do span não deve conter ids")
	}
}

// A mensagem SQS carrega o traceparent: o consumidor continua o trace do produtor.
func TestTraceFollowsSQSMessageIntoTheConsumer(t *testing.T) {
	if env.otlpEndpoint == "" {
		t.Skip("TEST_OTLP_ENDPOINT não definido")
	}
	q := newQueues(t)
	inst := startInstance(t, q)
	w := openWallet(t, inst, "100.00")

	traceID, producerSpan := randHex(16), randHex(8)
	o := op{external: "trace-sqs-" + w.id, kind: "BET", amount: "5.00"}
	msgID := "msg-trace-" + uuid.NewString()
	body, _ := json.Marshal(requestMsg(w, o, msgID))
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(q.wager), MessageBody: aws.String(string(body)),
		MessageGroupId: aws.String(w.id), MessageDeduplicationId: aws.String(msgID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"traceparent": {DataType: aws.String("String"), StringValue: aws.String(fmt.Sprintf("00-%s-%s-01", traceID, producerSpan))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	spans := fetchTrace(t, traceID, "sqs.process", "wager.execute", "db.transaction", "outbox.publish")
	consumer := spanNamed(t, spans, "sqs.process")
	exec := spanNamed(t, spans, "wager.execute")
	if consumer.parent() != producerSpan {
		t.Errorf("sqs.process deve ser filho do span do produtor (pai=%s)", consumer.parent())
	}
	if exec.parent() != consumer.SpanID {
		t.Errorf("wager.execute deve ser filho de sqs.process")
	}
	if s, _, _ := dbTxStatus(t, "provider-a", o.external); s != "PROCESSED" {
		t.Fatalf("status = %s", s)
	}
	if !strings.Contains(inst.out.String(), `"traceId":"`+traceID+`"`) {
		t.Error("o log do consumidor deveria trazer o traceId")
	}
}

// Sem traceparent o serviço abre um trace novo; com a telemetria desligada continua funcionando.
func TestTracingWorksWithoutIncomingContextAndWhenDisabled(t *testing.T) {
	q := newQueues(t)
	off := startInstance(t, q, withEnv("RUN_CONSUMER", "false", "OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	w := openWallet(t, off, "10.00")
	mustStatus(t, mustSubmit(t, off, w, op{kind: "BET", amount: "1.00"}), http.StatusOK)
	if strings.Contains(off.out.String(), `"traceId"`) {
		t.Error("com a telemetria desligada não há traceId nos logs")
	}
	if code := off.terminate(20 * time.Second); code != 0 {
		t.Fatalf("exit code = %d", code)
	}

	if env.otlpEndpoint == "" {
		return
	}
	on := startInstance(t, q, withEnv("RUN_CONSUMER", "false"))
	mustStatus(t, mustSubmit(t, on, w, op{kind: "BET", amount: "1.00"}), http.StatusOK)
	eventually(t, 10*time.Second, "traceId nos logs com a telemetria ligada", func() bool {
		return strings.Contains(on.out.String(), `"traceId"`)
	})
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
