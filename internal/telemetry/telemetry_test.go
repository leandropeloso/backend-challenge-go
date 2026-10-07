package telemetry

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func newRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	if _, err := Setup(context.Background(), Config{}); err != nil { // instala o propagador W3C
		t.Fatal(err)
	}
	return rec
}

func TestSetupWithoutEndpointIsANoop(t *testing.T) {
	p, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown do no-op: %v", err)
	}
	var nilProvider *Provider
	if err := nilProvider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupRejectsInvalidEndpoint(t *testing.T) {
	if _, err := Setup(context.Background(), Config{Endpoint: "::::"}); err == nil {
		t.Fatal("endpoint inválido deveria falhar na inicialização")
	}
}

func TestTraceparentRoundTripKeepsTheTrace(t *testing.T) {
	rec := newRecorder(t)

	ctx, parent := Start(context.Background(), "http.request")
	tp := Inject(ctx)
	if !strings.HasPrefix(tp, "00-") || len(strings.Split(tp, "-")) != 4 {
		t.Fatalf("traceparent W3C inválido: %q", tp)
	}
	parent.End()

	// outro processo: restaura o contexto e abre um filho
	remote := Extract(context.Background(), tp)
	_, child := Start(remote, "sqs.process")
	child.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[0].SpanContext().TraceID() != spans[1].SpanContext().TraceID() {
		t.Fatal("o filho remoto deve continuar o mesmo trace")
	}
	if spans[1].Parent().SpanID() != spans[0].SpanContext().SpanID() {
		t.Fatal("o filho remoto deve apontar para o span original como pai")
	}
	if !spans[1].Parent().IsRemote() {
		t.Fatal("o pai deve ser marcado como remoto")
	}
	if TraceID(ctx) != spans[0].SpanContext().TraceID().String() {
		t.Fatal("TraceID() deve devolver o trace do contexto")
	}
}

func TestInjectExtractIgnoreMissingOrInvalidContext(t *testing.T) {
	newRecorder(t)
	if got := Inject(context.Background()); got != "" {
		t.Fatalf("sem trace ativo não há traceparent: %q", got)
	}
	for _, bad := range []string{"", "   ", "lixo", "00-zz-zz-01"} {
		ctx := Extract(context.Background(), bad)
		if trace.SpanContextFromContext(ctx).IsValid() {
			t.Errorf("%q não deveria produzir um contexto válido", bad)
		}
	}
	if TraceID(context.Background()) != "" {
		t.Error("TraceID sem trace deve ser vazio")
	}
}

func TestFailMarksTheSpanAsError(t *testing.T) {
	rec := newRecorder(t)
	_, span := Start(context.Background(), "op")
	Fail(span, nil) // nil não marca nada
	Fail(span, context.DeadlineExceeded)
	span.End()
	s := rec.Ended()[0]
	if s.Status().Code != 1 /* Error */ || len(s.Events()) == 0 {
		t.Fatalf("status=%v eventos=%d", s.Status(), len(s.Events()))
	}
}
