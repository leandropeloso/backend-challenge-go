// Package telemetry configura o rastreamento distribuído com OpenTelemetry.
//
// Sem OTEL_EXPORTER_OTLP_ENDPOINT o tracer é o no-op do SDK (custo desprezível);
// o contexto W3C (traceparent) continua sendo propagado para que logs, eventos da
// outbox e mensagens SQS carreguem o mesmo trace entre processos.
package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/leandropeloso/wager-service"

var propagator = propagation.TraceContext{}

type Config struct {
	Endpoint    string // URL base OTLP/HTTP, ex.: http://jaeger:4318 (vazio = desligado)
	ServiceName string
}

// Provider encapsula o TracerProvider para que o Fx o encerre (flush) ao parar.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// Setup instala o tracer global. Com Endpoint vazio, nada é exportado.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	otel.SetTextMapPropagator(propagator)
	if cfg.Endpoint == "" {
		return &Provider{}, nil
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid OTEL_EXPORTER_OTLP_ENDPOINT %q", cfg.Endpoint)
	}
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
	if u.Scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	name := cfg.ServiceName
	if name == "" {
		name = "wager-service"
	}
	host, _ := os.Hostname()
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", name),
		attribute.String("service.instance.id", host+"-"+fmt.Sprint(os.Getpid())),
	))
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(500*time.Millisecond)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return &Provider{tp: tp}, nil
}

// Shutdown envia os spans pendentes e fecha o exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

// Tracer devolve o tracer da aplicação.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// Start abre um span filho do contexto atual.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

// Fail marca o span como erro, quando err não é nulo.
func Fail(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// Inject devolve o traceparent W3C do contexto ("" se não houver trace ativo).
func Inject(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract restaura, a partir de um traceparent, o contexto remoto; valores
// vazios ou inválidos devolvem ctx sem alteração.
func Extract(ctx context.Context, traceparent string) context.Context {
	if strings.TrimSpace(traceparent) == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// TraceID devolve o id do trace do contexto, para correlacionar logs.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
