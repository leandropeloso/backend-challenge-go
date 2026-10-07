package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
)

type ctxKey struct{}

// NewLogger cria um logger JSON em stdout.
func NewLogger() *slog.Logger { return NewLoggerTo(os.Stdout) }

func NewLoggerTo(w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// WithLogger guarda no contexto um logger já enriquecido com os identificadores da operação.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext devolve o logger do contexto, ou o padrão.
func FromContext(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return fallback
}
