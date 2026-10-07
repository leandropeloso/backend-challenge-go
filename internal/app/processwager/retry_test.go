package processwager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

type countingMetrics struct{ conflicts int }

func (m *countingMetrics) TransactionResult(wager.Kind, wager.Status, wager.FailureCode) {}
func (m *countingMetrics) Duplicate(string)                                              {}
func (m *countingMetrics) Retry(string)                                                  {}
func (m *countingMetrics) DLQ(string)                                                    {}
func (m *countingMetrics) ConcurrencyConflict()                                          { m.conflicts++ }
func (m *countingMetrics) ReconciliationDivergence()                                     {}
func (m *countingMetrics) ProcessingDuration(string, time.Duration)                      {}

func TestRetryingRepeatsOnlyRetryableConflicts(t *testing.T) {
	m := &countingMetrics{}
	s := &Service{metrics: m, cfg: Config{MaxTxRetries: 4}}

	calls := 0
	err := s.retrying(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("deadlock: %w", port.ErrRetryable)
		}
		return nil
	})
	if err != nil || calls != 3 || m.conflicts != 2 {
		t.Fatalf("err=%v chamadas=%d conflitos=%d (quer nil/3/2)", err, calls, m.conflicts)
	}

	// erros de negócio ou transitórios de infraestrutura não são repetidos aqui
	for _, boom := range []error{ErrIdempotencyConflict, port.ErrTransient, errors.New("x")} {
		calls = 0
		got := s.retrying(context.Background(), func(context.Context) error { calls++; return boom })
		if !errors.Is(got, boom) || calls != 1 {
			t.Fatalf("%v: chamadas=%d err=%v", boom, calls, got)
		}
	}
}

func TestRetryingGivesUpAfterTheLimit(t *testing.T) {
	m := &countingMetrics{}
	s := &Service{metrics: m, cfg: Config{MaxTxRetries: 3}}
	calls := 0
	err := s.retrying(context.Background(), func(context.Context) error {
		calls++
		return port.ErrRetryable
	})
	if !errors.Is(err, port.ErrRetryable) || calls != 3 {
		t.Fatalf("err=%v chamadas=%d", err, calls)
	}
}

func TestRetryingStopsWhenTheContextIsCancelled(t *testing.T) {
	s := &Service{metrics: &countingMetrics{}, cfg: Config{MaxTxRetries: 10}}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := s.retrying(ctx, func(context.Context) error {
		calls++
		cancel()
		return port.ErrRetryable
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v chamadas=%d", err, calls)
	}
}
