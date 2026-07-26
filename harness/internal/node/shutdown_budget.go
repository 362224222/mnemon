package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultGracefulShutdownBudget = 30 * time.Second

// ErrGracefulShutdownDeadline reports that mnemond returned lifecycle
// ownership to its executable before every owned component settled.
var ErrGracefulShutdownDeadline = errors.New("mnemond graceful shutdown deadline exhausted")

// gracefulShutdown owns the one process shutdown deadline. The timer starts
// lazily at the first shutdown signal so normal serving time never consumes
// the drain budget. Every component receives the same context and therefore
// observes only the remaining process-wide time.
type gracefulShutdown struct {
	budget time.Duration
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

func newGracefulShutdown(budget time.Duration) *gracefulShutdown {
	if budget <= 0 {
		budget = defaultGracefulShutdownBudget
	}
	return &gracefulShutdown{budget: budget}
}

func (shutdown *gracefulShutdown) Context() context.Context {
	if shutdown == nil {
		return nil
	}
	shutdown.once.Do(func() {
		shutdown.ctx, shutdown.cancel = context.WithTimeout(context.Background(), shutdown.budget)
	})
	return shutdown.ctx
}

func (shutdown *gracefulShutdown) finish() {
	if shutdown == nil {
		return
	}
	shutdown.Context()
	shutdown.cancel()
}

func gracefulShutdownDeadlineError(ctx context.Context, operation string) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", ErrGracefulShutdownDeadline, operation, ctx.Err())
}

func waitForGracefulShutdown(ctx context.Context, done <-chan struct{}, operation string) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return gracefulShutdownDeadlineError(ctx, operation)
	}
}
