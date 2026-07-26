package node

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGracefulShutdownStartsLazilyAndSharesOneDeadline(t *testing.T) {
	t.Parallel()
	shutdown := newGracefulShutdown(time.Second)
	if shutdown.ctx != nil {
		t.Fatal("shutdown deadline started before the first shutdown owner requested it")
	}
	first := shutdown.Context()
	second := shutdown.Context()
	if first == nil || first != second {
		t.Fatal("shutdown owners did not receive one shared context")
	}
	firstDeadline, firstOK := first.Deadline()
	secondDeadline, secondOK := second.Deadline()
	if !firstOK || !secondOK || !firstDeadline.Equal(secondDeadline) {
		t.Fatalf("shutdown deadlines = (%v, %t), (%v, %t), want one exact deadline",
			firstDeadline, firstOK, secondDeadline, secondOK)
	}
	shutdown.finish()
	if !errors.Is(first.Err(), context.Canceled) {
		t.Fatalf("finished shutdown context error = %v, want cancellation", first.Err())
	}
}

func TestWaitForGracefulShutdownUsesRemainingSharedDeadline(t *testing.T) {
	t.Parallel()
	clean := make(chan struct{})
	close(clean)
	shutdown := newGracefulShutdown(time.Second)
	if err := waitForGracefulShutdown(shutdown.Context(), clean, "clean drain"); err != nil {
		t.Fatalf("clean drain error = %v", err)
	}
	shutdown.finish()

	exhausted := newGracefulShutdown(time.Nanosecond)
	ctx := exhausted.Context()
	<-ctx.Done()
	err := waitForGracefulShutdown(ctx, make(chan struct{}), "blocked drain")
	if !errors.Is(err, ErrGracefulShutdownDeadline) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked drain error = %v, want shared deadline diagnostic", err)
	}
	exhausted.finish()
}
