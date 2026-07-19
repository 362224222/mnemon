package node

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedWorkerRunnerEnforcesBatchAndConcurrencyLimits(t *testing.T) {
	t.Parallel()
	entered := make(chan int, 8)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	runner, err := newBoundedWorkerRunner(boundedWorkBudget{MaxConcurrent: 3, MaxItems: 8},
		func(ctx context.Context, item int) error {
			current := active.Add(1)
			defer active.Add(-1)
			for prior := maximum.Load(); current > prior && !maximum.CompareAndSwap(prior, current); {
				prior = maximum.Load()
			}
			entered <- item
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background(), []int{0, 1, 2, 3, 4, 5, 6, 7}) }()
	for range 3 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("runner did not fill its declared concurrency")
		}
	}
	select {
	case item := <-entered:
		t.Fatalf("item %d entered above the concurrency limit", item)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not wait for its bounded batch")
	}
	if got := maximum.Load(); got != 3 {
		t.Fatalf("maximum concurrency = %d, want 3", got)
	}
	if err := runner.Run(context.Background(), make([]int, 9)); !errors.Is(err, errBoundedRunnerLimit) {
		t.Fatalf("oversized batch error = %v", err)
	}
}

func TestBoundedWorkerRunnerCancelsSiblingsAndWaitsOnFirstError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("work failed")
	started := make(chan int, 3)
	fail := make(chan struct{})
	settled := make(chan int, 2)
	runner, err := newBoundedWorkerRunner(boundedWorkBudget{MaxConcurrent: 3, MaxItems: 3},
		func(ctx context.Context, item int) error {
			started <- item
			if item == 0 {
				<-fail
				return wantErr
			}
			<-ctx.Done()
			settled <- item
			return ctx.Err()
		})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background(), []int{0, 1, 2}) }()
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("runner did not start the bounded batch")
		}
	}
	close(fail)
	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not settle after first error")
	}
	if len(settled) != 2 {
		t.Fatalf("settled siblings = %d, want 2", len(settled))
	}
}

func TestBoundedWorkerRunnerRejectsConcurrentRunAndInvalidAuthority(t *testing.T) {
	t.Parallel()
	if _, err := newBoundedWorkerRunner[int](boundedWorkBudget{}, func(context.Context, int) error {
		return nil
	}); !errors.Is(err, errBoundedRunnerLimit) {
		t.Fatalf("zero budget error = %v", err)
	}
	if _, err := newBoundedWorkerRunner[int](boundedWorkBudget{MaxConcurrent: 2, MaxItems: 1},
		nil); !errors.Is(err, errBoundedRunnerLimit) {
		t.Fatalf("invalid callback authority error = %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	runner, err := newBoundedWorkerRunner(boundedWorkBudget{MaxConcurrent: 1, MaxItems: 1},
		func(context.Context, int) error {
			close(entered)
			<-release
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background(), []int{1}) }()
	<-entered
	if err := runner.Run(context.Background(), []int{2}); !errors.Is(err, errBoundedRunnerBusy) {
		t.Fatalf("concurrent Run() error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runner.Run(cancelled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run() error = %v", err)
	}
}
