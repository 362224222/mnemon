package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	errBoundedRunnerBusy  = errors.New("bounded worker runner is already active")
	errBoundedRunnerLimit = errors.New("bounded worker runner limit exceeded")
)

// boundedWorkBudget freezes both total work retained by one batch and the
// maximum callbacks executing at once. The runner owns no durable retry or
// queue; domain workers claim a bounded batch and retain retry authority.
type boundedWorkBudget struct {
	MaxConcurrent uint32
	MaxItems      uint32
}

// boundedWorkerRunner is a typed mechanical primitive for one finite batch.
// The handler must observe ctx, must not retain item, and may perform blocking
// work only without a caller-held lock or database transaction.
type boundedWorkerRunner[T any] struct {
	budget  boundedWorkBudget
	handle  func(context.Context, T) error
	running atomic.Bool
}

func newBoundedWorkerRunner[T any](budget boundedWorkBudget,
	handle func(context.Context, T) error,
) (*boundedWorkerRunner[T], error) {
	if budget.MaxConcurrent == 0 || budget.MaxItems == 0 ||
		budget.MaxConcurrent > budget.MaxItems || handle == nil {
		return nil, errBoundedRunnerLimit
	}
	return &boundedWorkerRunner[T]{budget: budget, handle: handle}, nil
}

// Run copies and processes one finite batch. The first callback error cancels
// sibling callbacks, and Run always waits for every owned goroutine before it
// returns. Concurrent Run calls are rejected rather than multiplying the
// declared resource budget.
func (runner *boundedWorkerRunner[T]) Run(ctx context.Context, items []T) error {
	if runner == nil || runner.handle == nil || ctx == nil {
		return errBoundedRunnerLimit
	}
	if uint64(len(items)) > uint64(runner.budget.MaxItems) {
		return errBoundedRunnerLimit
	}
	if !runner.running.CompareAndSwap(false, true) {
		return errBoundedRunnerBusy
	}
	defer runner.running.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	work := append([]T(nil), items...)
	workerCount := int(runner.budget.MaxConcurrent)
	if workerCount > len(work) {
		workerCount = len(work)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Uint64
	var wait sync.WaitGroup
	var firstOnce sync.Once
	var firstErr error
	for range workerCount {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for workerCtx.Err() == nil {
				index := next.Add(1) - 1
				if index >= uint64(len(work)) {
					return
				}
				if err := runner.handle(workerCtx, work[int(index)]); err != nil {
					firstOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
