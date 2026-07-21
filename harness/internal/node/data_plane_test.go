package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestDaemonDataPlaneRejectsIncompleteOrUnboundedWorkers(t *testing.T) {
	t.Parallel()
	worker := newDataPlaneTestWorker(nil)
	for _, specs := range [][]daemonDataPlaneWorkerSpec{
		nil,
		{{name: "", worker: worker, maxConcurrent: 1}},
		{{name: "worker", maxConcurrent: 1}},
		{{name: "worker", worker: worker}},
		{{name: "worker", worker: worker, maxConcurrent: 1},
			{name: "worker", worker: worker, maxConcurrent: 1}},
	} {
		if plane, err := newDaemonDataPlane(specs); err == nil || plane != nil {
			t.Fatalf("newDaemonDataPlane(%#v) = (%#v, %v)", specs, plane, err)
		}
	}
}

func TestDaemonDataPlaneOwnsCancellationReadinessAndWait(t *testing.T) {
	t.Parallel()
	first := newDataPlaneTestWorker(nil)
	second := newDataPlaneTestWorker(nil)
	plane, err := newDaemonDataPlane([]daemonDataPlaneWorkerSpec{
		{name: "first", worker: first, readiness: first, maxConcurrent: 2},
		{name: "second", worker: second, readiness: second, maxConcurrent: 3},
	})
	if err != nil || plane.MaxConcurrent() != 5 {
		t.Fatalf("newDaemonDataPlane() = (%#v, %v)", plane, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- plane.Run(ctx) }()
	if err := plane.Readiness(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() cancellation = %v", err)
	}
	if !first.stopped() || !second.stopped() {
		t.Fatal("Run() returned before every owned worker stopped")
	}
	if err := plane.Readiness(context.Background()); err == nil {
		t.Fatal("Readiness() accepted a stopped data plane")
	}
}

func TestDaemonDataPlaneCancelsSiblingsOnFirstTerminalExit(t *testing.T) {
	t.Parallel()
	want := errors.New("durable worker failure")
	failed := newDataPlaneTestWorker(want)
	sibling := newDataPlaneTestWorker(nil)
	plane, err := newDaemonDataPlane([]daemonDataPlaneWorkerSpec{
		{name: "failed", worker: failed, maxConcurrent: 1},
		{name: "sibling", worker: sibling, maxConcurrent: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = plane.Run(context.Background())
	if !errors.Is(err, want) || !strings.Contains(err.Error(), `worker "failed"`) {
		t.Fatalf("Run() error = %v", err)
	}
	if !sibling.stopped() {
		t.Fatal("terminal worker did not cancel and wait for its sibling")
	}
}

type dataPlaneTestWorker struct {
	failure error
	ready   chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newDataPlaneTestWorker(failure error) *dataPlaneTestWorker {
	return &dataPlaneTestWorker{failure: failure, ready: make(chan struct{}), done: make(chan struct{})}
}

func (worker *dataPlaneTestWorker) Run(ctx context.Context) error {
	worker.once.Do(func() { close(worker.ready) })
	if worker.failure != nil {
		close(worker.done)
		return worker.failure
	}
	<-ctx.Done()
	close(worker.done)
	return nil
}

func (worker *dataPlaneTestWorker) Readiness(ctx context.Context) error {
	select {
	case <-worker.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (worker *dataPlaneTestWorker) stopped() bool {
	select {
	case <-worker.done:
		return true
	default:
		return false
	}
}
