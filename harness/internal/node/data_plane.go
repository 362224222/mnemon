package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const daemonDataPlaneWorkerLimit = 8

type daemonDataPlaneWorker interface {
	Run(context.Context) error
}

type daemonDataPlaneReadiness interface {
	Readiness(context.Context) error
}

type daemonDataPlaneWorkerSpec struct {
	name          string
	worker        daemonDataPlaneWorker
	readiness     daemonDataPlaneReadiness
	maxConcurrent uint32
}

type daemonDataPlaneResult struct {
	name string
	err  error
}

// daemonDataPlane owns the fixed set of durable network workers beneath one
// mnemond. Each worker owns its domain retries; this owner only provides one
// cancellation tree, a bounded goroutine set, readiness, and a complete wait
// path on the first terminal worker exit.
type daemonDataPlane struct {
	workers       []daemonDataPlaneWorkerSpec
	maxConcurrent uint32

	mu      sync.Mutex
	started bool
	running bool
	runErr  error
	ready   chan struct{}
	done    chan struct{}
}

func newDaemonDataPlane(workers []daemonDataPlaneWorkerSpec) (*daemonDataPlane, error) {
	if len(workers) == 0 || len(workers) > daemonDataPlaneWorkerLimit {
		return nil, errors.New("mnemond data plane requires a bounded worker set")
	}
	seen := make(map[string]struct{}, len(workers))
	owned := make([]daemonDataPlaneWorkerSpec, len(workers))
	var maximum uint32
	for index, candidate := range workers {
		if candidate.name == "" || candidate.worker == nil || candidate.maxConcurrent == 0 {
			return nil, errors.New("mnemond data plane worker authority is incomplete")
		}
		if _, duplicate := seen[candidate.name]; duplicate {
			return nil, fmt.Errorf("mnemond data plane worker %q is duplicated", candidate.name)
		}
		seen[candidate.name] = struct{}{}
		if ^uint32(0)-maximum < candidate.maxConcurrent {
			return nil, errors.New("mnemond data plane concurrency budget overflows")
		}
		maximum += candidate.maxConcurrent
		owned[index] = candidate
	}
	return &daemonDataPlane{workers: owned, maxConcurrent: maximum,
		ready: make(chan struct{}), done: make(chan struct{})}, nil
}

func (plane *daemonDataPlane) MaxConcurrent() uint32 {
	if plane == nil {
		return 0
	}
	return plane.maxConcurrent
}

func (plane *daemonDataPlane) Run(ctx context.Context) error {
	if plane == nil || ctx == nil || ctx.Err() != nil {
		return errors.New("mnemond data plane requires a live context")
	}
	plane.mu.Lock()
	if plane.started {
		plane.mu.Unlock()
		return errors.New("mnemond data plane can run only once")
	}
	plane.started, plane.running = true, true
	workers := append([]daemonDataPlaneWorkerSpec(nil), plane.workers...)
	plane.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	results := make(chan daemonDataPlaneResult, len(workers))
	for _, spec := range workers {
		go runDaemonDataPlaneWorker(runCtx, spec, results)
	}
	close(plane.ready)

	var terminal daemonDataPlaneResult
	receivedTerminal := false
	select {
	case terminal = <-results:
		receivedTerminal = true
	case <-ctx.Done():
	}
	cancel()
	remaining := len(workers)
	if receivedTerminal {
		remaining--
	}
	var joined error
	for ; remaining > 0; remaining-- {
		result := <-results
		if receivedTerminal && result.err != nil {
			joined = errors.Join(joined,
				fmt.Errorf("mnemond data plane worker %q shutdown: %w", result.name, result.err))
		}
	}
	// Parent cancellation is the sole graceful stop authority. It may race a
	// worker returning from that same cancellation, so decide only after every
	// owned worker has joined rather than from the select winner.
	if ctx.Err() == nil && receivedTerminal {
		if terminal.err == nil {
			terminal.err = errors.New("worker stopped before cancellation")
		}
		joined = errors.Join(fmt.Errorf("mnemond data plane worker %q: %w",
			terminal.name, terminal.err), joined)
	} else if ctx.Err() != nil {
		joined = nil
	}
	plane.finish(joined)
	return joined
}

func runDaemonDataPlaneWorker(ctx context.Context, spec daemonDataPlaneWorkerSpec,
	results chan<- daemonDataPlaneResult,
) {
	results <- daemonDataPlaneResult{name: spec.name, err: spec.worker.Run(ctx)}
}

func (plane *daemonDataPlane) Readiness(ctx context.Context) error {
	if plane == nil || ctx == nil {
		return errors.New("mnemond data plane readiness is unavailable")
	}
	select {
	case <-plane.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, spec := range plane.workers {
		if spec.readiness != nil {
			if err := spec.readiness.Readiness(ctx); err != nil {
				return fmt.Errorf("mnemond data plane worker %q readiness: %w", spec.name, err)
			}
		}
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	if !plane.running {
		return errors.Join(errors.New("mnemond data plane is not running"), plane.runErr)
	}
	return nil
}

func (plane *daemonDataPlane) finish(runErr error) {
	plane.mu.Lock()
	plane.running = false
	plane.runErr = runErr
	close(plane.done)
	plane.mu.Unlock()
}
