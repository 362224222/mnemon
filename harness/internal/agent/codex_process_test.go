package agent

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCodexProcessCleanupJoinsOwnedPipeReaders(t *testing.T) {
	fixture := newCodexAdapterFixture(t, fakeCodexScenario{stuckPipeDrain: true})
	finished := make(chan struct {
		result CodexWakeResult
		err    error
	}, 1)
	go func() {
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		finished <- struct {
			result CodexWakeResult
			err    error
		}{result: result, err: err}
	}()
	var stuck *stuckCodexReadCloser
	select {
	case stuck = <-fixture.starter.stuckReady:
	case <-time.After(time.Second):
		t.Fatal("stuck stdout was not installed")
	}
	t.Cleanup(stuck.releaseReader)
	select {
	case <-stuck.closed:
	case <-time.After(time.Second):
		t.Fatal("owned pipe reader was not closed")
	}
	select {
	case outcome := <-finished:
		t.Fatalf("Run() returned before its pipe reader exited: (%#v, %v)",
			outcome.result, outcome.err)
	case <-time.After(10 * time.Millisecond):
	}
	stuck.releaseReader()
	var outcome struct {
		result CodexWakeResult
		err    error
	}
	select {
	case outcome = <-finished:
	case <-time.After(time.Second):
		t.Fatal("Run() did not join the released pipe reader")
	}
	select {
	case <-stuck.returned:
	case <-time.After(time.Second):
		t.Fatal("owned test reader did not return after release")
	}
	assertCodexCleanupOutcome(t, fixture, outcome.result, outcome.err)
}

func assertCodexCleanupOutcome(t *testing.T, fixture *codexAdapterFixture,
	result CodexWakeResult, err error,
) {
	t.Helper()
	if !errors.Is(err, ErrCodexWakeAdapter) || !result.WakeDelivered ||
		!result.ProcessExited || result.At.IsZero() ||
		!strings.Contains(result.CompletionReceipt.String(), `"status":"cleanup_failed"`) ||
		fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err,
			fixture.starter.process.waitCount.Load())
	}
}

type stuckCodexReadCloser struct {
	delegate    io.ReadCloser
	release     chan struct{}
	returned    chan struct{}
	closed      chan struct{}
	releaseOnce sync.Once
	closeOnce   sync.Once
}

func newStuckCodexReadCloser(delegate io.ReadCloser) *stuckCodexReadCloser {
	return &stuckCodexReadCloser{delegate: delegate, release: make(chan struct{}),
		returned: make(chan struct{}), closed: make(chan struct{})}
}

func (reader *stuckCodexReadCloser) Read(value []byte) (int, error) {
	read, err := reader.delegate.Read(value)
	if err == nil {
		return read, nil
	}
	<-reader.release
	close(reader.returned)
	return read, err
}

// Close deliberately cannot wake Read. This test double proves the adapter
// retains ownership when an injected process violates the real pipe contract.
func (reader *stuckCodexReadCloser) Close() error {
	reader.closeOnce.Do(func() { close(reader.closed) })
	return nil
}

func (reader *stuckCodexReadCloser) releaseReader() {
	reader.releaseOnce.Do(func() {
		close(reader.release)
		_ = reader.delegate.Close()
	})
}
