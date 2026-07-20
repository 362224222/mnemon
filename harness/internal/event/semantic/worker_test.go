package semantic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestPeerInboxSemanticWorkerBoundsEachCycleAndCoalescesContinuation(t *testing.T) {
	backend := &semanticWorkerBackend{alwaysClaim: true,
		commitResult: peerInboxSemanticWorkerCommit{changed: true}}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{
		decision: peerInboxSemanticWorkerDecision{}})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.claimCalls != peerInboxSemanticWorkerBatch ||
		backend.probeCalls != peerInboxSemanticWorkerBatch ||
		backend.commitCalls != peerInboxSemanticWorkerBatch {
		t.Fatalf("bounded calls = claim %d probe %d commit %d", backend.claimCalls,
			backend.probeCalls, backend.commitCalls)
	}
	if len(worker.trigger) != 1 {
		t.Fatalf("continuation trigger depth = %d, want 1", len(worker.trigger))
	}
	snapshot := worker.Snapshot()
	if snapshot.Claims != peerInboxSemanticWorkerBatch ||
		snapshot.Committed != peerInboxSemanticWorkerBatch || snapshot.InFlight != 0 ||
		snapshot.MaximumActive != 1 {
		t.Fatalf("bounded cycle snapshot = %#v", snapshot)
	}
}

func TestPeerInboxSemanticWorkerPersistsClosedPolicyRetry(t *testing.T) {
	backend := &semanticWorkerBackend{remainingClaims: 1}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{decision: peerInboxSemanticWorkerDecision{retry: true, diagnostic: "missing_work"}})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.retryCalls != 1 ||
		backend.retryDiagnostic != store.PeerInboxSemanticRetryDependencyUnavailable ||
		backend.retryAfter != 4*time.Second || backend.probeCalls != 0 || backend.commitCalls != 0 {
		t.Fatalf("retry settlement = calls %d diagnostic %q after %s probe %d commit %d",
			backend.retryCalls, backend.retryDiagnostic, backend.retryAfter,
			backend.probeCalls, backend.commitCalls)
	}
	if snapshot := worker.Snapshot(); snapshot.Retries != 1 || snapshot.Stale != 0 {
		t.Fatalf("retry snapshot = %#v", snapshot)
	}
}

func TestPeerInboxSemanticWorkerRetriesUnavailableResponseAuthority(t *testing.T) {
	backend := &semanticWorkerBackend{remainingClaims: 1, prepareErr: store.ErrAudienceUnavailable}
	decision := peerInboxSemanticWorkerDecision{responses: []peerInboxSemanticWorkerIntent{{}},
		decisionAt: semanticWorkerTestTime()}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{decision: decision})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.prepareCalls != 1 || backend.retryCalls != 1 || backend.probeCalls != 0 ||
		backend.commitCalls != 0 {
		t.Fatalf("unavailable authority calls = prepare %d retry %d probe %d commit %d",
			backend.prepareCalls, backend.retryCalls, backend.probeCalls, backend.commitCalls)
	}
}

func TestPeerInboxSemanticWorkerTreatsFenceLossAsExpectedContention(t *testing.T) {
	backend := &semanticWorkerBackend{remainingClaims: 1,
		probeErr: store.ErrPeerInboxSemanticStale}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{
		decision: peerInboxSemanticWorkerDecision{}})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.probeCalls != 1 || backend.commitCalls != 0 {
		t.Fatalf("stale calls = probe %d commit %d", backend.probeCalls, backend.commitCalls)
	}
	if snapshot := worker.Snapshot(); snapshot.Stale != 1 || snapshot.InFlight != 0 {
		t.Fatalf("stale snapshot = %#v", snapshot)
	}
}

func TestPeerInboxSemanticWorkerDurablyRetriesLocalAdmissionRace(t *testing.T) {
	backend := &semanticWorkerBackend{remainingClaims: 1, commitErr: store.ErrAdmissionConflict}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{
		decision: peerInboxSemanticWorkerDecision{}})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.commitCalls != 1 || backend.retryCalls != 1 ||
		backend.retryDiagnostic != store.PeerInboxSemanticRetryBusy {
		t.Fatalf("admission race settlement = commit %d retry %d diagnostic %q",
			backend.commitCalls, backend.retryCalls, backend.retryDiagnostic)
	}
	if snapshot := worker.Snapshot(); snapshot.Retries != 1 || snapshot.Committed != 0 {
		t.Fatalf("admission race snapshot = %#v", snapshot)
	}
}

func TestPeerInboxSemanticWorkerRejectsUnknownRetryAndFailsClosed(t *testing.T) {
	backend := &semanticWorkerBackend{remainingClaims: 1, firstClaim: make(chan struct{})}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{decision: peerInboxSemanticWorkerDecision{retry: true, diagnostic: "future_retry"}})
	err := worker.Run(context.Background())
	if !errors.Is(err, ErrPeerInboxSemanticWorker) || backend.retryCalls != 0 {
		t.Fatalf("Run() = %v, retry calls %d", err, backend.retryCalls)
	}
	if snapshot := worker.Snapshot(); snapshot.State != PeerInboxSemanticWorkerFailed {
		t.Fatalf("failed snapshot = %#v", snapshot)
	}
}

func TestPeerInboxSemanticWorkerCancellationWaitsAndRunIsOneUse(t *testing.T) {
	backend := &semanticWorkerBackend{firstClaim: make(chan struct{})}
	worker := newSemanticWorkerForTest(t, backend, semanticWorkerPlanner{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-backend.firstClaim:
	case <-time.After(time.Second):
		t.Fatal("worker did not perform its immediate scan")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	if snapshot := worker.Snapshot(); snapshot.State != PeerInboxSemanticWorkerStopped ||
		snapshot.InFlight != 0 {
		t.Fatalf("stopped snapshot = %#v", snapshot)
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrPeerInboxSemanticWorkerRunning) {
		t.Fatalf("second Run() = %v", err)
	}
}

func TestPeerInboxSemanticWorkerConstructionAndBackoffAreBounded(t *testing.T) {
	if _, err := newPeerInboxSemanticWorker(nil, nil, nil, nil, nil, 0, ""); !errors.Is(err, ErrPeerInboxSemanticWorker) {
		t.Fatalf("incomplete constructor = %v", err)
	}
	if snapshot := (*PeerInboxSemanticWorker)(nil).Snapshot(); snapshot.State != PeerInboxSemanticWorkerFailed {
		t.Fatalf("nil snapshot = %#v", snapshot)
	}
	for _, test := range []struct {
		attempt uint32
		want    time.Duration
	}{{0, time.Second}, {1, time.Second}, {3, 4 * time.Second}, {32, 30 * time.Second}} {
		if got := peerInboxSemanticWorkerBackoff(test.attempt); got != test.want {
			t.Fatalf("backoff(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

type semanticWorkerClock struct{ at time.Time }

func (clock semanticWorkerClock) Now() time.Time { return clock.at }

type semanticWorkerTrigger struct {
	mu    sync.Mutex
	calls int
}

func (trigger *semanticWorkerTrigger) Trigger() {
	trigger.mu.Lock()
	trigger.calls++
	trigger.mu.Unlock()
}

type semanticWorkerSigner struct{}

func (semanticWorkerSigner) Sign(context.Context, []byte) ([]byte, error) {
	return make([]byte, 64), nil
}

type semanticWorkerPlanner struct {
	decision peerInboxSemanticWorkerDecision
	err      error
}

func (planner semanticWorkerPlanner) plan(_ peerInboxSemanticWorkerClaim,
	at time.Time,
) (peerInboxSemanticWorkerDecision, error) {
	decision := planner.decision
	if decision.decisionAt.IsZero() {
		decision.decisionAt = at
	}
	return decision, planner.err
}

type semanticWorkerBackend struct {
	remainingClaims int
	alwaysClaim     bool
	firstClaim      chan struct{}
	firstClaimOnce  sync.Once
	claimCalls      int
	probeCalls      int
	prepareCalls    int
	commitCalls     int
	retryCalls      int
	probeErr        error
	prepareErr      error
	commitErr       error
	retryErr        error
	commitResult    peerInboxSemanticWorkerCommit
	retryDiagnostic store.PeerInboxSemanticRetryDiagnostic
	retryAfter      time.Duration
}

func (backend *semanticWorkerBackend) claim(context.Context, string,
	time.Time,
) (peerInboxSemanticWorkerClaim, bool, error) {
	backend.claimCalls++
	backend.firstClaimOnce.Do(func() {
		if backend.firstClaim != nil {
			close(backend.firstClaim)
		}
	})
	if !backend.alwaysClaim && backend.remainingClaims == 0 {
		return peerInboxSemanticWorkerClaim{}, false, nil
	}
	if backend.remainingClaims > 0 {
		backend.remainingClaims--
	}
	return peerInboxSemanticWorkerClaim{attempt: 3}, true, nil
}

func (backend *semanticWorkerBackend) retry(_ context.Context, _ peerInboxSemanticWorkerClaim,
	diagnostic store.PeerInboxSemanticRetryDiagnostic, after time.Duration, _ time.Time,
) error {
	backend.retryCalls++
	backend.retryDiagnostic, backend.retryAfter = diagnostic, after
	return backend.retryErr
}

func (backend *semanticWorkerBackend) probe(context.Context, peerInboxSemanticWorkerClaim,
	time.Time,
) error {
	backend.probeCalls++
	return backend.probeErr
}

func (backend *semanticWorkerBackend) prepare(context.Context, peerInboxSemanticWorkerClaim,
	uint8,
) (peerInboxSemanticWorkerAdmission, error) {
	backend.prepareCalls++
	return peerInboxSemanticWorkerAdmission{}, backend.prepareErr
}

func (backend *semanticWorkerBackend) commit(context.Context, peerInboxSemanticWorkerClaim,
	peerInboxSemanticWorkerDecision, peerInboxSemanticWorkerAdmission,
	[]model.SignedPublication, time.Time,
) (peerInboxSemanticWorkerCommit, error) {
	backend.commitCalls++
	return backend.commitResult, backend.commitErr
}

func newSemanticWorkerForTest(t *testing.T, backend peerInboxSemanticWorkerBackend,
	planner peerInboxSemanticWorkerPlanner,
) *PeerInboxSemanticWorker {
	t.Helper()
	worker, err := newPeerInboxSemanticWorker(backend, planner, semanticWorkerSigner{},
		semanticWorkerClock{semanticWorkerTestTime()}, &semanticWorkerTrigger{},
		time.Millisecond, "semantic-worker-test")
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func semanticWorkerTestTime() time.Time {
	return time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
}
