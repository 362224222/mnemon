package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestWorkDeadlineWorkerScansImmediatelyAndIsolatesStaleCandidates(t *testing.T) {
	t.Parallel()
	base := deadlineWorkerTime(t, "2026-07-19T02:00:00Z")
	first := deadlineWorkerCandidate(t, "immediate-a", base)
	second := deadlineWorkerCandidate(t, "immediate-b", base)
	backend := newDeadlineWorkerBackend(workDeadlineScan{due: []workDeadlineCandidate{first, second},
		exhaustedCount: 3})
	backend.expireErrors = []error{nil, store.ErrWorkDeadlineStale}
	clock := &deadlineWorkerClock{now: base}
	publisher := newDeadlineWorkerPublisher()
	timers := newDeadlineWorkerTimerFactory()
	worker := deadlineWorkerForTest(t, backend, clock, publisher, timers)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadlineWorkerAwait(t, backend.expired, 2)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if err := worker.Readiness(readyCtx); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.Snapshot()
	if snapshot.Cycles != 1 || snapshot.Prepared != 2 || snapshot.Committed != 1 ||
		snapshot.Stale != 1 || snapshot.ExhaustedCount != 3 || snapshot.MaximumActive != 1 ||
		snapshot.InFlight != 0 || publisher.count.Load() != 1 {
		t.Fatalf("worker snapshot = %#v, publisher=%d", snapshot, publisher.count.Load())
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWorkDeadlineWorkerRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := deadlineWorkerResult(t, done); err != nil {
		t.Fatal(err)
	}
	if worker.Snapshot().State != WorkDeadlineWorkerStopped {
		t.Fatalf("worker state = %s", worker.Snapshot().State)
	}
}

func TestWorkDeadlineWorkerUsesEarliestTimerAndRecoversDueWork(t *testing.T) {
	t.Parallel()
	base := deadlineWorkerTime(t, "2026-07-19T03:00:00Z")
	dueAt := base.Add(500 * time.Millisecond)
	candidate := deadlineWorkerCandidate(t, "timer", dueAt)
	backend := newDeadlineWorkerBackend(
		workDeadlineScan{nextDeadlineNanos: dueAt.UnixNano()},
		workDeadlineScan{due: []workDeadlineCandidate{candidate}},
	)
	clock := &deadlineWorkerClock{now: base}
	publisher := newDeadlineWorkerPublisher()
	timers := newDeadlineWorkerTimerFactory()
	worker := deadlineWorkerForTest(t, backend, clock, publisher, timers)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	timer := deadlineWorkerTimerResult(t, timers.created)
	if timer.delay != 500*time.Millisecond {
		t.Fatalf("earliest timer delay = %s", timer.delay)
	}
	clock.set(dueAt)
	timer.fire <- dueAt
	deadlineWorkerAwait(t, publisher.triggered, 1)
	if publisher.count.Load() != 1 || worker.Snapshot().Cycles < 2 {
		t.Fatalf("deadline recovery snapshot = %#v, publisher=%d",
			worker.Snapshot(), publisher.count.Load())
	}
	cancel()
	if err := deadlineWorkerResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestWorkDeadlineWorkerSelfTriggersBoundedBacklog(t *testing.T) {
	t.Parallel()
	base := deadlineWorkerTime(t, "2026-07-19T04:00:00Z")
	backend := newDeadlineWorkerBackend(
		workDeadlineScan{due: []workDeadlineCandidate{deadlineWorkerCandidate(t, "backlog-a", base)}, moreDue: true},
		workDeadlineScan{due: []workDeadlineCandidate{deadlineWorkerCandidate(t, "backlog-b", base)}},
	)
	clock := &deadlineWorkerClock{now: base}
	publisher := newDeadlineWorkerPublisher()
	timers := newDeadlineWorkerTimerFactory()
	worker := deadlineWorkerForTest(t, backend, clock, publisher, timers)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadlineWorkerAwait(t, publisher.triggered, 2)
	if publisher.count.Load() != 2 || worker.Snapshot().MaximumActive != 1 {
		t.Fatalf("backlog snapshot = %#v, publisher=%d", worker.Snapshot(), publisher.count.Load())
	}
	cancel()
	if err := deadlineWorkerResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestWorkDeadlineWorkerTriggerCoalesces(t *testing.T) {
	t.Parallel()
	base := deadlineWorkerTime(t, "2026-07-19T04:30:00Z")
	worker := deadlineWorkerForTest(t, newDeadlineWorkerBackend(),
		&deadlineWorkerClock{now: base}, newDeadlineWorkerPublisher(),
		newDeadlineWorkerTimerFactory())
	for index := 0; index < 32; index++ {
		worker.Trigger()
	}
	if got := len(worker.trigger); got != 1 {
		t.Fatalf("coalesced trigger count = %d", got)
	}
}

func TestWorkDeadlineWorkerFailsOnDurableInvariant(t *testing.T) {
	t.Parallel()
	base := deadlineWorkerTime(t, "2026-07-19T05:00:00Z")
	backend := newDeadlineWorkerBackend(workDeadlineScan{
		due: []workDeadlineCandidate{deadlineWorkerCandidate(t, "fatal", base)}})
	backend.expireErrors = []error{store.ErrWorkDeadlineInvariant}
	worker := deadlineWorkerForTest(t, backend, &deadlineWorkerClock{now: base},
		newDeadlineWorkerPublisher(), newDeadlineWorkerTimerFactory())
	err := worker.Run(context.Background())
	if !errors.Is(err, store.ErrWorkDeadlineInvariant) ||
		worker.Snapshot().State != WorkDeadlineWorkerFailed {
		t.Fatalf("fatal worker = (%v, %#v)", err, worker.Snapshot())
	}
}

func TestWorkDeadlineDelayBoundsClockReconciliation(t *testing.T) {
	t.Parallel()
	now := deadlineWorkerTime(t, "2026-07-19T06:00:00Z")
	if got := workDeadlineDelay(now, 0, time.Second); got != time.Second {
		t.Fatalf("no-deadline delay = %s", got)
	}
	if got := workDeadlineDelay(now, now.Add(2*time.Second).UnixNano(), time.Second); got != time.Second {
		t.Fatalf("bounded reconciliation delay = %s", got)
	}
	if got := workDeadlineDelay(now, now.Add(250*time.Millisecond).UnixNano(), time.Second); got != 250*time.Millisecond {
		t.Fatalf("earliest deadline delay = %s", got)
	}
	if got := workDeadlineDelay(now, now.UnixNano(), time.Second); got != 0 {
		t.Fatalf("due delay = %s", got)
	}
}

type deadlineWorkerBackend struct {
	mu           sync.Mutex
	scans        []workDeadlineScan
	scanIndex    int
	expireErrors []error
	expireIndex  int
	expired      chan int
}

func newDeadlineWorkerBackend(scans ...workDeadlineScan) *deadlineWorkerBackend {
	return &deadlineWorkerBackend{scans: scans, expired: make(chan int, 16)}
}

func (backend *deadlineWorkerBackend) scan(context.Context, time.Time) (workDeadlineScan, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.scanIndex >= len(backend.scans) {
		return workDeadlineScan{}, nil
	}
	result := backend.scans[backend.scanIndex]
	backend.scanIndex++
	return result, nil
}

func (backend *deadlineWorkerBackend) expire(context.Context, workDeadlineCandidate,
	WorkDeadlineClock,
) (bool, error) {
	backend.mu.Lock()
	index := backend.expireIndex
	backend.expireIndex++
	var err error
	if index < len(backend.expireErrors) {
		err = backend.expireErrors[index]
	}
	backend.mu.Unlock()
	backend.expired <- index + 1
	return err == nil, err
}

type deadlineWorkerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *deadlineWorkerClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *deadlineWorkerClock) set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

type deadlineWorkerPublisher struct {
	count     atomic.Uint64
	triggered chan int
}

func newDeadlineWorkerPublisher() *deadlineWorkerPublisher {
	return &deadlineWorkerPublisher{triggered: make(chan int, 16)}
}

func (publisher *deadlineWorkerPublisher) Trigger() {
	value := publisher.count.Add(1)
	publisher.triggered <- int(value)
}

type deadlineWorkerTimerFactory struct{ created chan *deadlineWorkerTimer }

func newDeadlineWorkerTimerFactory() *deadlineWorkerTimerFactory {
	return &deadlineWorkerTimerFactory{created: make(chan *deadlineWorkerTimer, 16)}
}

func (factory *deadlineWorkerTimerFactory) newTimer(delay time.Duration) workDeadlineTimer {
	timer := &deadlineWorkerTimer{delay: delay, fire: make(chan time.Time, 1)}
	factory.created <- timer
	return timer
}

type deadlineWorkerTimer struct {
	delay   time.Duration
	fire    chan time.Time
	stopped atomic.Bool
}

func (timer *deadlineWorkerTimer) channel() <-chan time.Time { return timer.fire }
func (timer *deadlineWorkerTimer) stop()                     { timer.stopped.Store(true) }

func deadlineWorkerForTest(t *testing.T, backend workDeadlineBackend,
	clock WorkDeadlineClock, publisher WorkDeadlinePublicationTrigger,
	timers workDeadlineTimerFactory,
) *WorkDeadlineWorker {
	t.Helper()
	worker, err := newWorkDeadlineWorker(backend, clock, publisher, timers,
		workDeadlineReconcilePeriod)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func deadlineWorkerCandidate(t *testing.T, suffix string, deadline time.Time) workDeadlineCandidate {
	t.Helper()
	home, _ := model.ParsePeerID("peer-deadline-home-" + suffix)
	reviewer, _ := model.ParsePeerID("peer-deadline-reviewer-" + suffix)
	channel, _ := model.ParseChannelID("channel-deadline-" + suffix)
	workID, _ := model.ParseWorkID("work-deadline-" + suffix)
	ref, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatal(err)
	}
	participants, err := model.NewParticipantSnapshot(channel, 2, home, reviewer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	updatedBy, _ := model.ParseEventID("event-deadline-offered-" + suffix)
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel,
		Participants: participants, Version: 1, Iteration: 1, DeadlineUnixNano: deadline.UnixNano(),
		State: model.WorkOffered, StateData: state, UpdatedBy: updatedBy,
		UpdatedAt: deadline.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return workDeadlineCandidate{work: work}
}

func deadlineWorkerTime(t *testing.T, raw string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func deadlineWorkerAwait(t *testing.T, values <-chan int, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for count := 0; count < want; count++ {
		select {
		case <-values:
		case <-ctx.Done():
			t.Fatalf("timed out after %d of %d values", count, want)
		}
	}
}

func deadlineWorkerTimerResult(t *testing.T,
	values <-chan *deadlineWorkerTimer,
) *deadlineWorkerTimer {
	t.Helper()
	select {
	case timer := <-values:
		return timer
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deadline timer")
		return nil
	}
}

func deadlineWorkerResult(t *testing.T, values <-chan error) error {
	t.Helper()
	select {
	case err := <-values:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deadline worker")
		return nil
	}
}
