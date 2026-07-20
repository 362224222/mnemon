package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const workDeadlineReconcilePeriod = time.Second

var (
	ErrWorkDeadlineWorker        = errors.New("Work deadline worker")
	ErrWorkDeadlineWorkerRunning = fmt.Errorf("%w: worker has already run", ErrWorkDeadlineWorker)
)

type WorkDeadlineClock interface{ Now() time.Time }
type WorkDeadlinePublicationTrigger interface{ Trigger() }

type WorkDeadlineWorkerState string

const (
	WorkDeadlineWorkerIdle    WorkDeadlineWorkerState = "idle"
	WorkDeadlineWorkerRunning WorkDeadlineWorkerState = "running"
	WorkDeadlineWorkerStopped WorkDeadlineWorkerState = "stopped"
	WorkDeadlineWorkerFailed  WorkDeadlineWorkerState = "failed"
)

type WorkDeadlineWorkerSnapshot struct {
	State             WorkDeadlineWorkerState
	Cycles            uint64
	Prepared          uint64
	Committed         uint64
	Stale             uint64
	ExhaustedCount    uint64
	InFlight          int
	MaximumActive     int
	LastScanAt        time.Time
	NextDeadlineNanos int64
}

type WorkDeadlineWorker struct {
	backend   workDeadlineBackend
	clock     WorkDeadlineClock
	publisher WorkDeadlinePublicationTrigger
	timers    workDeadlineTimerFactory
	period    time.Duration
	trigger   chan struct{}
	ready     chan struct{}
	readyOnce sync.Once

	mu       sync.Mutex
	started  bool
	snapshot WorkDeadlineWorkerSnapshot
}

type workDeadlineCandidate struct {
	durable store.WorkDeadlineCandidate
	work    model.ReviewWork
}

type workDeadlineScan struct {
	due               []workDeadlineCandidate
	moreDue           bool
	exhaustedCount    uint64
	nextDeadlineNanos int64
}

type workDeadlineBackend interface {
	scan(context.Context, time.Time) (workDeadlineScan, error)
	expire(context.Context, workDeadlineCandidate, WorkDeadlineClock) (bool, error)
}

type workDeadlineTimer interface {
	channel() <-chan time.Time
	stop()
}

type workDeadlineTimerFactory interface {
	newTimer(time.Duration) workDeadlineTimer
}

type wallWorkDeadlineClock struct{}
type wallWorkDeadlineTimerFactory struct{}
type wallWorkDeadlineTimer struct{ timer *time.Timer }

func (wallWorkDeadlineClock) Now() time.Time { return time.Now() }

func (wallWorkDeadlineTimerFactory) newTimer(delay time.Duration) workDeadlineTimer {
	return &wallWorkDeadlineTimer{timer: time.NewTimer(delay)}
}

func (timer *wallWorkDeadlineTimer) channel() <-chan time.Time { return timer.timer.C }
func (timer *wallWorkDeadlineTimer) stop() {
	if timer == nil || timer.timer == nil {
		return
	}
	if !timer.timer.Stop() {
		select {
		case <-timer.timer.C:
		default:
		}
	}
}

func newWorkDeadlineWorker(backend workDeadlineBackend, clock WorkDeadlineClock,
	publisher WorkDeadlinePublicationTrigger, timers workDeadlineTimerFactory,
	period time.Duration,
) (*WorkDeadlineWorker, error) {
	if backend == nil || clock == nil || publisher == nil || timers == nil ||
		period <= 0 || period > workDeadlineReconcilePeriod {
		return nil, fmt.Errorf("%w: complete bounded configuration is required", ErrWorkDeadlineWorker)
	}
	return &WorkDeadlineWorker{backend: backend, clock: clock, publisher: publisher,
		timers: timers, period: period, trigger: make(chan struct{}, 1), ready: make(chan struct{}),
		snapshot: WorkDeadlineWorkerSnapshot{State: WorkDeadlineWorkerIdle}}, nil
}

func (worker *WorkDeadlineWorker) Trigger() {
	if worker == nil || worker.trigger == nil {
		return
	}
	select {
	case worker.trigger <- struct{}{}:
	default:
	}
}

func (worker *WorkDeadlineWorker) Readiness(ctx context.Context) error {
	if worker == nil || worker.ready == nil || ctx == nil {
		return fmt.Errorf("%w: readiness is unavailable", ErrWorkDeadlineWorker)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker.ready:
		return nil
	}
}

func (worker *WorkDeadlineWorker) Snapshot() WorkDeadlineWorkerSnapshot {
	if worker == nil {
		return WorkDeadlineWorkerSnapshot{State: WorkDeadlineWorkerFailed}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

func (worker *WorkDeadlineWorker) Run(ctx context.Context) error {
	if worker == nil || worker.backend == nil || worker.clock == nil || worker.publisher == nil ||
		worker.timers == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrWorkDeadlineWorker)
	}
	if !worker.start() {
		return ErrWorkDeadlineWorkerRunning
	}
	failed := false
	defer worker.stop(&failed)
	for {
		scan, err := worker.runCycle(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			worker.fail()
			return err
		}
		worker.readyOnce.Do(func() { close(worker.ready) })
		if scan.moreDue {
			worker.Trigger()
		}
		now, err := workDeadlineNow(worker.clock)
		if err != nil {
			failed = true
			worker.fail()
			return err
		}
		timer := worker.timers.newTimer(workDeadlineDelay(now,
			scan.nextDeadlineNanos, worker.period))
		if timer == nil || timer.channel() == nil {
			failed = true
			worker.fail()
			return fmt.Errorf("%w: timer factory returned no timer", ErrWorkDeadlineWorker)
		}
		select {
		case <-ctx.Done():
			timer.stop()
			return nil
		case <-worker.trigger:
			timer.stop()
		case <-timer.channel():
			timer.stop()
		}
	}
}

func (worker *WorkDeadlineWorker) runCycle(ctx context.Context) (workDeadlineScan, error) {
	now, err := workDeadlineNow(worker.clock)
	if err != nil {
		return workDeadlineScan{}, err
	}
	scan, err := worker.backend.scan(ctx, now)
	if err != nil {
		return workDeadlineScan{}, fmt.Errorf("%w: scan: %w", ErrWorkDeadlineWorker, err)
	}
	if len(scan.due) > store.WorkDeadlineScanLimit || scan.nextDeadlineNanos < 0 {
		return workDeadlineScan{}, fmt.Errorf("%w: backend exceeded scan bounds", ErrWorkDeadlineWorker)
	}
	worker.recordScan(now, scan)
	for _, candidate := range scan.due {
		if candidate.work.Ref().IsZero() || candidate.work.Version() == 0 {
			return workDeadlineScan{}, fmt.Errorf("%w: backend returned invalid candidate",
				ErrWorkDeadlineWorker)
		}
		worker.recordPrepared(true)
		changed, expireErr := worker.backend.expire(ctx, candidate, worker.clock)
		worker.recordPrepared(false)
		if expireErr != nil {
			if expectedWorkDeadlineRace(expireErr) {
				worker.recordStale()
				continue
			}
			return workDeadlineScan{}, fmt.Errorf("%w: expire: %w", ErrWorkDeadlineWorker, expireErr)
		}
		if changed {
			worker.recordCommitted()
			worker.publisher.Trigger()
		}
	}
	return scan, nil
}

func expectedWorkDeadlineRace(err error) bool {
	return errors.Is(err, store.ErrWorkDeadlineStale) ||
		errors.Is(err, store.ErrAdmissionConflict) ||
		errors.Is(err, store.ErrWorkCASConflict) ||
		errors.Is(err, store.ErrChannelUnavailable)
}

func workDeadlineNow(clock WorkDeadlineClock) (time.Time, error) {
	if clock == nil {
		return time.Time{}, fmt.Errorf("%w: trusted clock is unavailable", ErrWorkDeadlineWorker)
	}
	now := clock.Now().Round(0).UTC()
	if now.IsZero() || now.UnixNano() <= 0 || !time.Unix(0, now.UnixNano()).UTC().Equal(now) {
		return time.Time{}, fmt.Errorf("%w: trusted clock is invalid", ErrWorkDeadlineWorker)
	}
	return now, nil
}

func workDeadlineDelay(now time.Time, nextUnixNano int64, period time.Duration) time.Duration {
	if nextUnixNano <= 0 {
		return period
	}
	delay := time.Unix(0, nextUnixNano).UTC().Sub(now)
	if delay <= 0 {
		return 0
	}
	if delay < period {
		return delay
	}
	return period
}

func (worker *WorkDeadlineWorker) start() bool {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.started {
		return false
	}
	worker.started = true
	worker.snapshot.State = WorkDeadlineWorkerRunning
	return true
}

func (worker *WorkDeadlineWorker) stop(failed *bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if !*failed {
		worker.snapshot.State = WorkDeadlineWorkerStopped
	}
}

func (worker *WorkDeadlineWorker) fail() {
	worker.mu.Lock()
	worker.snapshot.State = WorkDeadlineWorkerFailed
	worker.mu.Unlock()
}

func (worker *WorkDeadlineWorker) recordScan(at time.Time, scan workDeadlineScan) {
	worker.mu.Lock()
	worker.snapshot.Cycles++
	worker.snapshot.LastScanAt = at
	worker.snapshot.ExhaustedCount = scan.exhaustedCount
	worker.snapshot.NextDeadlineNanos = scan.nextDeadlineNanos
	worker.mu.Unlock()
}

func (worker *WorkDeadlineWorker) recordPrepared(start bool) {
	worker.mu.Lock()
	if start {
		worker.snapshot.Prepared++
		worker.snapshot.InFlight++
		if worker.snapshot.InFlight > worker.snapshot.MaximumActive {
			worker.snapshot.MaximumActive = worker.snapshot.InFlight
		}
	} else {
		worker.snapshot.InFlight--
	}
	worker.mu.Unlock()
}

func (worker *WorkDeadlineWorker) recordCommitted() {
	worker.mu.Lock()
	worker.snapshot.Committed++
	worker.mu.Unlock()
}

func (worker *WorkDeadlineWorker) recordStale() {
	worker.mu.Lock()
	worker.snapshot.Stale++
	worker.mu.Unlock()
}
