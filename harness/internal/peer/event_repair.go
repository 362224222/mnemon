package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	eventRepairDefaultPeriod = 10 * time.Second
	eventRepairRetryInitial  = time.Second
	eventRepairRetryMaximum  = 10 * time.Second
	eventRepairPageLimit     = uint8(32)
)

var (
	ErrEventRepair          = errors.New("Mnemon Event repair")
	ErrEventRepairRunning   = fmt.Errorf("%w: worker is already running", ErrEventRepair)
	ErrEventRepairInvariant = fmt.Errorf("%w: invariant failed", ErrEventRepair)
)

// EventRepairStore is the complete durable surface used by outbound
// anti-entropy. It deliberately excludes SQL, task leasing and domain apply.
type EventRepairStore interface {
	ReadPeerRepairTargets(context.Context, time.Time) ([]store.PeerRepairTarget, error)
	PutPeerInboxPage(context.Context, store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error)
	CommitPeerRepair(context.Context, store.CommitPeerRepairSpec) (store.CommitPeerRepairResult, error)
}

type EventRepairClient interface {
	Pull(context.Context, model.PeerID, PullRequest) (PullPage, error)
	Acknowledge(context.Context, model.PeerID, CursorAck) error
}

// EventRepairReconciler is invoked before an authenticated authority error is
// durably paused. It may wake roster/baseline reconciliation but cannot mutate
// the repair checkpoint itself.
type EventRepairReconciler interface {
	ReconcileEventRepair(context.Context, model.ChannelID, model.PeerID) error
}

type EventRepairClock interface{ Now() time.Time }

type EventRepairOptions struct {
	Store      EventRepairStore
	Client     EventRepairClient
	Reconciler EventRepairReconciler
	Clock      EventRepairClock
	Period     time.Duration
}

type EventRepairState string

const (
	EventRepairIdle    EventRepairState = "idle"
	EventRepairRunning EventRepairState = "running"
	EventRepairStopped EventRepairState = "stopped"
	EventRepairFailed  EventRepairState = "failed"
)

type EventRepairFatalCode string

const (
	EventRepairFatalNone            EventRepairFatalCode = ""
	EventRepairFatalStoreInvariant  EventRepairFatalCode = "store_invariant"
	EventRepairFatalStoreFailure    EventRepairFatalCode = "store_failure"
	EventRepairFatalClientInvariant EventRepairFatalCode = "client_invariant"
)

// EventRepairSnapshot is a closed, read-only operational projection. Remote
// text and transport errors never enter it.
type EventRepairSnapshot struct {
	State                   EventRepairState
	FatalCode               EventRepairFatalCode
	Cycles                  uint64
	Targets                 uint64
	Pages                   uint64
	InboxItems              uint64
	Acknowledgements        uint64
	AcknowledgementFailures uint64
	Retries                 uint64
	Pauses                  uint64
	Terminals               uint64
	Conflicts               uint64
	Reconciliations         uint64
	ReconciliationFailures  uint64
	InFlight                int
	MaximumInFlight         int
	LastCycleAt             time.Time
}

type EventRepair struct {
	store       eventRepairBackend
	client      EventRepairClient
	reconciler  EventRepairReconciler
	clock       EventRepairClock
	period      time.Duration
	concurrency int
	trigger     chan struct{}

	mu       sync.Mutex
	snapshot EventRepairSnapshot
	running  bool
}

type eventRepairTarget struct {
	channelID   model.ChannelID
	originPeer  model.PeerID
	originEpoch model.OriginEpoch
	contiguous  uint64
	retryCount  uint64
	durable     store.PeerRepairTarget
	hasDurable  bool
}

type eventRepairCommit struct {
	status      store.PeerRepairStatus
	contiguous  uint64
	sourceFloor uint64
	sourceHead  uint64
	diagnostic  store.PeerRepairDiagnostic
	nextAttempt time.Time
	at          time.Time
}

type eventRepairBackend interface {
	readTargets(context.Context, time.Time) ([]eventRepairTarget, error)
	putPage(context.Context, store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error)
	commit(context.Context, eventRepairTarget, eventRepairCommit) (eventRepairTarget, error)
}

type durableEventRepairBackend struct{ store EventRepairStore }

type wallEventRepairClock struct{}

func (wallEventRepairClock) Now() time.Time { return time.Now() }

func NewEventRepair(options EventRepairOptions) (*EventRepair, error) {
	if options.Store == nil || options.Client == nil || options.Reconciler == nil {
		return nil, fmt.Errorf("%w: Store, client and reconciler are required", ErrEventRepair)
	}
	clock := options.Clock
	if clock == nil {
		clock = wallEventRepairClock{}
	}
	period := options.Period
	if period == 0 {
		period = eventRepairDefaultPeriod
	}
	if period <= 0 || period > eventRepairDefaultPeriod {
		return nil, fmt.Errorf("%w: periodic interval must be within 10 seconds", ErrEventRepair)
	}
	concurrency := HermeticLimits().ApplicationProtocolStreams
	if concurrency <= 0 {
		return nil, fmt.Errorf("%w: Hermetic Events concurrency is unavailable", ErrEventRepair)
	}
	return newEventRepair(durableEventRepairBackend{store: options.Store}, options.Client,
		options.Reconciler, clock, period, concurrency)
}

func newEventRepair(backend eventRepairBackend, client EventRepairClient,
	reconciler EventRepairReconciler, clock EventRepairClock, period time.Duration,
	concurrency int,
) (*EventRepair, error) {
	if backend == nil || client == nil || reconciler == nil || clock == nil || period <= 0 ||
		period > eventRepairDefaultPeriod || concurrency <= 0 ||
		concurrency > HermeticLimits().ApplicationProtocolStreams {
		return nil, fmt.Errorf("%w: complete bounded worker configuration is required", ErrEventRepair)
	}
	return &EventRepair{store: backend, client: client, reconciler: reconciler, clock: clock,
		period: period, concurrency: concurrency, trigger: make(chan struct{}, 1),
		snapshot: EventRepairSnapshot{State: EventRepairIdle}}, nil
}

// Trigger coalesces any number of wakeups into one future cycle and never
// waits for the worker or allocates a queued task.
func (repair *EventRepair) Trigger() {
	if repair == nil || repair.trigger == nil {
		return
	}
	select {
	case repair.trigger <- struct{}{}:
	default:
	}
}

func (repair *EventRepair) Snapshot() EventRepairSnapshot {
	if repair == nil {
		return EventRepairSnapshot{State: EventRepairFailed,
			FatalCode: EventRepairFatalStoreInvariant}
	}
	repair.mu.Lock()
	defer repair.mu.Unlock()
	return repair.snapshot
}

// Run performs startup repair immediately, then accepts only one coalesced
// manual wakeup and a fixed bounded periodic tick. It owns no durable or
// in-memory task queue.
func (repair *EventRepair) Run(ctx context.Context) error {
	if repair == nil || repair.store == nil || repair.client == nil || repair.reconciler == nil ||
		repair.clock == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrEventRepair)
	}
	repair.mu.Lock()
	if repair.running {
		repair.mu.Unlock()
		return ErrEventRepairRunning
	}
	repair.running = true
	repair.snapshot.State = EventRepairRunning
	repair.snapshot.FatalCode = EventRepairFatalNone
	repair.mu.Unlock()
	failed := false
	defer func() {
		repair.mu.Lock()
		repair.running = false
		if !failed {
			repair.snapshot.State = EventRepairStopped
		}
		repair.mu.Unlock()
	}()

	ticker := time.NewTicker(repair.period)
	defer ticker.Stop()
	for {
		if err := repair.runCycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			repair.fail(eventRepairFatalCode(err))
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-repair.trigger:
		}
	}
}

func (repair *EventRepair) runCycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	at := repair.clock.Now().Round(0).UTC()
	targets, err := repair.store.readTargets(ctx, at)
	if err != nil {
		return eventRepairStoreError("read targets", err)
	}
	repair.recordCycle(at, len(targets))
	if len(targets) == 0 {
		return nil
	}
	cycleContext, cancel := context.WithCancel(ctx)
	defer cancel()
	budget := make(chan struct{}, repair.concurrency)
	fatal := make(chan error, 1)
	var wait sync.WaitGroup
	for _, target := range targets {
		select {
		case budget <- struct{}{}:
		case <-cycleContext.Done():
			break
		}
		if cycleContext.Err() != nil {
			break
		}
		wait.Add(1)
		go func(target eventRepairTarget) {
			defer wait.Done()
			defer func() { <-budget }()
			repair.targetStarted()
			defer repair.targetFinished()
			if err := repair.processTarget(cycleContext, target); err != nil {
				select {
				case fatal <- err:
					cancel()
				default:
				}
			}
		}(target)
	}
	wait.Wait()
	select {
	case err := <-fatal:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (repair *EventRepair) processTarget(ctx context.Context, target eventRepairTarget) error {
	request, err := NewPullRequest(PullRequestSpec{ChannelID: target.channelID,
		OriginEpoch: target.originEpoch, AfterChannelSequence: target.contiguous,
		Limit: eventRepairPageLimit})
	if err != nil {
		return eventRepairFatal(EventRepairFatalClientInvariant, "construct Pull request", err)
	}
	page, err := repair.client.Pull(ctx, target.originPeer, request)
	if err != nil {
		return repair.handlePullFailure(ctx, target, err)
	}
	receivedAt := repair.clock.Now().Round(0).UTC()
	publications := page.Publications()
	put, err := repair.store.putPage(ctx, store.PutPeerInboxPageSpec{
		ChannelID: target.channelID, OriginPeerID: target.originPeer,
		OriginEpoch: target.originEpoch, TransportPeerID: target.originPeer,
		AfterChannelSequence: target.contiguous,
		ScannedChannelSeq:    page.ScannedChannelSequence(), SourceFloor: page.SourceFloor(),
		SourceHead: page.SourceHead(), Publications: publications, ReceivedAt: receivedAt})
	if err != nil {
		if errors.Is(err, store.ErrPeerInboxPressure) {
			return repair.commitInboxPressure(ctx, target)
		}
		return repair.handleStoreMutationError("put Inbox page", err)
	}
	if len(put.Items) > len(publications) || put.Cursor.ChannelID != target.channelID ||
		put.Cursor.OriginPeerID != target.originPeer || put.Cursor.OriginEpoch != target.originEpoch ||
		put.Cursor.ObservedChannelSequence < put.Cursor.ContiguousChannelSequence {
		return eventRepairFatal(EventRepairFatalStoreInvariant, "invalid Inbox page result", nil)
	}
	conflicted := false
	for index, item := range put.Items {
		if item.Cursor != put.Cursor {
			return eventRepairFatal(EventRepairFatalStoreInvariant, "misordered Inbox page result", nil)
		}
		if item.Disposition != store.PeerInboxConflicted {
			continue
		}
		if conflicted || index != len(put.Items)-1 || index >= len(publications) {
			return eventRepairFatal(EventRepairFatalStoreInvariant, "invalid Inbox conflict prefix", nil)
		}
		conflicted = true
	}
	if conflicted {
		if len(put.Items) == 0 || put.Cursor.ContiguousChannelSequence <
			publications[len(put.Items)-1].ChannelSequence() {
			return eventRepairFatal(EventRepairFatalStoreInvariant, "invalid Inbox conflict cursor", nil)
		}
	} else if len(put.Items) != len(publications) ||
		put.Cursor.ContiguousChannelSequence < page.ScannedChannelSequence() {
		return eventRepairFatal(EventRepairFatalStoreInvariant, "incomplete Inbox page result", nil)
	}
	repair.recordPage(len(put.Items))
	if conflicted {
		repair.recordConflict()
		return nil
	}
	commitAt := repair.clock.Now().Round(0).UTC()
	status := store.PeerRepairCaughtUp
	nextAttempt := commitAt.Add(repair.period)
	if put.Cursor.ContiguousChannelSequence < page.SourceHead() {
		status = store.PeerRepairProgress
		nextAttempt = commitAt
	}
	committed, err := repair.store.commit(ctx, target, eventRepairCommit{status: status,
		contiguous: put.Cursor.ContiguousChannelSequence, sourceFloor: page.SourceFloor(),
		sourceHead: page.SourceHead(), nextAttempt: nextAttempt, at: commitAt})
	if err != nil {
		return repair.handleStoreMutationError("commit page checkpoint", err)
	}
	if status == store.PeerRepairProgress {
		repair.Trigger()
	}
	ack, err := NewCursorAck(CursorAckSpec{ChannelID: committed.channelID,
		OriginEpoch:               committed.originEpoch,
		ContiguousChannelSequence: committed.contiguous})
	if err != nil {
		return eventRepairFatal(EventRepairFatalClientInvariant, "construct cursor ACK", err)
	}
	if err := repair.client.Acknowledge(ctx, committed.originPeer, ack); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		repair.recordAcknowledgement(false)
		return nil
	}
	repair.recordAcknowledgement(true)
	return nil
}

func (repair *EventRepair) handlePullFailure(ctx context.Context, target eventRepairTarget,
	cause error,
) error {
	if ctx.Err() != nil {
		return nil
	}
	at := repair.clock.Now().Round(0).UTC()
	var remote *EventRemoteFailure
	if errors.As(cause, &remote) {
		var commit eventRepairCommit
		var outcome store.PeerRepairStatus
		var reconciled, reconcileSucceeded bool
		switch remote.Code() {
		case EventErrorBusy:
			delay := remote.RetryAfter()
			if delay <= 0 {
				delay = eventRepairRetryInitial
			}
			if delay > eventRepairRetryMaximum {
				delay = eventRepairRetryMaximum
			}
			commit = eventRepairCommit{status: store.PeerRepairRetry,
				contiguous: target.contiguous, diagnostic: store.PeerRepairDiagnosticBusy,
				nextAttempt: at.Add(delay), at: at}
			outcome = store.PeerRepairRetry
		case EventErrorHistoryGap:
			commit = eventRepairCommit{status: store.PeerRepairTerminal,
				contiguous: target.contiguous, sourceFloor: remote.SourceFloor(),
				diagnostic: store.PeerRepairDiagnosticHistoryGap, at: at}
			outcome = store.PeerRepairTerminal
		case EventErrorNotOrigin, EventErrorNotMember, EventErrorMemberRevoked, EventErrorChannelClosed:
			diagnostic := repairAuthorityDiagnostic(remote.Code())
			reconcileErr := repair.reconciler.ReconcileEventRepair(ctx, target.channelID, target.originPeer)
			if reconcileErr != nil && ctx.Err() != nil {
				return nil
			}
			reconciled, reconcileSucceeded = true, reconcileErr == nil
			commit = eventRepairCommit{status: store.PeerRepairPaused,
				contiguous: target.contiguous, diagnostic: diagnostic, at: at}
			outcome = store.PeerRepairPaused
		case EventErrorOriginEpochMismatch:
			commit = eventRepairCommit{status: store.PeerRepairTerminal,
				contiguous: target.contiguous,
				diagnostic: store.PeerRepairDiagnosticOriginEpochMismatch, at: at}
			outcome = store.PeerRepairTerminal
		default:
			return eventRepairFatal(EventRepairFatalClientInvariant, "unknown remote failure", cause)
		}
		_, err := repair.store.commit(ctx, target, commit)
		if err != nil {
			return repair.handleStoreMutationError("commit remote failure", err)
		}
		if reconciled {
			repair.recordReconciliation(reconcileSucceeded)
		}
		repair.recordDurableOutcome(outcome)
		return nil
	}
	if errors.Is(cause, ErrEventClientTransport) || errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		delay := eventRepairTransportBackoff(target.retryCount)
		_, err := repair.store.commit(ctx, target, eventRepairCommit{status: store.PeerRepairRetry,
			contiguous: target.contiguous, diagnostic: store.PeerRepairDiagnosticTransportUnavailable,
			nextAttempt: at.Add(delay), at: at})
		if err != nil {
			return repair.handleStoreMutationError("commit transport retry", err)
		}
		repair.recordRetry()
		return nil
	}
	if errors.Is(cause, ErrEventClientResponse) {
		_, err := repair.store.commit(ctx, target, eventRepairCommit{status: store.PeerRepairTerminal,
			contiguous: target.contiguous,
			diagnostic: store.PeerRepairDiagnosticProtocolInvalid, at: at})
		if err != nil {
			return repair.handleStoreMutationError("commit invalid protocol response", err)
		}
		repair.recordTerminal()
		return nil
	}
	return eventRepairFatal(EventRepairFatalClientInvariant, "unexpected client failure", cause)
}

func (repair *EventRepair) handleStoreMutationError(operation string, cause error) error {
	if cause == nil || errors.Is(cause, store.ErrPeerRepairStale) ||
		errors.Is(cause, store.ErrPeerRepairAuthority) ||
		errors.Is(cause, store.ErrPeerInboxAuthority) ||
		errors.Is(cause, store.ErrPeerInboxQuarantined) {
		return nil
	}
	return eventRepairStoreError(operation, cause)
}

func (repair *EventRepair) commitInboxPressure(ctx context.Context, target eventRepairTarget) error {
	if ctx.Err() != nil {
		return nil
	}
	at := repair.clock.Now().Round(0).UTC()
	_, err := repair.store.commit(ctx, target, eventRepairCommit{status: store.PeerRepairRetry,
		contiguous: target.contiguous, diagnostic: store.PeerRepairDiagnosticBusy,
		nextAttempt: at.Add(eventRepairTransportBackoff(target.retryCount)), at: at})
	if err != nil {
		return repair.handleStoreMutationError("commit Inbox pressure retry", err)
	}
	repair.recordRetry()
	return nil
}

func eventRepairTransportBackoff(retryCount uint64) time.Duration {
	delay := eventRepairRetryInitial
	for attempt := uint64(0); attempt < retryCount && delay < eventRepairRetryMaximum; attempt++ {
		delay *= 2
		if delay > eventRepairRetryMaximum {
			delay = eventRepairRetryMaximum
		}
	}
	return delay
}

func repairAuthorityDiagnostic(code EventProtocolErrorCode) store.PeerRepairDiagnostic {
	switch code {
	case EventErrorNotOrigin:
		return store.PeerRepairDiagnosticNotOrigin
	case EventErrorNotMember:
		return store.PeerRepairDiagnosticNotMember
	case EventErrorMemberRevoked:
		return store.PeerRepairDiagnosticMemberRevoked
	case EventErrorChannelClosed:
		return store.PeerRepairDiagnosticChannelClosed
	default:
		return ""
	}
}

func eventRepairStoreError(operation string, cause error) error {
	code := EventRepairFatalStoreFailure
	if errors.Is(cause, store.ErrPeerRepairInvariant) || errors.Is(cause, store.ErrPeerRepairInput) ||
		errors.Is(cause, store.ErrPeerInboxInput) || errors.Is(cause, store.ErrPeerInboxPage) ||
		errors.Is(cause, store.ErrPeerInboxConflict) {
		code = EventRepairFatalStoreInvariant
	}
	return eventRepairFatal(code, operation, cause)
}

type eventRepairFatalError struct {
	code   EventRepairFatalCode
	detail string
	cause  error
}

func (failure *eventRepairFatalError) Error() string {
	if failure == nil {
		return ErrEventRepairInvariant.Error()
	}
	if failure.cause == nil {
		return fmt.Sprintf("%s: %s", ErrEventRepairInvariant, failure.detail)
	}
	return fmt.Sprintf("%s: %s: %v", ErrEventRepairInvariant, failure.detail, failure.cause)
}

func (failure *eventRepairFatalError) Unwrap() error { return ErrEventRepairInvariant }

func eventRepairFatal(code EventRepairFatalCode, detail string, cause error) error {
	return &eventRepairFatalError{code: code, detail: detail, cause: cause}
}

func eventRepairFatalCode(err error) EventRepairFatalCode {
	var fatal *eventRepairFatalError
	if errors.As(err, &fatal) && fatal.code != EventRepairFatalNone {
		return fatal.code
	}
	return EventRepairFatalStoreInvariant
}

func (backend durableEventRepairBackend) readTargets(ctx context.Context,
	at time.Time,
) ([]eventRepairTarget, error) {
	targets, err := backend.store.ReadPeerRepairTargets(ctx, at)
	if err != nil {
		return nil, err
	}
	result := make([]eventRepairTarget, len(targets))
	for index, target := range targets {
		result[index] = eventRepairTarget{channelID: target.ChannelID(),
			originPeer: target.OriginPeerID(), originEpoch: target.OriginEpoch(),
			contiguous: target.ContiguousChannelSequence(), retryCount: target.RetryCount(),
			durable: target, hasDurable: true}
	}
	return result, nil
}

func (backend durableEventRepairBackend) putPage(ctx context.Context,
	spec store.PutPeerInboxPageSpec,
) (store.PutPeerInboxPageResult, error) {
	return backend.store.PutPeerInboxPage(ctx, spec)
}

func (backend durableEventRepairBackend) commit(ctx context.Context, target eventRepairTarget,
	commit eventRepairCommit,
) (eventRepairTarget, error) {
	if !target.hasDurable {
		return eventRepairTarget{}, store.ErrPeerRepairInput
	}
	result, err := backend.store.CommitPeerRepair(ctx, store.CommitPeerRepairSpec{
		Target: target.durable, Status: commit.status,
		ContiguousChannelSequence: commit.contiguous, SourceFloor: commit.sourceFloor,
		SourceHead: commit.sourceHead, Diagnostic: commit.diagnostic,
		NextAttemptAt: commit.nextAttempt, At: commit.at})
	if err != nil {
		return eventRepairTarget{}, err
	}
	durable := result.Target
	return eventRepairTarget{channelID: durable.ChannelID(), originPeer: durable.OriginPeerID(),
		originEpoch: durable.OriginEpoch(), contiguous: durable.ContiguousChannelSequence(),
		retryCount: durable.RetryCount(), durable: durable, hasDurable: true}, nil
}

func (repair *EventRepair) recordCycle(at time.Time, targets int) {
	repair.mu.Lock()
	repair.snapshot.Cycles++
	repair.snapshot.Targets += uint64(targets)
	repair.snapshot.LastCycleAt = at
	repair.mu.Unlock()
}

func (repair *EventRepair) targetStarted() {
	repair.mu.Lock()
	repair.snapshot.InFlight++
	if repair.snapshot.InFlight > repair.snapshot.MaximumInFlight {
		repair.snapshot.MaximumInFlight = repair.snapshot.InFlight
	}
	repair.mu.Unlock()
}

func (repair *EventRepair) targetFinished() {
	repair.mu.Lock()
	repair.snapshot.InFlight--
	repair.mu.Unlock()
}

func (repair *EventRepair) recordPage(items int) {
	repair.mu.Lock()
	repair.snapshot.Pages++
	repair.snapshot.InboxItems += uint64(items)
	repair.mu.Unlock()
}

func (repair *EventRepair) recordAcknowledgement(success bool) {
	repair.mu.Lock()
	if success {
		repair.snapshot.Acknowledgements++
	} else {
		repair.snapshot.AcknowledgementFailures++
	}
	repair.mu.Unlock()
}

func (repair *EventRepair) recordRetry() {
	repair.mu.Lock()
	repair.snapshot.Retries++
	repair.mu.Unlock()
}

func (repair *EventRepair) recordPause() {
	repair.mu.Lock()
	repair.snapshot.Pauses++
	repair.mu.Unlock()
}

func (repair *EventRepair) recordTerminal() {
	repair.mu.Lock()
	repair.snapshot.Terminals++
	repair.mu.Unlock()
}

func (repair *EventRepair) recordConflict() {
	repair.mu.Lock()
	repair.snapshot.Conflicts++
	repair.mu.Unlock()
}

func (repair *EventRepair) recordReconciliation(success bool) {
	repair.mu.Lock()
	repair.snapshot.Reconciliations++
	if !success {
		repair.snapshot.ReconciliationFailures++
	}
	repair.mu.Unlock()
}

func (repair *EventRepair) recordDurableOutcome(status store.PeerRepairStatus) {
	switch status {
	case store.PeerRepairRetry:
		repair.recordRetry()
	case store.PeerRepairPaused:
		repair.recordPause()
	case store.PeerRepairTerminal:
		repair.recordTerminal()
	}
}

func (repair *EventRepair) fail(code EventRepairFatalCode) {
	repair.mu.Lock()
	repair.snapshot.State = EventRepairFailed
	repair.snapshot.FatalCode = code
	repair.mu.Unlock()
}
