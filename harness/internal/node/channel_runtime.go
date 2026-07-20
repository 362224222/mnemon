package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	channelRuntimeReconcilePeriod = time.Second
	channelRuntimeHealthyPeriod   = 10 * time.Second
	channelRuntimeMaximumTargets  = model.MaxChannelsPerNode * (model.MaxMembersPerChannel - 1)
)

var (
	ErrChannelRuntime        = errors.New("mnemond Channel runtime")
	ErrChannelRuntimeRunning = fmt.Errorf("%w: runtime has already run", ErrChannelRuntime)
)

// ChannelRuntimeStore is the durable projection consumed by ChannelRuntime.
// Every mutation is fenced by the exact authority generation supplied here.
type ChannelRuntimeStore interface {
	BeginChannelTopicRuntime(context.Context, time.Time) (store.BeginChannelTopicRuntimeResult, error)
	ReadChannelMeshAuthority(context.Context) (store.ChannelMeshAuthority, error)
	ReadChannelBaselineReadiness(context.Context, model.ChannelID) ([]store.ChannelPeerReadiness, error)
	CompareAndSetChannelTopicState(context.Context, store.CompareAndSetChannelTopicStateSpec) (store.CompareAndSetChannelTopicStateResult, error)
	ReserveOutboundChannelBaseline(context.Context, store.ReserveOutboundChannelBaselineSpec) (store.ReserveOutboundChannelBaselineResult, error)
	ConfirmOutboundChannelBaseline(context.Context, store.ConfirmOutboundChannelBaselineSpec) (store.ConfirmOutboundChannelBaselineResult, error)
	SetPeerReachability(context.Context, store.SetPeerReachabilitySpec) (store.SetPeerReachabilityResult, error)
}

// ChannelRuntimeTransport owns local topic sessions and authenticated direct
// member exchanges. Calls may block, must observe ctx, and run outside locks.
type ChannelRuntimeTransport interface {
	EnsureChannelTopic(context.Context, model.ChannelID) error
	HasCurrentChannelTopic(model.ChannelID) bool
	Hello(context.Context, model.PeerID, peer.MemberHello) (peer.MemberHelloAck, error)
	Sync(context.Context, model.PeerID, peer.SyncRequest) (peer.ChannelMemberSyncResult, error)
	Baseline(context.Context, model.PeerID, peer.DataBaseline) (peer.DataBaselineAck, error)
}

// ChannelRuntimeRosterUpdate carries only authenticated, owner-signed roster
// evidence. The coordinator remains the sole authority mutation owner.
type ChannelRuntimeRosterUpdate struct {
	ChannelID           model.ChannelID
	AuthenticatedPeerID model.PeerID
	Records             []model.Member
	At                  time.Time
}

// ChannelRuntimeAuthority serializes remote roster evidence with every other
// Channel authority transition.
type ChannelRuntimeAuthority interface {
	ReconcileRemoteRoster(context.Context, ChannelRuntimeRosterUpdate) (model.VerifiedRoster, error)
}

type ChannelRuntimeOptions struct {
	Store     ChannelRuntimeStore
	Transport ChannelRuntimeTransport
	Authority ChannelRuntimeAuthority
	Clock     Clock
	Period    time.Duration
	timers    channelRuntimeTimerFactory
	backoff   channelRuntimeBackoff
}

type ChannelRuntimeState string

const (
	ChannelRuntimeIdle    ChannelRuntimeState = "idle"
	ChannelRuntimeRunning ChannelRuntimeState = "running"
	ChannelRuntimeStopped ChannelRuntimeState = "stopped"
	ChannelRuntimeFailed  ChannelRuntimeState = "failed"
)

// ChannelRuntimeSnapshot is diagnostic runtime state, never durable authority.
type ChannelRuntimeSnapshot struct {
	State            ChannelRuntimeState
	Cycles           uint64
	ActiveTopics     int
	Targets          int
	ReadyTargets     int
	RetryingTargets  int
	PermanentTargets int
	InFlight         int
	MaximumInFlight  int
	TopicRetries     []ChannelRuntimeTopicRetry
	LocalTopicsReady bool
	FullyConverged   bool
	LastCycleAt      time.Time
}

// ChannelRuntimeTopicRetry is bounded diagnostic state for one local topic
// generation. It is never durable authority.
type ChannelRuntimeTopicRetry struct {
	ChannelID  model.ChannelID
	RosterHead model.RecordHead
	Attempts   uint32
	RetryAt    time.Time
	Diagnostic string
}

// ChannelRuntime owns deterministic local topic reconciliation and bounded
// peer convergence. One Run consumes its lifecycle permanently.
type ChannelRuntime struct {
	store          ChannelRuntimeStore
	transport      ChannelRuntimeTransport
	authority      ChannelRuntimeAuthority
	clock          Clock
	period         time.Duration
	requestTimeout time.Duration
	maximumFanout  int
	timers         channelRuntimeTimerFactory
	backoff        channelRuntimeBackoff
	trigger        chan struct{}
	ready          chan struct{}
	done           chan struct{}
	readyOnce      sync.Once
	doneOnce       sync.Once

	mu           sync.Mutex
	started      bool
	runErr       error
	snapshot     ChannelRuntimeSnapshot
	retries      map[channelRuntimeTargetKey]channelRuntimeRetryState
	topicRetries map[model.ChannelID]channelRuntimeTopicRetryState
}

func NewChannelRuntime(options ChannelRuntimeOptions) (*ChannelRuntime, error) {
	if isNilNodeInterface(options.Store) || isNilNodeInterface(options.Transport) ||
		isNilNodeInterface(options.Authority) {
		return nil, fmt.Errorf("%w: Store, transport, and authority are required", ErrChannelRuntime)
	}
	if isNilNodeInterface(options.Clock) {
		options.Clock = wallClock{}
	}
	if options.Period == 0 {
		options.Period = channelRuntimeReconcilePeriod
	}
	if options.Period <= 0 || options.Period > channelRuntimeReconcilePeriod {
		return nil, fmt.Errorf("%w: reconcile period must be positive and at most %s",
			ErrChannelRuntime, channelRuntimeReconcilePeriod)
	}
	if isNilNodeInterface(options.timers) {
		options.timers = wallChannelRuntimeTimerFactory{}
	}
	if isNilNodeInterface(options.backoff) {
		options.backoff = deterministicChannelRuntimeBackoff{}
	}
	limits := peer.HermeticLimits()
	if limits.ApplicationProtocolStreams <= 0 ||
		limits.ApplicationProtocolStreams > model.MaxMembersPerChannel ||
		limits.ChannelRequestTimeout <= 0 {
		return nil, fmt.Errorf("%w: Hermetic Channel fanout authority is invalid", ErrChannelRuntime)
	}
	return &ChannelRuntime{store: options.Store, transport: options.Transport,
		authority: options.Authority, clock: options.Clock, period: options.Period,
		requestTimeout: limits.ChannelRequestTimeout, maximumFanout: limits.ApplicationProtocolStreams,
		timers: options.timers, backoff: options.backoff,
		trigger: make(chan struct{}, 1), ready: make(chan struct{}), done: make(chan struct{}),
		retries:      make(map[channelRuntimeTargetKey]channelRuntimeRetryState),
		topicRetries: make(map[model.ChannelID]channelRuntimeTopicRetryState),
		snapshot:     ChannelRuntimeSnapshot{State: ChannelRuntimeIdle}}, nil
}

// Trigger coalesces authority and inbound-baseline notifications. The runtime
// still honors generation-fenced retry deadlines when the scan begins.
func (runtime *ChannelRuntime) Trigger() {
	if runtime == nil || runtime.trigger == nil {
		return
	}
	select {
	case runtime.trigger <- struct{}{}:
	default:
	}
}

// Readiness is a startup latch while Run remains live. Current topic health is
// reported by Snapshot; a terminated runtime is never ready.
func (runtime *ChannelRuntime) Readiness(ctx context.Context) error {
	if runtime == nil || runtime.ready == nil || runtime.done == nil || ctx == nil {
		return fmt.Errorf("%w: readiness is unavailable", ErrChannelRuntime)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-runtime.done:
		return runtime.readinessTerminal()
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-runtime.ready:
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-runtime.done:
			return runtime.readinessTerminal()
		default:
		}
		return nil
	case <-runtime.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return runtime.readinessTerminal()
	}
}

func (runtime *ChannelRuntime) readinessTerminal() error {
	runtime.mu.Lock()
	runErr := runtime.runErr
	runtime.mu.Unlock()
	if runErr != nil {
		return runErr
	}
	return fmt.Errorf("%w: runtime stopped", ErrChannelRuntime)
}

func (runtime *ChannelRuntime) Snapshot() ChannelRuntimeSnapshot {
	if runtime == nil {
		return ChannelRuntimeSnapshot{State: ChannelRuntimeFailed}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	snapshot := runtime.snapshot
	snapshot.TopicRetries = append([]ChannelRuntimeTopicRetry(nil), runtime.snapshot.TopicRetries...)
	return snapshot
}

func (runtime *ChannelRuntime) Run(ctx context.Context) error {
	if runtime == nil || ctx == nil || runtime.store == nil || runtime.transport == nil ||
		runtime.authority == nil || runtime.clock == nil {
		return fmt.Errorf("%w: runtime is unavailable", ErrChannelRuntime)
	}
	if !runtime.claimRun() {
		return ErrChannelRuntimeRunning
	}
	var runErr error
	defer func() { runtime.finishRun(runErr) }()
	runErr = runtime.runOwned(ctx)
	return runErr
}

func (runtime *ChannelRuntime) runOwned(ctx context.Context) error {
	now, err := channelRuntimeNow(runtime.clock)
	if err != nil {
		return err
	}
	if _, err := runtime.store.BeginChannelTopicRuntime(ctx, now); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("%w: begin topic runtime: %w", ErrChannelRuntime, err)
	}
	for {
		continueRun, err := runtime.runIteration(ctx)
		if err != nil {
			return err
		}
		if !continueRun {
			return nil
		}
	}
}

func (runtime *ChannelRuntime) runIteration(ctx context.Context) (bool, error) {
	now, err := channelRuntimeNow(runtime.clock)
	if err != nil {
		return false, err
	}
	cycle, err := runtime.runCycle(ctx, now)
	if ctx.Err() != nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	runtime.recordCycle(now, cycle)
	delay := runtime.nextDelay(now)
	if cycle.immediateRescan {
		delay = 0
	}
	return runtime.wait(ctx, delay)
}

func (runtime *ChannelRuntime) wait(ctx context.Context, delay time.Duration) (bool, error) {
	if delay < 0 || delay > runtime.period {
		delay = runtime.period
	}
	timer := runtime.timers.newTimer(delay)
	if timer == nil || timer.channel() == nil {
		return false, fmt.Errorf("%w: timer factory returned no timer", ErrChannelRuntime)
	}
	defer timer.stop()
	select {
	case <-ctx.Done():
		return false, nil
	case <-runtime.trigger:
		return true, nil
	case <-timer.channel():
		return true, nil
	}
}

func (runtime *ChannelRuntime) claimRun() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.started {
		return false
	}
	runtime.started = true
	runtime.snapshot.State = ChannelRuntimeRunning
	return true
}

func (runtime *ChannelRuntime) finishRun(err error) {
	runtime.mu.Lock()
	runtime.runErr = err
	if err == nil {
		runtime.snapshot.State = ChannelRuntimeStopped
	} else {
		runtime.snapshot.State = ChannelRuntimeFailed
	}
	runtime.mu.Unlock()
	runtime.doneOnce.Do(func() { close(runtime.done) })
}

func channelRuntimeNow(clock Clock) (time.Time, error) {
	if isNilNodeInterface(clock) {
		return time.Time{}, fmt.Errorf("%w: trusted clock is unavailable", ErrChannelRuntime)
	}
	now := clock.Now().Round(0).UTC()
	if now.IsZero() || now.UnixNano() <= 0 || !time.Unix(0, now.UnixNano()).UTC().Equal(now) {
		return time.Time{}, fmt.Errorf("%w: trusted clock is invalid", ErrChannelRuntime)
	}
	return now, nil
}
