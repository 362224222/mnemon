package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type ChannelMemberReconcilerState string

const (
	ChannelMemberReconcilerIdle    ChannelMemberReconcilerState = "idle"
	ChannelMemberReconcilerRunning ChannelMemberReconcilerState = "running"
	ChannelMemberReconcilerStopped ChannelMemberReconcilerState = "stopped"
	ChannelMemberReconcilerFailed  ChannelMemberReconcilerState = "failed"
)

type ChannelMemberReconcilerSnapshot struct {
	State             ChannelMemberReconcilerState
	Cycles            uint64
	Targets           uint64
	Hellos            uint64
	Syncs             uint64
	RosterMerges      uint64
	Baselines         uint64
	Reachable         uint64
	Unreachable       uint64
	RetryableFailures uint64
	PermanentFailures uint64
	StaleSettlements  uint64
	InFlight          int
	MaximumInFlight   int
	LastCycleAt       time.Time
	LastFailure       string
}

type ChannelMemberReconciler struct {
	backend channelMemberReconcileBackend
	client  ChannelMemberControlClient
	clock   ChannelMemberReconcilerClock
	period  time.Duration
	trigger chan struct{}
	ready   chan struct{}
	once    sync.Once

	mu        sync.Mutex
	started   bool
	snapshot  ChannelMemberReconcilerSnapshot
	schedules map[channelMemberTargetKey]channelMemberSchedule
}

type channelMemberSchedule struct {
	head      model.RecordHead
	attempt   uint8
	next      time.Time
	permanent bool
}

func newChannelMemberReconciler(backend channelMemberReconcileBackend,
	client ChannelMemberControlClient, clock ChannelMemberReconcilerClock, period time.Duration,
) (*ChannelMemberReconciler, error) {
	if backend == nil || client == nil || clock == nil || period <= 0 ||
		period > channelMemberReconcileDefaultPeriod {
		return nil, fmt.Errorf("%w: complete bounded configuration is required",
			ErrChannelMemberReconciler)
	}
	return &ChannelMemberReconciler{backend: backend, client: client, clock: clock,
		period: period, trigger: make(chan struct{}, 1), ready: make(chan struct{}),
		schedules: make(map[channelMemberTargetKey]channelMemberSchedule),
		snapshot:  ChannelMemberReconcilerSnapshot{State: ChannelMemberReconcilerIdle}}, nil
}

func (worker *ChannelMemberReconciler) Trigger() {
	if worker == nil || worker.trigger == nil {
		return
	}
	select {
	case worker.trigger <- struct{}{}:
	default:
	}
}

func (worker *ChannelMemberReconciler) ReconcileEventRepair(ctx context.Context,
	channelID model.ChannelID, peerID model.PeerID,
) error {
	if worker == nil || ctx == nil || ctx.Err() != nil || channelID.IsZero() || peerID.IsZero() {
		return ErrChannelMemberReconciler
	}
	worker.Trigger()
	return nil
}

func (worker *ChannelMemberReconciler) ReconcileArtifactReceiver(ctx context.Context,
	channelID model.ChannelID, peerID model.PeerID,
) error {
	if worker == nil || ctx == nil || ctx.Err() != nil || channelID.IsZero() || peerID.IsZero() {
		return ErrChannelMemberReconciler
	}
	worker.Trigger()
	return nil
}

func (worker *ChannelMemberReconciler) Readiness(ctx context.Context) error {
	if worker == nil || worker.ready == nil || ctx == nil {
		return fmt.Errorf("%w: readiness is unavailable", ErrChannelMemberReconciler)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker.ready:
		return nil
	}
}

func (worker *ChannelMemberReconciler) Snapshot() ChannelMemberReconcilerSnapshot {
	if worker == nil {
		return ChannelMemberReconcilerSnapshot{State: ChannelMemberReconcilerFailed}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

func (worker *ChannelMemberReconciler) Run(ctx context.Context) error {
	if worker == nil || worker.backend == nil || worker.client == nil || worker.clock == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrChannelMemberReconciler)
	}
	worker.mu.Lock()
	if worker.started {
		worker.mu.Unlock()
		return ErrChannelMemberReconcilerRunning
	}
	worker.started = true
	worker.snapshot.State = ChannelMemberReconcilerRunning
	worker.mu.Unlock()
	failed := false
	defer worker.finish(&failed)
	ticker := time.NewTicker(worker.period)
	defer ticker.Stop()
	force := true
	for {
		if err := worker.runCycle(ctx, force); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			worker.fail()
			return err
		}
		worker.once.Do(func() { close(worker.ready) })
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			force = false
		case <-worker.trigger:
			force = true
		}
	}
}

func (worker *ChannelMemberReconciler) runCycle(ctx context.Context, force bool) error {
	at := worker.clock.Now().Round(0).UTC()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if at.IsZero() || at.UnixNano() <= 0 {
		return fmt.Errorf("%w: trusted clock is invalid", ErrChannelMemberReconciler)
	}
	targets, err := worker.backend.targets(ctx)
	if err != nil {
		return fmt.Errorf("%w: read durable targets: %w", ErrChannelMemberReconciler, err)
	}
	if err := validateChannelMemberTargets(targets); err != nil {
		return err
	}
	if force {
		clear(worker.schedules)
	}
	worker.pruneSchedules(targets)
	worker.recordCycle(at, len(targets))
	for _, target := range targets {
		key := target.key()
		schedule := worker.schedules[key]
		if schedule.head == target.roster.Head() &&
			(schedule.permanent || !schedule.next.IsZero() && at.Before(schedule.next)) {
			continue
		}
		worker.recordInFlight(true)
		err := worker.processTarget(ctx, target)
		worker.recordInFlight(false)
		if err == nil {
			delete(worker.schedules, key)
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		disposition := classifyChannelMemberFailure(err)
		if disposition == channelMemberFailureFatal {
			return fmt.Errorf("%w: reconcile target: %w", ErrChannelMemberReconciler, err)
		}
		worker.scheduleFailure(key, target.roster.Head(), schedule, disposition, at, err)
	}
	return nil
}

func (worker *ChannelMemberReconciler) processTarget(ctx context.Context,
	target channelMemberTarget,
) error {
	hello, err := peer.NewMemberHello(peer.MemberHelloSpec{ChannelID: target.channel.ID(),
		ActiveMemberRecord: target.localMember, KnownRosterHead: target.roster.Head(),
		OwnerSignedProofChain: target.roster.Members()})
	if err != nil {
		return err
	}
	ack, err := worker.client.Hello(ctx, target.remoteMember.PeerID(),
		target.remoteMember.PublicKey(), hello)
	if err != nil {
		if reachabilityErr := worker.observeFailureReachability(ctx, target, err); reachabilityErr != nil {
			return reachabilityErr
		}
		return err
	}
	worker.recordHello()
	missing := ack.MissingRecords()
	if len(missing) > 0 {
		request, err := peer.NewSyncRequest(peer.SyncRequestSpec{ChannelID: target.channel.ID(),
			AfterHead: ack.RosterHead()})
		if err != nil {
			return err
		}
		pages, err := worker.client.Sync(ctx, target.remoteMember.PeerID(),
			target.remoteMember.PublicKey(), request)
		if err != nil {
			if reachabilityErr := worker.observeFailureReachability(ctx, target, err); reachabilityErr != nil {
				return reachabilityErr
			}
			return err
		}
		worker.recordSync()
		finalHead := ack.RosterHead()
		for _, page := range pages {
			missing = append(missing, page.OwnerSignedRecords()...)
			finalHead = page.RosterHead()
		}
		if err := worker.backend.merge(ctx, target, missing, finalHead, worker.now()); err != nil {
			return err
		}
		worker.recordMerge()
		worker.Trigger()
		return nil
	}
	if target.outboundReady {
		return worker.markReachability(ctx, target, model.ReachabilityReachable)
	}
	return worker.installTargetBaseline(ctx, target)
}

func (worker *ChannelMemberReconciler) installTargetBaseline(ctx context.Context,
	target channelMemberTarget,
) error {
	reserved, err := worker.backend.reserve(ctx, target, worker.now())
	if err != nil {
		return err
	}
	baseline, err := peer.NewDataBaseline(peer.DataBaselineSpec{ChannelID: reserved.ChannelID,
		OriginPeerID: reserved.OriginPeerID, OriginEpoch: reserved.OriginEpoch,
		BaselineChannelSequence: reserved.BaselineChannelSequence})
	if err != nil {
		return err
	}
	baselineAck, err := worker.client.InstallBaseline(ctx, target.remoteMember.PeerID(),
		target.remoteMember.PublicKey(), baseline)
	if err != nil {
		if reachabilityErr := worker.observeFailureReachability(ctx, target, err); reachabilityErr != nil {
			return reachabilityErr
		}
		return err
	}
	if err := worker.backend.confirm(ctx, target, baselineAck, worker.now()); err != nil {
		return err
	}
	worker.recordBaseline()
	return worker.markReachability(ctx, target, model.ReachabilityReachable)
}

func (worker *ChannelMemberReconciler) now() time.Time {
	return worker.clock.Now().Round(0).UTC()
}

func (worker *ChannelMemberReconciler) observeFailureReachability(ctx context.Context,
	target channelMemberTarget, cause error,
) error {
	var remote *peer.ChannelProtocolFailure
	switch {
	case errors.As(cause, &remote):
		return worker.markReachability(ctx, target, model.ReachabilityReachable)
	case errors.Is(cause, peer.ErrChannelMemberClientTransport):
		return worker.markReachability(ctx, target, model.ReachabilityUnreachable)
	}
	return nil
}

func (worker *ChannelMemberReconciler) markReachability(ctx context.Context,
	target channelMemberTarget, reachability model.Reachability,
) error {
	err := worker.backend.reachability(ctx, target, reachability, worker.now())
	if err == nil {
		worker.recordReachability(reachability)
		return nil
	}
	if errors.Is(err, store.ErrChannelRuntimeConflict) || errors.Is(err, store.ErrChannelRuntimeAuthority) {
		worker.recordStale()
		return nil
	}
	return err
}

type channelMemberFailureDisposition uint8

const (
	channelMemberFailureFatal channelMemberFailureDisposition = iota
	channelMemberFailureRetryable
	channelMemberFailurePermanent
)

func classifyChannelMemberFailure(err error) channelMemberFailureDisposition {
	var remote *peer.ChannelProtocolFailure
	if errors.As(err, &remote) {
		// An active signed target can race the remote install immediately after
		// enrollment acceptance. The wire answer remains fail-closed; this owner
		// retries only the same roster generation until the remote catches up.
		if remote.Code() == peer.ChannelErrorNotMember {
			return channelMemberFailureRetryable
		}
		if remote.Retryable() {
			return channelMemberFailureRetryable
		}
		return channelMemberFailurePermanent
	}
	if errors.Is(err, peer.ErrChannelMemberClientTransport) ||
		errors.Is(err, peer.ErrChannelMemberBusy) || errors.Is(err, peer.ErrChannelMemberRosterGap) ||
		errors.Is(err, peer.ErrChannelMemberNotMember) ||
		errors.Is(err, store.ErrChannelBaselineAuthority) ||
		errors.Is(err, store.ErrChannelRuntimeAuthority) || errors.Is(err, store.ErrChannelRuntimeConflict) {
		return channelMemberFailureRetryable
	}
	if errors.Is(err, peer.ErrChannelMemberClientResponse) || errors.Is(err, peer.ErrChannelMemberRevoked) ||
		errors.Is(err, peer.ErrChannelMemberClosed) || errors.Is(err, peer.ErrChannelMemberRosterConflict) ||
		errors.Is(err, peer.ErrChannelMemberBaselineConflict) ||
		errors.Is(err, peer.ErrChannelMemberEpochMismatch) ||
		errors.Is(err, store.ErrChannelBaselineConflict) ||
		errors.Is(err, store.ErrChannelBaselineEpochMismatch) {
		return channelMemberFailurePermanent
	}
	return channelMemberFailureFatal
}

func validateChannelMemberTargets(targets []channelMemberTarget) error {
	if len(targets) > model.MaxChannelsPerNode*(model.MaxMembersPerChannel-1) {
		return fmt.Errorf("%w: target bound exceeded", ErrChannelMemberReconciler)
	}
	sort.Slice(targets, func(left, right int) bool {
		if targets[left].channel.ID() != targets[right].channel.ID() {
			return targets[left].channel.ID().String() < targets[right].channel.ID().String()
		}
		return targets[left].remoteMember.PeerID().String() < targets[right].remoteMember.PeerID().String()
	})
	for index, target := range targets {
		if target.channel.Status() != model.ChannelActive || target.roster.IsZero() ||
			target.channel.RosterHead() != target.roster.Head() ||
			target.localMember.Status() != model.MemberActive ||
			target.remoteMember.Status() != model.MemberActive ||
			target.localMember.PeerID() == target.remoteMember.PeerID() ||
			target.binding.PeerID() != target.remoteMember.PeerID() ||
			target.binding.RosterHead() != target.roster.Head() ||
			target.binding.State() == model.BindingRevoked ||
			index > 0 && target.key() == targets[index-1].key() {
			return fmt.Errorf("%w: invalid or duplicate target", ErrChannelMemberReconciler)
		}
	}
	return nil
}
