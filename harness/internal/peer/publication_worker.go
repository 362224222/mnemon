package peer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	gossipPublicationPeriod       = time.Second
	gossipPublicationLease        = 30 * time.Second
	gossipPublicationRetryMinimum = time.Second
	gossipPublicationRetryMaximum = 10 * time.Second
	gossipPublicationOwnerBytes   = 12
)

var (
	ErrGossipPublicationWorker        = errors.New("Mnemon Gossip publication worker")
	ErrGossipPublicationWorkerRunning = fmt.Errorf("%w: worker has already run", ErrGossipPublicationWorker)
)

type GossipPublicationWorkerClock interface{ Now() time.Time }

type GossipPublicationWorkerOptions struct {
	Store   GossipPublicationWorkerStore
	Runtime *MeshRuntime
	Clock   GossipPublicationWorkerClock
	Period  time.Duration
}

type GossipPublicationWorkerStore interface {
	ReadChannelMeshAuthority(context.Context) (store.ChannelMeshAuthority, error)
	ClaimGossipPublication(context.Context, store.GossipPublicationClaimSpec) (store.GossipPublicationClaimResult, error)
	MarkGossipPublicationPublished(context.Context, store.MarkGossipPublicationPublishedSpec) (store.GossipPublicationSettlement, error)
	RetryGossipPublication(context.Context, store.RetryGossipPublicationSpec) (store.GossipPublicationSettlement, error)
}

type GossipPublicationWorkerState string

const (
	GossipPublicationWorkerIdle    GossipPublicationWorkerState = "idle"
	GossipPublicationWorkerRunning GossipPublicationWorkerState = "running"
	GossipPublicationWorkerStopped GossipPublicationWorkerState = "stopped"
	GossipPublicationWorkerFailed  GossipPublicationWorkerState = "failed"
)

type GossipPublicationWorkerSnapshot struct {
	State         GossipPublicationWorkerState
	Cycles        uint64
	Claims        uint64
	Published     uint64
	Retries       uint64
	Stale         uint64
	InFlight      int
	MaximumActive int
	LastCycleAt   time.Time
}

type GossipPublicationWorker struct {
	backend  gossipPublicationBackend
	sessions gossipPublicationSessions
	clock    GossipPublicationWorkerClock
	period   time.Duration
	owner    string
	trigger  chan struct{}

	mu       sync.Mutex
	started  bool
	snapshot GossipPublicationWorkerSnapshot
}

type gossipPublicationBackend interface {
	channels(context.Context) ([]model.ChannelID, error)
	claim(context.Context, model.ChannelID, string, time.Time, time.Time) (store.GossipPublicationClaimResult, error)
	mark(context.Context, store.GossipPublicationFence, time.Time) error
	retry(context.Context, store.GossipPublicationFence, time.Time, time.Time, string) error
}

type gossipPublicationSession interface {
	Publish(context.Context, model.SignedPublication) error
}

type gossipPublicationSessions interface {
	session(model.ChannelID) (gossipPublicationSession, error)
}

type durableGossipPublicationBackend struct{ store GossipPublicationWorkerStore }
type meshGossipPublicationSessions struct{ runtime *MeshRuntime }
type wallGossipPublicationWorkerClock struct{}

func (wallGossipPublicationWorkerClock) Now() time.Time { return time.Now() }

func NewGossipPublicationWorker(options GossipPublicationWorkerOptions) (*GossipPublicationWorker, error) {
	if options.Store == nil || options.Runtime == nil {
		return nil, fmt.Errorf("%w: Store and mesh runtime are required", ErrGossipPublicationWorker)
	}
	clock := options.Clock
	if clock == nil {
		clock = wallGossipPublicationWorkerClock{}
	}
	period := options.Period
	if period == 0 {
		period = gossipPublicationPeriod
	}
	owner, err := newGossipPublicationOwner()
	if err != nil {
		return nil, fmt.Errorf("%w: generate owner: %v", ErrGossipPublicationWorker, err)
	}
	return newGossipPublicationWorker(durableGossipPublicationBackend{store: options.Store},
		meshGossipPublicationSessions{runtime: options.Runtime}, clock, period, owner)
}

func newGossipPublicationWorker(backend gossipPublicationBackend,
	sessions gossipPublicationSessions, clock GossipPublicationWorkerClock,
	period time.Duration, owner string,
) (*GossipPublicationWorker, error) {
	if backend == nil || sessions == nil || clock == nil || period <= 0 ||
		period > gossipPublicationPeriod || owner == "" || len(owner) > 512 {
		return nil, fmt.Errorf("%w: complete bounded configuration is required", ErrGossipPublicationWorker)
	}
	return &GossipPublicationWorker{backend: backend, sessions: sessions, clock: clock,
		period: period, owner: owner, trigger: make(chan struct{}, 1),
		snapshot: GossipPublicationWorkerSnapshot{State: GossipPublicationWorkerIdle}}, nil
}

func newGossipPublicationOwner() (string, error) {
	value := make([]byte, gossipPublicationOwnerBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "gossip-publisher-" + hex.EncodeToString(value), nil
}

func (worker *GossipPublicationWorker) Trigger() {
	if worker == nil || worker.trigger == nil {
		return
	}
	select {
	case worker.trigger <- struct{}{}:
	default:
	}
}

func (worker *GossipPublicationWorker) Snapshot() GossipPublicationWorkerSnapshot {
	if worker == nil {
		return GossipPublicationWorkerSnapshot{State: GossipPublicationWorkerFailed}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

func (worker *GossipPublicationWorker) Run(ctx context.Context) error {
	if worker == nil || worker.backend == nil || worker.sessions == nil || worker.clock == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrGossipPublicationWorker)
	}
	worker.mu.Lock()
	if worker.started {
		worker.mu.Unlock()
		return ErrGossipPublicationWorkerRunning
	}
	worker.started = true
	worker.snapshot.State = GossipPublicationWorkerRunning
	worker.mu.Unlock()
	failed := false
	defer func() {
		worker.mu.Lock()
		if !failed {
			worker.snapshot.State = GossipPublicationWorkerStopped
		}
		worker.mu.Unlock()
	}()

	ticker := time.NewTicker(worker.period)
	defer ticker.Stop()
	for {
		if err := worker.runCycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			worker.mu.Lock()
			worker.snapshot.State = GossipPublicationWorkerFailed
			worker.mu.Unlock()
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-worker.trigger:
		}
	}
}

func (worker *GossipPublicationWorker) runCycle(ctx context.Context) error {
	at := worker.clock.Now().Round(0).UTC()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if at.IsZero() {
		return fmt.Errorf("%w: trusted clock returned zero", ErrGossipPublicationWorker)
	}
	channels, err := worker.backend.channels(ctx)
	if err != nil {
		return fmt.Errorf("%w: list publication Channels: %v", ErrGossipPublicationWorker, err)
	}
	if len(channels) > model.MaxChannelsPerNode {
		return fmt.Errorf("%w: Channel bound exceeded", ErrGossipPublicationWorker)
	}
	channels = append([]model.ChannelID(nil), channels...)
	sort.Slice(channels, func(left, right int) bool {
		return channels[left].String() < channels[right].String()
	})
	for index, channelID := range channels {
		if channelID.IsZero() || (index > 0 && channelID == channels[index-1]) {
			return fmt.Errorf("%w: invalid publication Channel set", ErrGossipPublicationWorker)
		}
	}
	worker.recordCycle(at)
	for _, channelID := range channels {
		if err := worker.processChannel(ctx, channelID, at); err != nil {
			return err
		}
	}
	return nil
}

func (worker *GossipPublicationWorker) processChannel(ctx context.Context,
	channelID model.ChannelID, claimAt time.Time,
) error {
	leaseUntil := claimAt.Add(gossipPublicationLease)
	result, err := worker.backend.claim(ctx, channelID, worker.owner, claimAt, leaseUntil)
	if err != nil {
		if errors.Is(err, store.ErrGossipPublicationAuthority) ||
			errors.Is(err, store.ErrGossipPublicationStale) {
			worker.recordStale()
			return nil
		}
		return fmt.Errorf("%w: claim publication: %v", ErrGossipPublicationWorker, err)
	}
	if !result.Claimed {
		return nil
	}
	lease := result.Lease
	if lease.Fence.ChannelID != channelID || lease.Record.Publication().Key().ChannelID() != channelID {
		return fmt.Errorf("%w: Store returned cross-Channel lease", ErrGossipPublicationWorker)
	}
	worker.recordClaim(true)
	defer worker.recordClaim(false)
	session, err := worker.sessions.session(channelID)
	if err == nil {
		publishCtx, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
		err = session.Publish(publishCtx, lease.Record.Publication())
		cancel()
	}
	settleAt := worker.clock.Now().Round(0).UTC()
	if settleAt.IsZero() {
		return fmt.Errorf("%w: trusted clock returned zero", ErrGossipPublicationWorker)
	}
	if err == nil {
		if markErr := worker.backend.mark(ctx, lease.Fence, settleAt); markErr == nil {
			worker.recordPublished()
			return nil
		} else if errors.Is(markErr, store.ErrGossipPublicationStale) ||
			errors.Is(markErr, store.ErrGossipPublicationAuthority) || ctx.Err() != nil {
			worker.recordStale()
			return nil
		} else {
			return fmt.Errorf("%w: mark publication: %v", ErrGossipPublicationWorker, markErr)
		}
	}
	if ctx.Err() != nil || !settleAt.Before(lease.Fence.LeaseUntil) {
		return nil
	}
	retryAt := settleAt.Add(gossipPublicationBackoff(lease.Fence.Attempt))
	retryErr := worker.backend.retry(ctx, lease.Fence, settleAt, retryAt,
		"Gossip publication transport unavailable")
	if retryErr == nil {
		worker.recordRetry()
		return nil
	}
	if errors.Is(retryErr, store.ErrGossipPublicationStale) ||
		errors.Is(retryErr, store.ErrGossipPublicationAuthority) {
		worker.recordStale()
		return nil
	}
	return fmt.Errorf("%w: retry publication: %v", ErrGossipPublicationWorker, retryErr)
}

func gossipPublicationBackoff(attempt uint32) time.Duration {
	delay := gossipPublicationRetryMinimum
	for current := uint32(1); current < attempt && delay < gossipPublicationRetryMaximum; current++ {
		delay *= 2
	}
	if delay > gossipPublicationRetryMaximum {
		return gossipPublicationRetryMaximum
	}
	return delay
}

func (backend durableGossipPublicationBackend) channels(ctx context.Context) ([]model.ChannelID, error) {
	mesh, err := backend.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return nil, err
	}
	channels := make([]model.ChannelID, 0, len(mesh.Channels()))
	for _, authority := range mesh.Channels() {
		channel := authority.Channel()
		if channel.Status() == model.ChannelActive && channel.TopicState() == model.TopicJoined {
			channels = append(channels, channel.ID())
		}
	}
	sort.Slice(channels, func(left, right int) bool {
		return channels[left].String() < channels[right].String()
	})
	return channels, nil
}

func (backend durableGossipPublicationBackend) claim(ctx context.Context, channelID model.ChannelID,
	owner string, at, leaseUntil time.Time,
) (store.GossipPublicationClaimResult, error) {
	return backend.store.ClaimGossipPublication(ctx, store.GossipPublicationClaimSpec{
		ChannelID: channelID, LeaseOwner: owner, At: at, LeaseUntil: leaseUntil})
}

func (backend durableGossipPublicationBackend) mark(ctx context.Context,
	fence store.GossipPublicationFence, at time.Time,
) error {
	_, err := backend.store.MarkGossipPublicationPublished(ctx,
		store.MarkGossipPublicationPublishedSpec{Fence: fence, At: at})
	return err
}

func (backend durableGossipPublicationBackend) retry(ctx context.Context,
	fence store.GossipPublicationFence, at, next time.Time, diagnostic string,
) error {
	_, err := backend.store.RetryGossipPublication(ctx, store.RetryGossipPublicationSpec{
		Fence: fence, At: at, NextAttemptAt: next, Diagnostic: diagnostic})
	return err
}

func (sessions meshGossipPublicationSessions) session(channelID model.ChannelID) (gossipPublicationSession, error) {
	return sessions.runtime.Session(channelID)
}

func (worker *GossipPublicationWorker) recordCycle(at time.Time) {
	worker.mu.Lock()
	worker.snapshot.Cycles++
	worker.snapshot.LastCycleAt = at
	worker.mu.Unlock()
}

func (worker *GossipPublicationWorker) recordClaim(start bool) {
	worker.mu.Lock()
	if start {
		worker.snapshot.Claims++
		worker.snapshot.InFlight++
		if worker.snapshot.InFlight > worker.snapshot.MaximumActive {
			worker.snapshot.MaximumActive = worker.snapshot.InFlight
		}
	} else {
		worker.snapshot.InFlight--
	}
	worker.mu.Unlock()
}

func (worker *GossipPublicationWorker) recordPublished() {
	worker.mu.Lock()
	worker.snapshot.Published++
	worker.mu.Unlock()
}

func (worker *GossipPublicationWorker) recordRetry() {
	worker.mu.Lock()
	worker.snapshot.Retries++
	worker.mu.Unlock()
}

func (worker *GossipPublicationWorker) recordStale() {
	worker.mu.Lock()
	worker.snapshot.Stale++
	worker.mu.Unlock()
}
