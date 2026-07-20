package peer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const runtimeIngressPeriod = 250 * time.Millisecond

var (
	ErrRuntimeIngress        = errors.New("Mnemon Gossip ingress runtime")
	ErrRuntimeIngressRunning = fmt.Errorf("%w: manager has already run", ErrRuntimeIngress)
)

type RuntimeIngressOptions struct {
	Store         RuntimeIngressStore
	Runtime       *MeshRuntime
	Clock         GossipIngressClock
	RepairTrigger GossipRepairTrigger
	InboxTrigger  GossipInboxTrigger
	Period        time.Duration
}

type RuntimeIngressStore interface {
	GossipIngressStore
	ReadChannelMeshAuthority(context.Context) (store.ChannelMeshAuthority, error)
}

type RuntimeIngressState string

const (
	RuntimeIngressIdle    RuntimeIngressState = "idle"
	RuntimeIngressRunning RuntimeIngressState = "running"
	RuntimeIngressStopped RuntimeIngressState = "stopped"
	RuntimeIngressFailed  RuntimeIngressState = "failed"
)

type RuntimeIngressSnapshot struct {
	State         RuntimeIngressState
	Reconciles    uint64
	Starts        uint64
	Restarts      uint64
	Active        int
	MaximumActive int
	LastIssue     GossipIngressDiagnosticCode
}

type RuntimeIngress struct {
	backend  runtimeIngressBackend
	sessions runtimeIngressSessions
	clock    GossipIngressClock
	repair   GossipRepairTrigger
	inbox    GossipInboxTrigger
	period   time.Duration
	trigger  chan struct{}
	exits    chan runtimeIngressExit

	mu       sync.Mutex
	started  bool
	snapshot RuntimeIngressSnapshot
}

type runtimeIngressBackend interface {
	channels(context.Context) ([]model.ChannelID, error)
	GossipIngressStore
}

type runtimeIngressSessions interface {
	session(model.ChannelID) (gossipIngressSession, error)
}

type durableRuntimeIngressBackend struct{ store RuntimeIngressStore }
type meshRuntimeIngressSessions struct{ runtime *MeshRuntime }

type runtimeIngressChild struct {
	channel model.ChannelID
	session gossipIngressSession
	cancel  context.CancelFunc
}

type runtimeIngressExit struct {
	child *runtimeIngressChild
	err   error
}

func NewRuntimeIngress(options RuntimeIngressOptions) (*RuntimeIngress, error) {
	if options.Store == nil || options.Runtime == nil || options.RepairTrigger == nil {
		return nil, fmt.Errorf("%w: Store, mesh runtime and repair trigger are required", ErrRuntimeIngress)
	}
	clock := options.Clock
	if clock == nil {
		clock = wallGossipIngressClock{}
	}
	period := options.Period
	if period == 0 {
		period = runtimeIngressPeriod
	}
	return newRuntimeIngress(durableRuntimeIngressBackend{store: options.Store},
		meshRuntimeIngressSessions{runtime: options.Runtime}, clock,
		options.RepairTrigger, options.InboxTrigger, period)
}

func newRuntimeIngress(backend runtimeIngressBackend, sessions runtimeIngressSessions,
	clock GossipIngressClock, repair GossipRepairTrigger, inbox GossipInboxTrigger,
	period time.Duration,
) (*RuntimeIngress, error) {
	if backend == nil || sessions == nil || clock == nil || repair == nil ||
		period <= 0 || period > runtimeIngressPeriod {
		return nil, fmt.Errorf("%w: complete bounded configuration is required", ErrRuntimeIngress)
	}
	return &RuntimeIngress{backend: backend, sessions: sessions, clock: clock,
		repair: repair, inbox: inbox, period: period, trigger: make(chan struct{}, 1),
		exits:    make(chan runtimeIngressExit, model.MaxChannelsPerNode),
		snapshot: RuntimeIngressSnapshot{State: RuntimeIngressIdle}}, nil
}

func (manager *RuntimeIngress) Trigger() {
	if manager == nil || manager.trigger == nil {
		return
	}
	select {
	case manager.trigger <- struct{}{}:
	default:
	}
}

func (manager *RuntimeIngress) Snapshot() RuntimeIngressSnapshot {
	if manager == nil {
		return RuntimeIngressSnapshot{State: RuntimeIngressFailed}
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.snapshot
}

func (manager *RuntimeIngress) Run(ctx context.Context) error {
	if manager == nil || manager.backend == nil || manager.sessions == nil ||
		manager.clock == nil || manager.repair == nil || ctx == nil {
		return fmt.Errorf("%w: manager is unavailable", ErrRuntimeIngress)
	}
	manager.mu.Lock()
	if manager.started {
		manager.mu.Unlock()
		return ErrRuntimeIngressRunning
	}
	manager.started = true
	manager.snapshot.State = RuntimeIngressRunning
	manager.mu.Unlock()
	children := make(map[model.ChannelID]*runtimeIngressChild)
	retries := make(map[model.ChannelID]time.Time)
	var wait sync.WaitGroup
	failed := false
	defer func() {
		for _, child := range children {
			child.cancel()
		}
		wait.Wait()
		manager.mu.Lock()
		manager.snapshot.Active = 0
		if !failed {
			manager.snapshot.State = RuntimeIngressStopped
		}
		manager.mu.Unlock()
	}()

	ticker := time.NewTicker(manager.period)
	defer ticker.Stop()
	for {
		if err := manager.reconcile(ctx, children, retries, &wait); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			manager.fail()
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case exit := <-manager.exits:
			if err := manager.handleExit(exit, children, retries); err != nil {
				failed = true
				manager.fail()
				return err
			}
		case <-manager.trigger:
		case <-ticker.C:
		}
	}
}

func (manager *RuntimeIngress) reconcile(ctx context.Context,
	children map[model.ChannelID]*runtimeIngressChild, retries map[model.ChannelID]time.Time,
	wait *sync.WaitGroup,
) error {
	channels, err := manager.backend.channels(ctx)
	if err != nil {
		return fmt.Errorf("%w: read active Channels: %v", ErrRuntimeIngress, err)
	}
	if len(channels) > model.MaxChannelsPerNode {
		return fmt.Errorf("%w: Channel bound exceeded", ErrRuntimeIngress)
	}
	channels = append([]model.ChannelID(nil), channels...)
	sort.Slice(channels, func(left, right int) bool {
		return channels[left].String() < channels[right].String()
	})
	desired := make(map[model.ChannelID]struct{}, len(channels))
	for index, channelID := range channels {
		if channelID.IsZero() || (index > 0 && channelID == channels[index-1]) {
			return fmt.Errorf("%w: invalid active Channel set", ErrRuntimeIngress)
		}
		desired[channelID] = struct{}{}
	}
	for channelID, child := range children {
		_, keep := desired[channelID]
		if !keep || !child.session.IsCurrent() {
			child.cancel()
		}
	}
	now := manager.clock.Now().Round(0).UTC()
	if now.IsZero() {
		return fmt.Errorf("%w: trusted clock returned zero", ErrRuntimeIngress)
	}
	for _, channelID := range channels {
		if children[channelID] != nil || now.Before(retries[channelID]) {
			continue
		}
		session, sessionErr := manager.sessions.session(channelID)
		if sessionErr != nil || session == nil || !session.IsCurrent() {
			retries[channelID] = now.Add(gossipIngressFastRetry)
			continue
		}
		ingress, err := newGossipIngress(session, manager.backend, manager.clock, manager.repair)
		if err != nil {
			return fmt.Errorf("%w: construct Channel ingress: %v", ErrRuntimeIngress, err)
		}
		ingress.inbox = manager.inbox
		childCtx, cancel := context.WithCancel(ctx)
		child := &runtimeIngressChild{channel: channelID, session: session, cancel: cancel}
		children[channelID] = child
		delete(retries, channelID)
		manager.recordStart()
		wait.Add(1)
		go manager.runChild(childCtx, child, ingress, wait)
	}
	manager.recordReconcile()
	return nil
}

func (manager *RuntimeIngress) runChild(ctx context.Context, child *runtimeIngressChild,
	ingress *GossipIngress, wait *sync.WaitGroup,
) {
	defer wait.Done()
	err := ingress.Run(ctx)
	// Every started child owns one slot in this MaxChannelsPerNode-sized queue.
	// Completion must reach the manager even when a per-child cancellation is
	// what ended ingress; otherwise a retired Topic generation can remain in
	// children forever and prevent its replacement from starting. During root
	// shutdown the bounded queue also lets every child report before wait joins.
	manager.exits <- runtimeIngressExit{child: child, err: err}
}

func (manager *RuntimeIngress) handleExit(exit runtimeIngressExit,
	children map[model.ChannelID]*runtimeIngressChild, retries map[model.ChannelID]time.Time,
) error {
	if exit.child == nil || children[exit.child.channel] != exit.child {
		return nil
	}
	delete(children, exit.child.channel)
	manager.recordExit()
	now := manager.clock.Now().Round(0).UTC()
	if exit.err == nil {
		retries[exit.child.channel] = now
		return nil
	}
	var failure *GossipIngressFailure
	if !errors.As(exit.err, &failure) {
		return fmt.Errorf("%w: Channel ingress failed: %v", ErrRuntimeIngress, exit.err)
	}
	manager.recordIssue(failure.Code())
	if failure.Code() == GossipIngressDiagnosticPublication {
		return fmt.Errorf("%w: validated publication invariant failed", ErrRuntimeIngress)
	}
	delay := failure.RetryAfter()
	if delay <= 0 {
		delay = gossipIngressFastRetry
	}
	retries[exit.child.channel] = now.Add(delay)
	return nil
}

func (backend durableRuntimeIngressBackend) channels(ctx context.Context) ([]model.ChannelID, error) {
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
	return channels, nil
}

func (backend durableRuntimeIngressBackend) PutPeerInbox(ctx context.Context,
	spec store.PutPeerInboxSpec,
) (store.PutPeerInboxResult, error) {
	return backend.store.PutPeerInbox(ctx, spec)
}

func (sessions meshRuntimeIngressSessions) session(channelID model.ChannelID) (gossipIngressSession, error) {
	return sessions.runtime.Session(channelID)
}

func (manager *RuntimeIngress) recordReconcile() {
	manager.mu.Lock()
	manager.snapshot.Reconciles++
	manager.mu.Unlock()
}

func (manager *RuntimeIngress) recordStart() {
	manager.mu.Lock()
	manager.snapshot.Starts++
	manager.snapshot.Active++
	if manager.snapshot.Active > manager.snapshot.MaximumActive {
		manager.snapshot.MaximumActive = manager.snapshot.Active
	}
	manager.mu.Unlock()
}

func (manager *RuntimeIngress) recordExit() {
	manager.mu.Lock()
	manager.snapshot.Restarts++
	manager.snapshot.Active--
	manager.mu.Unlock()
}

func (manager *RuntimeIngress) recordIssue(code GossipIngressDiagnosticCode) {
	manager.mu.Lock()
	manager.snapshot.LastIssue = code
	manager.mu.Unlock()
}

func (manager *RuntimeIngress) fail() {
	manager.mu.Lock()
	manager.snapshot.State = RuntimeIngressFailed
	manager.mu.Unlock()
}
