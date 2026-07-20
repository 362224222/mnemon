package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelRuntimePublishesVacuousReadinessAndUsesInjectedTimer(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)
	st := openEmptyChannelRuntimeStore(t, "runtime-empty", now)
	timers := newChannelRuntimeTestTimerFactory()
	runtime, err := NewChannelRuntime(ChannelRuntimeOptions{Store: st,
		Transport: channelRuntimeNoopTransport{}, Authority: channelRuntimeNoopAuthority{},
		Clock: channelRuntimeFixedClock{now: now}, timers: timers})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if err := runtime.Readiness(readyCtx); err != nil {
		t.Fatal(err)
	}
	timer := channelRuntimeReceive(t, timers.created)
	if timer.delay != channelRuntimeReconcilePeriod {
		t.Fatalf("first reconcile delay = %s", timer.delay)
	}
	snapshot := runtime.Snapshot()
	if !snapshot.LocalTopicsReady || !snapshot.FullyConverged || snapshot.Targets != 0 ||
		snapshot.State != ChannelRuntimeRunning {
		t.Fatalf("empty runtime snapshot = %#v", snapshot)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrChannelRuntimeRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := channelRuntimeReceive(t, done); err != nil {
		t.Fatal(err)
	}
	if runtime.Snapshot().State != ChannelRuntimeStopped || !timer.stopped {
		t.Fatalf("stopped runtime snapshot = %#v, timer stopped=%t",
			runtime.Snapshot(), timer.stopped)
	}
}

func TestChannelRuntimeReadinessPrecedesBlockedRemoteConvergence(t *testing.T) {
	fixture := newRealChannelAuthorityCoordinatorFixture(t)
	_, err := fixture.controller.ReconcileRemoteRoster(context.Background(), ChannelRuntimeRosterUpdate{
		ChannelID:           fixture.channel.Channel().ID(),
		AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
		Records:             []model.Member{fixture.remote.Member()}, At: fixture.remote.Member().CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	transport := newChannelRuntimeBlockingTransport()
	runtime, err := NewChannelRuntime(ChannelRuntimeOptions{Store: fixture.store,
		Transport: transport, Authority: fixture.controller,
		Clock: channelRuntimeFixedClock{now: fixture.at.Add(3 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if err := runtime.Readiness(readyCtx); err != nil {
		t.Fatal(err)
	}
	channelRuntimeReceive(t, transport.helloStarted)
	if snapshot := runtime.Snapshot(); !snapshot.LocalTopicsReady || snapshot.InFlight != 1 {
		t.Fatalf("blocked remote snapshot = %#v", snapshot)
	}
	cancel()
	if err := channelRuntimeReceive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRuntimeTriggerCoalescesAndNilTimerFailsClosed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 2, 0, 0, 0, time.UTC)
	st := openEmptyChannelRuntimeStore(t, "runtime-timer", now)
	runtime, err := NewChannelRuntime(ChannelRuntimeOptions{Store: st,
		Transport: channelRuntimeNoopTransport{}, Authority: channelRuntimeNoopAuthority{},
		Clock: channelRuntimeFixedClock{now: now}, timers: channelRuntimeNilTimerFactory{}})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		runtime.Trigger()
	}
	if len(runtime.trigger) != 1 {
		t.Fatalf("coalesced trigger count = %d", len(runtime.trigger))
	}
	<-runtime.trigger
	err = runtime.Run(context.Background())
	if !errors.Is(err, ErrChannelRuntime) || runtime.Snapshot().State != ChannelRuntimeFailed {
		t.Fatalf("nil timer result = (%v,%#v)", err, runtime.Snapshot())
	}
	if readyErr := runtime.Readiness(context.Background()); !errors.Is(readyErr, ErrChannelRuntime) {
		t.Fatalf("terminal readiness error = %v", readyErr)
	}
}

func TestChannelRuntimeReadinessIsLiveStartupLatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 2, 30, 0, 0, time.UTC)
	runtime, err := NewChannelRuntime(ChannelRuntimeOptions{
		Store:     openEmptyChannelRuntimeStore(t, "runtime-readiness-latch", now),
		Transport: channelRuntimeNoopTransport{}, Authority: channelRuntimeNoopAuthority{},
		Clock: channelRuntimeFixedClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.claimRun() {
		t.Fatal("failed to claim readiness test runtime")
	}
	runtime.publishLocalTopicsReady(1, now)
	runtime.recordCycle(now.Add(time.Second), channelRuntimeCycle{activeTopics: 1})
	if snapshot := runtime.Snapshot(); snapshot.LocalTopicsReady {
		t.Fatalf("degraded topic snapshot = %#v", snapshot)
	}
	if err := runtime.Readiness(context.Background()); err != nil {
		t.Fatalf("live startup latch error = %v", err)
	}
	runtime.finishRun(nil)
	if err := runtime.Readiness(context.Background()); !errors.Is(err, ErrChannelRuntime) {
		t.Fatalf("stopped readiness error = %v", err)
	}
}

type channelRuntimeFixedClock struct{ now time.Time }

func (clock channelRuntimeFixedClock) Now() time.Time { return clock.now }

type channelRuntimeNoopAuthority struct{}

func (channelRuntimeNoopAuthority) ReconcileRemoteRoster(context.Context,
	ChannelRuntimeRosterUpdate,
) (model.VerifiedRoster, error) {
	return model.VerifiedRoster{}, errors.New("unexpected roster reconciliation")
}

type channelRuntimeNoopTransport struct{}

func (channelRuntimeNoopTransport) EnsureChannelTopic(context.Context, model.ChannelID) error {
	return nil
}
func (channelRuntimeNoopTransport) HasCurrentChannelTopic(model.ChannelID) bool { return false }
func (channelRuntimeNoopTransport) Hello(context.Context, model.PeerID,
	peer.MemberHello,
) (peer.MemberHelloAck, error) {
	return peer.MemberHelloAck{}, errors.New("unexpected Hello")
}
func (channelRuntimeNoopTransport) Sync(context.Context, model.PeerID,
	peer.SyncRequest,
) (peer.ChannelMemberSyncResult, error) {
	return peer.ChannelMemberSyncResult{}, errors.New("unexpected Sync")
}
func (channelRuntimeNoopTransport) Baseline(context.Context, model.PeerID,
	peer.DataBaseline,
) (peer.DataBaselineAck, error) {
	return peer.DataBaselineAck{}, errors.New("unexpected Baseline")
}

type channelRuntimeBlockingTransport struct {
	mu           sync.Mutex
	current      map[model.ChannelID]bool
	helloStarted chan struct{}
}

func newChannelRuntimeBlockingTransport() *channelRuntimeBlockingTransport {
	return &channelRuntimeBlockingTransport{current: make(map[model.ChannelID]bool),
		helloStarted: make(chan struct{}, 1)}
}

func (transport *channelRuntimeBlockingTransport) EnsureChannelTopic(_ context.Context,
	channelID model.ChannelID,
) error {
	transport.mu.Lock()
	transport.current[channelID] = true
	transport.mu.Unlock()
	return nil
}

func (transport *channelRuntimeBlockingTransport) HasCurrentChannelTopic(
	channelID model.ChannelID,
) bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.current[channelID]
}

func (transport *channelRuntimeBlockingTransport) Hello(ctx context.Context, _ model.PeerID,
	_ peer.MemberHello,
) (peer.MemberHelloAck, error) {
	select {
	case transport.helloStarted <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return peer.MemberHelloAck{}, ctx.Err()
}

func (*channelRuntimeBlockingTransport) Sync(context.Context, model.PeerID,
	peer.SyncRequest,
) (peer.ChannelMemberSyncResult, error) {
	return peer.ChannelMemberSyncResult{}, errors.New("unexpected Sync")
}

func (*channelRuntimeBlockingTransport) Baseline(context.Context, model.PeerID,
	peer.DataBaseline,
) (peer.DataBaselineAck, error) {
	return peer.DataBaselineAck{}, errors.New("unexpected Baseline")
}

type channelRuntimeTestTimerFactory struct{ created chan *channelRuntimeTestTimer }

func newChannelRuntimeTestTimerFactory() *channelRuntimeTestTimerFactory {
	return &channelRuntimeTestTimerFactory{created: make(chan *channelRuntimeTestTimer, 8)}
}

func (factory *channelRuntimeTestTimerFactory) newTimer(delay time.Duration) channelRuntimeTimer {
	timer := &channelRuntimeTestTimer{delay: delay, fire: make(chan time.Time, 1)}
	factory.created <- timer
	return timer
}

type channelRuntimeTestTimer struct {
	delay   time.Duration
	fire    chan time.Time
	stopped bool
}

func (timer *channelRuntimeTestTimer) channel() <-chan time.Time { return timer.fire }
func (timer *channelRuntimeTestTimer) stop()                     { timer.stopped = true }

type channelRuntimeNilTimerFactory struct{}

func (channelRuntimeNilTimerFactory) newTimer(time.Duration) channelRuntimeTimer { return nil }

func openEmptyChannelRuntimeStore(t *testing.T, seed string, now time.Time) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	identity := testkit.NewIdentity(t, seed)
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(),
		OriginEpoch: identity.OriginEpoch(), NextOriginSequence: 1,
		ActiveAssetRevision: "asset-channel-runtime", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-channel-runtime", WorkspaceRoot: t.TempDir(), Host: model.HostCodex,
		Runtime:             model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("channel-runtime-credential")),
		ActiveAssetRevision: "asset-channel-runtime",
		HandlingBudget:      model.DefaultHandlingBudget().JSON(), CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
	return st
}

func channelRuntimeReceive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Channel runtime test signal")
		var zero T
		return zero
	}
}
