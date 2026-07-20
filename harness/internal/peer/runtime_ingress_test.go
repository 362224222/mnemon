package peer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestRuntimeIngressStartsActiveChannelsDeterministicallyAndStopsBounded(t *testing.T) {
	local := testAuthorityPeer(t, "runtime-ingress-bounded-local")
	channels := make([]model.ChannelID, model.MaxChannelsPerNode)
	for index := range channels {
		channels[index] = runtimeIngressChannel(t, fmt.Sprintf("bounded-%02d", index))
	}
	wantOrder := append([]model.ChannelID(nil), channels...)
	sort.Slice(wantOrder, func(left, right int) bool {
		return wantOrder[left].String() < wantOrder[right].String()
	})
	for left, right := 0, len(channels)-1; left < right; left, right = left+1, right-1 {
		channels[left], channels[right] = channels[right], channels[left]
	}

	activity := &runtimeIngressTestActivity{}
	sessions := newRuntimeIngressTestSessions()
	var held *runtimeIngressTestSession
	for index, channelID := range wantOrder {
		session := newRuntimeIngressTestSession(channelID, local.libp2pID, activity)
		if index == 0 {
			session.cancelRelease = make(chan struct{})
			held = session
		}
		sessions.Set(channelID, session)
	}
	backend := &runtimeIngressTestBackend{channelsValue: channels}
	manager := newRuntimeIngressTestManager(t, backend, sessions,
		newRuntimeIngressTestClock(), &runtimeIngressTestTrigger{}, nil)
	run := startRuntimeIngressTest(manager)
	t.Cleanup(func() {
		if held != nil {
			held.releaseCancellation()
		}
		run.Stop(t)
	})

	waitRuntimeIngress(t, "all bounded children to enter Next", func() bool {
		active, maximum := activity.Snapshot()
		return sessions.TotalCalls() == len(wantOrder) && active == len(wantOrder) &&
			maximum == len(wantOrder)
	})
	if got := sessions.CallOrder(); !equalRuntimeIngressChannels(got, wantOrder) {
		t.Fatalf("session start order = %v, want %v", got, wantOrder)
	}
	before := manager.Snapshot().Reconciles
	for count := 0; count < 32; count++ {
		manager.Trigger()
	}
	waitRuntimeIngress(t, "coalesced active reconcile", func() bool {
		return manager.Snapshot().Reconciles > before
	})
	if calls := sessions.TotalCalls(); calls != len(wantOrder) {
		t.Fatalf("active generations gained duplicate children: calls=%d want=%d", calls, len(wantOrder))
	}
	snapshot := manager.Snapshot()
	if snapshot.State != RuntimeIngressRunning || snapshot.Starts != uint64(len(wantOrder)) ||
		snapshot.Active != len(wantOrder) || snapshot.MaximumActive != model.MaxChannelsPerNode {
		t.Fatalf("bounded running snapshot = %#v", snapshot)
	}

	assertRuntimeIngressBoundedShutdown(t, run, held, activity, manager)
}

func assertRuntimeIngressBoundedShutdown(t *testing.T, run *runtimeIngressTestRun,
	held *runtimeIngressTestSession, activity *runtimeIngressTestActivity, manager *RuntimeIngress,
) {
	t.Helper()
	run.Cancel()
	held.WaitCancelled(t)
	select {
	case <-run.done:
		t.Fatal("root shutdown returned before its blocked child drained")
	default:
	}
	held.releaseCancellation()
	if err := run.Wait(t); err != nil {
		t.Fatalf("cancelled Runtime ingress = %v", err)
	}
	active, maximum := activity.Snapshot()
	snapshot := manager.Snapshot()
	if active != 0 || maximum != model.MaxChannelsPerNode || snapshot.Active != 0 ||
		snapshot.MaximumActive != model.MaxChannelsPerNode || snapshot.State != RuntimeIngressStopped {
		t.Fatalf("bounded shutdown = activity %d/%d snapshot %#v", active, maximum, snapshot)
	}
}

func TestRuntimeIngressRotatesOneChildPerSessionGeneration(t *testing.T) {
	local := testAuthorityPeer(t, "runtime-ingress-rotation-local")
	channelID := runtimeIngressChannel(t, "rotation")
	activity := &runtimeIngressTestActivity{}
	oldSession := newRuntimeIngressTestSession(channelID, local.libp2pID, activity)
	newSession := newRuntimeIngressTestSession(channelID, local.libp2pID, activity)
	sessions := newRuntimeIngressTestSessions()
	sessions.Set(channelID, oldSession)
	manager := newRuntimeIngressTestManager(t,
		&runtimeIngressTestBackend{channelsValue: []model.ChannelID{channelID}}, sessions,
		newRuntimeIngressTestClock(), &runtimeIngressTestTrigger{}, nil)
	run := startRuntimeIngressTest(manager)
	t.Cleanup(func() { run.Stop(t) })

	oldSession.WaitNextCalls(t, 1)
	before := manager.Snapshot().Reconciles
	for count := 0; count < 16; count++ {
		manager.Trigger()
	}
	waitRuntimeIngress(t, "stable generation reconcile", func() bool {
		return manager.Snapshot().Reconciles > before
	})
	if sessions.CallCount(channelID) != 1 || oldSession.NextCalls() != 1 {
		t.Fatal("one current generation acquired more than one ingress child")
	}

	oldSession.Retire()
	sessions.Set(channelID, newSession)
	manager.Trigger()
	oldSession.WaitCancelled(t)
	newSession.WaitNextCalls(t, 1)
	before = manager.Snapshot().Reconciles
	manager.Trigger()
	waitRuntimeIngress(t, "replacement generation reconcile", func() bool {
		return manager.Snapshot().Reconciles > before
	})
	if sessions.CallCount(channelID) != 2 || oldSession.NextCalls() != 1 || newSession.NextCalls() != 1 {
		t.Fatalf("generation calls = provider %d old %d new %d, want 2/1/1",
			sessions.CallCount(channelID), oldSession.NextCalls(), newSession.NextCalls())
	}
	snapshot := manager.Snapshot()
	if snapshot.Starts != 2 || snapshot.Restarts != 1 || snapshot.Active != 1 || snapshot.MaximumActive != 1 {
		t.Fatalf("rotation snapshot = %#v", snapshot)
	}
}

func TestRuntimeIngressRetriesOneFailedChannelWithoutStoppingOthers(t *testing.T) {
	for _, test := range []struct {
		name string
		seed string
		code GossipIngressDiagnosticCode
	}{
		{name: "pressure", seed: "pressure", code: GossipIngressDiagnosticPressure},
		{name: "session exit", seed: "session-exit", code: GossipIngressDiagnosticSession},
	} {
		t.Run(test.name, func(t *testing.T) {
			alpha := newGossipIngressFixture(t, "runtime-retry-"+test.seed+"-alpha")
			beta := newGossipIngressFixture(t, "runtime-retry-"+test.seed+"-beta")
			activity := &runtimeIngressTestActivity{}
			failed := newRuntimeIngressTestSession(alpha.channel.ChannelID,
				alpha.local.libp2pID, activity)
			replacement := newRuntimeIngressTestSession(alpha.channel.ChannelID,
				alpha.local.libp2pID, activity)
			unaffected := newRuntimeIngressTestSession(beta.channel.ChannelID,
				beta.local.libp2pID, activity)
			backend := &runtimeIngressTestBackend{channelsValue: []model.ChannelID{
				beta.channel.ChannelID, alpha.channel.ChannelID}}
			if test.code == GossipIngressDiagnosticPressure {
				failed.Push(runtimeIngressNext{publication: alpha.received(alpha.relay.libp2pID)})
				backend.outcomes = map[model.ChannelID][]runtimeIngressStoreOutcome{
					alpha.channel.ChannelID: {{err: store.ErrPeerInboxPressure}},
				}
			} else {
				failed.Push(runtimeIngressNext{err: errors.New("subscription generation exited")})
			}
			sessions := newRuntimeIngressTestSessions()
			sessions.Set(alpha.channel.ChannelID, failed)
			sessions.Set(beta.channel.ChannelID, unaffected)
			clock := newRuntimeIngressTestClock()
			repair := &runtimeIngressTestTrigger{}
			manager := newRuntimeIngressTestManager(t, backend, sessions, clock, repair, nil)
			run := startRuntimeIngressTest(manager)
			t.Cleanup(func() { run.Stop(t) })

			unaffected.WaitNextCalls(t, 1)
			waitRuntimeIngress(t, "failed child settlement", func() bool {
				snapshot := manager.Snapshot()
				return snapshot.Restarts == 1 && snapshot.Active == 1 &&
					snapshot.LastIssue == test.code
			})
			active, maximum := activity.Snapshot()
			if repair.Calls() != 1 || sessions.CallCount(alpha.channel.ChannelID) != 1 ||
				sessions.CallCount(beta.channel.ChannelID) != 1 || unaffected.NextCalls() != 1 ||
				active != 1 || maximum > 2 {
				t.Fatalf("isolated failure = repair %d calls alpha/beta %d/%d beta Next %d activity %d/%d",
					repair.Calls(), sessions.CallCount(alpha.channel.ChannelID),
					sessions.CallCount(beta.channel.ChannelID), unaffected.NextCalls(), active, maximum)
			}

			sessions.Set(alpha.channel.ChannelID, replacement)
			clock.Advance(gossipIngressFastRetry - time.Nanosecond)
			before := manager.Snapshot().Reconciles
			manager.Trigger()
			waitRuntimeIngress(t, "pre-deadline retry scan", func() bool {
				return manager.Snapshot().Reconciles > before
			})
			if sessions.CallCount(alpha.channel.ChannelID) != 1 {
				t.Fatal("failed Channel restarted before its bounded retry deadline")
			}
			clock.Advance(time.Nanosecond)
			manager.Trigger()
			replacement.WaitNextCalls(t, 1)
			if sessions.CallCount(alpha.channel.ChannelID) != 2 ||
				sessions.CallCount(beta.channel.ChannelID) != 1 || unaffected.NextCalls() != 1 {
				t.Fatal("retry restarted the unaffected Channel or duplicated a generation")
			}
			snapshot := manager.Snapshot()
			if snapshot.Starts != 3 || snapshot.Restarts != 1 || snapshot.Active != 2 ||
				snapshot.MaximumActive != 2 {
				t.Fatalf("isolated retry snapshot = %#v", snapshot)
			}
		})
	}
}

func TestRuntimeIngressSignalsInboxAfterDurablePut(t *testing.T) {
	fixture := newGossipIngressFixture(t, "runtime-inbox-trigger")
	activity := &runtimeIngressTestActivity{}
	session := newRuntimeIngressTestSession(fixture.channel.ChannelID,
		fixture.local.libp2pID, activity)
	session.Push(runtimeIngressNext{publication: fixture.received(fixture.relay.libp2pID)})
	sessions := newRuntimeIngressTestSessions()
	sessions.Set(fixture.channel.ChannelID, session)
	backend := &runtimeIngressTestBackend{channelsValue: []model.ChannelID{fixture.channel.ChannelID}}
	repair, inbox := &runtimeIngressTestTrigger{}, &runtimeIngressTestTrigger{}
	manager := newRuntimeIngressTestManager(t, backend, sessions,
		newRuntimeIngressTestClock(), repair, inbox)
	run := startRuntimeIngressTest(manager)
	t.Cleanup(func() { run.Stop(t) })

	backend.WaitPuts(t, 1)
	inbox.WaitCalls(t, 1)
	session.WaitNextCalls(t, 2)
	specs := backend.Specs()
	if len(specs) != 1 || specs[0].Publication.Event().Scope().ChannelID() != fixture.channel.ChannelID ||
		inbox.Calls() != 1 || repair.Calls() != 0 {
		t.Fatalf("durable put signals = specs %d inbox %d repair %d", len(specs), inbox.Calls(), repair.Calls())
	}
}

func TestRuntimeIngressInvalidPublicationFailsClosedAndDrainsSiblings(t *testing.T) {
	local := testAuthorityPeer(t, "runtime-ingress-invalid-local")
	invalidChannel := runtimeIngressChannel(t, "invalid-publication")
	otherChannel := runtimeIngressChannel(t, "invalid-sibling")
	activity := &runtimeIngressTestActivity{}
	invalid := newRuntimeIngressTestSession(invalidChannel, local.libp2pID, activity)
	invalid.Push(runtimeIngressNext{publication: ReceivedPublication{}})
	sibling := newRuntimeIngressTestSession(otherChannel, local.libp2pID, activity)
	sessions := newRuntimeIngressTestSessions()
	sessions.Set(invalidChannel, invalid)
	sessions.Set(otherChannel, sibling)
	backend := &runtimeIngressTestBackend{channelsValue: []model.ChannelID{otherChannel, invalidChannel}}
	manager := newRuntimeIngressTestManager(t, backend, sessions,
		newRuntimeIngressTestClock(), &runtimeIngressTestTrigger{}, &runtimeIngressTestTrigger{})
	run := startRuntimeIngressTest(manager)
	t.Cleanup(func() {
		run.Cancel()
		_ = run.Wait(t)
	})

	err := run.Wait(t)
	if !errors.Is(err, ErrRuntimeIngress) {
		t.Fatalf("invalid publication Run error = %v", err)
	}
	sibling.WaitCancelled(t)
	active, maximum := activity.Snapshot()
	snapshot := manager.Snapshot()
	if snapshot.State != RuntimeIngressFailed || snapshot.LastIssue != GossipIngressDiagnosticPublication ||
		snapshot.Active != 0 || snapshot.Starts != 2 || active != 0 || maximum > 2 || len(backend.Specs()) != 0 {
		t.Fatalf("fail-closed snapshot = %#v activity=%d/%d puts=%d",
			snapshot, active, maximum, len(backend.Specs()))
	}
}

func TestRuntimeIngressRejectsInvalidActiveChannelSetsBeforeStarting(t *testing.T) {
	valid := make([]model.ChannelID, model.MaxChannelsPerNode+1)
	for index := range valid {
		valid[index] = runtimeIngressChannel(t, fmt.Sprintf("invalid-set-%02d", index))
	}
	for _, test := range []struct {
		name     string
		channels []model.ChannelID
	}{
		{name: "over bound", channels: valid},
		{name: "duplicate", channels: []model.ChannelID{valid[0], valid[0]}},
		{name: "zero", channels: []model.ChannelID{{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sessions := newRuntimeIngressTestSessions()
			manager := newRuntimeIngressTestManager(t,
				&runtimeIngressTestBackend{channelsValue: test.channels}, sessions,
				newRuntimeIngressTestClock(), &runtimeIngressTestTrigger{}, nil)
			if err := manager.Run(context.Background()); !errors.Is(err, ErrRuntimeIngress) {
				t.Fatalf("invalid active set Run error = %v", err)
			}
			if sessions.TotalCalls() != 0 || manager.Snapshot().State != RuntimeIngressFailed {
				t.Fatalf("invalid active set started children: calls=%d snapshot=%#v",
					sessions.TotalCalls(), manager.Snapshot())
			}
		})
	}
}

type runtimeIngressTestBackend struct {
	mu            sync.Mutex
	channelsValue []model.ChannelID
	channelsErr   error
	outcomes      map[model.ChannelID][]runtimeIngressStoreOutcome
	specs         []store.PutPeerInboxSpec
	changed       chan struct{}
}

type runtimeIngressStoreOutcome struct {
	result store.PutPeerInboxResult
	err    error
}

var _ runtimeIngressBackend = (*runtimeIngressTestBackend)(nil)

func (backend *runtimeIngressTestBackend) channels(context.Context) ([]model.ChannelID, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]model.ChannelID(nil), backend.channelsValue...), backend.channelsErr
}

func (backend *runtimeIngressTestBackend) PutPeerInbox(_ context.Context,
	spec store.PutPeerInboxSpec,
) (store.PutPeerInboxResult, error) {
	backend.mu.Lock()
	backend.specs = append(backend.specs, spec)
	channelID := spec.Publication.Event().Scope().ChannelID()
	outcome := runtimeIngressStoreOutcome{result: store.PutPeerInboxResult{Disposition: store.PeerInboxStored}}
	if values := backend.outcomes[channelID]; len(values) > 0 {
		outcome = values[0]
		backend.outcomes[channelID] = values[1:]
	}
	if backend.changed != nil {
		select {
		case backend.changed <- struct{}{}:
		default:
		}
	}
	backend.mu.Unlock()
	return outcome.result, outcome.err
}

func (backend *runtimeIngressTestBackend) Specs() []store.PutPeerInboxSpec {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]store.PutPeerInboxSpec(nil), backend.specs...)
}

func (backend *runtimeIngressTestBackend) WaitPuts(t *testing.T, count int) {
	t.Helper()
	backend.mu.Lock()
	if backend.changed == nil {
		backend.changed = make(chan struct{}, 1)
	}
	changed := backend.changed
	backend.mu.Unlock()
	waitRuntimeIngress(t, fmt.Sprintf("%d Inbox puts", count), func() bool {
		if len(backend.Specs()) >= count {
			return true
		}
		select {
		case <-changed:
		default:
		}
		return false
	})
}

type runtimeIngressTestSessions struct {
	mu      sync.Mutex
	current map[model.ChannelID]*runtimeIngressTestSession
	calls   []model.ChannelID
}

var _ runtimeIngressSessions = (*runtimeIngressTestSessions)(nil)

func newRuntimeIngressTestSessions() *runtimeIngressTestSessions {
	return &runtimeIngressTestSessions{current: make(map[model.ChannelID]*runtimeIngressTestSession)}
}

func (sessions *runtimeIngressTestSessions) session(channelID model.ChannelID) (gossipIngressSession, error) {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	sessions.calls = append(sessions.calls, channelID)
	value := sessions.current[channelID]
	if value == nil {
		return nil, errors.New("test TopicSession unavailable")
	}
	return value, nil
}

func (sessions *runtimeIngressTestSessions) Set(channelID model.ChannelID,
	session *runtimeIngressTestSession,
) {
	sessions.mu.Lock()
	sessions.current[channelID] = session
	sessions.mu.Unlock()
}

func (sessions *runtimeIngressTestSessions) CallOrder() []model.ChannelID {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return append([]model.ChannelID(nil), sessions.calls...)
}

func (sessions *runtimeIngressTestSessions) TotalCalls() int {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return len(sessions.calls)
}

func (sessions *runtimeIngressTestSessions) CallCount(channelID model.ChannelID) int {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	count := 0
	for _, called := range sessions.calls {
		if called == channelID {
			count++
		}
	}
	return count
}

type runtimeIngressNext struct {
	publication ReceivedPublication
	err         error
}

type runtimeIngressTestSession struct {
	channelID model.ChannelID
	local     libp2ppeer.ID
	activity  *runtimeIngressTestActivity
	results   chan runtimeIngressNext
	changed   chan struct{}
	cancelled chan struct{}
	cancelOne sync.Once

	mu            sync.Mutex
	current       bool
	nextCalls     int
	cancelRelease chan struct{}
	releaseOne    sync.Once
}

var _ gossipIngressSession = (*runtimeIngressTestSession)(nil)

func newRuntimeIngressTestSession(channelID model.ChannelID, local libp2ppeer.ID,
	activity *runtimeIngressTestActivity,
) *runtimeIngressTestSession {
	return &runtimeIngressTestSession{channelID: channelID, local: local, activity: activity,
		results: make(chan runtimeIngressNext, 4), changed: make(chan struct{}, 1),
		cancelled: make(chan struct{}), current: true}
}

func (session *runtimeIngressTestSession) ChannelID() model.ChannelID { return session.channelID }
func (session *runtimeIngressTestSession) gossipIngressLocalPeerID() libp2ppeer.ID {
	return session.local
}

func (session *runtimeIngressTestSession) IsCurrent() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.current
}

func (session *runtimeIngressTestSession) Next(ctx context.Context) (ReceivedPublication, error) {
	session.mu.Lock()
	session.nextCalls++
	select {
	case session.changed <- struct{}{}:
	default:
	}
	release := session.cancelRelease
	session.mu.Unlock()
	if session.activity != nil {
		session.activity.Enter()
		defer session.activity.Leave()
	}
	select {
	case result := <-session.results:
		return result.publication, result.err
	case <-ctx.Done():
		session.cancelOne.Do(func() { close(session.cancelled) })
		if release != nil {
			<-release
		}
		return ReceivedPublication{}, ctx.Err()
	}
}

func (session *runtimeIngressTestSession) Push(result runtimeIngressNext) {
	session.results <- result
}

func (session *runtimeIngressTestSession) Retire() {
	session.mu.Lock()
	session.current = false
	session.mu.Unlock()
}

func (session *runtimeIngressTestSession) NextCalls() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.nextCalls
}

func (session *runtimeIngressTestSession) WaitNextCalls(t *testing.T, count int) {
	t.Helper()
	waitRuntimeIngress(t, fmt.Sprintf("%d Next calls", count), func() bool {
		if session.NextCalls() >= count {
			return true
		}
		select {
		case <-session.changed:
		default:
		}
		return false
	})
}

func (session *runtimeIngressTestSession) WaitCancelled(t *testing.T) {
	t.Helper()
	select {
	case <-session.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("Runtime ingress child did not observe cancellation")
	}
}

func (session *runtimeIngressTestSession) releaseCancellation() {
	session.releaseOne.Do(func() {
		if session.cancelRelease != nil {
			close(session.cancelRelease)
		}
	})
}

type runtimeIngressTestActivity struct {
	mu      sync.Mutex
	active  int
	maximum int
}

func (activity *runtimeIngressTestActivity) Enter() {
	activity.mu.Lock()
	activity.active++
	if activity.active > activity.maximum {
		activity.maximum = activity.active
	}
	activity.mu.Unlock()
}

func (activity *runtimeIngressTestActivity) Leave() {
	activity.mu.Lock()
	activity.active--
	activity.mu.Unlock()
}

func (activity *runtimeIngressTestActivity) Snapshot() (int, int) {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	return activity.active, activity.maximum
}

type runtimeIngressTestClock struct {
	mu sync.Mutex
	at time.Time
}

func newRuntimeIngressTestClock() *runtimeIngressTestClock {
	return &runtimeIngressTestClock{at: time.Date(2026, 7, 20, 1, 2, 3, 4, time.UTC)}
}

func (clock *runtimeIngressTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.at
}

func (clock *runtimeIngressTestClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.at = clock.at.Add(delta)
	clock.mu.Unlock()
}

type runtimeIngressTestTrigger struct {
	mu      sync.Mutex
	calls   int
	changed chan struct{}
}

func (trigger *runtimeIngressTestTrigger) Trigger() {
	trigger.mu.Lock()
	trigger.calls++
	if trigger.changed != nil {
		select {
		case trigger.changed <- struct{}{}:
		default:
		}
	}
	trigger.mu.Unlock()
}

func (trigger *runtimeIngressTestTrigger) Calls() int {
	trigger.mu.Lock()
	defer trigger.mu.Unlock()
	return trigger.calls
}

func (trigger *runtimeIngressTestTrigger) WaitCalls(t *testing.T, count int) {
	t.Helper()
	trigger.mu.Lock()
	if trigger.changed == nil {
		trigger.changed = make(chan struct{}, 1)
	}
	changed := trigger.changed
	trigger.mu.Unlock()
	waitRuntimeIngress(t, fmt.Sprintf("%d trigger calls", count), func() bool {
		if trigger.Calls() >= count {
			return true
		}
		select {
		case <-changed:
		default:
		}
		return false
	})
}

type runtimeIngressTestRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

func startRuntimeIngressTest(manager *RuntimeIngress) *runtimeIngressTestRun {
	ctx, cancel := context.WithCancel(context.Background())
	run := &runtimeIngressTestRun{cancel: cancel, done: make(chan struct{})}
	go func() {
		err := manager.Run(ctx)
		run.mu.Lock()
		run.err = err
		run.mu.Unlock()
		close(run.done)
	}()
	return run
}

func (run *runtimeIngressTestRun) Cancel() { run.cancel() }

func (run *runtimeIngressTestRun) Wait(t *testing.T) error {
	t.Helper()
	select {
	case <-run.done:
	case <-time.After(3 * time.Second):
		t.Fatal("Runtime ingress Run did not return")
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.err
}

func (run *runtimeIngressTestRun) Stop(t *testing.T) {
	t.Helper()
	run.Cancel()
	if err := run.Wait(t); err != nil {
		t.Errorf("stop Runtime ingress: %v", err)
	}
}

func newRuntimeIngressTestManager(t *testing.T, backend runtimeIngressBackend,
	sessions runtimeIngressSessions, clock GossipIngressClock,
	repair GossipRepairTrigger, inbox GossipInboxTrigger,
) *RuntimeIngress {
	t.Helper()
	manager, err := newRuntimeIngress(backend, sessions, clock, repair, inbox, runtimeIngressPeriod)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func runtimeIngressChannel(t *testing.T, suffix string) model.ChannelID {
	t.Helper()
	channelID, err := model.ParseChannelID("runtime-ingress-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return channelID
}

func equalRuntimeIngressChannels(left, right []model.ChannelID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func waitRuntimeIngress(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
