package node

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberReconcilerRunsBoundedHelloBaselineAndReachability(t *testing.T) {
	t.Parallel()
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-normal")
	backend := &fakeChannelMemberBackend{target: target, hasTarget: true}
	client := &fakeChannelMemberClient{}
	clock := &mutableChannelMemberClock{at: target.channel.UpdatedAt().Add(time.Hour)}
	worker, err := newChannelMemberReconciler(backend, client, clock, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readyCancel()
	if err := worker.Readiness(readyCtx); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 32; index++ {
		go worker.Trigger()
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snapshot := worker.Snapshot()
	if snapshot.State != ChannelMemberReconcilerStopped || snapshot.Cycles == 0 ||
		snapshot.Hellos == 0 || snapshot.Baselines != 1 || snapshot.Reachable == 0 ||
		snapshot.MaximumInFlight != 1 || snapshot.InFlight != 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if client.installCalls() != 1 || backend.confirmCalls() != 1 {
		t.Fatalf("baseline calls = client %d, Store %d", client.installCalls(), backend.confirmCalls())
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrChannelMemberReconcilerRunning) {
		t.Fatalf("second Run error = %v", err)
	}
}

func TestChannelMemberReconcilerSyncsRemoteSuffixBeforeBaseline(t *testing.T) {
	t.Parallel()
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-sync")
	fixture := targetFixture(t, target, "member-reconciler-sync")
	update := fixture.AppendActiveUpdate(t, target.remoteMember.PeerID()).Member()
	helloAck, err := peer.NewMemberHelloAck(peer.MemberHelloAckSpec{ChannelID: target.channel.ID(),
		MissingRecords: []model.Member{update}, RosterHead: fixture.Roster().Head()})
	if err != nil {
		t.Fatal(err)
	}
	syncPage, err := peer.NewSyncPage(peer.SyncPageSpec{ChannelID: target.channel.ID(),
		RosterHead: fixture.Roster().Head(), OwnerSignedRecords: []model.Member{}})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeChannelMemberBackend{target: target, hasTarget: true}
	client := &fakeChannelMemberClient{helloResponse: helloAck, syncResponse: []peer.SyncPage{syncPage}}
	clock := &mutableChannelMemberClock{at: target.channel.UpdatedAt().Add(time.Hour)}
	worker, err := newChannelMemberReconciler(backend, client, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.runCycle(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	merged, head := backend.merged()
	if len(merged) != 1 || merged[0].Head() != update.Head() || head != fixture.Roster().Head() ||
		client.syncCallsCount() != 1 || client.installCalls() != 0 {
		t.Fatalf("roster merge = records %#v head %#v sync %d baseline %d", merged, head,
			client.syncCallsCount(), client.installCalls())
	}
	snapshot := worker.Snapshot()
	if snapshot.Hellos != 1 || snapshot.Syncs != 1 || snapshot.RosterMerges != 1 ||
		snapshot.Baselines != 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestChannelMemberReconcilerBacksOffAndSuppressesPermanentGeneration(t *testing.T) {
	t.Parallel()
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-backoff")
	backend := &fakeChannelMemberBackend{target: target, hasTarget: true}
	client := &fakeChannelMemberClient{helloError: peer.ErrChannelMemberClientTransport}
	clock := &mutableChannelMemberClock{at: target.channel.UpdatedAt().Add(time.Hour)}
	worker, err := newChannelMemberReconciler(backend, client, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := worker.runCycle(ctx, false); err != nil {
		t.Fatal(err)
	}
	clock.advance(500 * time.Millisecond)
	if err := worker.runCycle(ctx, false); err != nil {
		t.Fatal(err)
	}
	if calls := client.helloCallsCount(); calls != 1 {
		t.Fatalf("Hello calls inside backoff = %d", calls)
	}
	clock.advance(2 * time.Second)
	if err := worker.runCycle(ctx, false); err != nil {
		t.Fatal(err)
	}
	client.setHelloError(peer.ErrChannelMemberClientResponse)
	clock.advance(3 * time.Second)
	if err := worker.runCycle(ctx, false); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Hour)
	if err := worker.runCycle(ctx, false); err != nil {
		t.Fatal(err)
	}
	if calls := client.helloCallsCount(); calls != 3 {
		t.Fatalf("permanent generation retried: %d calls", calls)
	}
	if err := worker.runCycle(ctx, true); err != nil {
		t.Fatal(err)
	}
	if calls := client.helloCallsCount(); calls != 4 {
		t.Fatalf("explicit authority wake did not retry: %d calls", calls)
	}
	snapshot := worker.Snapshot()
	if snapshot.RetryableFailures != 2 || snapshot.PermanentFailures != 2 ||
		snapshot.Unreachable != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestChannelMemberReconcilerExposesBoundedRepairWakeups(t *testing.T) {
	t.Parallel()
	first := newChannelMemberReconcilerTarget(t, "member-reconciler-wakeup-first")
	second := newChannelMemberReconcilerTarget(t, "member-reconciler-wakeup-second")
	firstKey, secondKey := first.key(), second.key()
	worker := &ChannelMemberReconciler{trigger: make(chan struct{}, 1),
		schedules: map[channelMemberTargetKey]channelMemberSchedule{
			firstKey:  {permanent: true},
			secondKey: {permanent: true},
		}}
	ctx := context.Background()
	worker.applyWake(false)
	if len(worker.schedules) != 2 {
		t.Fatal("ordinary cycle cleared member schedules without authority")
	}
	if err := worker.TriggerScope(first.channel.ID(), first.remoteMember.PeerID()); err != nil {
		t.Fatal(err)
	}
	if err := worker.ReconcileArtifactReceiver(ctx, first.channel.ID(),
		first.remoteMember.PeerID()); err != nil {
		t.Fatal(err)
	}
	if len(worker.trigger) != 1 {
		t.Fatalf("coalesced trigger depth = %d", len(worker.trigger))
	}
	worker.applyWake(false)
	if _, exists := worker.schedules[firstKey]; exists {
		t.Fatal("scoped repair wake retained its exact target schedule")
	}
	if _, exists := worker.schedules[secondKey]; !exists {
		t.Fatal("scoped repair wake cleared an unrelated target schedule")
	}
	worker.Trigger()
	worker.applyWake(false)
	if len(worker.schedules) != 0 {
		t.Fatalf("global authority wake retained %d schedules", len(worker.schedules))
	}
	if err := worker.ReconcileEventRepair(nil, model.ChannelID{}, model.PeerID{}); !errors.Is(err,
		ErrChannelMemberReconciler) {
		t.Fatalf("invalid repair wake error = %v", err)
	}
}

func newChannelMemberReconcilerTarget(t *testing.T, seed string) channelMemberTarget {
	t.Helper()
	created := time.Date(2026, 7, 21, 2, 0, 0, 0, time.UTC)
	fixture := testkit.NewSignedChannelAt(t, seed, created)
	remote := fixture.AppendActive(t, seed+"-remote").Member()
	local, _ := fixture.Roster().CurrentMember(fixture.Owner().PeerID())
	binding, err := model.NewPeerBinding(local.PeerID(), model.PeerBindingSpec{
		Channel: fixture.Channel(), Roster: fixture.Roster(), PeerID: remote.PeerID(),
		EffectiveAlias: "remote", State: model.BindingPending,
		Reachability: model.ReachabilityUnknown, JoinedAt: remote.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	return channelMemberTarget{channel: fixture.Channel(), roster: fixture.Roster(),
		localMember: local, remoteMember: remote, binding: binding}
}

func targetFixture(t *testing.T, target channelMemberTarget, seed string) *testkit.SignedChannel {
	t.Helper()
	fixture := testkit.NewSignedChannelForOwnerAt(t, seed, testkit.NewIdentity(t, "owner:"+seed),
		target.channel.CreatedAt())
	fixture.AppendActiveIdentity(t, testkit.NewIdentity(t, seed+"-remote"))
	return fixture
}

type mutableChannelMemberClock struct {
	mu sync.Mutex
	at time.Time
}

func (clock *mutableChannelMemberClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.at
}

func (clock *mutableChannelMemberClock) advance(delta time.Duration) {
	clock.mu.Lock()
	clock.at = clock.at.Add(delta)
	clock.mu.Unlock()
}

type fakeChannelMemberBackend struct {
	mu                  sync.Mutex
	target              channelMemberTarget
	hasTarget           bool
	leaveTarget         channelMemberLeaveTarget
	hasLeave            bool
	leaveStarts         int
	leaveFails          int
	leaveFailedAttempts uint64
	leaveFailure        store.ChannelLeaveFailureCode
	leaveSettle         int
	mergeValues         []model.Member
	mergeHead           model.RecordHead
	confirms            int
}

func (backend *fakeChannelMemberBackend) leaveTargets(context.Context,
	time.Time,
) ([]channelMemberLeaveTarget, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if !backend.hasLeave {
		return nil, nil
	}
	return []channelMemberLeaveTarget{backend.leaveTarget}, nil
}

func (backend *fakeChannelMemberBackend) startLeave(_ context.Context,
	_ channelMemberLeaveTarget, _, _ time.Time,
) error {
	backend.mu.Lock()
	backend.leaveStarts++
	backend.mu.Unlock()
	return nil
}

func (backend *fakeChannelMemberBackend) failLeave(_ context.Context,
	_ channelMemberLeaveTarget, attempts uint64, _ time.Time,
	failure store.ChannelLeaveFailureCode, _ time.Time,
) error {
	backend.mu.Lock()
	backend.leaveFails++
	backend.leaveFailedAttempts = attempts
	backend.leaveFailure = failure
	backend.hasLeave = false
	backend.mu.Unlock()
	return nil
}

func (backend *fakeChannelMemberBackend) settleLeave(_ context.Context,
	_ channelMemberLeaveTarget, _ model.SignedChannelLeaveReceipt, _ time.Time,
) error {
	backend.mu.Lock()
	backend.leaveSettle++
	backend.hasLeave = false
	backend.mu.Unlock()
	return nil
}

func (backend *fakeChannelMemberBackend) targets(context.Context) ([]channelMemberTarget, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if !backend.hasTarget {
		return nil, nil
	}
	return []channelMemberTarget{backend.target}, nil
}

func (backend *fakeChannelMemberBackend) merge(_ context.Context, _ channelMemberTarget,
	records []model.Member, head model.RecordHead, _ time.Time,
) error {
	backend.mu.Lock()
	backend.mergeValues = append([]model.Member(nil), records...)
	backend.mergeHead = head
	backend.mu.Unlock()
	return nil
}

func (backend *fakeChannelMemberBackend) reserve(_ context.Context, target channelMemberTarget,
	_ time.Time,
) (store.ChannelDataBaseline, error) {
	return store.ChannelDataBaseline{ChannelID: target.channel.ID(),
		OriginPeerID: target.localMember.PeerID(), OriginEpoch: target.localMember.OriginEpoch(),
		BaselineChannelSequence: 7}, nil
}

func (backend *fakeChannelMemberBackend) confirm(context.Context, channelMemberTarget,
	peer.DataBaselineAck, time.Time,
) error {
	backend.mu.Lock()
	backend.confirms++
	backend.target.outboundReady = true
	backend.mu.Unlock()
	return nil
}

func (*fakeChannelMemberBackend) reachability(context.Context, channelMemberTarget,
	model.Reachability, time.Time,
) error {
	return nil
}

func (backend *fakeChannelMemberBackend) merged() ([]model.Member, model.RecordHead) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]model.Member(nil), backend.mergeValues...), backend.mergeHead
}

func (backend *fakeChannelMemberBackend) confirmCalls() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.confirms
}

type fakeChannelMemberClient struct {
	mu            sync.Mutex
	helloResponse peer.MemberHelloAck
	helloError    error
	syncResponse  []peer.SyncPage
	hellos        int
	syncs         int
	installs      int
	leaveResponse peer.LeaveReceipt
	leaveError    error
	leaves        int
}

func (client *fakeChannelMemberClient) Leave(context.Context, model.PeerID, []byte,
	peer.LeaveRequest,
) (peer.LeaveReceipt, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.leaves++
	return client.leaveResponse, client.leaveError
}

func (client *fakeChannelMemberClient) Hello(_ context.Context, _ model.PeerID, _ []byte,
	hello peer.MemberHello,
) (peer.MemberHelloAck, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.hellos++
	if client.helloError != nil {
		return peer.MemberHelloAck{}, client.helloError
	}
	if !client.helloResponse.IsZero() {
		return client.helloResponse, nil
	}
	return peer.NewMemberHelloAck(peer.MemberHelloAckSpec{ChannelID: hello.ChannelID(),
		RosterHead: hello.KnownRosterHead()})
}

func (client *fakeChannelMemberClient) Sync(context.Context, model.PeerID, []byte,
	peer.SyncRequest,
) ([]peer.SyncPage, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.syncs++
	return append([]peer.SyncPage(nil), client.syncResponse...), nil
}

func (client *fakeChannelMemberClient) InstallBaseline(_ context.Context, _ model.PeerID,
	_ []byte, baseline peer.DataBaseline,
) (peer.DataBaselineAck, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.installs++
	return peer.NewDataBaselineAck(peer.DataBaselineSpec{ChannelID: baseline.ChannelID(),
		OriginPeerID: baseline.OriginPeerID(), OriginEpoch: baseline.OriginEpoch(),
		BaselineChannelSequence: baseline.BaselineChannelSequence()})
}

func (client *fakeChannelMemberClient) setHelloError(err error) {
	client.mu.Lock()
	client.helloError = err
	client.mu.Unlock()
}

func (client *fakeChannelMemberClient) helloCallsCount() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.hellos
}

func (client *fakeChannelMemberClient) syncCallsCount() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.syncs
}

func (client *fakeChannelMemberClient) installCalls() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.installs
}
