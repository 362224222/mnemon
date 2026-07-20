package node

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestChannelAuthorityCoordinatorJoinSerializesAndSessionIsOneUse(t *testing.T) {
	at := time.Date(2026, 7, 20, 5, 0, 0, 0, time.UTC)
	st, joiner := newInitializedChannelAuthorityStore(t, "node-managed-join-serialized", at)
	owner := testkit.NewIdentity(t, "node-managed-join-serialized-owner")
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-managed-join-serialized-channel",
		owner, at)
	join := realChannelCreateSpecWithGrant(t, channel, owner, at,
		"grant-node-managed-join-serialized").Token
	trace := []string{}
	runtime := &blockingChannelJoinRuntime{joiner: joiner, at: at.Add(time.Minute),
		entered: make(chan struct{}), resume: make(chan struct{}), trace: &trace}
	coordinator, err := newChannelAuthorityCoordinator(st, runtime,
		newChannelAuthorityTestSigner(t, joiner))
	if err != nil {
		t.Fatal(err)
	}

	joinDone := make(chan error, 1)
	go func() {
		_, joinErr := coordinator.JoinChannel(context.Background(), ChannelJoinSpec{Token: join,
			DisplayLabel: joiner.DisplayName(), LocalAlias: "serialized-team"})
		joinDone <- joinErr
	}()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("managed join did not enter its semantic session")
	}
	localChannel := testkit.NewSignedChannelForOwnerAt(t, "node-managed-join-local-create",
		joiner, at.Add(2*time.Minute))
	createSpec := realChannelCreateSpecWithGrant(t, localChannel, joiner, at.Add(2*time.Minute),
		"grant-node-managed-join-local-create")
	createDone := make(chan error, 1)
	go func() {
		_, createErr := coordinator.CreateChannel(context.Background(), createSpec)
		createDone <- createErr
	}()
	select {
	case err := <-createDone:
		t.Fatalf("create escaped the managed join authority token: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(runtime.resume)
	if err := <-joinDone; !errors.Is(err, errStoppedJoinTest) {
		t.Fatalf("managed join error = %v", err)
	}
	if err := <-createDone; err != nil {
		t.Fatalf("serialized create failed: %v", err)
	}
	if !errors.Is(runtime.secondBeginErr, ErrChannelAuthority) || runtime.released != 1 {
		t.Fatalf("session reuse/release = (%v,%d)", runtime.secondBeginErr, runtime.released)
	}
}

func TestChannelAuthorityJoinSessionMapsOnlyBeginBusinessFailures(t *testing.T) {
	at := time.Date(2026, 7, 20, 5, 30, 0, 0, time.UTC)
	st, joiner := newInitializedChannelAuthorityStore(t, "node-managed-join-phases", at)
	owner := testkit.NewIdentity(t, "node-managed-join-phases-owner")
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-managed-join-phases-channel", owner, at)
	token := realChannelCreateSpecWithGrant(t, channel, owner, at,
		"grant-node-managed-join-phases").Token
	frozen, err := freezeChannelJoinSpec(ChannelJoinSpec{Token: token,
		DisplayLabel: joiner.DisplayName(), LocalAlias: "phase-team"})
	if err != nil {
		t.Fatal(err)
	}
	control := peer.ChannelJoinPrepareControl{AuthenticatedLocalPeerID: joiner.PeerID(),
		LocalPublicKey: joiner.PublicKey(), AdvertisedMultiaddrs: joiner.Multiaddrs(),
		Descriptor: channel.Descriptor(), GrantID: token.Payload().GrantID(),
		LocalAlias: "phase-team", At: at.Add(time.Minute)}
	for _, test := range []struct {
		name     string
		cause    error
		wantCode peer.ChannelProtocolErrorCode
	}{
		{name: "node limit", cause: store.ErrNodeChannelLimit,
			wantCode: peer.ChannelErrorNodeChannelLimit},
		{name: "safe join conflict", cause: store.ErrChannelJoinConflict,
			wantCode: peer.ChannelErrorRosterConflict},
		{name: "authority invariant remains internal", cause: store.ErrChannelAuthorityInvariant},
		{name: "invalid Store input remains internal", cause: store.ErrChannelJoinInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapped := &channelJoinFailureStore{Store: st, prepareErr: test.cause}
			trace := []string{}
			session := &channelAuthorityJoinSession{store: wrapped,
				runtime: newChannelAuthorityRuntimeTrace(&trace), spec: frozen}
			_, err := session.BeginChannelJoin(context.Background(), control)
			var failure *peer.ChannelJoinControlFailure
			if test.wantCode != "" {
				if !errors.As(err, &failure) || failure.Code() != test.wantCode ||
					errors.Is(err, test.cause) {
					t.Fatalf("typed begin failure = %v / %#v", err, failure)
				}
			} else if !errors.Is(err, ErrChannelAuthority) || !errors.Is(err, test.cause) ||
				errors.As(err, &failure) {
				t.Fatalf("internal begin failure = %v / %#v", err, failure)
			}
		})
	}

	wrapped := &channelJoinFailureStore{Store: st, markErr: store.ErrNodeChannelLimit}
	trace := []string{}
	session := &channelAuthorityJoinSession{store: wrapped,
		runtime: newChannelAuthorityRuntimeTrace(&trace), spec: frozen}
	if _, err := session.BeginChannelJoin(context.Background(), control); err != nil {
		t.Fatal(err)
	}
	err = session.MarkChannelJoinCommitUnknown(context.Background(), at.Add(2*time.Minute))
	var failure *peer.ChannelJoinControlFailure
	if !errors.Is(err, ErrChannelAuthority) || !errors.Is(err, store.ErrNodeChannelLimit) ||
		errors.As(err, &failure) {
		t.Fatalf("post-begin Store failure escaped as a protocol decision: %v / %#v", err, failure)
	}
	if err := session.ReleaseChannelJoinReservation(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChannelAuthorityCoordinatorJoinsRealOwnerAndHidesTerminalReplica(t *testing.T) {
	fixture := newRealManagedJoinFixture(t, "production")
	spec := ChannelJoinSpec{Token: fixture.token, DisplayLabel: fixture.joiner.DisplayName(),
		LocalAlias: "managed-production-team"}
	injected := errors.New("injected preinstall response loss")
	failingStore := &joinedInstallPrepareFailureStore{Store: fixture.joinerStore, failure: injected}
	failing, err := newChannelAuthorityCoordinator(failingStore,
		meshChannelAuthorityRuntime{runtime: fixture.runtime},
		newChannelAuthorityTestSigner(t, fixture.joiner))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := failing.JoinChannel(fixture.ctx, spec); err == nil ||
		!errors.Is(err, peer.ErrChannelEnrollmentProtocol) || result.Status().Valid() {
		t.Fatalf("owner commit before local install = (%#v,%v)", result, err)
	}
	beforeRecovery, err := fixture.joinerStore.ReadChannelMeshAuthority(context.Background())
	if err != nil || len(beforeRecovery.Channels()) != 0 {
		t.Fatalf("pre-recovery local mesh = (%#v,%v)", beforeRecovery, err)
	}
	fresh, err := fixture.controller.JoinChannel(fixture.ctx, spec)
	assertFreshManagedJoin(t, fixture, spec, fresh, err)
	replayed, err := fixture.controller.JoinChannel(fixture.ctx, spec)
	assertReplayedManagedJoin(t, fresh, replayed, err)
	ownerMesh, err := fixture.ownerStore.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ownerMesh.Channels()) != 1 {
		t.Fatalf("owner mesh before terminal update = (%#v,%v)", ownerMesh, err)
	}
	ownerAuthority := ownerMesh.Channels()[0]
	terminalRoster := appendTerminalJoinMember(t, fixture.channel.Descriptor(),
		ownerAuthority.Roster(), fixture.owner, fixture.joiner.PeerID(), model.MemberRevoked)
	terminalMember := terminalRoster.Members()[len(terminalRoster.Members())-1]
	merged, err := fixture.ownerStore.MergeChannelRoster(context.Background(),
		store.MergeChannelRosterSpec{ChannelID: fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.joiner.PeerID(), Records: []model.Member{terminalMember},
			At: terminalMember.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Status != store.ChannelRosterApplied {
		t.Fatalf("owner terminal merge = (%#v,%v)", merged, err)
	}
	terminal, err := fixture.controller.JoinChannel(fixture.ctx, spec)
	assertTerminalManagedJoin(t, terminalRoster, terminal, err)
	joinerMesh, err := fixture.joinerStore.ReadChannelMeshAuthority(context.Background())
	assertDurableTerminalJoin(t, joinerMesh, model.ChannelLeft, err)

	closedRoster := appendTerminalJoinMember(t, fixture.channel.Descriptor(), terminalRoster,
		fixture.owner, fixture.owner.PeerID(), model.MemberLeft)
	ownerLeft := closedRoster.Members()[len(closedRoster.Members())-1]
	merged, err = fixture.ownerStore.MergeChannelRoster(context.Background(),
		store.MergeChannelRosterSpec{ChannelID: fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.owner.PeerID(), Records: []model.Member{ownerLeft},
			At: ownerLeft.CreatedAt()})
	if err != nil || merged.Status != store.ChannelRosterApplied {
		t.Fatalf("owner close merge = (%#v,%v)", merged, err)
	}
	closed, err := fixture.controller.JoinChannel(fixture.ctx, spec)
	assertTerminalManagedJoin(t, closedRoster, closed, err)
	joinerMesh, err = fixture.joinerStore.ReadChannelMeshAuthority(context.Background())
	assertDurableTerminalJoin(t, joinerMesh, model.ChannelClosed, err)
}

func assertFreshManagedJoin(t *testing.T, fixture realManagedJoinFixture,
	spec ChannelJoinSpec, fresh peer.ChannelJoinResult, err error,
) {
	t.Helper()
	if err != nil || !fresh.Installed() || fresh.Status() != peer.ChannelEnrollmentAccepted ||
		fresh.Channel().ID() != fixture.channel.Channel().ID() ||
		fresh.Channel().LocalAlias() != spec.LocalAlias || fresh.Roster().Head().Revision() != 2 {
		t.Fatalf("fresh managed join = (%#v,%v)", fresh, err)
	}
}

func assertReplayedManagedJoin(t *testing.T, fresh, replayed peer.ChannelJoinResult,
	err error,
) {
	t.Helper()
	if err != nil || replayed.Installed() || replayed.Status() != peer.ChannelEnrollmentReplayed ||
		replayed.Roster().Head() != fresh.Roster().Head() {
		t.Fatalf("replayed managed join = (%#v,%v)", replayed, err)
	}
}

func assertTerminalManagedJoin(t *testing.T, roster model.VerifiedRoster,
	terminal peer.ChannelJoinResult, err error,
) {
	t.Helper()
	if err != nil || terminal.Installed() || !terminal.Channel().ID().IsZero() ||
		terminal.Status() != peer.ChannelEnrollmentMemberRevoked ||
		terminal.Roster().Head() != roster.Head() {
		t.Fatalf("terminal managed join = (%#v,%v)", terminal, err)
	}
}

func assertDurableTerminalJoin(t *testing.T, mesh store.ChannelMeshAuthority,
	want model.ChannelStatus, err error,
) {
	t.Helper()
	if err != nil || len(mesh.Channels()) != 1 ||
		mesh.Channels()[0].Channel().Status() != want {
		t.Fatalf("durable terminal replica = (%#v,%v)", mesh, err)
	}
}

func TestChannelAuthorityCoordinatorResolvesJoinedInstallOutcomes(t *testing.T) {
	for _, test := range []struct {
		name      string
		seed      string
		mode      joinedInstallTestMode
		wantOK    bool
		wantTrace []string
	}{
		{name: "candidate after response loss", seed: "candidate", mode: joinedInstallResponseLoss,
			wantOK: true, wantTrace: []string{"begin", "install"}},
		{name: "nil result mismatch resolves candidate", seed: "mismatch", mode: joinedInstallResultMismatch,
			wantOK: true, wantTrace: []string{"begin", "install"}},
		{name: "unchanged aborts", seed: "unchanged", mode: joinedInstallUnchanged,
			wantTrace: []string{"begin", "abort"}},
		{name: "diverged fails closed", seed: "diverged", mode: joinedInstallDiverged,
			wantTrace: []string{"begin", "fail_closed"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRealManagedJoinFixture(t, "resolve-"+test.seed)
			wrapped := &joinedInstallBehaviorStore{Store: fixture.joinerStore,
				mode: test.mode, failure: errors.New("injected joined install outcome")}
			trace := []string{}
			runtime := &managedJoinTestRuntime{mesh: fixture.runtime,
				transition: newChannelAuthorityRuntimeTrace(&trace)}
			coordinator, err := newChannelAuthorityCoordinator(wrapped, runtime,
				newChannelAuthorityTestSigner(t, fixture.joiner))
			if err != nil {
				t.Fatal(err)
			}
			result, err := coordinator.JoinChannel(fixture.ctx, ChannelJoinSpec{Token: fixture.token,
				DisplayLabel: fixture.joiner.DisplayName(), LocalAlias: "resolve-team"})
			if test.wantOK {
				if err != nil || result.Installed() || result.Status() != peer.ChannelEnrollmentAccepted {
					t.Fatalf("resolved managed join = (%#v,%v)", result, err)
				}
			} else if err == nil || !errors.Is(err, peer.ErrChannelEnrollmentProtocol) ||
				result.Status().Valid() {
				t.Fatalf("failed managed join = (%#v,%v)", result, err)
			}
			assertChannelAuthorityTrace(t, trace, test.wantTrace...)
		})
	}
}

var errStoppedJoinTest = errors.New("stop managed join test")

type blockingChannelJoinRuntime struct {
	joiner testkit.Identity
	at     time.Time
	trace  *[]string

	entered chan struct{}
	resume  chan struct{}

	mu             sync.Mutex
	secondBeginErr error
	released       int
}

func (runtime *blockingChannelJoinRuntime) begin(
	store.ChannelMeshAuthority,
) (channelAuthorityTransition, error) {
	return &channelAuthorityTransitionTrace{trace: runtime.trace}, nil
}

func (runtime *blockingChannelJoinRuntime) joinChannel(ctx context.Context,
	spec peer.JoinChannelSpec, session peer.ChannelJoinSession,
) (peer.ChannelJoinResult, error) {
	payload := spec.Token.Payload()
	control := peer.ChannelJoinPrepareControl{AuthenticatedLocalPeerID: runtime.joiner.PeerID(),
		LocalPublicKey: runtime.joiner.PublicKey(), AdvertisedMultiaddrs: runtime.joiner.Multiaddrs(),
		Descriptor: payload.Descriptor(), GrantID: payload.GrantID(), LocalAlias: spec.LocalAlias,
		At: runtime.at}
	if _, err := session.BeginChannelJoin(ctx, control); err != nil {
		return peer.ChannelJoinResult{}, err
	}
	_, second := session.BeginChannelJoin(ctx, control)
	runtime.mu.Lock()
	runtime.secondBeginErr = second
	runtime.mu.Unlock()
	close(runtime.entered)
	select {
	case <-ctx.Done():
		return peer.ChannelJoinResult{}, ctx.Err()
	case <-runtime.resume:
	}
	if err := session.ReleaseChannelJoinReservation(ctx); err != nil {
		return peer.ChannelJoinResult{}, err
	}
	runtime.mu.Lock()
	runtime.released++
	runtime.mu.Unlock()
	return peer.ChannelJoinResult{}, errStoppedJoinTest
}

type channelJoinFailureStore struct {
	*store.Store
	prepareErr error
	markErr    error
}

func (st *channelJoinFailureStore) PrepareJoinedChannel(ctx context.Context,
	spec store.PrepareJoinedChannelSpec,
) (store.PrepareJoinedChannelResult, error) {
	if st.prepareErr != nil {
		return store.PrepareJoinedChannelResult{}, st.prepareErr
	}
	return st.Store.PrepareJoinedChannel(ctx, spec)
}

func (st *channelJoinFailureStore) MarkJoinedChannelCommitUnknown(ctx context.Context,
	requestID model.EnrollmentRequestID, localPeer model.PeerID, attempt uint64, at time.Time,
) error {
	if st.markErr != nil {
		return st.markErr
	}
	return st.Store.MarkJoinedChannelCommitUnknown(ctx, requestID, localPeer, attempt, at)
}

type testChannelEnrollmentClock struct{ at time.Time }

func (clock testChannelEnrollmentClock) Now() time.Time { return clock.at }

type realManagedJoinFixture struct {
	ctx         context.Context
	ownerStore  *store.Store
	joinerStore *store.Store
	owner       testkit.Identity
	joiner      testkit.Identity
	channel     *testkit.SignedChannel
	token       model.EnrollmentToken
	runtime     *peer.MeshRuntime
	controller  *ChannelAuthorityCoordinator
}

func newRealManagedJoinFixture(t *testing.T, seed string) realManagedJoinFixture {
	t.Helper()
	at := time.Date(2026, 7, 20, 6, 0, 0, 0, time.UTC)
	ownerStore, owner := newInitializedChannelAuthorityStore(t,
		"node-managed-join-owner-"+seed, at)
	ownerHost := newManagedJoinOwnerHost(t, owner)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-managed-join-channel-"+seed, owner, at)
	create := realChannelCreateSpecWithGrant(t, channel, owner, at,
		"grant-node-managed-join-"+seed)
	ownerTrace := []string{}
	ownerController, err := newChannelAuthorityCoordinator(ownerStore,
		newChannelAuthorityRuntimeTrace(&ownerTrace), newChannelAuthorityTestSigner(t, owner))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := ownerController.CreateChannel(context.Background(), create); err != nil ||
		!result.Created {
		t.Fatalf("create managed join owner Channel = (%#v,%v)", result, err)
	}
	ownerProtocol, err := peer.NewChannelEnrollmentOwner(peer.ChannelEnrollmentOwnerOptions{
		Controller: ownerController, Clock: testChannelEnrollmentClock{at: at.Add(time.Minute)},
		Random: cryptorand.Reader})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	dispatcher, err := peer.NewChannelDispatcher(ctx, ownerHost,
		peer.ChannelDispatcherOptions{Enrollment: ownerProtocol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispatcher.Close() })
	joinerStore, joiner := newInitializedChannelAuthorityStore(t,
		"node-managed-join-joiner-"+seed, at)
	runtime := newNodeJoinMeshRuntime(t, ctx, joiner, joinerStore)
	controller, err := NewChannelAuthorityCoordinator(ctx, joinerStore, runtime,
		newChannelAuthorityNodeIdentity(t, joiner))
	if err != nil {
		t.Fatal(err)
	}
	return realManagedJoinFixture{ctx: ctx, ownerStore: ownerStore, joinerStore: joinerStore,
		owner: owner, joiner: joiner, channel: channel, token: create.Token,
		runtime: runtime, controller: controller}
}

func newManagedJoinOwnerHost(t *testing.T, identity testkit.Identity) host.Host {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	ownerHost, err := libp2p.New(libp2p.Identity(privateKey),
		libp2p.ListenAddrStrings(identity.Multiaddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownerHost.Close() })
	return ownerHost
}

func newNodeJoinMeshRuntime(t *testing.T, ctx context.Context, identity testkit.Identity,
	st *store.Store,
) *peer.MeshRuntime {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	listen, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	mesh, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := peer.NewMeshRuntime(ctx, privateKey, []ma.Multiaddr{listen}, mesh)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtime
}

type joinedInstallTestMode uint8

const (
	joinedInstallResponseLoss joinedInstallTestMode = iota + 1
	joinedInstallResultMismatch
	joinedInstallUnchanged
	joinedInstallDiverged
)

type joinedInstallBehaviorStore struct {
	*store.Store
	mode    joinedInstallTestMode
	failure error
}

type joinedInstallPrepareFailureStore struct {
	*store.Store
	failure error
}

func (st *joinedInstallPrepareFailureStore) PrepareJoinedChannelInstall(context.Context,
	store.InstallJoinedChannelSpec,
) (store.JoinedChannelInstallPlan, error) {
	return store.JoinedChannelInstallPlan{}, st.failure
}

func (st *joinedInstallBehaviorStore) CommitJoinedChannelInstall(ctx context.Context,
	plan store.JoinedChannelInstallPlan,
) (store.InstallJoinedChannelResult, error) {
	if st.mode == joinedInstallUnchanged || st.mode == joinedInstallDiverged {
		return store.InstallJoinedChannelResult{}, st.failure
	}
	result, err := st.Store.CommitJoinedChannelInstall(ctx, plan)
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	if st.mode == joinedInstallResponseLoss {
		return store.InstallJoinedChannelResult{}, st.failure
	}
	result.Status = store.ChannelEnrollmentReplayed
	return result, nil
}

func (st *joinedInstallBehaviorStore) ResolveJoinedChannelInstall(ctx context.Context,
	plan store.JoinedChannelInstallPlan,
) (store.ChannelAuthorityPlanResolution, error) {
	if st.mode == joinedInstallDiverged {
		return store.ChannelAuthorityPlanDiverged, nil
	}
	return st.Store.ResolveJoinedChannelInstall(ctx, plan)
}

type managedJoinTestRuntime struct {
	mesh       *peer.MeshRuntime
	transition *channelAuthorityRuntimeTrace
}

func (runtime *managedJoinTestRuntime) begin(candidate store.ChannelMeshAuthority) (
	channelAuthorityTransition, error,
) {
	return runtime.transition.begin(candidate)
}

func (runtime *managedJoinTestRuntime) joinChannel(ctx context.Context,
	spec peer.JoinChannelSpec, session peer.ChannelJoinSession,
) (peer.ChannelJoinResult, error) {
	return runtime.mesh.JoinChannel(ctx, spec, session)
}
