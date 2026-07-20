package node

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestChannelAuthorityCoordinatorReconcilesRealStoreAndMeshRuntime(t *testing.T) {
	fixture := newRealChannelAuthorityCoordinatorFixture(t)
	initial, err := fixture.runtime.Session(fixture.channel.Channel().ID())
	if err != nil || !initial.IsCurrent() {
		t.Fatalf("initial topic session = (%#v,%v)", initial, err)
	}
	hello, err := fixture.controller.ReconcileMemberHelloGate(context.Background(),
		peer.ChannelMemberHelloControl{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			ChannelID: fixture.channel.Channel().ID(), ActiveMemberRecord: fixture.remote.Member(),
			KnownRosterHead: fixture.channel.Roster().Head(),
			ProofRecords:    fixture.channel.Roster().Members(), At: fixture.remote.Member().CreatedAt()})
	if err != nil || hello.Roster.Head() != fixture.channel.Roster().Head() || initial.IsCurrent() {
		t.Fatalf("real hello reconciliation = (%#v,%v), initial_current=%t",
			hello, err, initial.IsCurrent())
	}
	afterHello, err := fixture.runtime.Session(fixture.channel.Channel().ID())
	if err != nil || !afterHello.IsCurrent() {
		t.Fatalf("post-hello topic session = (%#v,%v)", afterHello, err)
	}
	assertRealChannelMemberBinding(t, fixture.store, fixture.channel.Channel().ID(),
		model.BindingPending, false)

	baseline := peer.DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: 0}
	installed, err := fixture.controller.InstallMemberBaselineGate(context.Background(),
		peer.ChannelMemberBaselineControl{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: baseline, At: fixture.at.Add(2 * time.Second)})
	if err != nil || installed.Baseline != baseline || afterHello.IsCurrent() {
		t.Fatalf("real baseline reconciliation = (%#v,%v), hello_current=%t",
			installed, err, afterHello.IsCurrent())
	}
	final, err := fixture.runtime.Session(fixture.channel.Channel().ID())
	if err != nil || !final.IsCurrent() {
		t.Fatalf("post-baseline topic session = (%#v,%v)", final, err)
	}
	assertRealChannelMemberBinding(t, fixture.store, fixture.channel.Channel().ID(),
		model.BindingActive, true)
}

type realChannelAuthorityCoordinatorFixture struct {
	store      *store.Store
	runtime    *peer.MeshRuntime
	controller *ChannelAuthorityCoordinator
	channel    *testkit.SignedChannel
	remote     testkit.MemberFixture
	at         time.Time
}

func newRealChannelAuthorityCoordinatorFixture(t *testing.T) realChannelAuthorityCoordinatorFixture {
	t.Helper()
	at := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	st, owner := newInitializedChannelAuthorityStore(t, "node-channel-member-owner", at)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-channel-member", owner, at)
	createSpec := realChannelCreateSpec(t, channel, owner, at)
	mesh, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := owner.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	listen, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := peer.NewMeshRuntime(context.Background(), privateKey, []ma.Multiaddr{listen}, mesh)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	controller, err := NewChannelAuthorityCoordinator(st, runtime)
	if err != nil {
		t.Fatal(err)
	}
	created, err := controller.CreateChannel(context.Background(), createSpec)
	if err != nil || !created.Created || created.Channel.ID() != channel.Channel().ID() {
		t.Fatalf("CreateChannel() = (%#v,%v)", created, err)
	}
	return realChannelAuthorityCoordinatorFixture{store: st, runtime: runtime, controller: controller,
		channel: channel, remote: channel.AppendActive(t, "node-channel-member-remote"), at: at}
}

func newInitializedChannelAuthorityStore(t *testing.T, seed string,
	at time.Time,
) (*store.Store, testkit.Identity) {
	t.Helper()
	owner := testkit.NewIdentity(t, seed)
	workspace := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(workspace, "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	initializeRealChannelAuthorityStore(t, st, owner, workspace, at)
	return st, owner
}

func initializeRealChannelAuthorityStore(t *testing.T, st *store.Store, owner testkit.Identity,
	workspace string, at time.Time,
) {
	t.Helper()
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: owner.PeerID(),
		OriginEpoch: owner.OriginEpoch(), NextOriginSequence: 1,
		ActiveAssetRevision: "asset-node-channel-member", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-node-channel-member", WorkspaceRoot: workspace,
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("node-channel-member-credential")),
		ActiveAssetRevision: "asset-node-channel-member",
		HandlingBudget:      model.DefaultHandlingBudget().JSON(), CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
}

func realChannelCreateSpec(t *testing.T,
	channel *testkit.SignedChannel, owner testkit.Identity, at time.Time,
) store.CreateChannelSpec {
	t.Helper()
	grantID, _ := model.ParseGrantID("grant-node-channel-member")
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: channel.Descriptor(), OwnerMultiaddrs: owner.Multiaddrs(), GrantID: grantID,
		BearerSecret: model.Sum([]byte("node-channel-member-secret")).Bytes(),
		ExpiresAt:    at.Add(time.Hour), MaxUses: channel.Channel().MemberLimit() - 1,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(channel.Channel().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := owner.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload,
		ed25519.Sign(ed25519.PrivateKey(raw), message))
	if err != nil {
		t.Fatal(err)
	}
	return store.CreateChannelSpec{Channel: channel.Channel(),
		Genesis: channel.OwnerMember().Member(), Token: token}
}

func assertRealChannelMemberBinding(t *testing.T, st *store.Store,
	channelID model.ChannelID, want model.BindingState, inbound bool,
) {
	t.Helper()
	readiness, err := st.ReadChannelBaselineReadiness(context.Background(), channelID)
	if err != nil || len(readiness) != 1 || readiness[0].BindingState != want ||
		readiness[0].InboundReady != inbound {
		t.Fatalf("real member binding = (%#v,%v), want state=%s inbound=%t",
			readiness, err, want, inbound)
	}
}

func TestChannelAuthorityCoordinatorCreateTransitionsPreparedCandidateInOrder(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC)
	st, owner := newInitializedChannelAuthorityStore(t, "node-channel-create-order-owner", at)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-channel-create-order", owner, at)
	trace := []string{}
	tracedStore := &channelAuthorityCreateTraceStore{Store: st, trace: &trace}
	runtime := newChannelAuthorityRuntimeTrace(&trace)
	coordinator, err := newChannelAuthorityCoordinator(tracedStore, runtime)
	if err != nil {
		t.Fatal(err)
	}

	result, err := coordinator.CreateChannel(context.Background(),
		realChannelCreateSpec(t, channel, owner, at))
	if err != nil || !result.Created || result.Channel.ID() != channel.Channel().ID() {
		t.Fatalf("CreateChannel() = (%#v,%v)", result, err)
	}
	assertChannelAuthorityTrace(t, trace, "prepare", "begin", "commit", "install")
}

func TestChannelAuthorityCoordinatorSerializesCreateMemberAndBaseline(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC)
	st, owner := newInitializedChannelAuthorityStore(t, "node-channel-serialize-owner", at)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-channel-serialize", owner, at)
	gatedStore := newGatedChannelAuthorityStore(st)
	t.Cleanup(func() { close(gatedStore.release) })
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(gatedStore,
		newChannelAuthorityRuntimeTrace(&trace))
	if err != nil {
		t.Fatal(err)
	}
	createSpec := realChannelCreateSpec(t, channel, owner, at)
	start := make(chan struct{})
	done := make(chan string, 3)
	go func() {
		<-start
		_, _ = coordinator.CreateChannel(context.Background(), createSpec)
		done <- "create"
	}()
	go func() {
		<-start
		_, _ = coordinator.FreezeMemberRosterForSync(context.Background(),
			peer.ChannelMemberSyncControl{ChannelID: channel.Channel().ID()})
		done <- "member"
	}()
	go func() {
		<-start
		_, _ = coordinator.InstallMemberBaselineGate(context.Background(),
			peer.ChannelMemberBaselineControl{})
		done <- "baseline"
	}()
	close(start)

	entered := make(map[string]bool, 3)
	for range 3 {
		select {
		case call := <-gatedStore.entered:
			if entered[call] {
				t.Fatalf("authority operation entered twice: %q", call)
			}
			entered[call] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for serialized authority operation")
		}
		select {
		case call := <-gatedStore.entered:
			t.Fatalf("authority operations overlapped at Store boundary: %q", call)
		case <-time.After(100 * time.Millisecond):
		}
		gatedStore.release <- struct{}{}
	}
	for range 3 {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("serialized authority operation did not finish")
		}
	}
	for _, want := range []string{"create", "member", "baseline"} {
		if !entered[want] {
			t.Fatalf("authority operation %q never entered Store", want)
		}
	}
}

func TestChannelAuthorityCoordinatorMutationWaitHonorsCancellation(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 7, 0, 0, 0, time.UTC)
	st, owner := newInitializedChannelAuthorityStore(t, "node-channel-cancel-owner", at)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-channel-cancel", owner, at)
	gatedStore := newGatedChannelAuthorityStore(st)
	t.Cleanup(func() { close(gatedStore.release) })
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(gatedStore,
		newChannelAuthorityRuntimeTrace(&trace))
	if err != nil {
		t.Fatal(err)
	}
	createSpec := realChannelCreateSpec(t, channel, owner, at)
	createDone := make(chan error, 1)
	go func() {
		_, createErr := coordinator.CreateChannel(context.Background(), createSpec)
		createDone <- createErr
	}()
	select {
	case call := <-gatedStore.entered:
		if call != "create" {
			t.Fatalf("first authority operation = %q, want create", call)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("create did not acquire Channel authority")
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() {
		_, waitErr := coordinator.InstallMemberBaselineGate(waitCtx,
			peer.ChannelMemberBaselineControl{})
		waitDone <- waitErr
	}()
	select {
	case waitErr := <-waitDone:
		t.Fatalf("authority waiter returned before cancellation: %v", waitErr)
	case <-time.After(100 * time.Millisecond):
	}
	cancelWait()
	waitErr := <-waitDone
	if !errors.Is(waitErr, context.Canceled) || !errors.Is(waitErr, ErrChannelAuthority) {
		t.Fatalf("canceled authority wait error = %v", waitErr)
	}
	select {
	case call := <-gatedStore.entered:
		t.Fatalf("canceled operation reached Store boundary: %q", call)
	default:
	}
	gatedStore.release <- struct{}{}
	if err := <-createDone; err != nil {
		t.Fatalf("CreateChannel() after canceled waiter = %v", err)
	}
}

type channelAuthorityCreateTraceStore struct {
	*store.Store
	trace *[]string
}

func (st *channelAuthorityCreateTraceStore) PrepareCreateChannel(ctx context.Context,
	spec store.CreateChannelSpec,
) (store.CreateChannelPlan, error) {
	*st.trace = append(*st.trace, "prepare")
	return st.Store.PrepareCreateChannel(ctx, spec)
}

func (st *channelAuthorityCreateTraceStore) CommitCreateChannel(ctx context.Context,
	plan store.CreateChannelPlan,
) (store.CreateChannelResult, error) {
	*st.trace = append(*st.trace, "commit")
	return st.Store.CommitCreateChannel(ctx, plan)
}

type gatedChannelAuthorityStore struct {
	*store.Store
	entered chan string
	release chan struct{}
}

func newGatedChannelAuthorityStore(st *store.Store) *gatedChannelAuthorityStore {
	return &gatedChannelAuthorityStore{Store: st, entered: make(chan string, 3),
		release: make(chan struct{})}
}

func (st *gatedChannelAuthorityStore) ReadChannelMeshAuthority(
	ctx context.Context,
) (store.ChannelMeshAuthority, error) {
	st.gate("member")
	return st.Store.ReadChannelMeshAuthority(ctx)
}

func (st *gatedChannelAuthorityStore) PrepareCreateChannel(ctx context.Context,
	spec store.CreateChannelSpec,
) (store.CreateChannelPlan, error) {
	st.gate("create")
	return st.Store.PrepareCreateChannel(ctx, spec)
}

func (st *gatedChannelAuthorityStore) PrepareInboundChannelBaseline(ctx context.Context,
	spec store.InstallInboundChannelBaselineSpec,
) (store.InboundChannelBaselinePlan, error) {
	st.gate("baseline")
	return st.Store.PrepareInboundChannelBaseline(ctx, spec)
}

func (st *gatedChannelAuthorityStore) gate(call string) {
	st.entered <- call
	<-st.release
}

func TestExecuteChannelAuthorityPlanCommitsBeforeInstall(t *testing.T) {
	t.Parallel()
	trace := []string{}
	runtime := newChannelAuthorityRuntimeTrace(&trace)
	result, err := executeChannelAuthorityPlan(context.Background(), runtime,
		channelAuthorityPlanSteps[string]{changes: true, expected: "expected",
			commit: func(context.Context) (string, error) {
				trace = append(trace, "commit")
				return "committed", nil
			},
			resolve: func(context.Context) (store.ChannelAuthorityPlanResolution, error) {
				t.Fatal("successful commit must not resolve")
				return "", nil
			},
		})
	if err != nil || result != "committed" {
		t.Fatalf("execute authority plan = (%q, %v)", result, err)
	}
	assertChannelAuthorityTrace(t, trace, "begin", "commit", "install")
}

func TestExecuteChannelAuthorityPlanResolvesUnknownCommitWithoutStaleAuthority(t *testing.T) {
	t.Parallel()
	commitErr := errors.New("commit response lost")
	tests := []struct {
		name       string
		changes    bool
		resolution store.ChannelAuthorityPlanResolution
		resolveErr error
		wantResult string
		wantErr    error
		wantFinal  string
	}{
		{name: "unchanged aborts", changes: true, resolution: store.ChannelAuthorityPlanUnchanged,
			wantErr: commitErr, wantFinal: "abort"},
		{name: "candidate installs", changes: true, resolution: store.ChannelAuthorityPlanCandidate,
			wantResult: "expected", wantFinal: "install"},
		{name: "diverged fails closed", changes: true, resolution: store.ChannelAuthorityPlanDiverged,
			wantErr: commitErr, wantFinal: "fail_closed"},
		{name: "unreadable fails closed", changes: true, resolveErr: errors.New("resolution unavailable"),
			wantErr: commitErr, wantFinal: "fail_closed"},
		{name: "runtime equivalent candidate installs", resolution: store.ChannelAuthorityPlanCandidate,
			wantResult: "expected", wantFinal: "install"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			trace := []string{}
			runtime := newChannelAuthorityRuntimeTrace(&trace)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			cancelRequest()
			result, err := executeChannelAuthorityPlan(requestCtx, runtime,
				channelAuthorityPlanSteps[string]{changes: test.changes, expected: "expected",
					commit: func(ctx context.Context) (string, error) {
						trace = append(trace, "commit")
						if !errors.Is(ctx.Err(), context.Canceled) {
							t.Fatal("commit did not receive request cancellation")
						}
						return "", commitErr
					},
					resolve: func(ctx context.Context) (store.ChannelAuthorityPlanResolution, error) {
						trace = append(trace, "resolve")
						if ctx.Err() != nil {
							t.Fatalf("resolution inherited request cancellation: %v", ctx.Err())
						}
						return test.resolution, test.resolveErr
					},
				})
			if result != test.wantResult || !errors.Is(err, test.wantErr) {
				t.Fatalf("execute unknown authority plan = (%q, %v)", result, err)
			}
			if test.changes {
				assertChannelAuthorityTrace(t, trace, "begin", "commit", "resolve", test.wantFinal)
			} else {
				assertChannelAuthorityTrace(t, trace, "commit", "begin", "resolve", test.wantFinal)
			}
		})
	}
}

func TestExecuteChannelAuthorityPlanCommitsRuntimeEquivalentPlanWithoutTransition(t *testing.T) {
	t.Parallel()
	trace := []string{}
	runtime := newChannelAuthorityRuntimeTrace(&trace)
	result, err := executeChannelAuthorityPlan(context.Background(), runtime,
		channelAuthorityPlanSteps[string]{expected: "replay",
			commit: func(context.Context) (string, error) {
				trace = append(trace, "commit")
				return "committed", nil
			},
			resolve: func(context.Context) (store.ChannelAuthorityPlanResolution, error) {
				t.Fatal("read-only plan resolved")
				return "", nil
			},
		})
	if err != nil || result != "committed" {
		t.Fatalf("runtime-equivalent authority plan = (%q, %v, %v)", result, err, trace)
	}
	assertChannelAuthorityTrace(t, trace, "commit")
}

func TestMapChannelAuthorityErrorPreservesStableProtocolCategories(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		input error
		want  error
	}{
		{store.ErrChannelBaselineConflict, peer.ErrChannelMemberBaselineConflict},
		{store.ErrChannelBaselineEpochMismatch, peer.ErrChannelMemberEpochMismatch},
		{store.ErrChannelBaselineAuthority, peer.ErrChannelMemberNotMember},
		{store.ErrChannelRosterConflict, peer.ErrChannelMemberRosterConflict},
		{store.ErrChannelRosterInput, peer.ErrChannelMemberRosterConflict},
	} {
		if got := mapChannelAuthorityError(test.input); !errors.Is(got, test.want) {
			t.Fatalf("map authority error %v = %v, want %v", test.input, got, test.want)
		}
	}
}

type channelAuthorityRuntimeTrace struct {
	trace      *[]string
	transition *channelAuthorityTransitionTrace
}

func newChannelAuthorityRuntimeTrace(trace *[]string) *channelAuthorityRuntimeTrace {
	return &channelAuthorityRuntimeTrace{trace: trace,
		transition: &channelAuthorityTransitionTrace{trace: trace}}
}

func (runtime *channelAuthorityRuntimeTrace) begin(
	store.ChannelMeshAuthority,
) (channelAuthorityTransition, error) {
	*runtime.trace = append(*runtime.trace, "begin")
	return runtime.transition, nil
}

type channelAuthorityTransitionTrace struct{ trace *[]string }

func (transition *channelAuthorityTransitionTrace) Install() error {
	*transition.trace = append(*transition.trace, "install")
	return nil
}

func (transition *channelAuthorityTransitionTrace) Abort() error {
	*transition.trace = append(*transition.trace, "abort")
	return nil
}

func (transition *channelAuthorityTransitionTrace) FailClosed(cause error) error {
	*transition.trace = append(*transition.trace, "fail_closed")
	return cause
}

func assertChannelAuthorityTrace(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("authority trace = %v, want %v", got, want)
	}
}
