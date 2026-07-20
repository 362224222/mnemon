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

func TestChannelMemberControllerReconcilesRealStoreAndMeshRuntime(t *testing.T) {
	fixture := newRealChannelMemberControllerFixture(t)
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

type realChannelMemberControllerFixture struct {
	store      *store.Store
	runtime    *peer.MeshRuntime
	controller *ChannelMemberController
	channel    *testkit.SignedChannel
	remote     testkit.MemberFixture
	at         time.Time
}

func newRealChannelMemberControllerFixture(t *testing.T) realChannelMemberControllerFixture {
	t.Helper()
	at := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	owner := testkit.NewIdentity(t, "node-channel-member-owner")
	workspace := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(workspace, "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	initializeRealChannelMemberStore(t, st, owner, workspace, at)
	channel := testkit.NewSignedChannelForOwnerAt(t, "node-channel-member", owner, at)
	createRealChannelMemberChannel(t, st, channel, owner, at)
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
	controller, err := NewChannelMemberController(st, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return realChannelMemberControllerFixture{store: st, runtime: runtime, controller: controller,
		channel: channel, remote: channel.AppendActive(t, "node-channel-member-remote"), at: at}
}

func initializeRealChannelMemberStore(t *testing.T, st *store.Store, owner testkit.Identity,
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

func createRealChannelMemberChannel(t *testing.T, st *store.Store,
	channel *testkit.SignedChannel, owner testkit.Identity, at time.Time,
) {
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
	if _, err := st.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: channel.Channel(), Genesis: channel.OwnerMember().Member(), Token: token}); err != nil {
		t.Fatal(err)
	}
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

func TestMapChannelMemberAuthorityErrorPreservesStableProtocolCategories(t *testing.T) {
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
		if got := mapChannelMemberAuthorityError(test.input); !errors.Is(got, test.want) {
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
) (channelMemberAuthorityTransition, error) {
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
