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

func TestChannelRuntimeTargetOwnTimeoutRecordsUnreachable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 5, 30, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "target-timeout", model.BindingPending, false, false)
	observed := make(chan store.SetPeerReachabilitySpec, 1)
	st := &channelRuntimeStoreStub{reachCtx: func(ctx context.Context, spec store.SetPeerReachabilitySpec) (
		store.SetPeerReachabilityResult, error,
	) {
		if _, bounded := ctx.Deadline(); !bounded {
			t.Fatal("timeout reachability observation has no deadline")
		}
		observed <- spec
		<-ctx.Done()
		return store.SetPeerReachabilityResult{}, ctx.Err()
	}}
	transport := &channelRuntimeTransportStub{hello: func(ctx context.Context, _ model.PeerID,
		_ peer.MemberHello,
	) (peer.MemberHelloAck, error) {
		<-ctx.Done()
		return peer.MemberHelloAck{}, ctx.Err()
	}}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	runtime.requestTimeout = 20 * time.Millisecond
	ownerCtx := context.Background()
	attemptCtx, cancel := context.WithTimeout(ownerCtx, 10*time.Millisecond)
	defer cancel()
	result := runtime.convergeTarget(ownerCtx, attemptCtx, target)
	if result.disposition != channelRuntimeTargetRetry {
		t.Fatalf("timeout disposition = %d", result.disposition)
	}
	spec := channelRuntimeReceive(t, observed)
	if spec.Reachability != model.ReachabilityUnreachable || spec.PeerID != target.key.peerID {
		t.Fatalf("timeout reachability = %#v", spec)
	}
}

func TestChannelRuntimeTargetObservationStopsWithOwner(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 5, 45, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "target-observation-cancel", model.BindingPending, false, false)
	started := make(chan struct{}, 1)
	st := &channelRuntimeStoreStub{reachCtx: func(ctx context.Context,
		_ store.SetPeerReachabilitySpec,
	) (store.SetPeerReachabilityResult, error) {
		started <- struct{}{}
		<-ctx.Done()
		return store.SetPeerReachabilityResult{}, ctx.Err()
	}}
	transport := &channelRuntimeTransportStub{hello: func(context.Context, model.PeerID,
		peer.MemberHello,
	) (peer.MemberHelloAck, error) {
		return peer.MemberHelloAck{}, context.DeadlineExceeded
	}}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	ownerCtx, cancel := context.WithCancel(context.Background())
	attemptCtx, attemptCancel := context.WithTimeout(ownerCtx, time.Second)
	defer attemptCancel()
	done := make(chan channelRuntimeTargetResult, 1)
	go func() { done <- runtime.convergeTarget(ownerCtx, attemptCtx, target) }()
	channelRuntimeReceive(t, started)
	cancel()
	if result := channelRuntimeReceive(t, done); result.disposition != channelRuntimeTargetCancelled {
		t.Fatalf("cancelled observation disposition = %d", result.disposition)
	}
}

func TestChannelRuntimeTargetMergesHelloSuffixBeforeBaseline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 6, 0, 0, 0, time.UTC)
	target, fixture := channelRuntimeTestTarget(t, "target-merge", model.BindingPending, false, false)
	newMember := fixture.AppendActive(t, "target-merge-new")
	updated := fixture.Roster()
	authority := &channelRuntimeAuthorityStub{roster: updated}
	transport := &channelRuntimeTransportStub{}
	transport.hello = func(context.Context, model.PeerID, peer.MemberHello) (peer.MemberHelloAck, error) {
		return peer.NewMemberHelloAck(peer.MemberHelloAckSpec{ChannelID: target.key.channelID,
			MissingRecords: []model.Member{newMember.Member()}, RosterHead: updated.Head()})
	}
	runtime := channelRuntimeWithStubs(t, &channelRuntimeStoreStub{}, transport, authority, now)
	result := runtime.convergeTarget(context.Background(), context.Background(), target)
	if result.disposition != channelRuntimeTargetRescan {
		t.Fatalf("roster suffix disposition = %d", result.disposition)
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if len(authority.updates) != 1 || authority.updates[0].ChannelID != target.key.channelID ||
		authority.updates[0].AuthenticatedPeerID != target.key.peerID ||
		len(authority.updates[0].Records) != 1 ||
		authority.updates[0].Records[0].Head() != newMember.Member().Head() {
		t.Fatalf("roster update = %#v", authority.updates)
	}
}

func TestChannelRuntimeRosterGapSyncStartsAtGenesis(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 6, 30, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "target-sync", model.BindingPending, false, false)
	var after model.RecordHead
	transport := &channelRuntimeTransportStub{
		sync: func(_ context.Context, _ model.PeerID,
			request peer.SyncRequest,
		) (peer.ChannelMemberSyncResult, error) {
			after = request.AfterHead()
			return peer.ChannelMemberSyncResult{}, nil
		},
	}
	runtime := channelRuntimeWithStubs(t, &channelRuntimeStoreStub{}, transport,
		channelRuntimeNoopAuthority{}, now)
	result := runtime.syncTargetRoster(context.Background(), context.Background(), target)
	if result.disposition != channelRuntimeTargetFatal ||
		after != target.roster.Members()[0].Head() {
		t.Fatalf("roster Sync = disposition %d, after %#v", result.disposition, after)
	}
}

func TestChannelRuntimeRemoteFailureClassificationIsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		err         error
		disposition channelRuntimeTargetDisposition
		observe     bool
		reachable   model.Reachability
	}{
		{"timeout", context.DeadlineExceeded, channelRuntimeTargetRetry, true,
			model.ReachabilityUnreachable},
		{"transport", peer.ErrChannelMemberClientTransport, channelRuntimeTargetRetry, true,
			model.ReachabilityUnreachable},
		{"invalid_response", peer.ErrChannelMemberClientResponse,
			channelRuntimeTargetPermanent, false, model.ReachabilityUnknown},
		{"bare_client", peer.ErrChannelMemberClient,
			channelRuntimeTargetFatal, false, model.ReachabilityUnknown},
		{"mesh_lifecycle", peer.ErrMeshTransport,
			channelRuntimeTargetFatal, false, model.ReachabilityUnknown},
		{"unknown", errors.New("unknown exchange"),
			channelRuntimeTargetFatal, false, model.ReachabilityUnknown},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			disposition, _, reachable, observe, rosterGap := channelRuntimeRemoteFailure(test.err)
			if disposition != test.disposition || observe != test.observe ||
				reachable != test.reachable || rosterGap {
				t.Fatalf("classification = (%d,%s,%t,%t)",
					disposition, reachable, observe, rosterGap)
			}
		})
	}
}

type channelRuntimeAuthorityStub struct {
	mu      sync.Mutex
	roster  model.VerifiedRoster
	err     error
	updates []ChannelRuntimeRosterUpdate
}

func (authority *channelRuntimeAuthorityStub) ReconcileRemoteRoster(_ context.Context,
	update ChannelRuntimeRosterUpdate,
) (model.VerifiedRoster, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	update.Records = append([]model.Member(nil), update.Records...)
	authority.updates = append(authority.updates, update)
	return authority.roster, authority.err
}

func channelRuntimeTestTarget(t *testing.T, seed string, state model.BindingState,
	inbound, outbound bool,
) (channelRuntimeTarget, *testkit.SignedChannel) {
	t.Helper()
	fixture := testkit.NewSignedChannel(t, seed)
	remote := fixture.AppendActive(t, seed+"-remote")
	baseChannel := fixture.Channel()
	joined, err := model.NewChannel(model.ChannelSpec{Descriptor: fixture.Descriptor(),
		LocalAlias: baseChannel.LocalAlias(), RosterHead: fixture.Roster().Head(),
		Status: model.ChannelActive, TopicState: model.TopicJoined,
		UpdatedAt: remote.Member().CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := model.NewPeerBinding(fixture.Owner().PeerID(), model.PeerBindingSpec{
		Channel: joined, Roster: fixture.Roster(), PeerID: remote.Identity().PeerID(),
		EffectiveAlias: remote.Member().DisplayLabel(), State: state,
		Reachability: model.ReachabilityUnknown, JoinedAt: remote.Member().CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	readiness := store.ChannelPeerReadiness{ChannelID: joined.ID(), PeerID: binding.PeerID(),
		OriginEpoch: binding.OriginEpoch(), BindingState: state, TopicState: model.TopicJoined,
		RosterHead: fixture.Roster().Head(), InboundReady: inbound, OutboundReady: outbound}
	key := channelRuntimeTargetKey{channelID: joined.ID(), peerID: binding.PeerID()}
	generation := channelRuntimeTargetGeneration{channelID: joined.ID(), peerID: binding.PeerID(),
		originEpoch: binding.OriginEpoch(), rosterHead: fixture.Roster().Head(),
		memberHead: binding.MemberHead(), bindingState: state}
	return channelRuntimeTarget{key: key, generation: generation, roster: fixture.Roster(),
		local: fixture.OwnerMember().Member(), binding: binding, readiness: readiness}, fixture
}
