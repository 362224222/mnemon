package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelRuntimeTargetSendsFullProofAndIgnoresStaleReachability(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 5, 0, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "target-baseline", model.BindingActive, true, false)
	baseline := store.ChannelDataBaseline{ChannelID: target.key.channelID,
		OriginPeerID: target.local.PeerID(), OriginEpoch: target.local.OriginEpoch(),
		BaselineChannelSequence: 7}
	var mu sync.Mutex
	var reaches []store.SetPeerReachabilitySpec
	var confirmed store.ConfirmOutboundChannelBaselineSpec
	st := &channelRuntimeStoreStub{
		reserve: func(spec store.ReserveOutboundChannelBaselineSpec) (store.ReserveOutboundChannelBaselineResult, error) {
			if spec.ChannelID != target.key.channelID || spec.TargetPeerID != target.key.peerID ||
				spec.ExpectedRosterHead != target.generation.rosterHead {
				t.Fatalf("baseline reservation = %#v", spec)
			}
			return store.ReserveOutboundChannelBaselineResult{Baseline: baseline, Reserved: true}, nil
		},
		confirm: func(spec store.ConfirmOutboundChannelBaselineSpec) (store.ConfirmOutboundChannelBaselineResult, error) {
			confirmed = spec
			return store.ConfirmOutboundChannelBaselineResult{Ack: spec.Ack,
				Confirmed: true, ConfirmedAt: spec.At}, nil
		},
		reach: func(spec store.SetPeerReachabilitySpec) (store.SetPeerReachabilityResult, error) {
			mu.Lock()
			reaches = append(reaches, spec)
			mu.Unlock()
			return store.SetPeerReachabilityResult{}, store.ErrChannelRuntimeConflict
		},
	}
	transport := &channelRuntimeTransportStub{}
	transport.hello = func(_ context.Context, remote model.PeerID,
		hello peer.MemberHello,
	) (peer.MemberHelloAck, error) {
		proof := hello.OwnerSignedProofChain()
		if remote != target.key.peerID || len(proof) != len(target.roster.Members()) ||
			proof[len(proof)-1].Head() != target.roster.Head() {
			t.Fatalf("full-proof Hello = remote %s, proof %#v", remote, proof)
		}
		return peer.NewMemberHelloAck(peer.MemberHelloAckSpec{ChannelID: target.key.channelID,
			RosterHead: target.roster.Head()})
	}
	transport.baseline = func(_ context.Context, remote model.PeerID,
		request peer.DataBaseline,
	) (peer.DataBaselineAck, error) {
		if remote != target.key.peerID || request.BaselineChannelSequence() != 7 {
			t.Fatalf("outbound baseline = (%s,%#v)", remote, request)
		}
		return peer.NewDataBaselineAck(peer.DataBaselineSpec{ChannelID: request.ChannelID(),
			OriginPeerID: request.OriginPeerID(), OriginEpoch: request.OriginEpoch(),
			BaselineChannelSequence: request.BaselineChannelSequence()})
	}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	result := runtime.convergeTarget(context.Background(), context.Background(), target)
	if result.disposition != channelRuntimeTargetConverged || confirmed.AuthenticatedPeerID != target.key.peerID ||
		confirmed.ExpectedRosterHead != target.generation.rosterHead ||
		confirmed.Ack.BaselineChannelSequence != baseline.BaselineChannelSequence {
		t.Fatalf("target convergence = %#v, confirmation=%#v", result, confirmed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reaches) != 2 {
		t.Fatalf("reachability observations = %#v", reaches)
	}
	for _, spec := range reaches {
		if spec.Reachability != model.ReachabilityReachable ||
			spec.ExpectedRosterHead != target.roster.Head() || spec.OriginEpoch != target.binding.OriginEpoch() {
			t.Fatalf("reachable fence = %#v", spec)
		}
	}
}
