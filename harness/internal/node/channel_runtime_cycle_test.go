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
)

type channelRuntimeStoreStub struct {
	mu         sync.Mutex
	compare    func(store.CompareAndSetChannelTopicStateSpec) (store.CompareAndSetChannelTopicStateResult, error)
	reserve    func(store.ReserveOutboundChannelBaselineSpec) (store.ReserveOutboundChannelBaselineResult, error)
	confirm    func(store.ConfirmOutboundChannelBaselineSpec) (store.ConfirmOutboundChannelBaselineResult, error)
	reach      func(store.SetPeerReachabilitySpec) (store.SetPeerReachabilityResult, error)
	reachCtx   func(context.Context, store.SetPeerReachabilitySpec) (store.SetPeerReachabilityResult, error)
	topicSpecs []store.CompareAndSetChannelTopicStateSpec
}

func (*channelRuntimeStoreStub) BeginChannelTopicRuntime(context.Context,
	time.Time,
) (store.BeginChannelTopicRuntimeResult, error) {
	return store.BeginChannelTopicRuntimeResult{}, nil
}

func (*channelRuntimeStoreStub) ReadChannelMeshAuthority(context.Context) (
	store.ChannelMeshAuthority, error,
) {
	return store.ChannelMeshAuthority{}, errors.New("unexpected mesh read")
}

func (*channelRuntimeStoreStub) ReadChannelBaselineReadiness(context.Context,
	model.ChannelID,
) ([]store.ChannelPeerReadiness, error) {
	return nil, errors.New("unexpected readiness read")
}

func (st *channelRuntimeStoreStub) CompareAndSetChannelTopicState(_ context.Context,
	spec store.CompareAndSetChannelTopicStateSpec,
) (store.CompareAndSetChannelTopicStateResult, error) {
	if st.compare == nil {
		return store.CompareAndSetChannelTopicStateResult{}, errors.New("unexpected topic CAS")
	}
	return st.compare(spec)
}

func (st *channelRuntimeStoreStub) ReserveOutboundChannelBaseline(_ context.Context,
	spec store.ReserveOutboundChannelBaselineSpec,
) (store.ReserveOutboundChannelBaselineResult, error) {
	if st.reserve == nil {
		return store.ReserveOutboundChannelBaselineResult{}, errors.New("unexpected baseline reserve")
	}
	return st.reserve(spec)
}

func (st *channelRuntimeStoreStub) ConfirmOutboundChannelBaseline(_ context.Context,
	spec store.ConfirmOutboundChannelBaselineSpec,
) (store.ConfirmOutboundChannelBaselineResult, error) {
	if st.confirm == nil {
		return store.ConfirmOutboundChannelBaselineResult{}, errors.New("unexpected baseline confirm")
	}
	return st.confirm(spec)
}

func (st *channelRuntimeStoreStub) SetPeerReachability(ctx context.Context,
	spec store.SetPeerReachabilitySpec,
) (store.SetPeerReachabilityResult, error) {
	if st.reachCtx != nil {
		return st.reachCtx(ctx, spec)
	}
	if st.reach == nil {
		return store.SetPeerReachabilityResult{Peer: store.PeerReachabilityProjection{
			ChannelID: spec.ChannelID, PeerID: spec.PeerID, OriginEpoch: spec.OriginEpoch,
			RosterHead: spec.ExpectedRosterHead, BindingState: spec.ExpectedBindingState,
			Reachability: spec.Reachability}}, nil
	}
	return st.reach(spec)
}

type channelRuntimeTransportStub struct {
	mu       sync.Mutex
	current  bool
	ensure   func(model.ChannelID) error
	hello    func(context.Context, model.PeerID, peer.MemberHello) (peer.MemberHelloAck, error)
	sync     func(context.Context, model.PeerID, peer.SyncRequest) (peer.ChannelMemberSyncResult, error)
	baseline func(context.Context, model.PeerID, peer.DataBaseline) (peer.DataBaselineAck, error)
}

func (transport *channelRuntimeTransportStub) EnsureChannelTopic(_ context.Context,
	channelID model.ChannelID,
) error {
	if transport.ensure == nil {
		return nil
	}
	return transport.ensure(channelID)
}

func (transport *channelRuntimeTransportStub) HasCurrentChannelTopic(model.ChannelID) bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.current
}

func (transport *channelRuntimeTransportStub) Hello(ctx context.Context, peerID model.PeerID,
	hello peer.MemberHello,
) (peer.MemberHelloAck, error) {
	return transport.hello(ctx, peerID, hello)
}

func (transport *channelRuntimeTransportStub) Sync(ctx context.Context, peerID model.PeerID,
	request peer.SyncRequest,
) (peer.ChannelMemberSyncResult, error) {
	return transport.sync(ctx, peerID, request)
}

func (transport *channelRuntimeTransportStub) Baseline(ctx context.Context, peerID model.PeerID,
	baseline peer.DataBaseline,
) (peer.DataBaselineAck, error) {
	return transport.baseline(ctx, peerID, baseline)
}

func channelRuntimeWithStubs(t *testing.T, st *channelRuntimeStoreStub,
	transport *channelRuntimeTransportStub, authority ChannelRuntimeAuthority,
	now time.Time,
) *ChannelRuntime {
	t.Helper()
	runtime, err := NewChannelRuntime(ChannelRuntimeOptions{Store: st, Transport: transport,
		Authority: authority, Clock: channelRuntimeFixedClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}
