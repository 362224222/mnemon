package node

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestAdvanceConfirmedMemberTopicRequiresBothDurableDirections(t *testing.T) {
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-topic-gate")
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: target.channel.Descriptor(),
		LocalAlias: target.channel.LocalAlias(), RosterHead: target.roster.Head(),
		Status: model.ChannelActive, TopicState: model.TopicJoining,
		UpdatedAt: target.channel.UpdatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	gate := &recordingMemberTopicGate{readiness: []store.ChannelPeerReadiness{{
		ChannelID: channel.ID(), PeerID: target.remoteMember.PeerID(),
		OriginEpoch: target.remoteMember.OriginEpoch(), BindingState: model.BindingActive,
		TopicState: model.TopicJoining, RosterHead: target.roster.Head(), InboundReady: true}}}
	at := channel.UpdatedAt().Add(time.Hour)
	if err := advanceConfirmedMemberTopic(context.Background(), gate, channel, at); err != nil {
		t.Fatal(err)
	}
	if len(gate.transitions) != 0 {
		t.Fatal("one directional baseline advanced topic readiness")
	}
	gate.readiness[0].OutboundReady = true
	if err := advanceConfirmedMemberTopic(context.Background(), gate, channel, at); err != nil {
		t.Fatal(err)
	}
	if len(gate.transitions) != 1 || gate.transitions[0].ExpectedTopicState != model.TopicJoining ||
		gate.transitions[0].TopicState != model.TopicJoined ||
		gate.transitions[0].ExpectedRosterHead != target.roster.Head() {
		t.Fatalf("post-ACK transitions = %#v", gate.transitions)
	}
}

func TestAdvanceConfirmedMemberTopicPreservesNotJoinedCASOrder(t *testing.T) {
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-topic-order")
	gate := &recordingMemberTopicGate{readiness: []store.ChannelPeerReadiness{{
		ChannelID: target.channel.ID(), PeerID: target.remoteMember.PeerID(),
		OriginEpoch: target.remoteMember.OriginEpoch(), BindingState: model.BindingActive,
		TopicState: model.TopicNotJoined, RosterHead: target.roster.Head(),
		InboundReady: true, OutboundReady: true}}}
	if err := advanceConfirmedMemberTopic(context.Background(), gate, target.channel,
		target.channel.UpdatedAt().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(gate.transitions) != 2 || gate.transitions[0].TopicState != model.TopicJoining ||
		gate.transitions[1].ExpectedTopicState != model.TopicJoining ||
		gate.transitions[1].TopicState != model.TopicJoined {
		t.Fatalf("post-ACK CAS order = %#v", gate.transitions)
	}
}

type recordingMemberTopicGate struct {
	readiness   []store.ChannelPeerReadiness
	transitions []store.CompareAndSetChannelTopicStateSpec
}

func (gate *recordingMemberTopicGate) ReadChannelBaselineReadiness(context.Context,
	model.ChannelID,
) ([]store.ChannelPeerReadiness, error) {
	return append([]store.ChannelPeerReadiness(nil), gate.readiness...), nil
}

func (gate *recordingMemberTopicGate) CompareAndSetChannelTopicState(_ context.Context,
	spec store.CompareAndSetChannelTopicStateSpec,
) (store.CompareAndSetChannelTopicStateResult, error) {
	gate.transitions = append(gate.transitions, spec)
	return store.CompareAndSetChannelTopicStateResult{Topic: store.ChannelTopicProjection{
		ChannelID: spec.ChannelID, Status: spec.ExpectedStatus, RosterHead: spec.ExpectedRosterHead,
		TopicState: spec.TopicState, UpdatedAt: spec.At}, Changed: true}, nil
}
