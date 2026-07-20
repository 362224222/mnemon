package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type channelMemberTopicGateStore interface {
	ReadChannelBaselineReadiness(context.Context, model.ChannelID) ([]store.ChannelPeerReadiness, error)
	CompareAndSetChannelTopicState(context.Context, store.CompareAndSetChannelTopicStateSpec) (store.CompareAndSetChannelTopicStateResult, error)
}

// ConfirmMemberBaselineRuntimeGate re-evaluates topic readiness only after the
// outbound ACK is durable. It serializes with every other Channel mutation and
// never infers readiness from the wire response alone.
func (manager *ChannelManager) ConfirmMemberBaselineRuntimeGate(ctx context.Context,
	channelID model.ChannelID,
) error {
	if manager == nil || manager.store == nil || manager.runtime == nil || manager.clock == nil ||
		ctx == nil || ctx.Err() != nil || channelID.IsZero() {
		return fmt.Errorf("%w: baseline runtime gate is unavailable", ErrChannelMemberReconciler)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return fmt.Errorf("%w: read post-ACK authority: %w", ErrChannelMemberReconciler, err)
	}
	if err := manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }); err != nil {
		return fmt.Errorf("%w: install post-ACK authority: %w", ErrChannelMemberReconciler, err)
	}
	channel, err := channelForMemberRuntimeGate(mesh, channelID)
	if err != nil || !manager.runtime.HasCurrentSession(channelID) {
		return fmt.Errorf("%w: post-ACK topic session is unavailable", ErrChannelMemberReconciler)
	}
	return advanceConfirmedMemberTopic(ctx, manager.store, channel, manager.clock.Now())
}

func channelForMemberRuntimeGate(mesh store.ChannelMeshAuthority,
	channelID model.ChannelID,
) (model.Channel, error) {
	for _, durable := range mesh.Channels() {
		if durable.Channel().ID() == channelID && durable.Channel().Status() == model.ChannelActive {
			return durable.Channel(), nil
		}
	}
	return model.Channel{}, errors.New("active Channel is unavailable")
}

func advanceConfirmedMemberTopic(ctx context.Context, st channelMemberTopicGateStore,
	channel model.Channel, at time.Time,
) error {
	if ctx == nil || st == nil || channel.ID().IsZero() || channel.Status() != model.ChannelActive ||
		at.IsZero() {
		return fmt.Errorf("%w: invalid post-ACK topic input", ErrChannelMemberReconciler)
	}
	readiness, err := st.ReadChannelBaselineReadiness(ctx, channel.ID())
	if err != nil {
		return fmt.Errorf("%w: read post-ACK readiness: %w", ErrChannelMemberReconciler, err)
	}
	if !channelBaselinesReady(readiness) || channel.TopicState() == model.TopicJoined {
		return nil
	}
	current := channel.TopicState()
	if current == model.TopicNotJoined {
		result, err := st.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
			ChannelID: channel.ID(), ExpectedStatus: model.ChannelActive,
			ExpectedRosterHead: channel.RosterHead(), ExpectedTopicState: model.TopicNotJoined,
			TopicState: model.TopicJoining, At: at})
		if err != nil {
			return fmt.Errorf("%w: begin post-ACK topic: %w", ErrChannelMemberReconciler, err)
		}
		current = result.Topic.TopicState
	}
	if current != model.TopicJoining {
		return fmt.Errorf("%w: invalid post-ACK topic phase", ErrChannelMemberReconciler)
	}
	_, err = st.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
		ChannelID: channel.ID(), ExpectedStatus: model.ChannelActive,
		ExpectedRosterHead: channel.RosterHead(), ExpectedTopicState: model.TopicJoining,
		TopicState: model.TopicJoined, At: at})
	if err != nil {
		return fmt.Errorf("%w: confirm post-ACK topic: %w", ErrChannelMemberReconciler, err)
	}
	return nil
}

var _ ChannelMemberReconcilerController = (*ChannelManager)(nil)
