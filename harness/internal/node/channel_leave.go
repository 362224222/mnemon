package node

import (
	"context"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// AcceptMemberLeaveGate serializes the owner's durable terminal record and
// exact receipt with every other Channel mutation. Runtime authority is
// refreshed before the receipt can be returned to the requester.
func (manager *ChannelManager) AcceptMemberLeaveGate(ctx context.Context,
	control peer.ChannelMemberLeaveControl,
) (peer.ChannelMemberLeaveAuthority, error) {
	if manager == nil || manager.store == nil || manager.runtime == nil || manager.identity == nil ||
		ctx == nil || ctx.Err() != nil || control.AuthenticatedPeerID.IsZero() ||
		control.Request.IsZero() || control.At.IsZero() {
		return peer.ChannelMemberLeaveAuthority{}, fmt.Errorf(
			"%w: leave gate is unavailable", peer.ErrChannelMemberRosterConflict)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result, err := manager.store.AcceptChannelLeave(ctx, store.AcceptChannelLeaveSpec{
		AuthenticatedPeerID: control.AuthenticatedPeerID, Request: control.Request,
		Signer: manager.identity.PublicationSigner(), At: control.At})
	if err != nil {
		return peer.ChannelMemberLeaveAuthority{}, channelMemberStoreError(err)
	}
	if err := manager.refreshChannelLeaveRuntime(ctx, result.Channel.ID()); err != nil {
		return peer.ChannelMemberLeaveAuthority{}, err
	}
	manager.triggerMemberReconcile()
	return peer.ChannelMemberLeaveAuthority{Descriptor: result.Channel.Descriptor(),
		ActiveMember: result.ActiveMember, Receipt: result.Receipt}, nil
}

// SettleMemberLeaveRuntimeGate installs a verified owner receipt locally and
// removes the leaving Channel from runtime before the serial reconciler
// considers the request settled.
func (manager *ChannelManager) SettleMemberLeaveRuntimeGate(ctx context.Context,
	requestID model.ChannelLeaveRequestID, receipt model.SignedChannelLeaveReceipt, at time.Time,
) error {
	if manager == nil || manager.store == nil || manager.runtime == nil || ctx == nil ||
		ctx.Err() != nil || requestID.IsZero() || receipt.IsZero() || at.IsZero() {
		return fmt.Errorf("%w: leave settlement gate is unavailable", ErrChannelMemberReconciler)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result, err := manager.store.SettleChannelLeave(ctx, store.SettleChannelLeaveSpec{
		RequestID: requestID, Receipt: receipt, At: at})
	if err != nil {
		return channelMemberStoreError(err)
	}
	if err := manager.refreshChannelLeaveRuntime(ctx, result.Channel.ID()); err != nil {
		return err
	}
	manager.triggerMemberReconcile()
	return nil
}

func (manager *ChannelManager) refreshChannelLeaveRuntime(ctx context.Context,
	channelID model.ChannelID,
) error {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return channelMemberStoreError(err)
	}
	if err := manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }); err != nil {
		return err
	}
	manager.markTopicForRoster(ctx, channelID)
	return nil
}

var _ peer.ChannelMemberLeaveController = (*ChannelManager)(nil)
