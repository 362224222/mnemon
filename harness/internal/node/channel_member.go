package node

import (
	"context"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) ReconcileMemberHelloGate(ctx context.Context,
	control peer.ChannelMemberHelloControl,
) (peer.ChannelMemberHelloAuthority, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	rosterChanged := false
	if len(control.ProofRecords) != 0 {
		result, err := manager.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
			ChannelID: control.ChannelID, AuthenticatedTransportPeerID: control.AuthenticatedPeerID,
			Records: control.ProofRecords, At: control.At})
		if err != nil {
			return peer.ChannelMemberHelloAuthority{}, channelMemberStoreError(err)
		}
		switch result.Status {
		case store.ChannelRosterApplied:
			rosterChanged = true
		case store.ChannelRosterDuplicate:
		case store.ChannelRosterGap:
			return peer.ChannelMemberHelloAuthority{}, peer.ErrChannelMemberRosterGap
		case store.ChannelRosterConflicted:
			return peer.ChannelMemberHelloAuthority{}, peer.ErrChannelMemberRosterConflict
		default:
			return peer.ChannelMemberHelloAuthority{}, peer.ErrChannelMemberRosterConflict
		}
	}
	roster, err := manager.refreshMemberAuthority(ctx, control.ChannelID)
	if err == nil && rosterChanged {
		manager.triggerMemberReconcile()
	}
	return peer.ChannelMemberHelloAuthority{Roster: roster}, err
}

func (manager *ChannelManager) FreezeMemberRosterForSync(ctx context.Context,
	control peer.ChannelMemberSyncControl,
) (peer.ChannelMemberRosterSnapshot, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	roster, err := manager.readMemberRoster(ctx, control.ChannelID)
	return peer.ChannelMemberRosterSnapshot{Roster: roster}, err
}

func (manager *ChannelManager) InstallMemberBaselineGate(ctx context.Context,
	control peer.ChannelMemberBaselineControl,
) (peer.ChannelMemberBaselineAuthority, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result, err := manager.store.InstallInboundChannelBaseline(ctx,
		store.InstallInboundChannelBaselineSpec{AuthenticatedPeerID: control.AuthenticatedPeerID,
			Baseline: store.ChannelDataBaseline{ChannelID: control.Baseline.ChannelID,
				OriginPeerID: control.Baseline.OriginPeerID, OriginEpoch: control.Baseline.OriginEpoch,
				BaselineChannelSequence: control.Baseline.BaselineChannelSequence}, At: control.At})
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, channelMemberStoreError(err)
	}
	roster, err := manager.refreshMemberAuthority(ctx, control.Baseline.ChannelID)
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, err
	}
	baseline := peer.DataBaselineSpec{ChannelID: result.Baseline.ChannelID,
		OriginPeerID: result.Baseline.OriginPeerID, OriginEpoch: result.Baseline.OriginEpoch,
		BaselineChannelSequence: result.Baseline.BaselineChannelSequence}
	manager.markTopicForRoster(ctx, control.Baseline.ChannelID)
	manager.triggerMemberReconcile()
	return peer.ChannelMemberBaselineAuthority{Baseline: baseline, Roster: roster}, nil
}

func (manager *ChannelManager) refreshMemberAuthority(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	if err := manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }); err != nil {
		return model.VerifiedRoster{}, err
	}
	return rosterFromMesh(mesh, channelID)
}

func (manager *ChannelManager) readMemberRoster(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	return rosterFromMesh(mesh, channelID)
}

func rosterFromMesh(mesh store.ChannelMeshAuthority,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Roster(), nil
		}
	}
	return model.VerifiedRoster{}, peer.ErrChannelMemberNotMember
}

func (manager *ChannelManager) markTopicForRoster(ctx context.Context, channelID model.ChannelID) {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return
	}
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			manager.markTopicJoined(ctx, channel.Channel())
			return
		}
	}
}

func channelMemberStoreError(err error) error {
	switch {
	case errors.Is(err, store.ErrChannelBaselineConflict):
		return peer.ErrChannelMemberBaselineConflict
	case errors.Is(err, store.ErrChannelBaselineEpochMismatch):
		return peer.ErrChannelMemberEpochMismatch
	case errors.Is(err, store.ErrChannelRosterConflict), errors.Is(err, store.ErrChannelRosterInput):
		return peer.ErrChannelMemberRosterConflict
	case errors.Is(err, store.ErrChannelLeaveInput), errors.Is(err, store.ErrChannelLeaveConflict):
		return peer.ErrChannelMemberRosterConflict
	case errors.Is(err, store.ErrChannelLeaveAuthority):
		return peer.ErrChannelMemberNotMember
	case errors.Is(err, store.ErrChannelBaselineAuthority):
		return peer.ErrChannelMemberNotMember
	default:
		return err
	}
}

var _ peer.ChannelMemberController = (*ChannelManager)(nil)
