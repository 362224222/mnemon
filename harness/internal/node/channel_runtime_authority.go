package node

import (
	"context"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ReconcileRemoteRoster is the consumer-semantic authority boundary used by
// ChannelRuntime after a validated outbound Hello or Sync exchange. It shares
// the coordinator's single mutation token with inbound member control.
func (coordinator *ChannelAuthorityCoordinator) ReconcileRemoteRoster(ctx context.Context,
	update ChannelRuntimeRosterUpdate,
) (model.VerifiedRoster, error) {
	if ctx == nil || update.ChannelID.IsZero() || update.AuthenticatedPeerID.IsZero() ||
		(len(update.Records) > 0 && update.At.IsZero()) {
		return model.VerifiedRoster{}, fmt.Errorf(
			"%w: complete remote roster authority is required", ErrChannelAuthority)
	}
	update.Records = append([]model.Member(nil), update.Records...)
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	defer release()
	roster, err := coordinator.reconcileRemoteRoster(ctx, update)
	if err != nil || len(update.Records) > 0 {
		return roster, err
	}
	member, active := roster.CurrentMember(update.AuthenticatedPeerID)
	if !active || member.Status() != model.MemberActive {
		return model.VerifiedRoster{}, peer.ErrChannelMemberNotMember
	}
	return roster, nil
}

// reconcileRemoteRoster runs only while the caller holds the coordinator's
// single authority mutation token. The inbound service binds the secure peer
// before entry, then verifies the resulting active record and key after a
// proof-carrying merge returns and before it writes an ACK.
func (coordinator *ChannelAuthorityCoordinator) reconcileRemoteRoster(ctx context.Context,
	update ChannelRuntimeRosterUpdate,
) (model.VerifiedRoster, error) {
	if len(update.Records) == 0 {
		return coordinator.readRoster(ctx, update.ChannelID)
	}
	plan, err := coordinator.store.PrepareChannelRosterMerge(ctx, store.MergeChannelRosterSpec{
		ChannelID: update.ChannelID, AuthenticatedTransportPeerID: update.AuthenticatedPeerID,
		Records: update.Records, At: update.At,
	})
	if err != nil {
		return model.VerifiedRoster{}, mapChannelAuthorityError(err)
	}
	result, err := executeChannelAuthorityPlan(ctx, coordinator.runtime,
		channelAuthorityPlanSteps[store.MergeChannelRosterResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.MergeChannelRosterResult, error) {
				return coordinator.store.CommitChannelRosterMerge(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return coordinator.store.ResolveChannelRosterMerge(resolveCtx, plan)
			},
		})
	if err != nil {
		return model.VerifiedRoster{}, mapChannelAuthorityError(err)
	}
	switch result.Status {
	case store.ChannelRosterApplied, store.ChannelRosterDuplicate:
		return result.Roster, nil
	case store.ChannelRosterGap:
		return model.VerifiedRoster{}, peer.ErrChannelMemberRosterGap
	case store.ChannelRosterConflicted:
		return model.VerifiedRoster{}, peer.ErrChannelMemberRosterConflict
	default:
		return model.VerifiedRoster{}, fmt.Errorf(
			"%w: Store returned unknown roster result", ErrChannelAuthority)
	}
}
