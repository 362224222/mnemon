package node

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
)

func (worker *ChannelMemberReconciler) processLeaveTargets(ctx context.Context,
	targets []channelMemberLeaveTarget, at time.Time,
) error {
	for _, target := range targets {
		worker.recordInFlight(true)
		err := worker.processLeaveTarget(ctx, target, at)
		worker.recordInFlight(false)
		if err == nil {
			worker.recordLeaveSettlement()
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		disposition := classifyChannelMemberFailure(err)
		if disposition == channelMemberFailureFatal {
			return fmt.Errorf("%w: reconcile leave target: %w", ErrChannelMemberReconciler, err)
		}
		worker.recordDurableFailure(disposition, err)
	}
	return nil
}

func (worker *ChannelMemberReconciler) processLeaveTarget(ctx context.Context,
	target channelMemberLeaveTarget, at time.Time,
) error {
	retryAt := at.Add(channelMemberRetryDelay(target.attempts + 1))
	if err := worker.backend.startLeave(ctx, target, at, retryAt); err != nil {
		return err
	}
	worker.recordLeaveRequest()
	request, err := peer.NewLeaveRequest(target.request)
	if err != nil {
		return err
	}
	receipt, err := worker.client.Leave(ctx, target.owner.PeerID(),
		target.owner.PublicKey(), request)
	if err != nil {
		return err
	}
	return worker.backend.settleLeave(ctx, target, receipt.SignedReceipt(), worker.now())
}

func validateChannelMemberLeaveTargets(targets []channelMemberLeaveTarget, at time.Time) error {
	if len(targets) > model.MaxChannelsPerNode {
		return fmt.Errorf("%w: leave target bound exceeded", ErrChannelMemberReconciler)
	}
	sort.Slice(targets, func(left, right int) bool {
		return targets[left].channel.ID().String() < targets[right].channel.ID().String()
	})
	for index, target := range targets {
		if !validChannelMemberLeaveTarget(target, at) ||
			index > 0 && target.channel.ID() == targets[index-1].channel.ID() {
			return fmt.Errorf("%w: invalid or duplicate leave target", ErrChannelMemberReconciler)
		}
	}
	return nil
}

func validChannelMemberLeaveTarget(target channelMemberLeaveTarget, at time.Time) bool {
	request := target.request.Record()
	local, localOK := target.roster.CurrentMember(request.MemberPeerID())
	owner, ownerOK := target.roster.CurrentMember(target.channel.OwnerPeerID())
	return target.channel.Status() == model.ChannelLeaving &&
		target.channel.TopicState() == model.TopicLeft && !target.roster.IsZero() &&
		target.channel.RosterHead() == target.roster.Head() && localOK && ownerOK &&
		local.Status() == model.MemberActive && owner.Status() == model.MemberActive &&
		owner.Head() == target.owner.Head() && owner.PeerID() != local.PeerID() &&
		request.ChannelID() == target.channel.ID() && request.ActiveMemberHead() == local.Head() &&
		request.KnownRosterHead() == target.roster.Head() &&
		request.RequestedAt() == target.channel.UpdatedAt() &&
		target.attempts < model.MaxSQLiteInteger && !target.nextAttemptAt.After(at) &&
		model.VerifyChannelLeaveRequest(target.channel.Descriptor(), local, target.request) == nil
}
