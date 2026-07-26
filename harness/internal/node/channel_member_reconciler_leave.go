package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (worker *ChannelMemberReconciler) processLeaveTargets(ctx context.Context,
	targets []channelMemberLeaveTarget, at time.Time,
) error {
	for _, target := range targets {
		if target.attempts == store.ChannelLeaveMaximumAttempts {
			if err := worker.failLeaveTarget(ctx, target, target.attempts,
				target.nextAttemptAt, store.ChannelLeaveFailureAttemptsExhausted, at); err != nil {
				return err
			}
			continue
		}
		worker.recordInFlight(true)
		retryAt, started, err := worker.processLeaveTarget(ctx, target, at)
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
		attempts := target.attempts + 1
		if started && (disposition == channelMemberFailurePermanent ||
			attempts == store.ChannelLeaveMaximumAttempts) {
			failure := store.ChannelLeaveFailurePermanent
			if disposition != channelMemberFailurePermanent {
				failure = store.ChannelLeaveFailureAttemptsExhausted
			}
			if failErr := worker.failLeaveTarget(ctx, target, attempts, retryAt,
				failure, worker.now()); failErr != nil {
				return failErr
			}
			continue
		}
		worker.recordDurableFailure(disposition, err)
	}
	return nil
}

func (worker *ChannelMemberReconciler) processLeaveTarget(ctx context.Context,
	target channelMemberLeaveTarget, at time.Time,
) (time.Time, bool, error) {
	retryAt := at.Add(channelMemberRetryDelay(target.attempts + 1))
	if err := worker.backend.startLeave(ctx, target, at, retryAt); err != nil {
		return time.Time{}, false, err
	}
	worker.recordLeaveRequest()
	request, err := peer.NewLeaveRequest(target.request)
	if err != nil {
		return retryAt, true, err
	}
	receipt, err := worker.client.Leave(ctx, target.owner.PeerID(),
		target.owner.PublicKey(), request)
	if err != nil {
		return retryAt, true, err
	}
	return retryAt, true,
		worker.backend.settleLeave(ctx, target, receipt.SignedReceipt(), worker.now())
}

func (worker *ChannelMemberReconciler) failLeaveTarget(ctx context.Context,
	target channelMemberLeaveTarget, attempts uint64, nextAttemptAt time.Time,
	failure store.ChannelLeaveFailureCode, at time.Time,
) error {
	if err := worker.backend.failLeave(ctx, target, attempts, nextAttemptAt, failure, at); err != nil {
		if errors.Is(err, store.ErrChannelLeaveConflict) {
			worker.recordStale()
			return nil
		}
		return fmt.Errorf("%w: terminalize leave attempt: %w", ErrChannelMemberReconciler, err)
	}
	worker.recordDurableFailure(channelMemberFailurePermanent, errors.New(string(failure)))
	return nil
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
		target.retryGeneration <= model.MaxSQLiteInteger &&
		target.attempts <= store.ChannelLeaveMaximumAttempts && !target.nextAttemptAt.After(at) &&
		model.VerifyChannelLeaveRequest(target.channel.Descriptor(), local, target.request) == nil
}
