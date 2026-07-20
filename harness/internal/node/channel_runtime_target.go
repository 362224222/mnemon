package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type channelRuntimeTargetDisposition uint8

const (
	channelRuntimeTargetConverged channelRuntimeTargetDisposition = iota + 1
	channelRuntimeTargetPending
	channelRuntimeTargetRetry
	channelRuntimeTargetPermanent
	channelRuntimeTargetRescan
	channelRuntimeTargetCancelled
	channelRuntimeTargetFatal
)

type channelRuntimeTargetResult struct {
	target      channelRuntimeTarget
	disposition channelRuntimeTargetDisposition
	retryAfter  time.Duration
	completedAt time.Time
	err         error
}

func (runtime *ChannelRuntime) convergeTarget(ownerCtx, ctx context.Context,
	target channelRuntimeTarget,
) channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		return target.cancelled()
	}
	hello, err := peer.NewMemberHello(peer.MemberHelloSpec{ChannelID: target.key.channelID,
		ActiveMemberRecord: target.local, KnownRosterHead: target.roster.Head(),
		OwnerSignedProofChain: target.roster.Members()})
	if err != nil {
		return target.fatal(fmt.Errorf("construct full-proof Hello: %w", err))
	}
	ack, err := runtime.transport.Hello(ctx, target.key.peerID, hello)
	if err != nil {
		return runtime.handleTargetExchangeFailure(ownerCtx, ctx, target, err, true)
	}
	observedAt, timeFailure := runtime.targetTime(target)
	if timeFailure != nil {
		return *timeFailure
	}
	if observed := runtime.observeTarget(ownerCtx, ctx, target,
		model.ReachabilityReachable, observedAt); observed != nil {
		return *observed
	}
	if ack.IsZero() || ack.ChannelID() != target.key.channelID {
		return target.fatal(errors.New("transport returned an invalid Hello acknowledgement"))
	}
	if records := ack.MissingRecords(); len(records) > 0 {
		merged, result := runtime.mergeTargetRoster(ownerCtx, ctx, target, records, observedAt)
		if result != nil {
			return *result
		}
		if merged.Head() != target.roster.Head() {
			return target.rescan()
		}
	}
	if ack.RosterHead() != target.roster.Head() {
		return target.rescan()
	}
	if target.readiness.OutboundReady {
		return target.success(target.readiness.Ready())
	}
	return runtime.establishTargetBaseline(ownerCtx, ctx, target)
}

func (runtime *ChannelRuntime) handleTargetExchangeFailure(ownerCtx, ctx context.Context,
	target channelRuntimeTarget, err error, allowSync bool,
) channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		return target.cancelled()
	}
	observedAt, timeFailure := runtime.targetTime(target)
	if timeFailure != nil {
		return *timeFailure
	}
	disposition, retryAfter, reachable, observe, rosterGap := channelRuntimeRemoteFailure(err)
	if observe {
		observeCtx, cancel := runtime.targetObservationContext(ownerCtx, ctx)
		defer cancel()
		if observed := runtime.observeTarget(ownerCtx, observeCtx,
			target, reachable, observedAt); observed != nil {
			return *observed
		}
	}
	if rosterGap && allowSync {
		return runtime.syncTargetRoster(ownerCtx, ctx, target)
	}
	if disposition == channelRuntimeTargetPermanent {
		return target.permanent()
	}
	if disposition == channelRuntimeTargetFatal {
		return target.fatal(fmt.Errorf("member exchange: %w", err))
	}
	return target.retry(retryAfter)
}

func channelRuntimeRemoteFailure(err error) (
	channelRuntimeTargetDisposition, time.Duration, model.Reachability, bool, bool,
) {
	var remote *peer.ChannelMemberRemoteFailure
	if errors.As(err, &remote) {
		if remote.Code() == peer.ChannelErrorRosterGap {
			return channelRuntimeTargetRetry, remote.RetryAfter(),
				model.ReachabilityReachable, true, true
		}
		if remote.Retryable() {
			return channelRuntimeTargetRetry, remote.RetryAfter(),
				model.ReachabilityReachable, true, false
		}
		return channelRuntimeTargetPermanent, 0, model.ReachabilityReachable, true, false
	}
	if errors.Is(err, peer.ErrChannelMemberClientResponse) {
		return channelRuntimeTargetPermanent, 0, model.ReachabilityUnknown, false, false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, peer.ErrChannelMemberClientTransport) {
		return channelRuntimeTargetRetry, 0, model.ReachabilityUnreachable, true, false
	}
	return channelRuntimeTargetFatal, 0, model.ReachabilityUnknown, false, false
}

func (runtime *ChannelRuntime) targetObservationContext(ownerCtx,
	attemptCtx context.Context,
) (context.Context, context.CancelFunc) {
	if attemptCtx.Err() == nil {
		return attemptCtx, func() {}
	}
	return context.WithTimeout(ownerCtx, runtime.requestTimeout)
}

func (runtime *ChannelRuntime) syncTargetRoster(ownerCtx, ctx context.Context,
	target channelRuntimeTarget,
) channelRuntimeTargetResult {
	members := target.roster.Members()
	if len(members) == 0 {
		return target.fatal(errors.New("verified roster has no genesis"))
	}
	request, err := peer.NewSyncRequest(peer.SyncRequestSpec{ChannelID: target.key.channelID,
		AfterHead: members[0].Head()})
	if err != nil {
		return target.fatal(fmt.Errorf("construct roster Sync: %w", err))
	}
	result, err := runtime.transport.Sync(ctx, target.key.peerID, request)
	if err != nil {
		return runtime.handleTargetExchangeFailure(ownerCtx, ctx, target, err, false)
	}
	observedAt, timeFailure := runtime.targetTime(target)
	if timeFailure != nil {
		return *timeFailure
	}
	if observed := runtime.observeTarget(ownerCtx, ctx, target,
		model.ReachabilityReachable, observedAt); observed != nil {
		return *observed
	}
	if result.IsZero() || result.ChannelID() != target.key.channelID {
		return target.fatal(errors.New("transport returned an invalid roster Sync result"))
	}
	merged, mergeResult := runtime.mergeTargetRoster(ownerCtx, ctx, target,
		result.OwnerSignedRecords(), observedAt)
	if mergeResult != nil {
		return *mergeResult
	}
	if merged.Head() != target.roster.Head() {
		return target.rescan()
	}
	return target.retry(0)
}

func (runtime *ChannelRuntime) mergeTargetRoster(ownerCtx, ctx context.Context,
	target channelRuntimeTarget, records []model.Member, now time.Time,
) (model.VerifiedRoster, *channelRuntimeTargetResult) {
	if ownerCtx.Err() != nil {
		result := target.cancelled()
		return model.VerifiedRoster{}, &result
	}
	roster, err := runtime.authority.ReconcileRemoteRoster(ctx, ChannelRuntimeRosterUpdate{
		ChannelID: target.key.channelID, AuthenticatedPeerID: target.key.peerID,
		Records: append([]model.Member(nil), records...), At: now})
	if err != nil {
		result := runtime.classifyRosterMergeFailure(ownerCtx, target, err)
		return model.VerifiedRoster{}, &result
	}
	if ownerCtx.Err() != nil {
		result := target.cancelled()
		return model.VerifiedRoster{}, &result
	}
	if roster.IsZero() || roster.Descriptor().Descriptor().ID() != target.key.channelID {
		result := target.fatal(errors.New("authority returned an invalid roster"))
		return model.VerifiedRoster{}, &result
	}
	return roster, nil
}

func (runtime *ChannelRuntime) classifyRosterMergeFailure(ownerCtx context.Context,
	target channelRuntimeTarget,
	err error,
) channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		return target.cancelled()
	}
	switch {
	case errors.Is(err, peer.ErrChannelMemberRosterGap):
		return target.retry(0)
	case errors.Is(err, peer.ErrChannelMemberRosterConflict),
		errors.Is(err, store.ErrChannelAuthorityPlanDiverged),
		errors.Is(err, store.ErrChannelRuntimeConflict):
		return target.rescan()
	case errors.Is(err, peer.ErrChannelMemberNotMember),
		errors.Is(err, peer.ErrChannelMemberRevoked),
		errors.Is(err, peer.ErrChannelMemberClosed):
		return target.permanent()
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return target.retry(0)
	default:
		return target.fatal(fmt.Errorf("merge remote roster: %w", err))
	}
}

func (runtime *ChannelRuntime) observeTarget(ownerCtx, ctx context.Context,
	target channelRuntimeTarget, reachability model.Reachability, now time.Time,
) *channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		result := target.cancelled()
		return &result
	}
	observed, err := runtime.store.SetPeerReachability(ctx, store.SetPeerReachabilitySpec{
		ChannelID: target.key.channelID, PeerID: target.key.peerID,
		OriginEpoch: target.generation.originEpoch, ExpectedRosterHead: target.generation.rosterHead,
		ExpectedMemberHead: target.generation.memberHead, ExpectedBindingState: target.generation.bindingState,
		Reachability: reachability, At: now})
	if err == nil {
		if !channelRuntimeExactReachability(target, reachability, observed.Peer) {
			invalid := target.fatal(errors.New("Store returned an unfenced Peer reachability observation"))
			return &invalid
		}
		return nil
	}
	var result channelRuntimeTargetResult
	if ownerCtx.Err() != nil {
		result = target.cancelled()
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result = target.retry(0)
	} else if expectedChannelRuntimeRace(err) {
		return nil
	} else {
		result = target.fatal(fmt.Errorf("record Peer reachability: %w", err))
	}
	return &result
}

func channelRuntimeExactReachability(target channelRuntimeTarget, reachability model.Reachability,
	projection store.PeerReachabilityProjection,
) bool {
	return projection.ChannelID == target.key.channelID && projection.PeerID == target.key.peerID &&
		projection.OriginEpoch == target.generation.originEpoch &&
		projection.RosterHead == target.generation.rosterHead &&
		projection.BindingState == target.generation.bindingState &&
		projection.Reachability == reachability
}

func expectedChannelRuntimeRace(err error) bool {
	return errors.Is(err, store.ErrChannelRuntimeConflict) ||
		errors.Is(err, store.ErrChannelRuntimeAuthority) ||
		errors.Is(err, store.ErrChannelBaselineConflict) ||
		errors.Is(err, store.ErrChannelBaselineEpochMismatch) ||
		errors.Is(err, store.ErrChannelBaselineAuthority) ||
		errors.Is(err, store.ErrChannelAuthorityPlanDiverged)
}

func (runtime *ChannelRuntime) targetTime(target channelRuntimeTarget) (
	time.Time, *channelRuntimeTargetResult,
) {
	now, err := channelRuntimeNow(runtime.clock)
	if err == nil {
		return now, nil
	}
	result := target.fatal(err)
	return time.Time{}, &result
}

func (target channelRuntimeTarget) success(ready bool) channelRuntimeTargetResult {
	disposition := channelRuntimeTargetPending
	if ready {
		disposition = channelRuntimeTargetConverged
	}
	return channelRuntimeTargetResult{target: target, disposition: disposition}
}

func (target channelRuntimeTarget) retry(after time.Duration) channelRuntimeTargetResult {
	return channelRuntimeTargetResult{target: target,
		disposition: channelRuntimeTargetRetry, retryAfter: after}
}

func (target channelRuntimeTarget) permanent() channelRuntimeTargetResult {
	return channelRuntimeTargetResult{target: target, disposition: channelRuntimeTargetPermanent}
}

func (target channelRuntimeTarget) rescan() channelRuntimeTargetResult {
	return channelRuntimeTargetResult{target: target, disposition: channelRuntimeTargetRescan}
}

func (target channelRuntimeTarget) cancelled() channelRuntimeTargetResult {
	return channelRuntimeTargetResult{target: target, disposition: channelRuntimeTargetCancelled}
}

func (target channelRuntimeTarget) fatal(err error) channelRuntimeTargetResult {
	return channelRuntimeTargetResult{target: target, disposition: channelRuntimeTargetFatal, err: err}
}
