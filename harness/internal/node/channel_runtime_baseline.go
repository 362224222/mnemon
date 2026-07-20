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

func (runtime *ChannelRuntime) establishTargetBaseline(ownerCtx, ctx context.Context,
	target channelRuntimeTarget,
) channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		return target.cancelled()
	}
	baseline, failure := runtime.reserveTargetBaseline(ownerCtx, ctx, target)
	if failure != nil {
		return *failure
	}
	ack, confirmedAt, failure := runtime.exchangeTargetBaseline(ownerCtx, ctx, target, baseline)
	if failure != nil {
		return *failure
	}
	if failure := runtime.confirmTargetBaseline(ownerCtx, ctx, target,
		baseline, ack, confirmedAt); failure != nil {
		return *failure
	}
	ready := target.readiness.BindingState == model.BindingActive && target.readiness.InboundReady
	return target.success(ready)
}

func (runtime *ChannelRuntime) reserveTargetBaseline(ownerCtx, ctx context.Context,
	target channelRuntimeTarget,
) (store.ChannelDataBaseline, *channelRuntimeTargetResult) {
	reservedAt, timeFailure := runtime.targetTime(target)
	if timeFailure != nil {
		return store.ChannelDataBaseline{}, timeFailure
	}
	reserved, err := runtime.store.ReserveOutboundChannelBaseline(ctx,
		store.ReserveOutboundChannelBaselineSpec{ChannelID: target.key.channelID,
			TargetPeerID: target.key.peerID, ExpectedRosterHead: target.generation.rosterHead,
			At: reservedAt})
	if err != nil {
		result := runtime.classifyTargetBaselineStoreFailure(ownerCtx, target,
			"reserve outbound baseline", err)
		return store.ChannelDataBaseline{}, &result
	}
	baseline := reserved.Baseline
	if baseline.ChannelID != target.key.channelID || baseline.OriginPeerID != target.local.PeerID() ||
		baseline.OriginEpoch != target.local.OriginEpoch() ||
		baseline.BaselineChannelSequence > model.MaxSQLiteInteger {
		result := target.fatal(errors.New("Store returned an unfenced outbound baseline"))
		return store.ChannelDataBaseline{}, &result
	}
	return baseline, nil
}

func (runtime *ChannelRuntime) exchangeTargetBaseline(ownerCtx, ctx context.Context,
	target channelRuntimeTarget, baseline store.ChannelDataBaseline,
) (peer.DataBaselineAck, time.Time, *channelRuntimeTargetResult) {
	request, err := peer.NewDataBaseline(peer.DataBaselineSpec{ChannelID: baseline.ChannelID,
		OriginPeerID: baseline.OriginPeerID, OriginEpoch: baseline.OriginEpoch,
		BaselineChannelSequence: baseline.BaselineChannelSequence})
	if err != nil {
		result := target.fatal(fmt.Errorf("construct outbound baseline: %w", err))
		return peer.DataBaselineAck{}, time.Time{}, &result
	}
	ack, err := runtime.transport.Baseline(ctx, target.key.peerID, request)
	if err != nil {
		result := runtime.handleTargetExchangeFailure(ownerCtx, ctx, target, err, true)
		return peer.DataBaselineAck{}, time.Time{}, &result
	}
	confirmedAt, timeFailure := runtime.targetTime(target)
	if timeFailure != nil {
		return peer.DataBaselineAck{}, time.Time{}, timeFailure
	}
	if observed := runtime.observeTarget(ownerCtx, ctx, target,
		model.ReachabilityReachable, confirmedAt); observed != nil {
		return peer.DataBaselineAck{}, time.Time{}, observed
	}
	if !channelRuntimeExactBaselineAck(baseline, ack) {
		result := target.fatal(errors.New("transport returned an invalid baseline acknowledgement"))
		return peer.DataBaselineAck{}, time.Time{}, &result
	}
	return ack, confirmedAt, nil
}

func (runtime *ChannelRuntime) confirmTargetBaseline(ownerCtx, ctx context.Context,
	target channelRuntimeTarget, baseline store.ChannelDataBaseline,
	ack peer.DataBaselineAck, confirmedAt time.Time,
) *channelRuntimeTargetResult {
	confirmed, err := runtime.store.ConfirmOutboundChannelBaseline(ctx,
		store.ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: target.key.peerID,
			ExpectedRosterHead: target.generation.rosterHead,
			Ack: store.ChannelDataBaselineAck{ChannelID: ack.ChannelID(),
				OriginPeerID: ack.OriginPeerID(), OriginEpoch: ack.OriginEpoch(),
				BaselineChannelSequence: ack.BaselineChannelSequence()}, At: confirmedAt})
	if err != nil {
		result := runtime.classifyTargetBaselineStoreFailure(ownerCtx, target,
			"confirm outbound baseline", err)
		return &result
	}
	if confirmed.Ack.ChannelID != baseline.ChannelID ||
		confirmed.Ack.OriginPeerID != baseline.OriginPeerID ||
		confirmed.Ack.OriginEpoch != baseline.OriginEpoch ||
		confirmed.Ack.BaselineChannelSequence != baseline.BaselineChannelSequence {
		result := target.fatal(errors.New("Store confirmed a different outbound baseline"))
		return &result
	}
	return nil
}

func (runtime *ChannelRuntime) classifyTargetBaselineStoreFailure(ownerCtx context.Context,
	target channelRuntimeTarget, operation string, err error,
) channelRuntimeTargetResult {
	if ownerCtx.Err() != nil {
		return target.cancelled()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return target.retry(0)
	}
	if expectedChannelRuntimeRace(err) {
		return target.rescan()
	}
	return target.fatal(fmt.Errorf("%s: %w", operation, err))
}

func channelRuntimeExactBaselineAck(baseline store.ChannelDataBaseline,
	ack peer.DataBaselineAck,
) bool {
	return !ack.IsZero() && ack.ChannelID() == baseline.ChannelID &&
		ack.OriginPeerID() == baseline.OriginPeerID && ack.OriginEpoch() == baseline.OriginEpoch &&
		ack.BaselineChannelSequence() == baseline.BaselineChannelSequence
}
