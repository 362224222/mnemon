package node

import (
	"context"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const channelReplayProbeLimit = 5 * time.Second

func (manager *ChannelManager) ChannelReplayProbe(ctx context.Context,
	metadata RequestMetadata, request ChannelReplayProbeRequest,
) (ChannelReplayProbeResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelReplayProbeResponse{}, apiErr
	}
	if request.SourceChannel == "" || request.TargetChannel == "" ||
		request.SourceChannel == request.TargetChannel {
		return ChannelReplayProbeResponse{}, NewAPIError(CodeInvalidArgument,
			"Channel replay probe requires distinct source and target Channels")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	source, target, apiErr := manager.selectReplayProbeChannels(ctx, request)
	if apiErr != nil {
		return ChannelReplayProbeResponse{}, apiErr
	}
	candidate, err := manager.store.ReadWrongTopicReplayCandidate(ctx,
		source.Channel().ID(), target.Channel().ID())
	if err != nil {
		return ChannelReplayProbeResponse{}, channelAPIError(err)
	}
	session, err := manager.runtime.Session(target.Channel().ID())
	if err != nil {
		return ChannelReplayProbeResponse{}, channelAPIError(err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, channelReplayProbeLimit)
	defer cancel()
	result, err := session.ProbeWrongTopicReplay(probeCtx, candidate.Publication)
	if err != nil {
		return ChannelReplayProbeResponse{}, channelAPIError(err)
	}
	after, err := manager.store.ReadChannelMutationCounts(ctx, target.Channel().ID())
	if err != nil {
		return ChannelReplayProbeResponse{}, channelAPIError(err)
	}
	status := result.Reason
	rejection := ""
	if result.Rejected {
		status = "rejected"
		rejection = result.Reason
	}
	before := channelMutationCountsView(candidate.TargetMutationCounts)
	afterView := channelMutationCountsView(after)
	return ChannelReplayProbeResponse{SchemaVersion: SchemaVersion,
		Status: status, ReplayAttempted: true, Rejection: rejection,
		SourceChannel: source.Channel().LocalAlias(), TargetChannel: target.Channel().LocalAlias(),
		SourceChannelIDDigest: candidate.SourceChannelDigest.String(),
		TargetChannelIDDigest: candidate.TargetChannelDigest.String(),
		PublicationDigest:     candidate.PublicationDigest.String(),
		EventKey:              channelEventKeyView(candidate.EventKey),
		EventDigest:           candidate.EventDigest.String(),
		TargetBefore:          before, TargetAfter: afterView,
		TargetMutationSuppressed: before == afterView}, nil
}

func (manager *ChannelManager) selectReplayProbeChannels(ctx context.Context,
	request ChannelReplayProbeRequest,
) (store.ChannelControlChannel, store.ChannelControlChannel, *APIError) {
	authority, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		return store.ChannelControlChannel{}, store.ChannelControlChannel{}, channelAPIError(err)
	}
	var source, target store.ChannelControlChannel
	sourceFound, targetFound := false, false
	for _, channel := range authority.Channels() {
		switch channel.Channel().LocalAlias() {
		case request.SourceChannel:
			source, sourceFound = channel, true
		case request.TargetChannel:
			target, targetFound = channel, true
		}
	}
	if !sourceFound || !targetFound {
		return store.ChannelControlChannel{}, store.ChannelControlChannel{},
			NewAPIError(CodeNotMember, "Channel is not present on this Node")
	}
	return source, target, nil
}

func channelMutationCountsView(counts store.ChannelMutationCounts) ChannelMutationCounts {
	return ChannelMutationCounts{Events: counts.Events, Works: counts.Works}
}
