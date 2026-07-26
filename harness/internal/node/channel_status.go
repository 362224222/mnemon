package node

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"sort"
	"time"
)

func (manager *ChannelManager) ChannelStatus(ctx context.Context, metadata RequestMetadata,
) (ChannelStatusResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelStatusResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	observation, err := manager.store.ReadChannelObservation(ctx)
	if err != nil {
		return ChannelStatusResponse{}, channelAPIError(err)
	}
	channels := make([]ChannelView, 0, len(observation.Channels()))
	for _, channel := range observation.Channels() {
		channels = append(channels, manager.projectChannelView(observation.LocalPeerID(), channel))
	}
	sort.Slice(channels, func(left, right int) bool { return channels[left].Alias < channels[right].Alias })
	return ChannelStatusResponse{SchemaVersion: SchemaVersion,
		Status: "ok", Channels: channels}, nil
}

func (manager *ChannelManager) validateCall(ctx context.Context,
	metadata RequestMetadata,
) *APIError {
	if manager == nil || manager.store == nil || manager.runtime == nil || ctx == nil || ctx.Err() != nil {
		return NewAPIError(CodeMnemondUnavailable, "Channel controller is unavailable")
	}
	if metadata.Profile.ID() != model.TeamworkProfileID() || !metadata.Profile.Enabled() {
		return NewAPIError(CodeAuthenticationFailed, "profile authentication failed")
	}
	return nil
}

func (manager *ChannelManager) selectChannel(ctx context.Context,
	selector string,
) (store.ChannelControlAuthority, store.ChannelControlChannel, *APIError) {
	authority, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		return store.ChannelControlAuthority{}, store.ChannelControlChannel{}, channelAPIError(err)
	}
	channels := authority.Channels()
	if selector == "" {
		if len(channels) != 1 {
			return store.ChannelControlAuthority{}, store.ChannelControlChannel{},
				NewAPIError(CodeInvalidArgument, "Channel selector is required")
		}
		return authority, channels[0], nil
	}
	for _, channel := range channels {
		if channel.Channel().LocalAlias() == selector {
			return authority, channel, nil
		}
	}
	return store.ChannelControlAuthority{}, store.ChannelControlChannel{},
		NewAPIError(CodeNotMember, "Channel is not present on this Node")
}

func (manager *ChannelManager) readChannelView(ctx context.Context,
	channelID model.ChannelID,
) (ChannelView, error) {
	observation, err := manager.store.ReadChannelObservation(ctx)
	if err != nil {
		return ChannelView{}, err
	}
	for _, channel := range observation.Channels() {
		if channel.Channel().ID() == channelID {
			return manager.projectChannelView(observation.LocalPeerID(), channel), nil
		}
	}
	return ChannelView{}, store.ErrChannelStatusAuthority
}

func (manager *ChannelManager) channelTopicJoined(channel model.Channel) bool {
	return channel.TopicState() == model.TopicJoined && manager.channelSessionReady(channel.ID())
}

func (manager *ChannelManager) channelTopicStatus(channel model.Channel) string {
	return observedChannelTopicState(channel, manager.channelSessionReady(channel.ID()))
}

func (manager *ChannelManager) refreshAndJoin(ctx context.Context, channel model.Channel) {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil || manager.runtime.Reconcile(mesh) != nil {
		return
	}
	manager.markTopicJoined(ctx, channel)
	manager.triggerMemberReconcile()
}

func (manager *ChannelManager) markTopicJoined(ctx context.Context, channel model.Channel) {
	if channel.ID().IsZero() || !manager.runtime.HasCurrentSession(channel.ID()) {
		return
	}
	current := channel.TopicState()
	if current == model.TopicNotJoined {
		result, err := manager.store.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
			ChannelID: channel.ID(), ExpectedStatus: model.ChannelActive,
			ExpectedRosterHead: channel.RosterHead(), ExpectedTopicState: model.TopicNotJoined,
			TopicState: model.TopicJoining, At: manager.clock.Now()})
		if err != nil {
			return
		}
		current = result.Topic.TopicState
	}
	readiness, err := manager.store.ReadChannelBaselineReadiness(ctx, channel.ID())
	if err != nil {
		return
	}
	if !channelBaselinesReady(readiness) {
		return
	}
	if current == model.TopicJoining {
		_, _ = manager.store.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
			ChannelID: channel.ID(), ExpectedStatus: model.ChannelActive,
			ExpectedRosterHead: channel.RosterHead(), ExpectedTopicState: model.TopicJoining,
			TopicState: model.TopicJoined, At: manager.clock.Now()})
	}
}

func channelBaselinesReady(readiness []store.ChannelPeerReadiness) bool {
	for _, remote := range readiness {
		if remote.BindingState == model.BindingRevoked {
			continue
		}
		if remote.BindingState != model.BindingActive || !remote.InboundReady || !remote.OutboundReady {
			return false
		}
	}
	return true
}

func inviteView(expiresAt time.Time, maxUses, usedUses uint8, status string,
	now time.Time,
) ChannelInviteView {
	if status == "open" && !now.Before(expiresAt) {
		status = "expired"
	}
	remaining := uint8(0)
	if usedUses < maxUses {
		remaining = maxUses - usedUses
	}
	return ChannelInviteView{ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
		RemainingUses: remaining, Status: status}
}

func remainingChannelSeats(roster model.VerifiedRoster, limit uint8) uint8 {
	latest := make(map[model.PeerID]model.MemberStatus)
	for _, member := range roster.Members() {
		latest[member.PeerID()] = member.Status()
	}
	active := uint8(0)
	for _, status := range latest {
		if status == model.MemberActive {
			active++
		}
	}
	if active >= limit {
		return 0
	}
	return limit - active
}

func memberAlias(peerID model.PeerID) string {
	digest := model.Sum([]byte(peerID.String())).Bytes()
	return "member-" + hex.EncodeToString(digest[:4])
}

func channelAPIError(err error) *APIError {
	if err == nil {
		return nil
	}
	var protocolFailure *peer.ChannelProtocolFailure
	if errors.As(err, &protocolFailure) {
		code := ErrorCode(protocolFailure.Code())
		if code.Valid() {
			return NewAPIError(code, "Channel operation was rejected")
		}
	}
	switch {
	case errors.Is(err, store.ErrNodeChannelLimit):
		return NewAPIError(CodeNodeChannelLimit, "Node Channel limit reached")
	case errors.Is(err, store.ErrChannelFull):
		return NewAPIError(CodeChannelFull, "Channel member limit reached")
	case errors.Is(err, store.ErrChannelInviteOwner):
		return NewAPIError(CodeWrongOwner, "local Node is not the Channel owner")
	case errors.Is(err, store.ErrChannelInviteUnavailable):
		return NewAPIError(CodeChannelClosed, "Channel invite is unavailable")
	case errors.Is(err, store.ErrChannelJoinConflict), errors.Is(err, store.ErrChannelAuthorityInvariant):
		return NewAPIError(CodeRosterConflict, "Channel authority conflicts with durable state")
	case errors.Is(err, peer.ErrChannelEnrollmentOutcomeUnknown), errors.Is(err, peer.ErrMeshRuntime),
		errors.Is(err, peer.ErrChannelEnrollmentProtocol):
		return NewAPIError(CodeOwnerUnreachable, "Channel owner could not be reached")
	case errors.Is(err, peer.ErrGossipTopic):
		return NewAPIError(CodeOperationPending, "Channel topic is unavailable")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return NewAPIError(CodeOwnerUnreachable, "Channel operation was cancelled")
	case errors.Is(err, store.ErrChannelCreateInput), errors.Is(err, store.ErrChannelInviteInput),
		errors.Is(err, store.ErrChannelJoinInput), errors.Is(err, store.ErrChannelAbandonInput):
		return NewAPIError(CodeInvalidArgument, "Channel operation input is invalid")
	case errors.Is(err, store.ErrChannelAbandonMissing):
		return NewAPIError(CodeNotMember, "Channel is not present on this Node")
	case errors.Is(err, store.ErrChannelAbandonTerminal):
		return NewAPIError(CodeChannelClosed, "Channel is already terminal")
	case errors.Is(err, store.ErrChannelAbandonStale):
		return NewAPIError(CodeOperationMismatch, "Channel authority changed")
	default:
		return NewAPIError(CodeInternal, "durable Channel operation failed")
	}
}

type channelEnrollmentOwnerStore struct{ manager *ChannelManager }

func (owned channelEnrollmentOwnerStore) PrepareChannelEnrollment(ctx context.Context,
	spec store.PrepareChannelEnrollmentSpec,
) (store.PrepareChannelEnrollmentResult, error) {
	owned.manager.mu.Lock()
	defer owned.manager.mu.Unlock()
	return owned.manager.store.PrepareChannelEnrollment(ctx, spec)
}

func (owned channelEnrollmentOwnerStore) AcceptChannelEnrollment(ctx context.Context,
	spec store.AcceptChannelEnrollmentSpec,
) (store.AcceptChannelEnrollmentResult, error) {
	owned.manager.mu.Lock()
	defer owned.manager.mu.Unlock()
	result, err := owned.manager.store.AcceptChannelEnrollment(ctx, spec)
	if err != nil {
		return result, err
	}
	mesh, err := owned.manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return result, err
	}
	if err := owned.manager.runtime.Reconcile(mesh); err != nil {
		return result, err
	}
	owned.manager.markTopicJoined(ctx, result.Channel)
	owned.manager.triggerMemberReconcile()
	return result, nil
}

func (manager *ChannelManager) EnrollmentOwnerStore() peer.ChannelEnrollmentOwnerStore {
	return channelEnrollmentOwnerStore{manager: manager}
}
