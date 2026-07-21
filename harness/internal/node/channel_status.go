package node

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) ChannelStatus(ctx context.Context, metadata localapi.RequestMetadata,
) (localapi.ChannelStatusResponse, *localapi.APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return localapi.ChannelStatusResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, err := manager.store.ReadChannelStatusAuthority(ctx)
	if err != nil {
		return localapi.ChannelStatusResponse{}, channelAPIError(err)
	}
	channels := make([]localapi.ChannelView, 0, len(authority.Channels()))
	for _, channel := range authority.Channels() {
		channels = append(channels, manager.projectChannelView(ctx, authority.LocalPeerID(), channel))
	}
	sort.Slice(channels, func(left, right int) bool { return channels[left].Alias < channels[right].Alias })
	return localapi.ChannelStatusResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "ok", Channels: channels}, nil
}

func (manager *ChannelManager) validateCall(ctx context.Context,
	metadata localapi.RequestMetadata,
) *localapi.APIError {
	if manager == nil || manager.store == nil || manager.runtime == nil || ctx == nil || ctx.Err() != nil {
		return localapi.NewAPIError(localapi.CodeMnemondUnavailable, "Channel controller is unavailable")
	}
	if metadata.Profile.ID() != model.TeamworkProfileID() || !metadata.Profile.Enabled() {
		return localapi.NewAPIError(localapi.CodeAuthenticationFailed, "profile authentication failed")
	}
	return nil
}

func (manager *ChannelManager) selectChannel(ctx context.Context,
	selector string,
) (store.ChannelControlAuthority, store.ChannelControlChannel, *localapi.APIError) {
	authority, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		return store.ChannelControlAuthority{}, store.ChannelControlChannel{}, channelAPIError(err)
	}
	channels := authority.Channels()
	if selector == "" {
		if len(channels) != 1 {
			return store.ChannelControlAuthority{}, store.ChannelControlChannel{},
				localapi.NewAPIError(localapi.CodeInvalidArgument, "Channel selector is required")
		}
		return authority, channels[0], nil
	}
	for _, channel := range channels {
		if channel.Channel().LocalAlias() == selector {
			return authority, channel, nil
		}
	}
	return store.ChannelControlAuthority{}, store.ChannelControlChannel{},
		localapi.NewAPIError(localapi.CodeNotMember, "Channel is not present on this Node")
}

func (manager *ChannelManager) readChannelView(ctx context.Context,
	channelID model.ChannelID,
) (localapi.ChannelView, error) {
	authority, err := manager.store.ReadChannelStatusAuthority(ctx)
	if err != nil {
		return localapi.ChannelView{}, err
	}
	for _, channel := range authority.Channels() {
		if channel.Channel().ID() == channelID {
			return manager.projectChannelView(ctx, authority.LocalPeerID(), channel), nil
		}
	}
	return localapi.ChannelView{}, store.ErrChannelStatusAuthority
}

func (manager *ChannelManager) projectChannelView(ctx context.Context, local model.PeerID,
	durable store.ChannelStatusChannel,
) localapi.ChannelView {
	channel, roster := durable.Channel(), durable.Roster()
	readiness := manager.channelReadiness(ctx, channel.ID())
	bindings := make(map[model.PeerID]model.PeerBinding)
	for _, binding := range durable.Bindings() {
		bindings[binding.PeerID()] = binding
	}
	latest := make(map[model.PeerID]model.Member)
	for _, member := range roster.Members() {
		latest[member.PeerID()] = member
	}
	members := make([]localapi.ChannelMemberView, 0, len(latest))
	ready := uint8(0)
	for peerID, member := range latest {
		projected := localapi.ChannelMemberView{Alias: memberAlias(peerID), PeerID: peerID.String(),
			Status: string(member.Status()), Binding: "none", Reachability: "unknown"}
		if peerID == local {
			projected.Binding, projected.Reachability = "self", "self"
			projected.BaselineReady = manager.channelTopicJoined(channel)
		} else if binding, ok := bindings[peerID]; ok {
			projected.Alias = binding.EffectiveAlias()
			projected.Binding, projected.Reachability = string(binding.State()), string(binding.Reachability())
			projected.BaselineReady = readiness[peerID]
		}
		if projected.BaselineReady {
			ready++
		}
		members = append(members, projected)
	}
	sort.Slice(members, func(left, right int) bool { return members[left].Alias < members[right].Alias })
	ownerLocal := channel.OwnerPeerID() == local
	ownerReachability := "unknown"
	if ownerLocal {
		ownerReachability = "self"
	} else if binding, ok := bindings[channel.OwnerPeerID()]; ok {
		ownerReachability = string(binding.Reachability())
	}
	statusHead := durable.RosterHead()
	head := statusHead.RecordHead()
	publications := make([]localapi.ChannelPublicationView, 0, len(durable.Publications()))
	for _, publication := range durable.Publications() {
		ref := publication.PublicationRef()
		event := publication.EventKey()
		audience := publication.AudiencePeerIDs()
		audienceView := make([]string, len(audience))
		for index, peerID := range audience {
			audienceView[index] = peerID.String()
		}
		ignored := publication.IgnoredPeerIDs()
		ignoredView := make([]string, len(ignored))
		for index, peerID := range ignored {
			ignoredView[index] = peerID.String()
		}
		var artifactSource *string
		if peerID, ok := publication.ArtifactDirectSourcePeerID(); ok {
			value := peerID.String()
			artifactSource = &value
		}
		var causality *localapi.ChannelEventKeyView
		if key, ok := publication.CausalityEventKey(); ok {
			value := channelEventKeyView(key)
			causality = &value
		}
		publications = append(publications, localapi.ChannelPublicationView{
			Arrival: string(publication.Arrival()), ArtifactDirectSourcePeerID: artifactSource,
			AudiencePeerIDs: audienceView, CausalityEventKey: causality,
			ChannelIDDigest: publication.ChannelIDDigest().String(),
			EventDigest:     publication.EventDigest().String(), EventKey: channelEventKeyView(event),
			IgnoredPeerIDs:           ignoredView,
			ImmediateTransportPeerID: publication.ImmediateTransportPeerID().String(),
			OriginPeerID:             publication.OriginPeerID().String(),
			PublicationDigest:        publication.PublicationDigest().String(),
			PublicationRef: localapi.ChannelPublicationRefView{
				ChannelSequence: ref.ChannelSequence(), OriginEpoch: ref.OriginEpoch().String(),
				OriginPeerID: ref.OriginPeerID().String()},
			SemanticOutcome: string(publication.SemanticOutcome()),
		})
	}
	view := localapi.ChannelView{Alias: channel.LocalAlias(), Name: channel.Name(),
		ChannelIDDigest: durable.ChannelIDDigest().String(), Publications: publications,
		Membership: string(channel.Status()), RosterRevision: channel.RosterHead().Revision(),
		RosterHead: localapi.ChannelRosterHeadView{Revision: head.Revision(), Digest: head.Digest().String(),
			OwnerPeerID:    statusHead.OwnerPeerID().String(),
			OwnerSignature: base64.StdEncoding.EncodeToString(statusHead.OwnerSignature())},
		Owner: localapi.ChannelOwnerView{Local: ownerLocal, Reachability: ownerReachability}, Members: members,
		Topic: localapi.ChannelTopicView{Status: channelViewTopicStatus(manager.channelTopicStatus(channel), members),
			ReadyMembers: ready, TotalMembers: uint8(len(members))}}
	if grant, ok := durable.OpenGrant(); ok {
		invite := inviteView(grant.ExpiresAt(), grant.MaxUses(), grant.UsedUses(), grant.Status(), manager.clock.Now())
		view.Invite = &invite
	}
	return view
}

func channelEventKeyView(key model.EventKey) localapi.ChannelEventKeyView {
	return localapi.ChannelEventKeyView{OriginPeerID: key.OriginPeerID().String(),
		OriginEpoch: key.OriginEpoch().String(), EventID: key.EventID().String()}
}

func channelViewTopicStatus(status string, members []localapi.ChannelMemberView) string {
	if status != "joined" {
		return status
	}
	for _, member := range members {
		if member.Status == string(model.MemberActive) && member.Binding != "self" &&
			(member.Binding != string(model.BindingActive) || !member.BaselineReady) {
			return "converging"
		}
	}
	return status
}

func (manager *ChannelManager) channelTopicJoined(channel model.Channel) bool {
	return channel.TopicState() == model.TopicJoined && manager.runtime.HasCurrentSession(channel.ID())
}

func (manager *ChannelManager) channelTopicStatus(channel model.Channel) string {
	if channel.Status().Terminal() || channel.TopicState() == model.TopicLeft {
		return "left"
	}
	if channel.Status() == model.ChannelConflicted || channel.TopicState() == model.TopicBlocked {
		return "blocked"
	}
	if manager.channelTopicJoined(channel) {
		return "joined"
	}
	return "converging"
}

func (manager *ChannelManager) refreshAndJoin(ctx context.Context, channel model.Channel) {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil || manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }) != nil {
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

func (manager *ChannelManager) channelReadiness(ctx context.Context,
	channelID model.ChannelID,
) map[model.PeerID]bool {
	result := make(map[model.PeerID]bool)
	readiness, err := manager.store.ReadChannelBaselineReadiness(ctx, channelID)
	if err != nil {
		return result
	}
	for _, remote := range readiness {
		result[remote.PeerID] = remote.Ready()
	}
	return result
}

func inviteView(expiresAt time.Time, maxUses, usedUses uint8, status string,
	now time.Time,
) localapi.ChannelInviteView {
	if status == "open" && !now.Before(expiresAt) {
		status = "expired"
	}
	remaining := uint8(0)
	if usedUses < maxUses {
		remaining = maxUses - usedUses
	}
	return localapi.ChannelInviteView{ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
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

func channelAPIError(err error) *localapi.APIError {
	if err == nil {
		return nil
	}
	var protocolFailure *peer.ChannelProtocolFailure
	if errors.As(err, &protocolFailure) {
		code := localapi.ErrorCode(protocolFailure.Code())
		if code.Valid() {
			return localapi.NewAPIError(code, "Channel operation was rejected")
		}
	}
	switch {
	case errors.Is(err, store.ErrNodeChannelLimit):
		return localapi.NewAPIError(localapi.CodeNodeChannelLimit, "Node Channel limit reached")
	case errors.Is(err, store.ErrChannelFull):
		return localapi.NewAPIError(localapi.CodeChannelFull, "Channel member limit reached")
	case errors.Is(err, store.ErrChannelInviteOwner):
		return localapi.NewAPIError(localapi.CodeWrongOwner, "local Node is not the Channel owner")
	case errors.Is(err, store.ErrChannelInviteUnavailable):
		return localapi.NewAPIError(localapi.CodeChannelClosed, "Channel invite is unavailable")
	case errors.Is(err, store.ErrChannelJoinConflict), errors.Is(err, store.ErrChannelAuthorityInvariant):
		return localapi.NewAPIError(localapi.CodeRosterConflict, "Channel authority conflicts with durable state")
	case errors.Is(err, peer.ErrChannelEnrollmentOutcomeUnknown), errors.Is(err, peer.ErrMeshRuntime),
		errors.Is(err, peer.ErrChannelEnrollmentProtocol):
		return localapi.NewAPIError(localapi.CodeOwnerUnreachable, "Channel owner could not be reached")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return localapi.NewAPIError(localapi.CodeOwnerUnreachable, "Channel operation was cancelled")
	case errors.Is(err, store.ErrChannelCreateInput), errors.Is(err, store.ErrChannelInviteInput),
		errors.Is(err, store.ErrChannelJoinInput), errors.Is(err, store.ErrChannelAbandonInput):
		return localapi.NewAPIError(localapi.CodeInvalidArgument, "Channel operation input is invalid")
	case errors.Is(err, store.ErrChannelAbandonMissing):
		return localapi.NewAPIError(localapi.CodeNotMember, "Channel is not present on this Node")
	case errors.Is(err, store.ErrChannelAbandonTerminal):
		return localapi.NewAPIError(localapi.CodeChannelClosed, "Channel is already terminal")
	case errors.Is(err, store.ErrChannelAbandonStale):
		return localapi.NewAPIError(localapi.CodeOperationMismatch, "Channel authority changed")
	default:
		return localapi.NewAPIError(localapi.CodeInternal, "durable Channel operation failed")
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
	if err := owned.manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }); err != nil {
		return result, err
	}
	owned.manager.markTopicJoined(ctx, result.Channel)
	owned.manager.triggerMemberReconcile()
	return result, nil
}

func (manager *ChannelManager) EnrollmentOwnerStore() peer.ChannelEnrollmentOwnerStore {
	return channelEnrollmentOwnerStore{manager: manager}
}
