package node

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) ChannelRemove(ctx context.Context, metadata RequestMetadata,
	request ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	at := manager.clock.Now()
	manager.mu.Lock()
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		manager.mu.Unlock()
		return ChannelRemoveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		manager.mu.Unlock()
		return ChannelRemoveResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may remove a member")
	}
	target, replay, apiErr := selectChannelRemoveMember(authority.LocalPeerID(), channel,
		request.Member)
	if apiErr != nil {
		manager.mu.Unlock()
		return ChannelRemoveResponse{}, apiErr
	}
	if replay {
		manager.mu.Unlock()
		return manager.replayChannelRemove(ctx, channel.Channel(), target)
	}
	if target == authority.LocalPeerID() {
		manager.mu.Unlock()
		return ChannelRemoveResponse{}, NewAPIError(CodeActionNotAllowed,
			"the Channel owner must use channel leave")
	}
	manager.mu.Unlock()
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		target, model.MemberRevoked, nil, at)
	if err != nil {
		if errors.Is(err, store.ErrChannelRosterConflict) {
			response, apiErr, recovered := manager.recoverFencedChannelRemove(ctx, request)
			if recovered {
				return response, apiErr
			}
		}
		return ChannelRemoveResponse{}, channelAPIError(err)
	}
	return manager.replayChannelRemove(ctx, updated, target)
}

func (manager *ChannelManager) recoverFencedChannelRemove(ctx context.Context,
	request ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError, bool) {
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return ChannelRemoveResponse{}, nil, false
	}
	target, apiErr := selectRevokedChannelMember(authority.LocalPeerID(), channel, request.Member)
	if apiErr != nil {
		return ChannelRemoveResponse{}, apiErr, apiErr.Code != CodeNotMember
	}
	response, apiErr := manager.replayChannelRemove(ctx, channel.Channel(), target)
	return response, apiErr, true
}

func (manager *ChannelManager) ChannelLeave(ctx context.Context, metadata RequestMetadata,
	request ChannelLeaveRequest,
) (ChannelLeaveResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	operation, apiErr := channelLeaveOperation(metadata)
	if apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	at := manager.clock.Now()
	manager.mu.Lock()
	replay, found, err := manager.store.ReadChannelLeaveOperation(ctx, operation)
	if err != nil {
		manager.mu.Unlock()
		return ChannelLeaveResponse{}, channelLeaveOperationAPIError(err)
	}
	if found {
		manager.mu.Unlock()
		return manager.replayChannelLeaveOperation(ctx, replay.ChannelID())
	}
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		manager.mu.Unlock()
		return ChannelLeaveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		manager.mu.Unlock()
		channelID, err := manager.beginNonOwnerChannelLeave(ctx, authority.LocalPeerID(),
			channel, operation, at)
		if err != nil {
			return ChannelLeaveResponse{}, channelAPIError(err)
		}
		if err := manager.reconcileChannelLeaveOperation(ctx, channelID); err != nil {
			return ChannelLeaveResponse{}, channelAPIError(err)
		}
		return manager.channelLeaveOperationResponse(ctx, channelID)
	}
	if channel.Channel().Status() == model.ChannelClosed {
		manager.mu.Unlock()
		return manager.channelLeaveOperationResponse(ctx, channel.Channel().ID())
	}
	manager.mu.Unlock()
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		authority.LocalPeerID(), model.MemberLeft, &operation, at)
	if err != nil {
		if errors.Is(err, store.ErrChannelRosterConflict) {
			return manager.recoverFencedOwnerLeave(ctx, operation, err)
		}
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	if err := manager.refreshTerminalMemberRuntime(ctx, updated,
		authority.LocalPeerID()); err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	return manager.channelLeaveOperationResponse(ctx, updated.ID())
}

func (manager *ChannelManager) recoverFencedOwnerLeave(ctx context.Context,
	operation store.ChannelLeaveOperation, fenceErr error,
) (ChannelLeaveResponse, *APIError) {
	replay, found, err := manager.store.ReadChannelLeaveOperation(ctx, operation)
	if err != nil {
		return ChannelLeaveResponse{}, channelLeaveOperationAPIError(err)
	}
	if !found {
		return ChannelLeaveResponse{}, channelAPIError(fenceErr)
	}
	return manager.replayChannelLeaveOperation(ctx, replay.ChannelID())
}

func (manager *ChannelManager) beginNonOwnerChannelLeave(ctx context.Context, local model.PeerID,
	durable store.ChannelControlChannel, operation store.ChannelLeaveOperation, at time.Time,
) (model.ChannelID, error) {
	channel := durable.Channel()
	request, err := manager.newChannelLeaveRequest(ctx, local, durable, at)
	if err != nil {
		return model.ChannelID{}, err
	}
	result, err := manager.store.BeginChannelLeave(ctx, store.BeginChannelLeaveSpec{
		ChannelID: channel.ID(), Request: request, Operation: operation, At: at})
	if err != nil {
		return model.ChannelID{}, err
	}
	return result.Channel.ID(), nil
}

func (manager *ChannelManager) channelLeaveOperationResponse(ctx context.Context,
	channelID model.ChannelID,
) (ChannelLeaveResponse, *APIError) {
	view, err := manager.readChannelView(ctx, channelID)
	if err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	status := ""
	switch model.ChannelStatus(view.Membership) {
	case model.ChannelLeaving:
		status = "leaving"
	case model.ChannelLeft, model.ChannelClosed:
		status = "left"
	default:
		return ChannelLeaveResponse{},
			NewAPIError(CodeInternal, "durable Channel leave result is invalid")
	}
	return ChannelLeaveResponse{SchemaVersion: SchemaVersion, Status: status, Channel: view}, nil
}

func (manager *ChannelManager) replayChannelLeaveOperation(ctx context.Context,
	channelID model.ChannelID,
) (ChannelLeaveResponse, *APIError) {
	if err := manager.reconcileChannelLeaveOperation(ctx, channelID); err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	return manager.channelLeaveOperationResponse(ctx, channelID)
}

func (manager *ChannelManager) reconcileChannelLeaveOperation(ctx context.Context,
	channelID model.ChannelID,
) error {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return err
	}
	var owner model.PeerID
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			owner = channel.Channel().OwnerPeerID()
			break
		}
	}
	if owner.IsZero() {
		return store.ErrChannelLeaveAuthority
	}
	if err := manager.runtime.Reconcile(mesh); err != nil {
		return err
	}
	manager.triggerMemberReconcileScope(channelID, owner)
	return nil
}

func (manager *ChannelManager) newChannelLeaveRequest(ctx context.Context, local model.PeerID,
	durable store.ChannelControlChannel, at time.Time,
) (model.SignedChannelLeaveRequest, error) {
	if durable.Channel().Status() == model.ChannelLeaving {
		return model.SignedChannelLeaveRequest{}, nil
	}
	member, ok := durable.Roster().CurrentMember(local)
	if !ok || member.Status() != model.MemberActive || durable.Channel().Status() != model.ChannelActive {
		return model.SignedChannelLeaveRequest{}, store.ErrChannelLeaveConflict
	}
	record, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: durable.Channel().ID(), MemberPeerID: local, ActiveMemberHead: member.Head(),
		KnownRosterHead: durable.Roster().Head(), RequestedAt: at})
	if err != nil {
		return model.SignedChannelLeaveRequest{}, err
	}
	message, _ := model.ChannelLeaveRequestSigningMessage(record.ChannelID(), record.Digest())
	signature, err := manager.identity.PublicationSigner().Sign(ctx, message)
	if err != nil {
		return model.SignedChannelLeaveRequest{}, err
	}
	return model.AttachChannelLeaveRequestSignature(record, signature)
}

func (manager *ChannelManager) commitTerminalMember(ctx context.Context, local model.PeerID,
	durable store.ChannelControlChannel, target model.PeerID,
	status model.MemberStatus, leaveOperation *store.ChannelLeaveOperation, at time.Time,
) (model.Channel, error) {
	channel, roster := durable.Channel(), durable.Roster()
	member, ok := roster.CurrentMember(target)
	if !ok || member.Status() != model.MemberActive || channel.OwnerPeerID() != local {
		return model.Channel{}, store.ErrChannelRosterInput
	}
	previous := roster.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: channel.ID(),
		DescriptorDigest: channel.Descriptor().Descriptor().Digest(),
		Revision:         roster.Head().Revision() + 1, PreviousDigest: &previous, PeerID: target,
		OriginEpoch: member.OriginEpoch(), DisplayLabel: member.DisplayLabel(), PublicKey: member.PublicKey(),
		Multiaddrs: member.Multiaddrs(), Protocols: member.Protocols(), Limits: member.Limits(),
		Status: status, CreatedAt: at})
	if err != nil {
		return model.Channel{}, err
	}
	message, _ := model.MemberRecordSigningMessage(channel.ID(), record.Digest())
	signature, err := manager.identity.PublicationSigner().Sign(ctx, message)
	if err != nil {
		return model.Channel{}, err
	}
	terminal, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		return model.Channel{}, err
	}
	merged, err := manager.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID: channel.ID(), AuthenticatedTransportPeerID: local,
		Records: []model.Member{terminal}, ExpectedRosterHead: roster.Head(),
		LeaveOperation: leaveOperation, At: at})
	if err != nil || merged.Status != store.ChannelRosterApplied {
		if err == nil {
			err = store.ErrChannelRosterConflict
		}
		return model.Channel{}, err
	}
	return merged.Channel, nil
}

func (manager *ChannelManager) refreshTerminalMemberRuntime(ctx context.Context,
	channel model.Channel, target model.PeerID,
) error {
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return err
	}
	if err := manager.runtime.Reconcile(mesh); err != nil {
		return err
	}
	manager.markTopicJoined(ctx, channel)
	manager.triggerMemberReconcileScope(channel.ID(), target)
	return nil
}

func (manager *ChannelManager) replayChannelRemove(ctx context.Context,
	channel model.Channel, target model.PeerID,
) (ChannelRemoveResponse, *APIError) {
	if err := manager.refreshTerminalMemberRuntime(ctx, channel, target); err != nil {
		return ChannelRemoveResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, channel.ID())
	if err != nil {
		return ChannelRemoveResponse{}, channelAPIError(err)
	}
	return ChannelRemoveResponse{SchemaVersion: SchemaVersion,
		Status: "removed", Channel: view}, nil
}

func selectChannelMember(local model.PeerID, durable store.ChannelControlChannel,
	selector string,
) (model.PeerID, *APIError) {
	return selectChannelMemberStatus(local, durable, selector, model.MemberActive,
		"member is not active in this Channel")
}

func selectChannelRemoveMember(local model.PeerID, durable store.ChannelControlChannel,
	selector string,
) (model.PeerID, bool, *APIError) {
	target, apiErr := selectChannelMember(local, durable, selector)
	if apiErr == nil || apiErr.Code != CodeNotMember {
		return target, false, apiErr
	}
	replayed, replayErr := selectRevokedChannelMember(local, durable, selector)
	if replayErr == nil {
		return replayed, true, nil
	}
	if replayErr.Code != CodeNotMember {
		return model.PeerID{}, false, replayErr
	}
	return model.PeerID{}, false, apiErr
}

func selectRevokedChannelMember(local model.PeerID, durable store.ChannelControlChannel,
	selector string,
) (model.PeerID, *APIError) {
	return selectChannelMemberStatus(local, durable, selector, model.MemberRevoked,
		"member is not revoked in this Channel")
}

func selectChannelMemberStatus(local model.PeerID, durable store.ChannelControlChannel,
	selector string, status model.MemberStatus, notFound string,
) (model.PeerID, *APIError) {
	aliases := make(map[model.PeerID]string)
	for _, binding := range durable.SelectorBindings() {
		aliases[binding.PeerID()] = binding.EffectiveAlias()
	}
	var selected model.PeerID
	for _, member := range durable.Roster().Members() {
		current, ok := durable.Roster().CurrentMember(member.PeerID())
		if !ok || current.Head() != member.Head() || current.Status() != status {
			continue
		}
		alias := aliases[member.PeerID()]
		if member.PeerID() == local || alias == "" {
			alias = memberAlias(member.PeerID())
		}
		if alias == selector {
			if !selected.IsZero() && selected != member.PeerID() {
				return model.PeerID{}, NewAPIError(CodeAmbiguousParticipant,
					"member selector is ambiguous")
			}
			selected = member.PeerID()
		}
	}
	if selected.IsZero() {
		return model.PeerID{}, NewAPIError(CodeNotMember, notFound)
	}
	return selected, nil
}
