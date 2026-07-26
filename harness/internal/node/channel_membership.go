package node

import (
	"context"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) ChannelRemove(ctx context.Context, metadata RequestMetadata,
	request ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		return ChannelRemoveResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may remove a member")
	}
	target, apiErr := selectChannelMember(authority.LocalPeerID(), channel, request.Member)
	if apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	if target == authority.LocalPeerID() {
		return ChannelRemoveResponse{}, NewAPIError(CodeActionNotAllowed,
			"the Channel owner must use channel leave")
	}
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		target, model.MemberRevoked, nil)
	if err != nil {
		return ChannelRemoveResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, updated.ID())
	if err != nil {
		return ChannelRemoveResponse{}, channelAPIError(err)
	}
	return ChannelRemoveResponse{SchemaVersion: SchemaVersion,
		Status: "removed", Channel: view}, nil
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
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if replay, found, err := manager.store.ReadChannelLeaveOperation(ctx, operation); err != nil {
		return ChannelLeaveResponse{}, channelLeaveOperationAPIError(err)
	} else if found {
		if err := manager.reconcileChannelLeaveOperation(ctx, replay.ChannelID()); err != nil {
			return ChannelLeaveResponse{}, channelAPIError(err)
		}
		return manager.channelLeaveOperationResponse(ctx, replay.ChannelID())
	}
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		return manager.beginNonOwnerChannelLeave(ctx, authority.LocalPeerID(), channel, operation)
	}
	if channel.Channel().Status() == model.ChannelClosed {
		return manager.channelLeaveOperationResponse(ctx, channel.Channel().ID())
	}
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		authority.LocalPeerID(), model.MemberLeft, &operation)
	if err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	return manager.channelLeaveOperationResponse(ctx, updated.ID())
}

func (manager *ChannelManager) beginNonOwnerChannelLeave(ctx context.Context, local model.PeerID,
	durable store.ChannelControlChannel, operation store.ChannelLeaveOperation,
) (ChannelLeaveResponse, *APIError) {
	channel := durable.Channel()
	request, err := manager.newChannelLeaveRequest(ctx, local, durable)
	if err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	result, err := manager.store.BeginChannelLeave(ctx, store.BeginChannelLeaveSpec{
		ChannelID: channel.ID(), Request: request, Operation: operation, At: manager.clock.Now()})
	if err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	if err := manager.reconcileChannelLeaveOperation(ctx, channel.ID()); err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, result.Channel.ID())
	if err != nil {
		return ChannelLeaveResponse{}, channelAPIError(err)
	}
	return ChannelLeaveResponse{SchemaVersion: SchemaVersion,
		Status: "leaving", Channel: view}, nil
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
	durable store.ChannelControlChannel,
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
		KnownRosterHead: durable.Roster().Head(), RequestedAt: manager.clock.Now()})
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
	status model.MemberStatus, leaveOperation *store.ChannelLeaveOperation,
) (model.Channel, error) {
	channel, roster := durable.Channel(), durable.Roster()
	member, ok := roster.CurrentMember(target)
	if !ok || member.Status() != model.MemberActive || channel.OwnerPeerID() != local {
		return model.Channel{}, store.ErrChannelRosterInput
	}
	previous := roster.Head().Digest()
	at := manager.clock.Now()
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
		Records: []model.Member{terminal}, LeaveOperation: leaveOperation, At: at})
	if err != nil || merged.Status != store.ChannelRosterApplied {
		if err == nil {
			err = store.ErrChannelRosterConflict
		}
		return model.Channel{}, err
	}
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.Channel{}, err
	}
	if err := manager.runtime.Reconcile(mesh); err != nil {
		return model.Channel{}, err
	}
	manager.markTopicJoined(ctx, merged.Channel)
	manager.triggerMemberReconcileScope(channel.ID(), target)
	return merged.Channel, nil
}

func selectChannelMember(local model.PeerID, durable store.ChannelControlChannel,
	selector string,
) (model.PeerID, *APIError) {
	aliases := make(map[model.PeerID]string)
	for _, binding := range durable.Bindings() {
		aliases[binding.PeerID()] = binding.EffectiveAlias()
	}
	var selected model.PeerID
	for _, member := range durable.Roster().Members() {
		current, ok := durable.Roster().CurrentMember(member.PeerID())
		if !ok || current.Head() != member.Head() || current.Status() != model.MemberActive {
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
		return model.PeerID{}, NewAPIError(CodeNotMember,
			"member is not active in this Channel")
	}
	return selected, nil
}
