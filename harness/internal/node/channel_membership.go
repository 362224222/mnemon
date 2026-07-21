package node

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) ChannelRemove(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.ChannelRemoveRequest,
) (localapi.ChannelRemoveResponse, *localapi.APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return localapi.ChannelRemoveResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return localapi.ChannelRemoveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		return localapi.ChannelRemoveResponse{}, localapi.NewAPIError(localapi.CodeWrongOwner,
			"only the Channel owner may remove a member")
	}
	target, apiErr := selectChannelMember(authority.LocalPeerID(), channel, request.Member)
	if apiErr != nil {
		return localapi.ChannelRemoveResponse{}, apiErr
	}
	if target == authority.LocalPeerID() {
		return localapi.ChannelRemoveResponse{}, localapi.NewAPIError(localapi.CodeActionNotAllowed,
			"the Channel owner must use channel leave")
	}
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		target, model.MemberRevoked)
	if err != nil {
		return localapi.ChannelRemoveResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, updated.ID())
	if err != nil {
		return localapi.ChannelRemoveResponse{}, channelAPIError(err)
	}
	return localapi.ChannelRemoveResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "removed", Channel: view}, nil
}

func (manager *ChannelManager) ChannelLeave(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.ChannelLeaveRequest,
) (localapi.ChannelLeaveResponse, *localapi.APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return localapi.ChannelLeaveResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, channel, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return localapi.ChannelLeaveResponse{}, apiErr
	}
	if channel.Channel().OwnerPeerID() != authority.LocalPeerID() {
		return localapi.ChannelLeaveResponse{}, localapi.NewAPIError(localapi.CodeOwnerUnreachable,
			"Channel owner acknowledgement is required to leave")
	}
	updated, err := manager.commitTerminalMember(ctx, authority.LocalPeerID(), channel,
		authority.LocalPeerID(), model.MemberLeft)
	if err != nil {
		return localapi.ChannelLeaveResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, updated.ID())
	if err != nil {
		return localapi.ChannelLeaveResponse{}, channelAPIError(err)
	}
	return localapi.ChannelLeaveResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "left", Channel: view}, nil
}

func (manager *ChannelManager) commitTerminalMember(ctx context.Context, local model.PeerID,
	durable store.ChannelControlChannel, target model.PeerID,
	status model.MemberStatus,
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
		Records: []model.Member{terminal}, At: at})
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
	if err := manager.runtime.ReconcileWithCommit(mesh, func() error { return nil }); err != nil {
		return model.Channel{}, err
	}
	manager.markTopicJoined(ctx, merged.Channel)
	manager.triggerMemberReconcile()
	return merged.Channel, nil
}

func selectChannelMember(local model.PeerID, durable store.ChannelControlChannel,
	selector string,
) (model.PeerID, *localapi.APIError) {
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
				return model.PeerID{}, localapi.NewAPIError(localapi.CodeAmbiguousParticipant,
					"member selector is ambiguous")
			}
			selected = member.PeerID()
		}
	}
	if selected.IsZero() {
		return model.PeerID{}, localapi.NewAPIError(localapi.CodeNotMember,
			"member is not active in this Channel")
	}
	return selected, nil
}
