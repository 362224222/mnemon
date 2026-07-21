package node

import (
	"encoding/base64"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"sort"
)

type channelMemberProjection struct {
	members  []ChannelMemberView
	bindings map[model.PeerID]model.PeerBinding
	ready    uint8
}

func (manager *ChannelManager) projectChannelView(local model.PeerID,
	durable store.ChannelStatusChannel,
) ChannelView {
	channel := durable.Channel()
	members := manager.projectChannelMembers(local, durable)
	statusHead := durable.RosterHead()
	head := statusHead.RecordHead()
	view := ChannelView{Alias: channel.LocalAlias(), Name: channel.Name(),
		ChannelIDDigest: durable.ChannelIDDigest().String(),
		Publications:    projectChannelPublications(durable.Publications()),
		Membership:      string(channel.Status()), RosterRevision: channel.RosterHead().Revision(),
		RosterHead: ChannelRosterHeadView{Revision: head.Revision(), Digest: head.Digest().String(),
			OwnerPeerID:    statusHead.OwnerPeerID().String(),
			OwnerSignature: base64.StdEncoding.EncodeToString(statusHead.OwnerSignature())},
		Owner: projectChannelOwner(channel, local, members.bindings), Members: members.members,
		Topic: ChannelTopicView{
			Status:       channelViewTopicStatus(manager.channelTopicStatus(channel), members.members),
			ReadyMembers: members.ready, TotalMembers: uint8(len(members.members))}}
	if grant, ok := durable.OpenGrant(); ok {
		invite := inviteView(grant.ExpiresAt(), grant.MaxUses(), grant.UsedUses(), grant.Status(),
			manager.clock.Now())
		view.Invite = &invite
	}
	return view
}

func (manager *ChannelManager) projectChannelMembers(local model.PeerID,
	durable store.ChannelStatusChannel,
) channelMemberProjection {
	readiness := make(map[model.PeerID]bool)
	for _, remote := range durable.Progress().Readiness() {
		readiness[remote.PeerID] = remote.Ready()
	}
	bindings := make(map[model.PeerID]model.PeerBinding)
	for _, binding := range durable.Bindings() {
		bindings[binding.PeerID()] = binding
	}
	latest := make(map[model.PeerID]model.Member)
	for _, member := range durable.Roster().Members() {
		latest[member.PeerID()] = member
	}
	projection := channelMemberProjection{members: make([]ChannelMemberView, 0, len(latest)),
		bindings: bindings}
	for peerID, member := range latest {
		projected := ChannelMemberView{Alias: memberAlias(peerID), PeerID: peerID.String(),
			Status: string(member.Status()), Binding: "none", Reachability: "unknown"}
		if peerID == local {
			projected.Binding, projected.Reachability = "self", "self"
			projected.BaselineReady = manager.channelTopicJoined(durable.Channel())
		} else if binding, ok := bindings[peerID]; ok {
			projected.Alias = binding.EffectiveAlias()
			projected.Binding, projected.Reachability = string(binding.State()), string(binding.Reachability())
			projected.BaselineReady = readiness[peerID]
		}
		if projected.BaselineReady {
			projection.ready++
		}
		projection.members = append(projection.members, projected)
	}
	sort.Slice(projection.members, func(left, right int) bool {
		return projection.members[left].Alias < projection.members[right].Alias
	})
	return projection
}

func projectChannelOwner(channel model.Channel, local model.PeerID,
	bindings map[model.PeerID]model.PeerBinding,
) ChannelOwnerView {
	if channel.OwnerPeerID() == local {
		return ChannelOwnerView{Local: true, Reachability: "self"}
	}
	reachability := "unknown"
	if binding, ok := bindings[channel.OwnerPeerID()]; ok {
		reachability = string(binding.Reachability())
	}
	return ChannelOwnerView{Reachability: reachability}
}

func projectChannelPublications(values []store.ChannelStatusPublication,
) []ChannelPublicationView {
	result := make([]ChannelPublicationView, len(values))
	for index, publication := range values {
		result[index] = projectChannelPublication(publication)
	}
	return result
}

func projectChannelPublication(publication store.ChannelStatusPublication,
) ChannelPublicationView {
	ref := publication.PublicationRef()
	var artifactSource *string
	if peerID, ok := publication.ArtifactDirectSourcePeerID(); ok {
		value := peerID.String()
		artifactSource = &value
	}
	var causality *ChannelEventKeyView
	if key, ok := publication.CausalityEventKey(); ok {
		value := channelEventKeyView(key)
		causality = &value
	}
	return ChannelPublicationView{Arrival: string(publication.Arrival()),
		ArtifactDirectSourcePeerID: artifactSource,
		AudiencePeerIDs:            channelPeerIDsView(publication.AudiencePeerIDs()),
		CausalityEventKey:          causality,
		ChannelIDDigest:            publication.ChannelIDDigest().String(),
		EventDigest:                publication.EventDigest().String(),
		EventKey:                   channelEventKeyView(publication.EventKey()),
		IgnoredPeerIDs:             channelPeerIDsView(publication.IgnoredPeerIDs()),
		ImmediateTransportPeerID:   publication.ImmediateTransportPeerID().String(),
		OriginPeerID:               publication.OriginPeerID().String(),
		PublicationDigest:          publication.PublicationDigest().String(),
		PublicationRef: ChannelPublicationRefView{ChannelSequence: ref.ChannelSequence(),
			OriginEpoch: ref.OriginEpoch().String(), OriginPeerID: ref.OriginPeerID().String()},
		SemanticOutcome: string(publication.SemanticOutcome())}
}

func channelPeerIDsView(values []model.PeerID) []string {
	result := make([]string, len(values))
	for index, peerID := range values {
		result[index] = peerID.String()
	}
	return result
}

func channelEventKeyView(key model.EventKey) ChannelEventKeyView {
	return ChannelEventKeyView{OriginPeerID: key.OriginPeerID().String(),
		OriginEpoch: key.OriginEpoch().String(), EventID: key.EventID().String()}
}

func channelViewTopicStatus(status string, members []ChannelMemberView) string {
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
