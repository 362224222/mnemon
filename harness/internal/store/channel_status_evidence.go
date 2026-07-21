package store

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelStatusAuthority = errors.New("durable Channel status authority is unavailable")

type ChannelStatusArrival string

const (
	ChannelStatusArrivalLocal  ChannelStatusArrival = "local"
	ChannelStatusArrivalGossip ChannelStatusArrival = "gossip"
	ChannelStatusArrivalRepair ChannelStatusArrival = "repair"
)

func (arrival ChannelStatusArrival) Valid() bool {
	return arrival == ChannelStatusArrivalLocal || arrival == ChannelStatusArrivalGossip ||
		arrival == ChannelStatusArrivalRepair
}

type ChannelStatusSemanticOutcome string

const ChannelStatusOutcomeOriginated ChannelStatusSemanticOutcome = "originated"

func (outcome ChannelStatusSemanticOutcome) Valid() bool {
	if outcome == ChannelStatusOutcomeOriginated {
		return true
	}
	return model.InboxStatus(outcome).Valid()
}

// ChannelStatusAuthority is one coherent, complete and bounded public evidence
// snapshot. It contains no bearer material, private keys, lease owners, tokens,
// payloads or raw signed envelopes.
type ChannelStatusAuthority struct {
	localPeerID model.PeerID
	channels    []ChannelStatusChannel
}

type ChannelStatusChannel struct {
	control         ChannelControlChannel
	channelIDDigest model.Digest
	rosterHead      ChannelStatusRosterHead
	publications    []ChannelStatusPublication
	progress        ChannelStatusProgress
}

type ChannelStatusRosterHead struct {
	recordHead     model.RecordHead
	ownerPeerID    model.PeerID
	ownerSignature []byte
}

type ChannelStatusPublicationRef struct {
	originPeerID    model.PeerID
	originEpoch     model.OriginEpoch
	channelSequence uint64
}

type ChannelStatusPublication struct {
	publicationRef             ChannelStatusPublicationRef
	publicationDigest          model.Digest
	eventKey                   model.EventKey
	eventDigest                model.Digest
	channelIDDigest            model.Digest
	originPeerID               model.PeerID
	immediateTransportPeerID   model.PeerID
	arrival                    ChannelStatusArrival
	audiencePeerIDs            []model.PeerID
	ignoredPeerIDs             []model.PeerID
	semanticOutcome            ChannelStatusSemanticOutcome
	artifactDirectSourcePeerID model.PeerID
	hasArtifactDirectSource    bool
	causalityEventKey          model.EventKey
	hasCausalityEventKey       bool
}

func (authority ChannelStatusAuthority) LocalPeerID() model.PeerID { return authority.localPeerID }
func (authority ChannelStatusAuthority) Channels() []ChannelStatusChannel {
	result := make([]ChannelStatusChannel, len(authority.channels))
	for index, channel := range authority.channels {
		result[index] = channel.clone()
	}
	return result
}
func (channel ChannelStatusChannel) Channel() model.Channel        { return channel.control.Channel() }
func (channel ChannelStatusChannel) Roster() model.VerifiedRoster  { return channel.control.Roster() }
func (channel ChannelStatusChannel) Bindings() []model.PeerBinding { return channel.control.Bindings() }
func (channel ChannelStatusChannel) OpenGrant() (ChannelControlGrant, bool) {
	return channel.control.OpenGrant()
}
func (channel ChannelStatusChannel) ChannelIDDigest() model.Digest       { return channel.channelIDDigest }
func (channel ChannelStatusChannel) RosterHead() ChannelStatusRosterHead { return channel.rosterHead }
func (channel ChannelStatusChannel) Progress() ChannelStatusProgress     { return channel.progress.clone() }
func (channel ChannelStatusChannel) Publications() []ChannelStatusPublication {
	result := make([]ChannelStatusPublication, len(channel.publications))
	for index, publication := range channel.publications {
		result[index] = publication.clone()
	}
	return result
}
func (channel ChannelStatusChannel) clone() ChannelStatusChannel {
	channel.publications = channel.Publications()
	channel.rosterHead.ownerSignature = channel.rosterHead.OwnerSignature()
	channel.progress = channel.Progress()
	return channel
}
func (head ChannelStatusRosterHead) RecordHead() model.RecordHead { return head.recordHead }
func (head ChannelStatusRosterHead) OwnerPeerID() model.PeerID    { return head.ownerPeerID }
func (head ChannelStatusRosterHead) OwnerSignature() []byte {
	return append([]byte(nil), head.ownerSignature...)
}
func (reference ChannelStatusPublicationRef) OriginPeerID() model.PeerID {
	return reference.originPeerID
}
func (reference ChannelStatusPublicationRef) OriginEpoch() model.OriginEpoch {
	return reference.originEpoch
}
func (reference ChannelStatusPublicationRef) ChannelSequence() uint64 {
	return reference.channelSequence
}
func (publication ChannelStatusPublication) PublicationRef() ChannelStatusPublicationRef {
	return publication.publicationRef
}
func (publication ChannelStatusPublication) PublicationDigest() model.Digest {
	return publication.publicationDigest
}
func (publication ChannelStatusPublication) EventKey() model.EventKey { return publication.eventKey }
func (publication ChannelStatusPublication) EventDigest() model.Digest {
	return publication.eventDigest
}
func (publication ChannelStatusPublication) ChannelIDDigest() model.Digest {
	return publication.channelIDDigest
}
func (publication ChannelStatusPublication) OriginPeerID() model.PeerID {
	return publication.originPeerID
}
func (publication ChannelStatusPublication) ImmediateTransportPeerID() model.PeerID {
	return publication.immediateTransportPeerID
}
func (publication ChannelStatusPublication) Arrival() ChannelStatusArrival {
	return publication.arrival
}
func (publication ChannelStatusPublication) AudiencePeerIDs() []model.PeerID {
	return append([]model.PeerID(nil), publication.audiencePeerIDs...)
}
func (publication ChannelStatusPublication) IgnoredPeerIDs() []model.PeerID {
	return append([]model.PeerID(nil), publication.ignoredPeerIDs...)
}
func (publication ChannelStatusPublication) SemanticOutcome() ChannelStatusSemanticOutcome {
	return publication.semanticOutcome
}
func (publication ChannelStatusPublication) ArtifactDirectSourcePeerID() (model.PeerID, bool) {
	return publication.artifactDirectSourcePeerID, publication.hasArtifactDirectSource
}
func (publication ChannelStatusPublication) CausalityEventKey() (model.EventKey, bool) {
	return publication.causalityEventKey, publication.hasCausalityEventKey
}
func (publication ChannelStatusPublication) clone() ChannelStatusPublication {
	publication.audiencePeerIDs = publication.AudiencePeerIDs()
	publication.ignoredPeerIDs = publication.IgnoredPeerIDs()
	return publication
}
