package store

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelStatusAuthority = errors.New("durable Channel observation authority is unavailable")

// ChannelObservation is one coherent, bounded operational snapshot. It
// verifies current Channel, roster, binding and progress authority without
// loading retained publication evidence.
type ChannelObservation struct {
	localPeerID model.PeerID
	channels    []ChannelObservationChannel
}

type ChannelObservationChannel struct {
	control         ChannelControlChannel
	channelIDDigest model.Digest
	rosterHead      ChannelObservationRosterHead
	progress        ChannelStatusProgress
}

type ChannelObservationRosterHead struct {
	recordHead     model.RecordHead
	ownerPeerID    model.PeerID
	ownerSignature []byte
}

func (observation ChannelObservation) LocalPeerID() model.PeerID {
	return observation.localPeerID
}
func (observation ChannelObservation) Channels() []ChannelObservationChannel {
	result := make([]ChannelObservationChannel, len(observation.channels))
	for index, channel := range observation.channels {
		result[index] = channel.clone()
	}
	return result
}
func (channel ChannelObservationChannel) Channel() model.Channel {
	return channel.control.Channel()
}
func (channel ChannelObservationChannel) Roster() model.VerifiedRoster {
	return channel.control.Roster()
}
func (channel ChannelObservationChannel) Bindings() []model.PeerBinding {
	return channel.control.Bindings()
}
func (channel ChannelObservationChannel) OpenGrant() (ChannelControlGrant, bool) {
	return channel.control.OpenGrant()
}
func (channel ChannelObservationChannel) ChannelIDDigest() model.Digest {
	return channel.channelIDDigest
}
func (channel ChannelObservationChannel) RosterHead() ChannelObservationRosterHead {
	return channel.rosterHead
}
func (channel ChannelObservationChannel) Progress() ChannelStatusProgress {
	return channel.progress.clone()
}
func (channel ChannelObservationChannel) clone() ChannelObservationChannel {
	channel.rosterHead.ownerSignature = channel.rosterHead.OwnerSignature()
	channel.progress = channel.Progress()
	return channel
}
func (head ChannelObservationRosterHead) RecordHead() model.RecordHead { return head.recordHead }
func (head ChannelObservationRosterHead) OwnerPeerID() model.PeerID    { return head.ownerPeerID }
func (head ChannelObservationRosterHead) OwnerSignature() []byte {
	return append([]byte(nil), head.ownerSignature...)
}
