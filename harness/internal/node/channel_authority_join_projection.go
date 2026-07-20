package node

import (
	"bytes"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type frozenChannelJoinEvidence struct {
	owner      model.PeerID
	status     peer.ChannelEnrollmentStatus
	descriptor model.SignedChannelDescriptor
	transcript model.EnrollmentTranscript
	receipt    model.EnrollmentReceipt
	roster     model.VerifiedRoster
}

func freezeChannelJoinEvidence(spec frozenChannelJoinSpec,
	control peer.ChannelJoinPrepareControl, prepared store.PrepareJoinedChannelResult,
	accepted peer.VerifiedChannelEnrollment,
) (frozenChannelJoinEvidence, error) {
	descriptor, err := model.ParseSignedChannelDescriptor(accepted.Descriptor().WireJSON().Bytes())
	if err != nil {
		return frozenChannelJoinEvidence{}, joinProjectionError("clone descriptor")
	}
	transcript, err := model.ParseEnrollmentTranscript(accepted.Transcript().CanonicalJSON().Bytes())
	if err != nil {
		return frozenChannelJoinEvidence{}, joinProjectionError("clone transcript")
	}
	receipt, err := model.ParseEnrollmentReceipt(accepted.Receipt().WireJSON().Bytes())
	if err != nil {
		return frozenChannelJoinEvidence{}, joinProjectionError("clone receipt")
	}
	members := accepted.Roster().Members()
	cloned := make([]model.Member, len(members))
	for index, member := range members {
		cloned[index], err = model.ParseMember(member.WireJSON().Bytes())
		if err != nil {
			return frozenChannelJoinEvidence{}, joinProjectionError("clone roster")
		}
	}
	roster, err := model.NewVerifiedRoster(descriptor, cloned)
	if err != nil {
		return frozenChannelJoinEvidence{}, joinProjectionError("verify roster")
	}
	addresses, err := model.AdvertisedAddressDigest(control.AdvertisedMultiaddrs)
	if err != nil || !validChannelJoinOwnerEvidence(spec, accepted, descriptor) ||
		!validChannelJoinTranscriptEvidence(spec, prepared, accepted, descriptor, transcript) ||
		!validChannelJoinLocalEvidence(spec, control, prepared, transcript, addresses, roster,
			accepted.Roster()) {
		return frozenChannelJoinEvidence{}, joinProjectionError("authority binding changed")
	}
	return frozenChannelJoinEvidence{owner: accepted.AuthenticatedOwnerPeerID(),
		status: accepted.Status(), descriptor: descriptor, transcript: transcript,
		receipt: receipt, roster: roster}, nil
}

func validChannelJoinOwnerEvidence(spec frozenChannelJoinSpec,
	accepted peer.VerifiedChannelEnrollment, descriptor model.SignedChannelDescriptor,
) bool {
	return !accepted.AuthenticatedOwnerPeerID().IsZero() && accepted.Status().Valid() &&
		accepted.AuthenticatedOwnerPeerID() == descriptor.Descriptor().OwnerPeerID() &&
		bytes.Equal(descriptor.WireJSON().Bytes(),
			spec.token.Payload().Descriptor().WireJSON().Bytes())
}

func validChannelJoinTranscriptEvidence(spec frozenChannelJoinSpec,
	prepared store.PrepareJoinedChannelResult, accepted peer.VerifiedChannelEnrollment,
	descriptor model.SignedChannelDescriptor, transcript model.EnrollmentTranscript,
) bool {
	return transcript.ChannelID() == descriptor.Descriptor().ID() &&
		transcript.GrantID() == spec.token.Payload().GrantID() &&
		transcript.RequestID() == prepared.RequestID &&
		transcript.OwnerPeerID() == accepted.AuthenticatedOwnerPeerID()
}

func validChannelJoinLocalEvidence(spec frozenChannelJoinSpec,
	control peer.ChannelJoinPrepareControl, prepared store.PrepareJoinedChannelResult,
	transcript model.EnrollmentTranscript, addresses model.Digest,
	roster, acceptedRoster model.VerifiedRoster,
) bool {
	return transcript.JoinerPeerID() == control.AuthenticatedLocalPeerID &&
		transcript.JoinerOriginEpoch() == prepared.OriginEpoch &&
		transcript.JoinerDisplayLabel() == spec.displayLabel &&
		bytes.Equal(transcript.JoinerPublicKey(), control.LocalPublicKey) &&
		transcript.AdvertisedAddressDigest() == addresses &&
		sameJoinedChannelRoster(roster, acceptedRoster)
}

func storeChannelJoinStatus(status peer.ChannelEnrollmentStatus) (
	store.ChannelEnrollmentStatus, error,
) {
	switch status {
	case peer.ChannelEnrollmentAccepted:
		return store.ChannelEnrollmentAccepted, nil
	case peer.ChannelEnrollmentReplayed:
		return store.ChannelEnrollmentReplayed, nil
	case peer.ChannelEnrollmentMemberRevoked:
		return store.ChannelEnrollmentMemberRevoked, nil
	case peer.ChannelEnrollmentChannelClosed:
		return store.ChannelEnrollmentChannelClosed, nil
	default:
		return "", joinProjectionError("unknown peer status")
	}
}

func peerChannelJoinStatus(status store.ChannelEnrollmentStatus) (
	peer.ChannelEnrollmentStatus, error,
) {
	switch status {
	case store.ChannelEnrollmentAccepted:
		return peer.ChannelEnrollmentAccepted, nil
	case store.ChannelEnrollmentReplayed:
		return peer.ChannelEnrollmentReplayed, nil
	case store.ChannelEnrollmentMemberRevoked:
		return peer.ChannelEnrollmentMemberRevoked, nil
	case store.ChannelEnrollmentChannelClosed:
		return peer.ChannelEnrollmentChannelClosed, nil
	default:
		return "", joinProjectionError("unknown Store status")
	}
}

func projectChannelJoinResult(evidence frozenChannelJoinEvidence, localAlias string,
	result store.InstallJoinedChannelResult,
) (peer.ChannelJoinResult, error) {
	status, err := peerChannelJoinStatus(result.Status)
	if err != nil || !compatibleJoinedChannelStatus(evidence.status, status) ||
		!sameJoinedChannelRoster(result.Roster, evidence.roster) {
		return peer.ChannelJoinResult{}, joinProjectionError("Store result differs from owner evidence")
	}
	channel := result.Channel
	if status == peer.ChannelEnrollmentMemberRevoked || status == peer.ChannelEnrollmentChannelClosed {
		if result.Installed || !validTerminalJoinedChannel(channel, status, evidence, localAlias) {
			return peer.ChannelJoinResult{}, joinProjectionError("invalid terminal Store projection")
		}
		// Durable replicas retain their terminal Channel for local history, but
		// a join result never presents that terminal state as a fresh install.
		channel = model.Channel{}
	} else if !validActiveJoinedChannel(channel, status, result.Installed, evidence, localAlias) {
		return peer.ChannelJoinResult{}, joinProjectionError("invalid active Store projection")
	}
	projected, err := peer.NewChannelJoinResult(peer.ChannelJoinResultSpec{
		Installed: result.Installed, Status: status, Channel: channel, Roster: evidence.roster})
	if err != nil {
		return peer.ChannelJoinResult{}, fmt.Errorf("%w: project joined Channel result: %w",
			ErrChannelAuthority, err)
	}
	return projected, nil
}

func compatibleJoinedChannelStatus(owner, local peer.ChannelEnrollmentStatus) bool {
	return owner == local || owner == peer.ChannelEnrollmentReplayed &&
		local == peer.ChannelEnrollmentAccepted
}

func validActiveJoinedChannel(channel model.Channel, status peer.ChannelEnrollmentStatus,
	installed bool, evidence frozenChannelJoinEvidence, localAlias string,
) bool {
	return (status == peer.ChannelEnrollmentAccepted || status == peer.ChannelEnrollmentReplayed) &&
		(!installed || status == peer.ChannelEnrollmentAccepted) && !channel.ID().IsZero() &&
		channel.ID() == evidence.descriptor.Descriptor().ID() && channel.LocalAlias() == localAlias &&
		channel.Status() == model.ChannelActive && channel.RosterHead() == evidence.roster.Head() &&
		bytes.Equal(channel.Descriptor().WireJSON().Bytes(), evidence.descriptor.WireJSON().Bytes())
}

func validTerminalJoinedChannel(channel model.Channel, status peer.ChannelEnrollmentStatus,
	evidence frozenChannelJoinEvidence, localAlias string,
) bool {
	if channel.ID().IsZero() {
		return true
	}
	if status == peer.ChannelEnrollmentMemberRevoked {
		return (channel.Status() == model.ChannelLeft || channel.Status() == model.ChannelClosed) &&
			channel.ID() == evidence.descriptor.Descriptor().ID() &&
			channel.LocalAlias() == localAlias && channel.RosterHead() == evidence.roster.Head() &&
			bytes.Equal(channel.Descriptor().WireJSON().Bytes(), evidence.descriptor.WireJSON().Bytes())
	}
	return channel.ID() == evidence.descriptor.Descriptor().ID() &&
		channel.LocalAlias() == localAlias && channel.Status() == model.ChannelClosed &&
		channel.RosterHead() == evidence.roster.Head() &&
		bytes.Equal(channel.Descriptor().WireJSON().Bytes(), evidence.descriptor.WireJSON().Bytes())
}

func sameJoinedChannelInstallResult(actual, expected store.InstallJoinedChannelResult,
	changesAuthority bool,
) bool {
	installed := actual.Installed == expected.Installed || changesAuthority && actual.Installed &&
		!expected.Installed && expected.Status == store.ChannelEnrollmentAccepted
	return installed && actual.Status == expected.Status &&
		sameJoinedChannel(actual.Channel, expected.Channel) &&
		sameJoinedChannelRoster(actual.Roster, expected.Roster)
}

func sameJoinedChannel(left, right model.Channel) bool {
	if left.ID().IsZero() || right.ID().IsZero() {
		return left.ID().IsZero() && right.ID().IsZero()
	}
	return left.ID() == right.ID() && left.LocalAlias() == right.LocalAlias() &&
		left.RosterHead() == right.RosterHead() && left.Status() == right.Status() &&
		left.TopicState() == right.TopicState() && left.UpdatedAt() == right.UpdatedAt() &&
		bytes.Equal(left.Descriptor().WireJSON().Bytes(), right.Descriptor().WireJSON().Bytes())
}

func sameJoinedChannelRoster(left, right model.VerifiedRoster) bool {
	if left.IsZero() || right.IsZero() || left.Head() != right.Head() ||
		!bytes.Equal(left.Descriptor().WireJSON().Bytes(), right.Descriptor().WireJSON().Bytes()) {
		return false
	}
	leftMembers, rightMembers := left.Members(), right.Members()
	if len(leftMembers) != len(rightMembers) {
		return false
	}
	for index := range leftMembers {
		if !bytes.Equal(leftMembers[index].WireJSON().Bytes(), rightMembers[index].WireJSON().Bytes()) {
			return false
		}
	}
	return true
}

func joinProjectionError(detail string) error {
	return fmt.Errorf("%w: joined Channel %s", ErrChannelAuthority, detail)
}
