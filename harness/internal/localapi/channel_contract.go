package localapi

import (
	"crypto/ed25519"
	"encoding/base64"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validChannelCreateRequest(request ChannelCreateRequest) bool {
	return request.Name == "" || validChannelLabel(request.Name)
}

func validChannelJoinRequest(request ChannelJoinRequest) bool {
	_, err := model.ParseEnrollmentToken(request.Token)
	return err == nil
}

func validChannelInviteRequest(request ChannelInviteRequest) bool {
	return (request.Channel == "" || validChannelAlias(request.Channel)) && request.Uses < 8 &&
		(request.ExpiresSeconds == 0 || request.ExpiresSeconds >= 300 && request.ExpiresSeconds <= 7*24*60*60)
}

func validChannelInviteCloseRequest(request ChannelInviteCloseRequest) bool {
	return request.Channel == "" || validChannelAlias(request.Channel)
}

func validChannelRemoveRequest(request ChannelRemoveRequest) bool {
	return (request.Channel == "" || validChannelAlias(request.Channel)) &&
		validInitiationAlias(request.Member)
}

func validChannelLeaveRequest(request ChannelLeaveRequest) bool {
	return request.Channel == "" || validChannelAlias(request.Channel)
}

func validChannelAbandonRequest(request ChannelAbandonRequest) bool {
	return request.Force && validChannelAlias(request.Channel) &&
		request.Channel == request.ConfirmChannel
}

func validateChannelCreateResponse(response ChannelCreateResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "created" ||
		validateChannelView(response.Channel) != nil || validateChannelInviteView(response.Invite) != nil ||
		response.Channel.Invite == nil || *response.Channel.Invite != response.Invite {
		return invalidControlResponse("Channel create response is invalid")
	}
	if _, err := model.ParseEnrollmentToken(response.InviteToken); err != nil {
		return invalidControlResponse("Channel create response token is invalid")
	}
	return nil
}

func validateChannelJoinResponse(response ChannelJoinResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || !validChannelJoinStatus(response.Status) ||
		validateChannelView(response.Channel) != nil {
		return invalidControlResponse("Channel join response is invalid")
	}
	return nil
}

func validChannelJoinStatus(status string) bool {
	switch status {
	case "joined", "replayed", "member_revoked", "channel_closed":
		return true
	default:
		return false
	}
}

func validateChannelInviteResponse(response ChannelInviteResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "created" ||
		validateChannelView(response.Channel) != nil || validateChannelInviteView(response.Invite) != nil ||
		response.Channel.Invite == nil || *response.Channel.Invite != response.Invite {
		return invalidControlResponse("Channel invite response is invalid")
	}
	if _, err := model.ParseEnrollmentToken(response.InviteToken); err != nil {
		return invalidControlResponse("Channel invite response token is invalid")
	}
	return nil
}

func validateChannelInviteCloseResponse(response ChannelInviteCloseResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "closed" ||
		validateChannelView(response.Channel) != nil || response.Channel.Invite != nil {
		return invalidControlResponse("Channel invite close response is invalid")
	}
	return nil
}

func validateChannelRemoveResponse(response ChannelRemoveResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "removed" ||
		validateChannelView(response.Channel) != nil {
		return invalidControlResponse("Channel remove response is invalid")
	}
	return nil
}

func validateChannelAbandonResponse(response ChannelAbandonResponse) *APIError {
	transitionedAt, err := time.Parse(time.RFC3339Nano, response.TransitionedAt)
	if response.SchemaVersion != SchemaVersion || response.Status != "abandoned" ||
		!validChannelAlias(response.Channel) || err != nil || transitionedAt.IsZero() ||
		transitionedAt.Format(time.RFC3339Nano) != response.TransitionedAt {
		return invalidControlResponse("Channel abandon response is invalid")
	}
	return nil
}

func validateChannelStatusResponse(response ChannelStatusResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "ok" || response.Channels == nil ||
		len(response.Channels) > model.MaxChannelsPerNode {
		return invalidControlResponse("Channel status response is invalid")
	}
	previous := ""
	for _, channel := range response.Channels {
		if validateChannelView(channel) != nil || previous != "" && previous >= channel.Alias {
			return invalidControlResponse("Channel status response is invalid")
		}
		previous = channel.Alias
	}
	if raw, err := model.CanonicalMarshal(response); err != nil || len(raw)+1 > MaxChannelResponseBytes {
		return invalidControlResponse("Channel status response exceeds its closed bound")
	}
	return nil
}

func validateChannelView(channel ChannelView) *APIError {
	if !validChannelViewHeader(channel) {
		return invalidControlResponse("Channel projection is invalid")
	}
	if channel.Invite != nil && validateChannelInviteView(*channel.Invite) != nil {
		return invalidControlResponse("Channel projection invite is invalid")
	}
	_, apiErr := validateChannelMembers(channel.Members)
	if apiErr != nil {
		return apiErr
	}
	return validateChannelEvidence(channel)
}

func validChannelViewHeader(channel ChannelView) bool {
	return validChannelAlias(channel.Alias) && validChannelLabel(channel.Name) &&
		validChannelMembership(channel.Membership) && channel.RosterRevision != 0 &&
		channel.Members != nil && len(channel.Members) > 0 &&
		len(channel.Members) <= model.MaxMembersPerChannel && validChannelTopicView(channel.Topic) &&
		validChannelOwnerView(channel.Owner)
}

func validChannelAlias(value string) bool {
	if value == "" || len(value) > model.MaxLabelBytes || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	previousSeparator := false
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			previousSeparator = false
			continue
		}
		if character != '-' || previousSeparator {
			return false
		}
		previousSeparator = true
	}
	return true
}

func validateChannelMembers(members []ChannelMemberView) (string, *APIError) {
	previous := ""
	self := ""
	peers := make(map[string]struct{}, len(members))
	for _, member := range members {
		peer, peerErr := model.ParsePeerID(member.PeerID)
		if !validInitiationAlias(member.Alias) || previous != "" && previous >= member.Alias ||
			peerErr != nil || peer.String() != member.PeerID ||
			!validMemberState(member.Status) || !validBindingState(member.Binding) ||
			!validReachability(member.Reachability) || member.BaselineReady &&
			(member.Binding != "active" && member.Binding != "self") {
			return "", invalidControlResponse("Channel member projection is invalid")
		}
		if _, exists := peers[member.PeerID]; exists {
			return "", invalidControlResponse("Channel member projection is invalid")
		}
		peers[member.PeerID] = struct{}{}
		if member.Binding == "self" {
			if self != "" || member.Reachability != "self" {
				return "", invalidControlResponse("Channel member projection is invalid")
			}
			self = member.PeerID
		}
		previous = member.Alias
	}
	if self == "" {
		return "", invalidControlResponse("Channel member projection is invalid")
	}
	return self, nil
}

func validateChannelEvidence(channel ChannelView) *APIError {
	channelDigest, err := model.ParseDigest(channel.ChannelIDDigest)
	rosterDigest, rosterErr := model.ParseDigest(channel.RosterHead.Digest)
	owner, ownerErr := model.ParsePeerID(channel.RosterHead.OwnerPeerID)
	signature, signatureErr := base64.StdEncoding.DecodeString(channel.RosterHead.OwnerSignature)
	if err != nil || channelDigest.IsZero() || channelDigest.String() != channel.ChannelIDDigest ||
		rosterErr != nil || rosterDigest.IsZero() || rosterDigest.String() != channel.RosterHead.Digest || ownerErr != nil ||
		owner.String() != channel.RosterHead.OwnerPeerID ||
		channel.RosterHead.Revision != channel.RosterRevision || signatureErr != nil ||
		len(signature) != ed25519.SignatureSize ||
		base64.StdEncoding.EncodeToString(signature) != channel.RosterHead.OwnerSignature {
		return invalidControlResponse("Channel evidence roster head is invalid")
	}
	memberOwner := false
	for _, member := range channel.Members {
		memberOwner = memberOwner || member.PeerID == channel.RosterHead.OwnerPeerID
	}
	if !memberOwner {
		return invalidControlResponse("Channel evidence roster owner is absent")
	}
	return nil
}
