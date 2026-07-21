package localapi

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const MaxChannelResponseBytes = 1 << 20

type ChannelCreateRequest struct {
	Name string `json:"name,omitempty"`
}

type ChannelJoinRequest struct {
	Token string `json:"token"`
}

type ChannelInviteRequest struct {
	Channel        string `json:"channel,omitempty"`
	ExpiresSeconds int64  `json:"expires_seconds,omitempty"`
	Uses           uint8  `json:"uses,omitempty"`
}

type ChannelInviteCloseRequest struct {
	Channel string `json:"channel,omitempty"`
}

type ChannelRemoveRequest struct {
	Channel string `json:"channel,omitempty"`
	Member  string `json:"member"`
}

type ChannelLeaveRequest struct {
	Channel string `json:"channel,omitempty"`
}

type ChannelAbandonRequest struct {
	Channel        string `json:"channel"`
	ConfirmChannel string `json:"confirm_channel"`
	Force          bool   `json:"force"`
}

type ChannelTopicView struct {
	ReadyMembers uint8  `json:"ready_members"`
	Status       string `json:"status"`
	TotalMembers uint8  `json:"total_members"`
}

type ChannelOwnerView struct {
	Local        bool   `json:"local"`
	Reachability string `json:"reachability"`
}

type ChannelMemberView struct {
	Alias         string `json:"alias"`
	BaselineReady bool   `json:"baseline_ready"`
	Binding       string `json:"binding"`
	PeerID        string `json:"peer_id"`
	Reachability  string `json:"reachability"`
	Status        string `json:"status"`
}

type ChannelInviteView struct {
	ExpiresAt     string `json:"expires_at"`
	RemainingUses uint8  `json:"remaining_uses"`
	Status        string `json:"status"`
}

type ChannelRosterHeadView struct {
	Digest         string `json:"digest"`
	OwnerPeerID    string `json:"owner_peer_id"`
	OwnerSignature string `json:"owner_signature"`
	Revision       uint64 `json:"revision"`
}

// ChannelPublicationRefView is deliberately structured: Channel, Peer and
// epoch identifiers may themselves contain separators, so concatenation is
// not an unambiguous public reference.
type ChannelPublicationRefView struct {
	ChannelSequence uint64 `json:"channel_sequence"`
	OriginEpoch     string `json:"origin_epoch"`
	OriginPeerID    string `json:"origin_peer_id"`
}

type ChannelEventKeyView struct {
	EventID      string `json:"event_id"`
	OriginEpoch  string `json:"origin_epoch"`
	OriginPeerID string `json:"origin_peer_id"`
}

type ChannelPublicationView struct {
	Arrival                    string                    `json:"arrival"`
	ArtifactDirectSourcePeerID *string                   `json:"artifact_direct_source_peer_id"`
	AudiencePeerIDs            []string                  `json:"audience_peer_ids"`
	CausalityEventKey          *ChannelEventKeyView      `json:"causality_event_key"`
	ChannelIDDigest            string                    `json:"channel_id_digest"`
	EventDigest                string                    `json:"event_digest"`
	EventKey                   ChannelEventKeyView       `json:"event_key"`
	IgnoredPeerIDs             []string                  `json:"ignored_peer_ids"`
	ImmediateTransportPeerID   string                    `json:"immediate_transport_peer_id"`
	OriginPeerID               string                    `json:"origin_peer_id"`
	PublicationDigest          string                    `json:"publication_digest"`
	PublicationRef             ChannelPublicationRefView `json:"publication_ref"`
	SemanticOutcome            string                    `json:"semantic_outcome"`
}

type ChannelView struct {
	Alias           string                   `json:"alias"`
	ChannelIDDigest string                   `json:"channel_id_digest"`
	Invite          *ChannelInviteView       `json:"invite,omitempty"`
	Members         []ChannelMemberView      `json:"members"`
	Membership      string                   `json:"membership"`
	Name            string                   `json:"name"`
	Owner           ChannelOwnerView         `json:"owner"`
	Publications    []ChannelPublicationView `json:"publications"`
	RosterHead      ChannelRosterHeadView    `json:"roster_head"`
	RosterRevision  uint64                   `json:"roster_revision"`
	Topic           ChannelTopicView         `json:"topic"`
}

type ChannelCreateResponse struct {
	Channel       ChannelView       `json:"channel"`
	Invite        ChannelInviteView `json:"invite"`
	InviteToken   string            `json:"invite_token"`
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
}

type ChannelJoinResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelInviteResponse struct {
	Channel       ChannelView       `json:"channel"`
	Invite        ChannelInviteView `json:"invite"`
	InviteToken   string            `json:"invite_token"`
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
}

type ChannelInviteCloseResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelRemoveResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelLeaveResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelForensicCounts struct {
	Bindings      uint64 `json:"bindings"`
	Conflicts     uint64 `json:"conflicts"`
	Cursors       uint64 `json:"cursors"`
	Deliveries    uint64 `json:"deliveries"`
	Events        uint64 `json:"events"`
	Inboxes       uint64 `json:"inboxes"`
	MemberRecords uint64 `json:"member_records"`
	Publications  uint64 `json:"publications"`
	PullACKs      uint64 `json:"pull_acks"`
	Works         uint64 `json:"works"`
}

type ChannelAbandonResponse struct {
	Channel        string                `json:"channel"`
	Evidence       ChannelForensicCounts `json:"evidence"`
	Replayed       bool                  `json:"replayed"`
	SchemaVersion  int                   `json:"schema_version"`
	Status         string                `json:"status"`
	TransitionedAt string                `json:"transitioned_at"`
}

type ChannelStatusResponse struct {
	Channels      []ChannelView `json:"channels"`
	SchemaVersion int           `json:"schema_version"`
	Status        string        `json:"status"`
}

type ChannelService interface {
	ChannelCreate(context.Context, RequestMetadata, ChannelCreateRequest) (ChannelCreateResponse, *APIError)
	ChannelJoin(context.Context, RequestMetadata, ChannelJoinRequest) (ChannelJoinResponse, *APIError)
	ChannelInvite(context.Context, RequestMetadata, ChannelInviteRequest) (ChannelInviteResponse, *APIError)
	ChannelInviteClose(context.Context, RequestMetadata,
		ChannelInviteCloseRequest) (ChannelInviteCloseResponse, *APIError)
	ChannelRemove(context.Context, RequestMetadata, ChannelRemoveRequest) (ChannelRemoveResponse, *APIError)
	ChannelLeave(context.Context, RequestMetadata, ChannelLeaveRequest) (ChannelLeaveResponse, *APIError)
	ChannelAbandon(context.Context, RequestMetadata, ChannelAbandonRequest) (ChannelAbandonResponse, *APIError)
	ChannelStatus(context.Context, RequestMetadata) (ChannelStatusResponse, *APIError)
}

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
	if response.SchemaVersion != SchemaVersion || response.Status != "joined" ||
		validateChannelView(response.Channel) != nil {
		return invalidControlResponse("Channel join response is invalid")
	}
	return nil
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
	publicationCount := 0
	for _, channel := range response.Channels {
		if validateChannelView(channel) != nil || previous != "" && previous >= channel.Alias {
			return invalidControlResponse("Channel status response is invalid")
		}
		publicationCount += len(channel.Publications)
		if publicationCount > model.MaxChannelStatusPublications {
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
	self, apiErr := validateChannelMembers(channel.Members)
	if apiErr != nil {
		return apiErr
	}
	return validateChannelEvidence(channel, self)
}

func validChannelViewHeader(channel ChannelView) bool {
	return validChannelAlias(channel.Alias) && validChannelLabel(channel.Name) &&
		validChannelMembership(channel.Membership) && channel.RosterRevision != 0 &&
		channel.Members != nil && len(channel.Members) > 0 &&
		len(channel.Members) <= model.MaxMembersPerChannel && validChannelTopicView(channel.Topic) &&
		validChannelOwnerView(channel.Owner) && channel.Publications != nil &&
		len(channel.Publications) <= model.MaxChannelStatusPublications
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

func validateChannelEvidence(channel ChannelView, self string) *APIError {
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
	var previous *ChannelPublicationRefView
	for index := range channel.Publications {
		publication := &channel.Publications[index]
		if validateChannelPublication(*publication, channel.ChannelIDDigest, self) != nil ||
			previous != nil && compareChannelPublicationRefs(*previous, publication.PublicationRef) >= 0 {
			return invalidControlResponse("Channel publication evidence is invalid")
		}
		previous = &publication.PublicationRef
	}
	return nil
}

func validateChannelPublication(publication ChannelPublicationView, channelDigest, self string) *APIError {
	if !validChannelPublicationShape(publication, channelDigest) ||
		!validChannelPublicationIdentity(publication) ||
		!validChannelPublicationAudience(publication, self) ||
		!validChannelPublicationArtifactSource(publication) ||
		!validChannelPublicationPath(publication, self) {
		return invalidControlResponse("Channel publication evidence is invalid")
	}
	return nil
}

func validChannelPublicationShape(publication ChannelPublicationView, channelDigest string) bool {
	if publication.ChannelIDDigest != channelDigest ||
		publication.PublicationRef.ChannelSequence == 0 ||
		publication.PublicationRef.OriginPeerID != publication.OriginPeerID ||
		publication.EventKey.OriginPeerID != publication.OriginPeerID ||
		publication.EventKey.OriginEpoch != publication.PublicationRef.OriginEpoch ||
		publication.AudiencePeerIDs == nil || publication.IgnoredPeerIDs == nil ||
		len(publication.AudiencePeerIDs) == 0 ||
		len(publication.AudiencePeerIDs) > model.MaxMembersPerChannel-1 ||
		len(publication.IgnoredPeerIDs) > 1 {
		return false
	}
	return validChannelEvidenceDigest(publication.PublicationDigest) &&
		validChannelEvidenceDigest(publication.EventDigest)
}

func validChannelEvidenceDigest(value string) bool {
	digest, err := model.ParseDigest(value)
	return err == nil && !digest.IsZero() && digest.String() == value
}

func validChannelPublicationIdentity(publication ChannelPublicationView) bool {
	origin, originErr := model.ParsePeerID(publication.OriginPeerID)
	transport, transportErr := model.ParsePeerID(publication.ImmediateTransportPeerID)
	epoch, epochErr := model.ParseOriginEpoch(publication.PublicationRef.OriginEpoch)
	if originErr != nil || transportErr != nil || epochErr != nil ||
		origin.String() != publication.OriginPeerID || transport.String() != publication.ImmediateTransportPeerID ||
		epoch.String() != publication.PublicationRef.OriginEpoch ||
		validateChannelEventKey(publication.EventKey) != nil {
		return false
	}
	return publication.CausalityEventKey == nil || validateChannelEventKey(*publication.CausalityEventKey) == nil
}

func validChannelPublicationAudience(publication ChannelPublicationView, self string) bool {
	return validChannelPublicationPeers(publication.AudiencePeerIDs, publication.OriginPeerID) &&
		validChannelPublicationPeers(publication.IgnoredPeerIDs, "") &&
		(len(publication.IgnoredPeerIDs) == 0 || publication.IgnoredPeerIDs[0] == self) &&
		(publication.Arrival == "local" ||
			containsChannelPeer(publication.AudiencePeerIDs, self) != (len(publication.IgnoredPeerIDs) != 0))
}

func validChannelPublicationArtifactSource(publication ChannelPublicationView) bool {
	if publication.ArtifactDirectSourcePeerID == nil {
		return true
	}
	source, err := model.ParsePeerID(*publication.ArtifactDirectSourcePeerID)
	return err == nil && source.String() == publication.OriginPeerID && publication.Arrival != "local"
}

func validChannelPublicationPath(publication ChannelPublicationView, self string) bool {
	switch publication.Arrival {
	case "local":
		return publication.OriginPeerID == self && publication.SemanticOutcome == "originated" &&
			publication.ImmediateTransportPeerID == publication.OriginPeerID &&
			publication.ArtifactDirectSourcePeerID == nil && len(publication.IgnoredPeerIDs) == 0
	case "gossip":
		return validChannelImportedPublicationPath(publication, self)
	case "repair":
		return validChannelImportedPublicationPath(publication, self) &&
			publication.ImmediateTransportPeerID == publication.OriginPeerID
	default:
		return false
	}
}

func validChannelImportedPublicationPath(publication ChannelPublicationView, self string) bool {
	if publication.OriginPeerID == self || publication.ImmediateTransportPeerID == self ||
		publication.SemanticOutcome == "originated" ||
		!validChannelInboxOutcome(publication.SemanticOutcome) {
		return false
	}
	if len(publication.IgnoredPeerIDs) != 0 {
		return publication.SemanticOutcome == "ignored" || publication.SemanticOutcome == "quarantined"
	}
	return publication.SemanticOutcome != "ignored"
}

func validateChannelEventKey(key ChannelEventKeyView) error {
	peer, peerErr := model.ParsePeerID(key.OriginPeerID)
	epoch, epochErr := model.ParseOriginEpoch(key.OriginEpoch)
	event, eventErr := model.ParseEventID(key.EventID)
	if peerErr != nil || epochErr != nil || eventErr != nil || peer.String() != key.OriginPeerID ||
		epoch.String() != key.OriginEpoch || event.String() != key.EventID {
		return model.ErrInvalid
	}
	return nil
}

func validChannelPublicationPeers(peers []string, excluded string) bool {
	previous := ""
	for _, value := range peers {
		peer, err := model.ParsePeerID(value)
		if err != nil || peer.String() != value || value == excluded || previous != "" && previous >= value {
			return false
		}
		previous = value
	}
	return true
}

func containsChannelPeer(peers []string, value string) bool {
	for _, peer := range peers {
		if peer == value {
			return true
		}
	}
	return false
}

func compareChannelPublicationRefs(left, right ChannelPublicationRefView) int {
	if left.ChannelSequence < right.ChannelSequence {
		return -1
	}
	if left.ChannelSequence > right.ChannelSequence {
		return 1
	}
	if left.OriginPeerID < right.OriginPeerID {
		return -1
	}
	if left.OriginPeerID > right.OriginPeerID {
		return 1
	}
	return strings.Compare(left.OriginEpoch, right.OriginEpoch)
}

func validChannelInboxOutcome(value string) bool {
	switch value {
	case "stored", "waiting_artifact", "ready", "processing", "accepted", "rejected",
		"conflicted", "retry", "quarantined", "ignored":
		return true
	default:
		return false
	}
}

func validateChannelInviteView(invite ChannelInviteView) *APIError {
	expiresAt, err := time.Parse(time.RFC3339Nano, invite.ExpiresAt)
	if err != nil || expiresAt.UTC().Format(time.RFC3339Nano) != invite.ExpiresAt ||
		invite.RemainingUses > model.MaxMembersPerChannel-1 ||
		(invite.Status != "open" && invite.Status != "closed" && invite.Status != "expired" &&
			invite.Status != "exhausted") {
		return invalidControlResponse("Channel invite projection is invalid")
	}
	return nil
}

func validChannelTopicView(topic ChannelTopicView) bool {
	return topic.TotalMembers > 0 && topic.TotalMembers <= model.MaxMembersPerChannel &&
		topic.ReadyMembers <= topic.TotalMembers &&
		(topic.Status == "joined" || topic.Status == "converging" || topic.Status == "blocked" ||
			topic.Status == "left")
}

func validChannelOwnerView(owner ChannelOwnerView) bool {
	return validReachability(owner.Reachability) && (owner.Local == (owner.Reachability == "self"))
}

func validChannelMembership(value string) bool {
	return value == "active" || value == "leaving" || value == "conflicted" || value == "left" ||
		value == "closed" || value == "abandoned"
}

func validMemberState(value string) bool {
	return value == "active" || value == "left" || value == "revoked"
}

func validBindingState(value string) bool {
	return value == "self" || value == "pending" || value == "active" || value == "revoked" || value == "none"
}

func validReachability(value string) bool {
	return value == "self" || value == "unknown" || value == "reachable" || value == "unreachable"
}

func validChannelLabel(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxLabelBytes || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
