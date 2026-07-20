package localapi

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const MaxChannelResponseBytes = 64 << 10

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
	Reachability  string `json:"reachability"`
	Status        string `json:"status"`
}

type ChannelInviteView struct {
	ExpiresAt     string `json:"expires_at"`
	RemainingUses uint8  `json:"remaining_uses"`
	Status        string `json:"status"`
}

type ChannelView struct {
	Alias          string              `json:"alias"`
	Invite         *ChannelInviteView  `json:"invite,omitempty"`
	Members        []ChannelMemberView `json:"members"`
	Membership     string              `json:"membership"`
	Name           string              `json:"name"`
	Owner          ChannelOwnerView    `json:"owner"`
	RosterRevision uint64              `json:"roster_revision"`
	Topic          ChannelTopicView    `json:"topic"`
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

func validateChannelLeaveResponse(response ChannelLeaveResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Status != "left" ||
		validateChannelView(response.Channel) != nil {
		return invalidControlResponse("Channel leave response is invalid")
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
	return nil
}

func validateChannelView(channel ChannelView) *APIError {
	if !validChannelViewHeader(channel) {
		return invalidControlResponse("Channel projection is invalid")
	}
	if channel.Invite != nil && validateChannelInviteView(*channel.Invite) != nil {
		return invalidControlResponse("Channel projection invite is invalid")
	}
	return validateChannelMembers(channel.Members)
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

func validateChannelMembers(members []ChannelMemberView) *APIError {
	previous := ""
	for _, member := range members {
		if !validInitiationAlias(member.Alias) || previous != "" && previous >= member.Alias ||
			!validMemberState(member.Status) || !validBindingState(member.Binding) ||
			!validReachability(member.Reachability) || member.BaselineReady &&
			(member.Binding != "active" && member.Binding != "self") {
			return invalidControlResponse("Channel member projection is invalid")
		}
		previous = member.Alias
	}
	return nil
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
