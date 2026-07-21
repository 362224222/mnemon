package localapi

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

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
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxLabelBytes ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
