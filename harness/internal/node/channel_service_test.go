package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelAliasIsStableSanitizedAndCollisionAware(t *testing.T) {
	t.Parallel()
	if got := channelAlias("  Review / Team  "); got != "review-team" {
		t.Fatalf("channelAlias() = %q", got)
	}
	if got := channelAlias("Проект Réview"); got != "r-view" {
		t.Fatalf("non-ASCII channelAlias() = %q", got)
	}
	if got := uniqueChannelAlias("review-team", store.ChannelControlAuthority{}); got != "review-team" {
		t.Fatalf("uniqueChannelAlias() = %q", got)
	}
}

func TestChannelJoinResponseStatusReportsReplayAndTerminal(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		status store.ChannelEnrollmentStatus
		want   string
	}{
		{name: "accepted", status: store.ChannelEnrollmentAccepted, want: "joined"},
		{name: "replayed", status: store.ChannelEnrollmentReplayed, want: "replayed"},
		{name: "member revoked", status: store.ChannelEnrollmentMemberRevoked, want: "member_revoked"},
		{name: "channel closed", status: store.ChannelEnrollmentChannelClosed, want: "channel_closed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := channelJoinResponseStatus(test.status); got != test.want {
				t.Fatalf("channelJoinResponseStatus(%q) = %q, want %q", test.status, got,
					test.want)
			}
		})
	}
}
