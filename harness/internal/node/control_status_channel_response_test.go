package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestStatusChannelsSortAndDeriveClosedState(t *testing.T) {
	t.Parallel()
	alpha := statusChannelResponseSnapshot("alpha")
	alpha.Inbox.Durable = 1
	alpha.Inbox.Pending = 1
	beta := statusChannelResponseSnapshot("beta")
	channels, err := newStatusChannels([]StatusChannelSnapshot{beta, alpha})
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 2 || channels[0].Alias != "alpha" || channels[1].Alias != "beta" ||
		channels[0].State != statusChannelQueued || channels[1].State != statusChannelReady ||
		channels[0].Semantic.Pending != 1 {
		t.Fatalf("status Channels = %#v", channels)
	}
	if err := ValidateStatusChannels(channels); err != nil {
		t.Fatal(err)
	}
}

func TestStatusChannelsRejectDuplicateAlias(t *testing.T) {
	t.Parallel()
	_, err := newStatusChannels([]StatusChannelSnapshot{
		statusChannelResponseSnapshot("alpha"),
		statusChannelResponseSnapshot("alpha"),
	})
	if err == nil {
		t.Fatal("duplicate Channel aliases accepted")
	}
}

func statusChannelResponseSnapshot(alias string) StatusChannelSnapshot {
	return StatusChannelSnapshot{Alias: alias, Membership: string(model.ChannelActive),
		RosterRevision: 1, Topic: StatusChannelTopic{ReadyMembers: 1, State: "joined",
			TotalMembers: 1},
		LocalCommit: StatusChannelCommit{Accepted: 1},
		Publication: StatusChannelPublication{Published: 1}}
}
