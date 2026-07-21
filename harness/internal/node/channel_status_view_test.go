package node

import (
	"testing"
)

func TestChannelViewTopicStatusRequiresEveryActiveRemoteBaseline(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		members []ChannelMemberView
		want    string
	}{
		{name: "ready", members: []ChannelMemberView{
			{Status: "active", Binding: "self", BaselineReady: true},
			{Status: "active", Binding: "active", BaselineReady: true},
		}, want: "joined"},
		{name: "pending binding", members: []ChannelMemberView{
			{Status: "active", Binding: "self", BaselineReady: true},
			{Status: "active", Binding: "pending", BaselineReady: false},
		}, want: "converging"},
		{name: "missing outbound baseline", members: []ChannelMemberView{
			{Status: "active", Binding: "self", BaselineReady: true},
			{Status: "active", Binding: "active", BaselineReady: false},
		}, want: "converging"},
		{name: "terminal remote", members: []ChannelMemberView{
			{Status: "active", Binding: "self", BaselineReady: true},
			{Status: "revoked", Binding: "none", BaselineReady: false},
		}, want: "joined"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := channelViewTopicStatus("joined", test.members); got != test.want {
				t.Fatalf("channelViewTopicStatus() = %q, want %q", got, test.want)
			}
		})
	}
	if got := channelViewTopicStatus("blocked", nil); got != "blocked" {
		t.Fatalf("blocked status = %q", got)
	}
}
