package localapi

import "testing"

func TestStatusChannelDerivesReadyQueuedDegradedAndTerminal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*StatusChannelSnapshot)
		want   string
	}{
		{name: "ready", want: statusChannelReady},
		{name: "queued offline", mutate: func(snapshot *StatusChannelSnapshot) {
			snapshot.Topic.UnreachableMembers = 1
		}, want: statusChannelQueued},
		{name: "degraded quarantine", mutate: func(snapshot *StatusChannelSnapshot) {
			snapshot.Inbox.Durable, snapshot.Inbox.Quarantined = 1, 1
		}, want: statusChannelDegraded},
		{name: "degraded remote publication", mutate: func(snapshot *StatusChannelSnapshot) {
			snapshot.Publication.RemoteBlocked = 1
		}, want: statusChannelDegraded},
		{name: "terminal", mutate: func(snapshot *StatusChannelSnapshot) {
			snapshot.Membership, snapshot.Topic.State = "closed", "left"
		}, want: statusChannelTerminal},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := validStatusChannelSnapshot()
			if test.mutate != nil {
				test.mutate(&snapshot)
			}
			channel, err := newStatusChannel(snapshot)
			if err != nil || channel.State != test.want {
				t.Fatalf("newStatusChannel() = (%#v, %v), want %q", channel, err, test.want)
			}
		})
	}
}

func TestStatusChannelRejectsCrossStageCardinalityAndSortsAliases(t *testing.T) {
	t.Parallel()
	invalid := validStatusChannelSnapshot()
	invalid.Publication.Queued = 1
	if _, err := newStatusChannel(invalid); err == nil {
		t.Fatal("status Channel accepted a publication without a local commit")
	}
	alpha, beta := validStatusChannelSnapshot(), validStatusChannelSnapshot()
	alpha.Alias, beta.Alias = "alpha", "beta"
	channels, err := newStatusChannels([]StatusChannelSnapshot{beta, alpha})
	if err != nil || len(channels) != 2 || channels[0].Alias != "alpha" || channels[1].Alias != "beta" {
		t.Fatalf("newStatusChannels() = (%#v, %v)", channels, err)
	}
	if _, err := newStatusChannels([]StatusChannelSnapshot{alpha, alpha}); err == nil {
		t.Fatal("status Channels accepted a duplicate alias")
	}
}

func validStatusChannelSnapshot() StatusChannelSnapshot {
	return StatusChannelSnapshot{Alias: "alpha", Membership: "active", RosterRevision: 1,
		Topic: StatusChannelTopic{State: "joined", ReadyMembers: 2, TotalMembers: 2}}
}
