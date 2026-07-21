package cli

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestDoctorAppReportsClosedChannelProgress(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	status, err := localapi.NewStatusResponse(localapi.StatusSnapshot{
		AssetRevision: fixture.bundle.Manifest().AssetRevision, ActivationReady: true,
		Runtime: localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true},
		Channels: []localapi.StatusChannelSnapshot{{Alias: "alpha", Membership: "active",
			RosterRevision: 1, Topic: localapi.StatusChannelTopic{State: "joined",
				ReadyMembers: 1, TotalMembers: 1}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeDoctorClient{statuses: []localapi.StatusResponse{status},
		authorities: []localapi.AuthorityResponse{fixture.authority}}
	app, stdout, stderr := fixture.app(t, client, doctorTestOverrides{})
	exit := app.run(context.Background(), nil)
	report := decodeDoctorReport(t, stdout.String())
	if exit != 0 || stderr.Len() != 0 || len(report.Channels) != 1 ||
		report.Channels[0].Alias != "alpha" || report.Channels[0].State != "ready" ||
		report.Checks[6] != passedDoctorCheck(doctorCheckNames[6]) {
		t.Fatalf("Channel doctor = exit %d report %#v stderr %q", exit, report,
			stderr.String())
	}
}

func TestDoctorChannelCheckDistinguishesQueuedAndDegradedProgress(t *testing.T) {
	t.Parallel()
	ready := doctorStatusChannel(t, "active", "joined", 0, 0)
	queued := doctorStatusChannel(t, "active", "converging", 0, 0)
	degraded := doctorStatusChannel(t, "active", "joined", 1, 1)
	tests := []struct {
		name  string
		value localapi.StatusChannel
		issue string
		exit  int
	}{
		{name: "ready", value: ready, issue: doctorIssueNone},
		{name: "queued", value: queued, issue: doctorIssueChannelQueued, exit: 5},
		{name: "degraded", value: degraded, issue: doctorIssueChannelDegraded, exit: 1},
	}
	for _, test := range tests {
		check, exit := doctorChannelCheck([]localapi.StatusChannel{test.value})
		if check.Issue != test.issue || exit != test.exit {
			t.Fatalf("%s Channel check = (%#v, %d)", test.name, check, exit)
		}
	}
}

func doctorStatusChannel(t *testing.T, membership, topic string,
	durable, quarantined uint64,
) localapi.StatusChannel {
	t.Helper()
	response, err := localapi.NewStatusResponse(localapi.StatusSnapshot{
		AssetRevision: statusRevision(), ActivationReady: true,
		Runtime: localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true},
		Channels: []localapi.StatusChannelSnapshot{{Alias: "alpha", Membership: membership,
			RosterRevision: 1, Topic: localapi.StatusChannelTopic{State: topic,
				ReadyMembers: 1, TotalMembers: 1},
			Inbox: localapi.StatusChannelInbox{Durable: durable, Quarantined: quarantined}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.Channels[0]
}
