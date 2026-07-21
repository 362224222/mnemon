package node

import (
	"context"
	"testing"
)

func TestProjectStatusChannelsExposesCoherentIdleAndQueuedPublicationProgress(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: wallClock{}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = daemon.Close() })
	created, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		RequestMetadata{Profile: fixture.profile},
		ChannelCreateRequest{Name: "status-progress"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	authority, err := daemon.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channels := projectStatusChannels(authority, daemon.channels.manager)
	if len(channels) != 1 || channels[0].Alias != created.Channel.Alias ||
		channels[0].Topic.TotalMembers != 1 || channels[0].LocalCommit.Accepted != 0 ||
		channels[0].Runtime != (StatusChannelRuntime{}) {
		t.Fatalf("idle status Channels = %#v", channels)
	}
}

func TestControllerStatusPublishesChannelStagesWithoutRawIdentities(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	serving := openServingDataPlaneDaemon(t, fixture)
	client, err := NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	created, apiErr := client.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: "public-progress"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	status, apiErr := client.ReadStatus(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if len(status.Channels) != 1 || status.Channels[0].Alias != created.Channel.Alias ||
		status.Channels[0].Membership != "active" || status.Channels[0].RosterRevision != 1 ||
		status.Channels[0].LocalCommit.Accepted != 0 || status.Channels[0].Inbox.Durable != 0 {
		t.Fatalf("public Channel status = %#v", status.Channels)
	}
	if err := serving.Daemon.Close(); err != nil {
		t.Fatal(err)
	}
}
