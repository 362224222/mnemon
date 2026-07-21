package store

import (
	"context"
	"testing"
)

func TestReadChannelStatusProgressKeepsIdleChannelHealthyAndCountsAcceptedPublication(t *testing.T) {
	t.Parallel()
	fixture, _ := acceptedGossipFixtureWithPublication(t, "channel-status-progress")
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	progress := requireChannelStatusChannel(t, authority, fixture.channel).Progress()
	publication := progress.Publication()
	if progress.Commit().Accepted != 1 || publication.Queued != 1 || publication.Published != 0 ||
		publication.Blocked != 0 || progress.Inbox().Durable != 0 ||
		progress.Runtime() != (ChannelStatusRuntimeProgress{}) {
		t.Fatalf("Channel progress = %#v", progress)
	}
}

func TestReadChannelStatusProgressCountsDurableInboxAndCursorWithoutPayload(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "channel-status-progress-inbox", 0)
	publication := fixture.publication(t, 1, 1, "progress", true)
	fixture.put(t, publication, fixture.at)
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	progress := requireChannelStatusChannel(t, authority, fixture.channel.Channel().ID()).Progress()
	if progress.Inbox().Durable != 1 || progress.Inbox().Pending != 1 ||
		progress.Cursor().InboundOrigins != 1 || progress.Cursor().InboundGapped != 0 {
		t.Fatalf("imported Channel progress = %#v", progress)
	}
}
