package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReadChannelStatusAuthorityProjectsVerifiedLocalEvidence(t *testing.T) {
	t.Parallel()
	fixture, signed := acceptedGossipFixtureWithPublication(t, "channel-status-local")
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channel := requireChannelStatusChannel(t, authority, fixture.channel)
	head := channel.RosterHead()
	rosterMembers := channel.Roster().Members()
	if authority.LocalPeerID() != fixture.node.PeerID() ||
		channel.ChannelIDDigest() != model.Sum([]byte(fixture.channel.String())) ||
		head.RecordHead() != channel.Channel().RosterHead() ||
		head.OwnerPeerID() != channel.Channel().OwnerPeerID() ||
		!bytes.Equal(head.OwnerSignature(), rosterMembers[len(rosterMembers)-1].OwnerSignature()) {
		t.Fatalf("Channel status head differs from signed authority: %#v", channel)
	}
	publication := requireChannelStatusPublication(t, channel, signed.Digest())
	reference := publication.PublicationRef()
	if reference.OriginPeerID() != signed.Key().OriginPeerID() ||
		reference.OriginEpoch() != signed.Key().OriginEpoch() ||
		reference.ChannelSequence() != signed.Key().ChannelSequence() ||
		publication.EventKey() != signed.Event().Key() ||
		publication.EventDigest() != signed.Event().Digest() ||
		publication.OriginPeerID() != fixture.node.PeerID() ||
		publication.ImmediateTransportPeerID() != fixture.node.PeerID() ||
		publication.Arrival() != ChannelStatusArrivalLocal ||
		publication.SemanticOutcome() != ChannelStatusOutcomeOriginated ||
		publication.ChannelIDDigest() != channel.ChannelIDDigest() {
		t.Fatalf("local publication projection = %#v", publication)
	}
	if _, ok := publication.ArtifactDirectSourcePeerID(); ok {
		t.Fatal("local publication fabricated an Artifact network source")
	}
}

func TestReadChannelStatusAuthorityPreservesRelayAndIgnoredPaths(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxObserverFixture(t, "channel-status-relay")
	observer := *fixture.observer
	if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: observer.Identity().PeerID(),
			Baseline: ChannelDataBaseline{ChannelID: fixture.channel.Channel().ID(),
				OriginPeerID: observer.Identity().PeerID(), OriginEpoch: observer.Identity().OriginEpoch(),
				BaselineChannelSequence: 0}, At: fixture.at}); err != nil {
		t.Fatal(err)
	}
	fixture.at = fixture.at.Add(time.Second)
	relayed := fixture.publication(t, 1, 1, "relayed", true)
	if _, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: relayed, TransportPeerID: observer.Identity().PeerID(),
		ArrivalSource: model.ArrivalGossip, ReceivedAt: fixture.at}); err != nil {
		t.Fatal(err)
	}
	ignored := fixture.publication(t, 2, 2, "ignored", false)
	if _, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: ignored, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channel := requireChannelStatusChannel(t, authority, fixture.channel.Channel().ID())
	relayProjection := requireChannelStatusPublication(t, channel, relayed.Digest())
	if relayProjection.OriginPeerID() != fixture.remote.Identity().PeerID() ||
		relayProjection.ImmediateTransportPeerID() != observer.Identity().PeerID() ||
		relayProjection.OriginPeerID() == relayProjection.ImmediateTransportPeerID() ||
		relayProjection.Arrival() != ChannelStatusArrivalGossip ||
		relayProjection.SemanticOutcome() != ChannelStatusSemanticOutcome(model.InboxStored) ||
		len(relayProjection.IgnoredPeerIDs()) != 0 {
		t.Fatalf("relayed publication projection = %#v", relayProjection)
	}
	ignoredProjection := requireChannelStatusPublication(t, channel, ignored.Digest())
	ignoredPeers := ignoredProjection.IgnoredPeerIDs()
	if ignoredProjection.Arrival() != ChannelStatusArrivalRepair ||
		ignoredProjection.SemanticOutcome() != ChannelStatusSemanticOutcome(model.InboxIgnored) ||
		len(ignoredPeers) != 1 || ignoredPeers[0] != authority.LocalPeerID() {
		t.Fatalf("ignored publication projection = %#v", ignoredProjection)
	}
}

func TestReadChannelStatusAuthorityFailsClosedAboveCompleteBound(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "channel-status-bound", 0)
	for sequence := uint64(1); sequence <= model.MaxChannelStatusPublications+1; sequence++ {
		publication := fixture.publication(t, sequence, sequence, "bound", true)
		fixture.put(t, publication, fixture.at.Add(time.Duration(sequence)*time.Nanosecond))
	}
	if _, err := fixture.store.ReadChannelStatusAuthority(context.Background()); !errors.Is(err, ErrChannelStatusAuthority) {
		t.Fatalf("over-bound status error = %v", err)
	}
}

func TestReadChannelStatusAuthorityFailsClosedOnPublicationProjectionCorruption(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "channel-status-corruption", 0)
	publication := fixture.publication(t, 1, 1, "corruption", true)
	result := fixture.put(t, publication, fixture.at)
	mustExec(t, fixture.store, `DROP TRIGGER peer_inbox_identity_immutable`)
	mustExec(t, fixture.store, `UPDATE peer_inbox SET event_digest=zeroblob(32) WHERE inbox_id=?`,
		result.InboxID.String())
	if _, err := fixture.store.ReadChannelStatusAuthority(context.Background()); !errors.Is(err, ErrChannelStatusAuthority) {
		t.Fatalf("corrupt publication status error = %v", err)
	}
}

func requireChannelStatusChannel(t *testing.T, authority ChannelStatusAuthority,
	channelID model.ChannelID,
) ChannelStatusChannel {
	t.Helper()
	for _, channel := range authority.Channels() {
		if channel.Channel().ID() == channelID {
			return channel
		}
	}
	t.Fatalf("Channel %s absent from status authority", channelID.String())
	return ChannelStatusChannel{}
}

func requireChannelStatusPublication(t *testing.T, channel ChannelStatusChannel,
	digest model.Digest,
) ChannelStatusPublication {
	t.Helper()
	for _, publication := range channel.Publications() {
		if publication.PublicationDigest() == digest {
			return publication
		}
	}
	t.Fatalf("publication %s absent from status authority", digest.String())
	return ChannelStatusPublication{}
}
