package model

import (
	"errors"
	"testing"
	"time"
)

func TestInboxEnumsAreClosed(t *testing.T) {
	t.Parallel()

	statuses := []InboxStatus{InboxStored, InboxWaitingArtifact, InboxReady, InboxProcessing, InboxAccepted,
		InboxRejected, InboxConflicted, InboxRetry, InboxQuarantined, InboxIgnored}
	for _, status := range statuses {
		if !status.Valid() {
			t.Fatalf("InboxStatus(%q).Valid() = false", status)
		}
	}
	if InboxStatus("handled").Valid() || ArrivalSource("relay").Valid() {
		t.Fatalf("unknown Inbox enum was accepted")
	}
}

func TestPeerInboxAudienceAndPullAuthority(t *testing.T) {
	t.Parallel()

	home, reviewer, relay := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer"), mustPeer(t, "peer-relay")
	observer := mustPeer(t, "peer-observer")
	eventSpec := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	eventSpec.Source = EventSourceImported
	event, err := NewEvent(eventSpec)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	body, _ := NewPublicationBody(event)
	publication, _ := AttachSignature(body, testPublicationSignature())
	inboxID, _ := ParseInboxID("inbox-a")
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	spec := PeerInboxSpec{inboxID, publication, relay, ArrivalGossip, true, nil, InboxStored,
		0, now, "", nil, nil, nil, nil, "", now, now}
	if _, err := NewPeerInbox(reviewer, spec); err != nil {
		t.Fatalf("NewPeerInbox(gossip) error = %v", err)
	}
	spec.ArrivalSource = ArrivalPull
	if _, err := NewPeerInbox(reviewer, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("relay Pull error = %v", err)
	}
	spec.TransportPeerID = home
	if _, err := NewPeerInbox(reviewer, spec); err != nil {
		t.Fatalf("origin Pull error = %v", err)
	}

	spec.TransportPeerID, spec.ArrivalSource = relay, ArrivalGossip
	if _, err := NewPeerInbox(observer, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("non-audience stored error = %v", err)
	}
	spec.IsAudience, spec.Status = false, InboxIgnored
	if _, err := NewPeerInbox(observer, spec); err != nil {
		t.Fatalf("non-audience ignored error = %v", err)
	}
}

func TestPeerInboxArtifactAndLeaseInvariants(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer")
	specEvent := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	artifact, _ := NewArtifactRef(Sum([]byte("artifact")), ArtifactProduced)
	specEvent.Artifacts = []ArtifactRef{artifact}
	specEvent.Source = EventSourceImported
	event, _ := NewEvent(specEvent)
	body, _ := NewPublicationBody(event)
	publication, _ := AttachSignature(body, testPublicationSignature())
	inboxID, _ := ParseInboxID("inbox-a")
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	lease := now.Add(time.Minute)
	spec := PeerInboxSpec{inboxID, publication, home, ArrivalPull, true, []Digest{artifact.RootDigest()},
		InboxProcessing, 1, now, "worker-a", &lease, nil, nil, nil, "", now, now}
	if _, err := NewPeerInbox(reviewer, spec); err != nil {
		t.Fatalf("NewPeerInbox(processing) error = %v", err)
	}
	spec.RequiredArtifactRoots = nil
	if _, err := NewPeerInbox(reviewer, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("Artifact mismatch error = %v", err)
	}
	spec.RequiredArtifactRoots = []Digest{artifact.RootDigest()}
	spec.Status, spec.LeaseOwner, spec.LeaseUntil = InboxAccepted, "", nil
	if _, err := NewPeerInbox(reviewer, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("accepted without local Event error = %v", err)
	}
}
