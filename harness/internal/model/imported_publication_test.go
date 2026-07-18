package model

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestProjectImportedPublicationPreservesIdentityAndBuildsInbox(t *testing.T) {
	t.Parallel()

	publication, publicKey, origin, receiver := importedPublicationFixture(t)
	if err := VerifyPublication(publicKey, publication); err != nil {
		t.Fatalf("VerifyPublication() error = %v", err)
	}

	imported, err := ProjectImportedPublication(&publication)
	if err != nil {
		t.Fatalf("ProjectImportedPublication() error = %v", err)
	}
	assertImportedPublicationIdentity(t, publication, imported)

	inboxID, _ := ParseInboxID("inbox-imported-publication")
	now := time.Date(2026, 7, 19, 1, 2, 3, 4, time.UTC)
	roots := artifactDigests(imported.Event().Artifacts())
	inbox, err := NewPeerInbox(receiver, PeerInboxSpec{
		ID:                    inboxID,
		Publication:           imported,
		TransportPeerID:       origin,
		ArrivalSource:         ArrivalPull,
		IsAudience:            true,
		RequiredArtifactRoots: roots,
		Status:                InboxStored,
		NextAttemptAt:         now,
		ReceivedAt:            now,
		UpdatedAt:             now,
	})
	if err != nil {
		t.Fatalf("NewPeerInbox(imported publication) error = %v", err)
	}
	if inbox.Publication().Event().Source() != EventSourceImported || inbox.OriginPeerID() != origin {
		t.Fatal("Peer Inbox lost the imported source or immutable origin")
	}
}

func TestProjectImportedPublicationRejectsNilZeroAndInconsistentValues(t *testing.T) {
	t.Parallel()

	if _, err := ProjectImportedPublication(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil publication error = %v, want ErrInvalid", err)
	}
	zero := SignedPublication{}
	if _, err := ProjectImportedPublication(&zero); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero publication error = %v, want ErrInvalid", err)
	}

	publication, _, _, _ := importedPublicationFixture(t)
	tests := map[string]func(SignedPublication) SignedPublication{
		"invalid Event source": func(value SignedPublication) SignedPublication {
			value.body.event.spec.Source = EventSource("remote")
			return value
		},
		"Event canonical drift": func(value SignedPublication) SignedPublication {
			value.body.event.canonical, _ = NewJSON([]byte(`{"drift":true}`))
			return value
		},
		"Event collection drift": func(value SignedPublication) SignedPublication {
			other, _ := NewArtifactRef(Sum([]byte("other-artifact")), ArtifactReferenced)
			value.body.event.spec.Artifacts = []ArtifactRef{other}
			return value
		},
		"publication digest drift": func(value SignedPublication) SignedPublication {
			value.body.digest = Sum([]byte("other-publication"))
			return value
		},
		"signature drift": func(value SignedPublication) SignedPublication {
			value.originSignature = string(bytes.Repeat([]byte{'x'}, ed25519.SignatureSize))
			return value
		},
		"wire drift": func(value SignedPublication) SignedPublication {
			value.wire, _ = NewJSON([]byte(`{"publication":{}}`))
			return value
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inconsistent := mutate(publication)
			if _, err := ProjectImportedPublication(&inconsistent); err == nil {
				t.Fatal("ProjectImportedPublication() accepted inconsistent input")
			}
		})
	}
}

func TestProjectImportedPublicationDefensivelyCopiesCollections(t *testing.T) {
	t.Parallel()

	publication, _, _, _ := importedPublicationFixture(t)
	imported, err := ProjectImportedPublication(&publication)
	if err != nil {
		t.Fatalf("ProjectImportedPublication() error = %v", err)
	}
	wantWire := imported.WireJSON().String()
	wantAudience := imported.Event().Audience().Peers()
	wantArtifacts := imported.Event().Artifacts()
	wantCausedBy := imported.Event().CausedBy()

	otherPeer := mustPeer(t, "peer-import-mutated")
	otherArtifact, _ := NewArtifactRef(Sum([]byte("mutated-artifact")), ArtifactReferenced)
	otherEventID, _ := ParseEventID("event-mutated-cause")
	otherCause, _ := NewEventKey(otherPeer, imported.Event().Scope().OriginEpoch(), otherEventID)
	publication.body.event.spec.Audience.peers[0] = otherPeer
	publication.body.event.spec.Artifacts[0] = otherArtifact
	publication.body.event.spec.CausedBy[0] = otherCause

	audienceCopy := imported.Event().Audience().Peers()
	artifactCopy := imported.Event().Artifacts()
	causalityCopy := imported.Event().CausedBy()
	signatureCopy := imported.OriginSignature()
	wireCopy := imported.WireJSON().Bytes()
	audienceCopy[0] = otherPeer
	artifactCopy[0] = otherArtifact
	causalityCopy[0] = otherCause
	signatureCopy[0] ^= 0xff
	wireCopy[0] ^= 0xff

	if imported.WireJSON().String() != wantWire ||
		!reflect.DeepEqual(imported.Event().Audience().Peers(), wantAudience) ||
		!reflect.DeepEqual(imported.Event().Artifacts(), wantArtifacts) ||
		!reflect.DeepEqual(imported.Event().CausedBy(), wantCausedBy) {
		t.Fatal("imported publication retained an input or accessor slice alias")
	}
}

func importedPublicationFixture(t *testing.T) (SignedPublication, ed25519.PublicKey, PeerID, PeerID) {
	t.Helper()

	origin := mustPeer(t, "peer-import-origin")
	receiver := mustPeer(t, "peer-import-receiver")
	observer := mustPeer(t, "peer-import-observer")
	spec := validEventSpec(t, mustEventScope(t, origin, origin), EventReviewOffered, receiver)
	spec.Audience, _ = NewAudience([]PeerID{observer, receiver})
	artifact, _ := NewArtifactRef(Sum([]byte("imported-artifact")), ArtifactProduced)
	causeEventID, _ := ParseEventID("event-import-cause")
	cause, _ := NewEventKey(origin, spec.Scope.OriginEpoch(), causeEventID)
	spec.Artifacts = []ArtifactRef{artifact}
	spec.CausedBy = []EventKey{cause}
	event, err := NewEvent(spec)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	body, err := NewPublicationBody(event)
	if err != nil {
		t.Fatalf("NewPublicationBody() error = %v", err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey() error = %v", err)
	}
	message, err := PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	if err != nil {
		t.Fatalf("PublicationSigningMessage() error = %v", err)
	}
	publication, err := AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatalf("AttachSignature() error = %v", err)
	}
	return publication, publicKey, origin, receiver
}

func assertImportedPublicationIdentity(t *testing.T, want, got SignedPublication) {
	t.Helper()

	wantEvent, gotEvent := want.Event(), got.Event()
	if gotEvent.Source() != EventSourceImported || wantEvent.ID() != gotEvent.ID() ||
		wantEvent.Key() != gotEvent.Key() || wantEvent.Scope() != gotEvent.Scope() ||
		wantEvent.ActorPrincipal() != gotEvent.ActorPrincipal() || wantEvent.Type() != gotEvent.Type() ||
		wantEvent.Summary() != gotEvent.Summary() || wantEvent.Payload().String() != gotEvent.Payload().String() ||
		wantEvent.CreatedAt() != gotEvent.CreatedAt() || wantEvent.AcceptedAt() != gotEvent.AcceptedAt() ||
		wantEvent.Digest() != gotEvent.Digest() ||
		!reflect.DeepEqual(wantEvent.Audience().Peers(), gotEvent.Audience().Peers()) ||
		!reflect.DeepEqual(wantEvent.Artifacts(), gotEvent.Artifacts()) ||
		!reflect.DeepEqual(wantEvent.CausedBy(), gotEvent.CausedBy()) ||
		!bytes.Equal(wantEvent.CanonicalJSON().Bytes(), gotEvent.CanonicalJSON().Bytes()) {
		t.Fatal("imported Event projection changed immutable Event identity or content")
	}
	if want.Key() != got.Key() || want.Digest() != got.Digest() ||
		!bytes.Equal(want.CanonicalJSON().Bytes(), got.CanonicalJSON().Bytes()) ||
		!bytes.Equal(want.OriginSignature(), got.OriginSignature()) ||
		!bytes.Equal(want.WireJSON().Bytes(), got.WireJSON().Bytes()) {
		t.Fatal("imported Event projection changed signed publication identity or wire")
	}
}
