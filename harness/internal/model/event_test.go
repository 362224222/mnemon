package model

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAudienceCanonicalizationAndCopy(t *testing.T) {
	t.Parallel()

	a := mustPeer(t, "peer-a")
	b := mustPeer(t, "peer-b")
	input := []PeerID{b, a}
	audience, err := NewAudience(input)
	if err != nil {
		t.Fatalf("NewAudience() error = %v", err)
	}
	input[0] = a
	got := audience.Peers()
	got[0] = b
	if audience.Peers()[0] != a || !audience.Contains(b) {
		t.Fatalf("Audience was not canonical or immutable: %#v", audience.Peers())
	}
	if _, err := NewAudience([]PeerID{a, a}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate audience error = %v", err)
	}
}

func TestEventScopeAndAuthorityInvariants(t *testing.T) {
	t.Parallel()

	home := mustPeer(t, "peer-home")
	reviewer := mustPeer(t, "peer-reviewer")
	scope := mustEventScope(t, home, home)
	spec := validEventSpec(t, scope, EventReviewOffered, reviewer)
	event, err := NewEvent(spec)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if event.Scope().WorkRef().HomePeerID() != home || event.Digest() != Sum(event.CanonicalJSON().Bytes()) {
		t.Fatalf("Event scope or digest mismatch")
	}

	badScope := mustEventScope(t, reviewer, home)
	spec = validEventSpec(t, badScope, EventReviewOffered, home)
	if _, err := NewEvent(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("non-home authoritative Event error = %v", err)
	}

	participant := validEventSpec(t, badScope, EventReviewAcceptRequested, home)
	if _, err := NewEvent(participant); err != nil {
		t.Fatalf("participant Event error = %v", err)
	}
	participant.Audience, _ = NewAudience([]PeerID{mustPeer(t, "peer-other")})
	if _, err := NewEvent(participant); !errors.Is(err, ErrInvariant) {
		t.Fatalf("participant missing home audience error = %v", err)
	}
}

func TestEventRejectsNonIdentifierActorPrincipal(t *testing.T) {
	t.Parallel()
	home := mustPeer(t, "peer-actor-home")
	reviewer := mustPeer(t, "peer-actor-reviewer")
	spec := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	for _, actor := range []string{"principal with spaces", "principal\nsecond-line", "principal\tfield"} {
		spec.ActorPrincipal = actor
		if _, err := NewEvent(spec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("actor %q error = %v", actor, err)
		}
	}
}

func TestEventCanonicalizationIsOrderIndependent(t *testing.T) {
	t.Parallel()

	home := mustPeer(t, "peer-home")
	a := mustPeer(t, "peer-a")
	b := mustPeer(t, "peer-b")
	scope := mustEventScope(t, home, home)
	first := validEventSpec(t, scope, EventReviewOffered, a)
	first.Audience, _ = NewAudience([]PeerID{b, a})
	second := first
	second.Audience, _ = NewAudience([]PeerID{a, b})
	eventA, err := NewEvent(first)
	if err != nil {
		t.Fatalf("NewEvent(first) error = %v", err)
	}
	eventB, err := NewEvent(second)
	if err != nil {
		t.Fatalf("NewEvent(second) error = %v", err)
	}
	if eventA.CanonicalJSON().String() != eventB.CanonicalJSON().String() || eventA.Digest() != eventB.Digest() {
		t.Fatalf("canonical Event depends on audience input order")
	}
	if !strings.Contains(eventA.CanonicalJSON().String(), `"artifact_roots":[]`) ||
		!strings.Contains(eventA.CanonicalJSON().String(), `"caused_by":[]`) {
		t.Fatalf("empty bounded Event collections must be arrays: %s", eventA.CanonicalJSON().String())
	}
	if eventA.Artifacts() == nil || eventA.CausedBy() == nil {
		t.Fatal("empty Event accessors must preserve canonical array shape")
	}
	artifactProjection, _ := JSONFrom(eventA.Artifacts())
	causalityProjection, _ := JSONFrom(eventA.CausedBy())
	if artifactProjection.String() != "[]" || causalityProjection.String() != "[]" {
		t.Fatalf("empty Event projections = %s / %s", artifactProjection.String(), causalityProjection.String())
	}
}

func TestEventBodyHasPublicationCeiling(t *testing.T) {
	t.Parallel()

	home := mustPeer(t, "peer-home")
	reviewer := mustPeer(t, "peer-reviewer")
	spec := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	payload, err := JSONFrom(map[string]any{"content": strings.Repeat("x", MaxPublicationBytes)})
	if err != nil {
		t.Fatalf("JSONFrom() error = %v", err)
	}
	spec.Payload = payload
	if _, err := NewEvent(spec); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized Event error = %v, want ErrLimit", err)
	}
}

func mustPeer(t *testing.T, value string) PeerID {
	t.Helper()
	peer, err := ParsePeerID(value)
	if err != nil {
		t.Fatalf("ParsePeerID(%q): %v", value, err)
	}
	return peer
}

func mustEventScope(t *testing.T, origin, home PeerID) EventScope {
	t.Helper()
	channel, _ := ParseChannelID("channel-a")
	epoch, _ := ParseOriginEpoch("epoch-a")
	workID, _ := ParseWorkID("work-a")
	work, _ := NewWorkRef(home, workID)
	head, _ := NewRecordHead(1, Sum([]byte("roster")))
	scope, err := NewEventScope(channel, origin, epoch, 1, 1, head, head, work)
	if err != nil {
		t.Fatalf("NewEventScope(): %v", err)
	}
	return scope
}

func validEventSpec(t *testing.T, scope EventScope, kind EventType, audiencePeer PeerID) EventSpec {
	t.Helper()
	id, _ := ParseEventID("event-a")
	audience, _ := NewAudience([]PeerID{audiencePeer})
	payload, _ := NewJSON([]byte(`{"iteration":1}`))
	now := time.Date(2026, 7, 16, 0, 0, 0, 123, time.UTC)
	return EventSpec{id, scope, EventSourceLocal, "principal-a", kind, audience, "review", payload, nil, nil, now, now}
}
