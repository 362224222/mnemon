package agency

import (
	"errors"
	"testing"
	"time"
)

func TestVerifiedPeerDeliveryRequiresParsedEnvelopeAndCompleteArtifacts(t *testing.T) {
	route, delivery := peerDeliveryFixture(t, "route:verified")
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	metadata := make([]VerifiedPeerArtifact, 0, len(delivery.Artifacts()))
	for index, digest := range delivery.Artifacts() {
		artifact, artifactErr := NewVerifiedPeerArtifact(digest, int64(index+1), testTime.Add(time.Minute))
		if artifactErr != nil {
			t.Fatal(artifactErr)
		}
		metadata = append(metadata, artifact)
	}
	source, target := mustPrincipal(t, "peer:source"), mustPrincipal(t, "agent:target")
	verified, err := NewVerifiedPeerDelivery(parsed, source, target, metadata)
	if err != nil {
		t.Fatalf("NewVerifiedPeerDelivery() error = %v", err)
	}
	if verified.LocalSource() != source || verified.LocalTarget() != target ||
		verified.Consequence() != ConsequenceCreateHandlings || verified.SuccessorCount() != 1 {
		t.Fatalf("VerifiedPeerDelivery effect = source %v target %v consequence %v successors %d",
			verified.LocalSource(), verified.LocalTarget(), verified.Consequence(), verified.SuccessorCount())
	}
	if _, err := NewVerifiedPeerDelivery(ParsedPeerDelivery{}, source, target, metadata); err == nil {
		t.Fatal("unparsed delivery unexpectedly crossed verification boundary")
	}
	if _, err := NewVerifiedPeerDelivery(parsed, AgentPrincipalID{}, target, metadata); err == nil {
		t.Fatal("unresolved local source unexpectedly crossed verification boundary")
	}
	if _, err := NewVerifiedPeerDelivery(parsed, source, target, metadata[:len(metadata)-1]); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing Artifact error = %v, want ErrInvariant", err)
	}
	extra, err := NewVerifiedPeerArtifact(Sum([]byte("extra")), 1, testTime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifiedPeerDelivery(parsed, source, target, append(metadata, extra)); !errors.Is(err, ErrInvariant) {
		t.Fatalf("extra Artifact error = %v, want ErrInvariant", err)
	}
}

func TestVerifiedPeerDeliveryDefensiveCopies(t *testing.T) {
	route, delivery := peerDeliveryFixture(t, "route:verified-copies")
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	metadata := make([]VerifiedPeerArtifact, 0, len(delivery.Artifacts()))
	for _, digest := range delivery.Artifacts() {
		artifact, artifactErr := NewVerifiedPeerArtifact(digest, 1, testTime)
		if artifactErr != nil {
			t.Fatal(artifactErr)
		}
		metadata = append(metadata, artifact)
	}
	verified, err := NewVerifiedPeerDelivery(parsed, mustPrincipal(t, "peer:source"),
		mustPrincipal(t, "agent:target"), metadata)
	if err != nil {
		t.Fatal(err)
	}
	gotArtifacts := verified.Artifacts()
	gotDelivery := verified.Delivery()
	gotArtifacts[0] = VerifiedPeerArtifact{}
	canonical := gotDelivery.CanonicalJSON()
	canonical[0] = '!'
	if verified.Artifacts()[0].Digest().IsZero() || verified.Delivery().CanonicalJSON()[0] == '!' {
		t.Fatal("VerifiedPeerDelivery exposed mutable internal state")
	}
}

func TestVerifiedPeerArtifactBounds(t *testing.T) {
	digest := Sum([]byte("artifact"))
	if _, err := NewVerifiedPeerArtifact(digest, MaxPeerArtifactBytes, testTime); err != nil {
		t.Fatalf("maximum-size verified Artifact rejected: %v", err)
	}
	for name, input := range map[string]struct {
		digest Digest
		size   int64
		at     time.Time
	}{
		"zero digest": {size: 1, at: testTime},
		"negative":    {digest: digest, size: -1, at: testTime},
		"too large":   {digest: digest, size: MaxPeerArtifactBytes + 1, at: testTime},
		"zero time":   {digest: digest, size: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewVerifiedPeerArtifact(input.digest, input.size, input.at); err == nil {
				t.Fatal("NewVerifiedPeerArtifact() unexpectedly succeeded")
			}
		})
	}
}

func TestVerifiedPeerDeliveryAllowsNoArtifactForNoncompletionCandidate(t *testing.T) {
	route := mustRoute(t, "route:no-artifact")
	delivery, err := NewPeerDelivery(route, PeerDeliverySpec{
		OriginEvent: mustEventRef(t, "event:no-artifact", "origin"), OriginSequence: 1,
		OriginAcceptedAt: testTime, OriginSource: mustPrincipal(t, "agent:origin"),
		OriginConsequence: ConsequenceCreateHandlings, OriginTargetCount: 2,
		TargetAlias: mustHandle(t, "agent/target"), Kind: mustLabel(t, "opaque.request"),
		Payload: mustPayload(t, "Request without content bytes."), CausalDepth: 1,
		ExpiresAt: testTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifiedPeerDelivery(parsed, mustPrincipal(t, "peer:source"),
		mustPrincipal(t, "agent:target"), nil); err != nil {
		t.Fatalf("artifact-free peer candidate rejected: %v", err)
	}
}
