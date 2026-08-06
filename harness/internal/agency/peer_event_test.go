package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"
)

func TestNewPeerEventSealsInboundAuthorityAndCanonicalRebuild(t *testing.T) {
	verified, delivery := peerEventFixture(t, "route:peer-event")
	stamp := EventStamp{ID: mustEventID(t, "event:peer-local"),
		AcceptedAt: testTime.Add(time.Minute), OriginSequence: 9, CausalDepth: delivery.CausalDepth()}
	event, err := NewPeerEvent(verified, stamp)
	if err != nil {
		t.Fatalf("NewPeerEvent() error = %v", err)
	}
	rebuilt, err := NewPeerEvent(verified, stamp)
	if err != nil {
		t.Fatalf("canonical peer Event rebuild: %v", err)
	}
	if event.Digest() != rebuilt.Digest() {
		t.Fatal("canonical peer Event rebuild changed its digest")
	}
	if !bytes.Equal(event.CanonicalJSON(), rebuilt.CanonicalJSON()) {
		t.Fatal("canonical peer Event rebuild changed its bytes")
	}
	assertPeerEventAuthority(t, event, verified, delivery)
	assertPeerEventProvenance(t, event, delivery)
	assertPeerEventCanonical(t, event, verified.LocalTarget())
}

func assertPeerEventAuthority(t *testing.T, event Event, verified VerifiedPeerDelivery,
	delivery PeerDelivery,
) {
	t.Helper()
	if event.Source() != verified.LocalSource() {
		t.Fatal("peer Event source did not come from verified route authority")
	}
	if event.OperationKey().String() != delivery.ID().String() {
		t.Fatal("peer Event operation is not bound to DeliveryID")
	}
	if event.RequestDigest() != delivery.EnvelopeDigest() {
		t.Fatal("peer Event request digest is not bound to the Delivery envelope")
	}
	if event.Kind() != delivery.Kind() || event.Payload() != delivery.Payload() {
		t.Fatal("peer Event semantics did not come from the Delivery")
	}
	if event.Consequence() != ConsequenceCreateHandlings || event.CausalDepth() != delivery.CausalDepth() {
		t.Fatal("peer Event effect or causal depth was not sealed by the Delivery")
	}
	if _, exists := event.Subject(); exists {
		t.Fatal("peer Event unexpectedly carries a subject authority")
	}
	if _, exists := event.ExpectedReference(); exists {
		t.Fatal("peer Event unexpectedly carries Reference authority")
	}
	assertPeerEventTarget(t, event.Targets(), verified.LocalTarget(), delivery.TargetAlias())
}

func assertPeerEventTarget(t *testing.T, targets []ResolvedTarget, principal AgentPrincipalID,
	alias OpaqueHandle,
) {
	t.Helper()
	if len(targets) != 1 {
		t.Fatalf("peer Event target count = %d, want 1", len(targets))
	}
	target := targets[0]
	if target.Destination() != TargetDestinationLocal || target.LocalPrincipal() != principal {
		t.Fatal("peer Event target is not the machine-resolved local Principal")
	}
	if target.Requested().Alias() != alias {
		t.Fatal("peer Event target lost its opaque Delivery alias")
	}
	if !target.RemoteRoute().IsZero() || !target.RemoteAlias().IsZero() {
		t.Fatal("peer Event local target contains remote authority")
	}
}

func assertPeerEventProvenance(t *testing.T, event Event, delivery PeerDelivery) {
	t.Helper()
	if !slices.Equal(event.Artifacts(), delivery.Artifacts()) {
		t.Fatalf("peer Event Artifacts = %v, want %v", event.Artifacts(), delivery.Artifacts())
	}
	wantCausation := []EventRef{delivery.OriginEvent()}
	if !slices.Equal(event.Causation(), wantCausation) {
		t.Fatalf("peer Event causation = %v, want %v", event.Causation(), wantCausation)
	}
	wantCorrelation, wantExists := delivery.OriginCorrelation()
	gotCorrelation, gotExists := event.Correlation()
	if gotExists != wantExists || gotCorrelation != wantCorrelation {
		t.Fatalf("peer Event correlation = (%v, %v), want (%v, %v)",
			gotCorrelation, gotExists, wantCorrelation, wantExists)
	}
}

func assertPeerEventCanonical(t *testing.T, event Event, target AgentPrincipalID) {
	t.Helper()
	var wire eventWire
	if err := json.Unmarshal(event.CanonicalJSON(), &wire); err != nil {
		t.Fatalf("decode canonical peer Event: %v", err)
	}
	reencoded, digest, err := canonicalJSON(wire)
	if err != nil {
		t.Fatalf("reencode canonical peer Event: %v", err)
	}
	if !bytes.Equal(reencoded, event.CanonicalJSON()) || digest != event.Digest() {
		t.Fatal("canonical peer Event JSON round trip differs")
	}
	if wire.SchemaVersion != 2 || wire.Machine.Consequence != ConsequenceCreateHandlings.String() {
		t.Fatalf("canonical peer Event has the wrong schema or effect: %s", event.CanonicalJSON())
	}
	if len(wire.Machine.Targets) != 1 || wire.Machine.Targets[0].Destination != "local" ||
		wire.Machine.Targets[0].LocalPrincipal != target.String() {
		t.Fatalf("canonical peer Event has the wrong target: %s", event.CanonicalJSON())
	}
	if wire.Machine.Subject != nil || wire.Machine.ExpectedReference != nil {
		t.Fatalf("canonical peer Event exposes forbidden authority: %s", event.CanonicalJSON())
	}
}

func TestNewPeerEventRejectsIncompleteAuthorityAndDepthMismatch(t *testing.T) {
	verified, delivery := peerEventFixture(t, "route:peer-event-invalid")
	validStamp := EventStamp{ID: mustEventID(t, "event:peer-valid"), AcceptedAt: testTime,
		OriginSequence: 1, CausalDepth: delivery.CausalDepth()}
	for name, input := range map[string]struct {
		verified VerifiedPeerDelivery
		stamp    EventStamp
		category error
	}{
		"zero verified delivery": {stamp: validStamp, category: ErrInvalid},
		"zero source": {verified: func() VerifiedPeerDelivery {
			copyValue := verified
			copyValue.source = AgentPrincipalID{}
			return copyValue
		}(), stamp: validStamp, category: ErrInvalid},
		"zero target": {verified: func() VerifiedPeerDelivery {
			copyValue := verified
			copyValue.target = AgentPrincipalID{}
			return copyValue
		}(), stamp: validStamp, category: ErrInvalid},
		"corrupt canonical delivery": {verified: func() VerifiedPeerDelivery {
			copyValue := verified
			copyValue.delivery.canonical = []byte(`{}`)
			return copyValue
		}(), stamp: validStamp, category: ErrInvalid},
		"zero event ID": {verified: verified, stamp: EventStamp{AcceptedAt: testTime,
			OriginSequence: 1, CausalDepth: delivery.CausalDepth()}, category: ErrInvalid},
		"zero sequence": {verified: verified, stamp: EventStamp{ID: mustEventID(t, "event:zero-seq"),
			AcceptedAt: testTime, CausalDepth: delivery.CausalDepth()}, category: ErrInvalid},
		"wrong causal depth": {verified: verified, stamp: EventStamp{ID: mustEventID(t, "event:wrong-depth"),
			AcceptedAt: testTime, OriginSequence: 1, CausalDepth: delivery.CausalDepth() + 1},
			category: ErrInvariant},
		"zero accepted time": {verified: verified, stamp: EventStamp{ID: mustEventID(t, "event:zero-time"),
			OriginSequence: 1, CausalDepth: delivery.CausalDepth()}, category: ErrInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPeerEvent(input.verified, input.stamp); !errors.Is(err, input.category) {
				t.Fatalf("NewPeerEvent() error = %v, want %v", err, input.category)
			}
		})
	}
}

func TestNewPeerEventKeepsOnlyDirectCauseFromMaximumRemoteChain(t *testing.T) {
	route := mustRoute(t, "route:peer-provenance-bound")
	remoteChain := make([]EventRef, MaxCausationHandles)
	for index := range remoteChain {
		remoteChain[index] = mustEventRef(t, fmt.Sprintf("event:imported-%d", index),
			fmt.Sprintf("imported-%d", index))
	}
	delivery, err := NewPeerDelivery(route, PeerDeliverySpec{
		OriginEvent: mustEventRef(t, "event:provenance-origin", "origin"), OriginSequence: 1,
		OriginAcceptedAt: testTime, OriginSource: mustPrincipal(t, "agent:origin"),
		OriginConsequence: ConsequenceCreateHandlings, OriginTargetCount: 2,
		OriginCausation: remoteChain, TargetAlias: mustHandle(t, "remote/target"),
		Kind: mustLabel(t, "opaque.request"), CausalDepth: 1, ExpiresAt: testTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := NewVerifiedPeerDelivery(parsed, mustPrincipal(t, "peer:source"),
		mustPrincipal(t, "agent:target"), nil)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewPeerEvent(verified, EventStamp{ID: mustEventID(t, "event:local-bound"),
		AcceptedAt: testTime, OriginSequence: 1, CausalDepth: delivery.CausalDepth()})
	if err != nil {
		t.Fatalf("promote maximum remote causation chain: %v", err)
	}
	sort.Slice(remoteChain, func(i, j int) bool { return eventRefLess(remoteChain[i], remoteChain[j]) })
	if !slices.Equal(delivery.OriginCausation(), remoteChain) {
		t.Fatal("signed PeerDelivery lost the full remote causation chain")
	}
	if got := event.Causation(); len(got) != 1 || got[0] != delivery.OriginEvent() {
		t.Fatalf("local Event causation = %v, want only immediate origin %v", got, delivery.OriginEvent())
	}
}

func TestNewPeerEventDefensiveCopies(t *testing.T) {
	verified, delivery := peerEventFixture(t, "route:peer-event-copies")
	event, err := NewPeerEvent(verified, EventStamp{ID: mustEventID(t, "event:peer-copies"),
		AcceptedAt: testTime, OriginSequence: 1, CausalDepth: delivery.CausalDepth()})
	if err != nil {
		t.Fatal(err)
	}
	verified.delivery.artifacts[0] = Digest{}
	verified.delivery.originCausation[0] = EventRef{}
	verified.artifacts[0] = VerifiedPeerArtifact{}
	artifacts := event.Artifacts()
	causation := event.Causation()
	targets := event.Targets()
	canonical := event.CanonicalJSON()
	artifacts[0] = Digest{}
	causation[0] = EventRef{}
	targets[0] = ResolvedTarget{}
	canonical[0] = '!'
	if event.Artifacts()[0].IsZero() || event.Causation()[0].IsZero() ||
		event.Targets()[0].LocalPrincipal().IsZero() || event.CanonicalJSON()[0] == '!' {
		t.Fatal("peer Event exposed mutable input or output state")
	}
}

func peerEventFixture(t *testing.T, routeName string) (VerifiedPeerDelivery, PeerDelivery) {
	t.Helper()
	route, delivery := peerDeliveryFixture(t, routeName)
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := make([]VerifiedPeerArtifact, 0, len(delivery.Artifacts()))
	for _, digest := range delivery.Artifacts() {
		artifact, artifactErr := NewVerifiedPeerArtifact(digest, 1, testTime)
		if artifactErr != nil {
			t.Fatal(artifactErr)
		}
		artifacts = append(artifacts, artifact)
	}
	verified, err := NewVerifiedPeerDelivery(parsed, mustPrincipal(t, "peer:route-surrogate"),
		mustPrincipal(t, "agent:local-target"), artifacts)
	if err != nil {
		t.Fatal(err)
	}
	return verified, delivery
}
