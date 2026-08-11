package agency

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestParseEventCanonicalJSONRoundTripsLocalAndPeerEvents(t *testing.T) {
	local, err := NewEvent(mustBoundRoot(t, "operation:event-parse-local"), EventStamp{
		ID: mustEventID(t, "event:parse-local"), AcceptedAt: testTime,
		OriginSequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	verified, delivery := peerEventFixture(t, "route:event-parse-peer")
	peer, err := NewPeerEvent(verified, EventStamp{
		ID: mustEventID(t, "event:parse-peer"), AcceptedAt: testTime.Add(time.Second),
		OriginSequence: 2, CausalDepth: delivery.CausalDepth(),
	}, ConsequenceCreateHandlings, decidedLocalPeerTargets(t, verified, delivery))
	if err != nil {
		t.Fatal(err)
	}

	for name, original := range map[string]Event{"local": local, "peer": peer} {
		t.Run(name, func(t *testing.T) {
			parsed, parseErr := ParseEventCanonicalJSON(original.CanonicalJSON())
			if parseErr != nil {
				t.Fatalf("ParseEventCanonicalJSON() error = %v", parseErr)
			}
			if !bytes.Equal(parsed.CanonicalJSON(), original.CanonicalJSON()) ||
				parsed.Digest() != original.Digest() || parsed.Ref() != original.Ref() ||
				parsed.Source() != original.Source() || parsed.OperationKey() != original.OperationKey() ||
				parsed.RequestDigest() != original.RequestDigest() || parsed.Kind() != original.Kind() ||
				parsed.Payload() != original.Payload() || parsed.Consequence() != original.Consequence() ||
				parsed.OriginSequence() != original.OriginSequence() ||
				parsed.CausalDepth() != original.CausalDepth() ||
				!parsed.AcceptedAt().Equal(original.AcceptedAt()) ||
				!slices.Equal(parsed.Artifacts(), original.Artifacts()) ||
				!slices.Equal(parsed.Causation(), original.Causation()) {
				t.Fatalf("parsed Event differs\n got: %s\nwant: %s",
					parsed.CanonicalJSON(), original.CanonicalJSON())
			}
			canonical := parsed.CanonicalJSON()
			canonical[0] = '!'
			if parsed.CanonicalJSON()[0] == '!' {
				t.Fatal("parsed Event exposed mutable canonical bytes")
			}
		})
	}
}

func TestParseEventCanonicalJSONRejectsMalformedNoncanonicalAndUnboundedData(t *testing.T) {
	base := eventParserFixtureWire()
	baseJSON := mustCanonicalEventWire(t, base)
	unknownTop := bytes.Replace(baseJSON, []byte("{"), []byte(`{"unknown":true,`), 1)
	unknownNested := bytes.Replace(baseJSON, []byte(`"machine":{`),
		[]byte(`"machine":{"unknown":true,`), 1)
	duplicateTop := bytes.Replace(baseJSON, []byte(`"schema_version":3`),
		[]byte(`"schema_version":3,"schema_version":3`), 1)
	duplicateNested := bytes.Replace(baseJSON, []byte(`"origin_sequence":1`),
		[]byte(`"origin_sequence":1,"origin_sequence":1`), 1)
	nonUTCTime := bytes.Replace(baseJSON, []byte(`2026-08-03T08:00:00Z`),
		[]byte(`2026-08-03T16:00:00+08:00`), 1)

	cases := map[string][]byte{
		"unknown top-level field": unknownTop,
		"unknown nested field":    unknownNested,
		"duplicate top-level key": duplicateTop,
		"duplicate nested key":    duplicateNested,
		"leading whitespace":      append([]byte(" \n"), baseJSON...),
		"trailing value":          append(append([]byte(nil), baseJSON...), []byte(` {}`)...),
		"excess canonical bytes":  bytes.Repeat([]byte("x"), MaxEventCanonicalBytes+1),
		"noncanonical timestamp":  nonUTCTime,
		"wrong schema": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.SchemaVersion++
		}),
		"zero origin sequence": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.OriginSequence = 0
		}),
		"excess causal depth": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.CausalDepth = MaxPeerCausalDepth + 1
		}),
		"invalid semantic kind": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Semantic.Kind = ""
		}),
		"invalid semantic payload": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Semantic.Payload = string(bytes.Repeat([]byte("p"), MaxSemanticPayloadBytes+1))
		}),
		"unknown consequence": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Consequence = "handling.teleport"
		}),
		"zero subject fence": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Subject = &subjectBindingWire{HandlingID: "handling:subject",
				Head: eventRefWire{ID: "event:head", Digest: Sum([]byte("head")).String()}}
		}),
		"absent Reference with head": canonicalEventMutation(t, base, func(wire *eventWire) {
			head := eventRefWire{ID: "event:head", Digest: Sum([]byte("head")).String()}
			wire.Machine.ExpectedReference = &referenceExpectationWire{
				Absent: true, Key: "reference:new", Head: &head}
		}),
		"malformed target": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Targets[0].Destination = "somewhere"
		}),
		"duplicate target": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Targets = append(wire.Machine.Targets, wire.Machine.Targets[0])
		}),
		"excess targets": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Targets = make([]resolvedTargetWire, MaxSuccessors+1)
			for index := range wire.Machine.Targets {
				wire.Machine.Targets[index] = resolvedTargetWire{Destination: "local",
					LocalPrincipal: fmt.Sprintf("agent:target-%02d", index)}
			}
		}),
		"duplicate Artifact": canonicalEventMutation(t, base, func(wire *eventWire) {
			digest := Sum([]byte("artifact")).String()
			wire.Evidence.Artifacts = []string{digest, digest}
		}),
		"unsorted Artifacts": canonicalEventMutation(t, base, func(wire *eventWire) {
			left, right := Sum([]byte("left")).String(), Sum([]byte("right")).String()
			if left < right {
				wire.Evidence.Artifacts = []string{right, left}
			} else {
				wire.Evidence.Artifacts = []string{left, right}
			}
		}),
		"excess Artifacts": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Evidence.Artifacts = make([]string, MaxArtifactInputs+1)
			for index := range wire.Evidence.Artifacts {
				wire.Evidence.Artifacts[index] = Sum([]byte(fmt.Sprintf("artifact-%d", index))).String()
			}
		}),
		"duplicate causation": canonicalEventMutation(t, base, func(wire *eventWire) {
			ref := eventRefWire{ID: "event:cause", Digest: Sum([]byte("cause")).String()}
			wire.Evidence.Causation = []eventRefWire{ref, ref}
		}),
		"excess causation": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Evidence.Causation = make([]eventRefWire, MaxCausationHandles+1)
			for index := range wire.Evidence.Causation {
				wire.Evidence.Causation[index] = eventRefWire{
					ID:     fmt.Sprintf("event:cause-%02d", index),
					Digest: Sum([]byte(fmt.Sprintf("cause-%d", index))).String()}
			}
		}),
		"create without target": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Targets = nil
		}),
		"remote create without local anchor": canonicalEventMutation(t, base, func(wire *eventWire) {
			wire.Machine.Targets = []resolvedTargetWire{{Destination: "remote",
				RemoteRoute: "route:remote", RemoteAlias: "agent/remote"}}
		}),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseEventCanonicalJSON(data); err == nil {
				t.Fatalf("ParseEventCanonicalJSON(%s) unexpectedly succeeded", data)
			}
		})
	}
}

func FuzzParseEventCanonicalJSON(f *testing.F) {
	canonical, _, err := canonicalJSON(eventParserFixtureWire())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte(`{"schema_version":3,"schema_version":3}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		event, err := ParseEventCanonicalJSON(data)
		if err != nil {
			return
		}
		if !bytes.Equal(event.CanonicalJSON(), data) || event.Digest() != Sum(data) {
			t.Fatalf("successful parse was not exact canonical input: %s", data)
		}
		if _, err := ParseEventCanonicalJSON(event.CanonicalJSON()); err != nil {
			t.Fatalf("canonical Event reparse failed: %v", err)
		}
	})
}

func eventParserFixtureWire() eventWire {
	return eventWire{SchemaVersion: eventSchemaVersion,
		Machine: eventMachineWire{ID: "event:parse-fixture",
			AcceptedAt: testTime.Format(time.RFC3339Nano), OriginSequence: 1,
			Source: "agent:parse-fixture", OperationKey: "operation:parse-fixture",
			RequestDigest: Sum([]byte("request")).String(),
			Consequence:   ConsequenceCreateHandlings.String(),
			Targets: []resolvedTargetWire{{Destination: "local",
				LocalPrincipal: "agent:parse-fixture"}}},
		Semantic: eventSemanticWire{Kind: "work.request", Payload: "Inspect the bounded work."}}
}

func canonicalEventMutation(t *testing.T, original eventWire, mutate func(*eventWire)) []byte {
	t.Helper()
	var wire eventWire
	data := mustCanonicalEventWire(t, original)
	if err := decodeCanonicalObject("Event test fixture", data, MaxEventCanonicalBytes, &wire); err != nil {
		t.Fatal(err)
	}
	mutate(&wire)
	return mustCanonicalEventWire(t, wire)
}

func mustCanonicalEventWire(t *testing.T, wire eventWire) []byte {
	t.Helper()
	canonical, _, err := canonicalJSON(wire)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
