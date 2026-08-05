package agency

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestParseAgentIntentJSONRoundTripsCanonicalAgentWire(t *testing.T) {
	alias := mustAliasTarget(t, "target:assistant")
	intent, err := NewAgentIntent(IntentSpec{
		Kind: mustLabel(t, "custom.agent.signal"), Payload: mustPayload(t, "Bounded semantic content."),
		Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget(), alias},
		Artifacts: []ArtifactInput{
			mustCandidate(t, "candidate:output"), mustViewArtifact(t, "artifact:offered"),
		},
		CausationHandles:  []OpaqueHandle{mustHandle(t, "cause:one"), mustHandle(t, "cause:two")},
		CorrelationHandle: mustHandle(t, "correlation:one"),
	})
	if err != nil {
		t.Fatalf("NewAgentIntent() error = %v", err)
	}
	parsed, err := ParseAgentIntentJSON(intent.CanonicalJSON())
	if err != nil {
		t.Fatalf("ParseAgentIntentJSON() error = %v", err)
	}
	if !bytes.Equal(parsed.CanonicalJSON(), intent.CanonicalJSON()) {
		t.Fatalf("canonical round trip differs\n got: %s\nwant: %s", parsed.CanonicalJSON(), intent.CanonicalJSON())
	}

	formatted := append([]byte(" \n"), intent.CanonicalJSON()...)
	formatted = append(formatted, '\n')
	parsed, err = ParseAgentIntentJSON(formatted)
	if err != nil || !bytes.Equal(parsed.CanonicalJSON(), intent.CanonicalJSON()) {
		t.Fatalf("formatted round trip = %s, %v", parsed.CanonicalJSON(), err)
	}
}

func TestParseAgentIntentJSONRejectsAuthorityAndMalformedShapes(t *testing.T) {
	canonical := mustRootIntent(t, []TargetRef{SelfTarget()}).CanonicalJSON()
	withMachineField := func(field string) []byte {
		return bytes.Replace(canonical, []byte("{"), []byte(`{"`+field+`":"forged",`), 1)
	}
	invalid := map[string][]byte{
		"machine source":         withMachineField("source_principal"),
		"operation key":          withMachineField("operation_key"),
		"private digest":         withMachineField("view_digest"),
		"unknown field":          withMachineField("future_field"),
		"wrong field case":       bytes.Replace(canonical, []byte(`"kind"`), []byte(`"Kind"`), 1),
		"missing payload":        bytes.Replace(canonical, []byte(`"payload":"Continue the accepted responsibility.",`), nil, 1),
		"trailing value":         append(append([]byte(nil), canonical...), []byte(` {}`)...),
		"duplicate key":          []byte(`{"kind":"a","kind":"b","payload":"","consequence":"handling.create","successors":[{"self":true}]}`),
		"unknown consequence":    bytes.Replace(canonical, []byte("handling.create"), []byte("machine.override"), 1),
		"mixed target":           []byte(`{"kind":"custom.agent.signal","payload":"","consequence":"handling.create","successors":[{"self":true,"alias":"target:other"}]}`),
		"unknown target field":   []byte(`{"kind":"custom.agent.signal","payload":"","consequence":"handling.create","successors":[{"self":true,"route":"private"}]}`),
		"invalid target handle":  []byte(`{"kind":"custom.agent.signal","payload":"","consequence":"handling.create","successors":[{"alias":" target"}]}`),
		"unknown Artifact kind":  []byte(`{"kind":"custom.agent.signal","payload":"","consequence":"handling.create","successors":[{"self":true}],"artifacts":[{"kind":"authority","handle":"artifact:one"}]}`),
		"machine Artifact field": []byte(`{"kind":"custom.agent.signal","payload":"","consequence":"handling.create","successors":[{"self":true}],"artifacts":[{"kind":"candidate","handle":"artifact:one","digest":"sha256:forged"}]}`),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAgentIntentJSON(data); err == nil {
				t.Fatalf("ParseAgentIntentJSON(%s) unexpectedly succeeded", data)
			}
		})
	}
}

func TestParseAgentIntentJSONEnforcesInputByteLimit(t *testing.T) {
	data := []byte(`{"kind":"custom.agent.signal","payload":"` +
		strings.Repeat("p", MaxIntentCanonicalBytes) + `","consequence":"handling.create"}`)
	if _, err := ParseAgentIntentJSON(data); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized Intent JSON error = %v, want ErrLimit", err)
	}
}
