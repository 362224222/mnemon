package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type viewParserFixture struct {
	attachment Attachment
	authority  ViewAuthority
	public     AgentView
}

func newViewParserFixture(t *testing.T) viewParserFixture {
	t.Helper()
	principal := mustPrincipal(t, "agent:parse")
	attachment := mustAttachment(t, "attachment:parse", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	remoteRef := mustAliasTarget(t, "target:remote")
	remote, _ := ResolveRemoteTarget(remoteRef, mustRoute(t, "route:remote"), mustHandle(t, "peer:agent"))
	subject := mustHandle(t, "subject:current")
	currentArtifact := mustHandle(t, "artifact:current")
	referenceArtifact := mustHandle(t, "artifact:reference")
	activeHead := mustHandle(t, "reference:active")
	retractedHead := mustHandle(t, "reference:retracted")
	activeReference := mustReference(t, activeHead, "playbook-active", "event:active", "active")
	activeReference.outcomes = AgentViewTerminalOutcomes{Completed: 2, Unresolved: 1}
	authority := mustView(t, MachineViewSpec{
		Attachment: attachment,
		Consequences: []Consequence{
			ConsequenceCreateHandlings, ConsequenceAdvanceHandling, ConsequenceResolveCompleted,
			ConsequenceSupersedeReference, ConsequenceRetractReference,
		},
		Subjects: []SubjectBinding{mustSubject(t, subject, "handling:private", "event:head", "head", 7)},
		References: []ReferenceExpectation{
			activeReference,
			mustReference(t, retractedHead, "playbook-retracted", "event:retracted", "retracted"),
		},
		Targets:     []ResolvedTarget{remote, self},
		ReplyTarget: remoteRef,
		Artifacts: []ViewArtifactOffer{
			mustViewOffer(t, currentArtifact, "current bytes"),
			mustViewOffer(t, referenceArtifact, "reference bytes"),
		},
		Provenance: []ProvenanceOffer{
			mustProvenance(t, mustHandle(t, "cause:prior"), "event:prior", "prior"),
			mustProvenance(t, subject, "event:head", "head"),
		},
	})
	public, err := NewAgentView(AgentViewSpec{
		Handle: mustHandle(t, "view:public"), Authority: authority,
		Current: &AgentViewCurrentSpec{
			Subject: subject, ReplyTo: subject, Kind: mustLabel(t, "agent.work.continue"),
			Payload: mustPayload(t, "Continue from the bounded accepted state."), Artifacts: []OpaqueHandle{currentArtifact},
		},
		References: []AgentViewReferenceSpec{
			{Head: activeHead, State: AgentViewReferenceStateActive, Artifact: referenceArtifact,
				TerminalOutcomes: AgentViewTerminalOutcomes{Completed: 2, Unresolved: 1}},
			{Head: retractedHead, State: AgentViewReferenceStateRetracted},
		},
	})
	if err != nil {
		t.Fatalf("NewAgentView() error = %v", err)
	}
	return viewParserFixture{attachment: attachment, authority: authority, public: public}
}

func TestParseViewAuthorityCanonicalJSONRoundTripsWithAuthenticatedAttachment(t *testing.T) {
	fixture := newViewParserFixture(t)
	parsed, err := ParseViewAuthorityCanonicalJSON(fixture.authority.CanonicalJSON(), fixture.attachment)
	if err != nil {
		t.Fatalf("ParseViewAuthorityCanonicalJSON() error = %v", err)
	}
	if !bytes.Equal(parsed.CanonicalJSON(), fixture.authority.CanonicalJSON()) ||
		parsed.Digest() != fixture.authority.Digest() || parsed.Attachment() != fixture.attachment {
		t.Fatal("parsed private View differs from original")
	}

	rebound, err := NewAttachment(mustAttachmentID(t, "attachment:rebound"), fixture.attachment.Principal(),
		fixture.attachment.MayInitiate(), testTime.Add(time.Hour), testTime.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("NewAttachment(rebound) error = %v", err)
	}
	parsed, err = ParseViewAuthorityCanonicalJSON(fixture.authority.CanonicalJSON(), rebound)
	if err != nil || parsed.Attachment() != rebound || parsed.Digest() != fixture.authority.Digest() {
		t.Fatalf("rebound parse = %#v, %v", parsed, err)
	}
}

func TestParseViewAuthorityCanonicalJSONRejectsUnboundOrMalformedData(t *testing.T) {
	fixture := newViewParserFixture(t)
	canonical := fixture.authority.CanonicalJSON()
	wrongPrincipal := mustAttachment(t, "attachment:wrong", mustPrincipal(t, "agent:wrong"), true)
	wrongMode := mustAttachment(t, "attachment:wrong-mode", fixture.attachment.Principal(), false)
	for name, attachment := range map[string]Attachment{
		"wrong principal":       wrongPrincipal,
		"wrong initiation mode": wrongMode,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseViewAuthorityCanonicalJSON(canonical, attachment); !errors.Is(err, ErrInvariant) {
				t.Fatalf("error = %v, want ErrInvariant", err)
			}
		})
	}

	var wire machineViewWire
	if err := json.Unmarshal(canonical, &wire); err != nil {
		t.Fatal(err)
	}
	duplicateSubject := wire
	duplicateSubject.Subjects = append(duplicateSubject.Subjects, duplicateSubject.Subjects[0])
	duplicateBytes, _ := json.Marshal(duplicateSubject)
	malformedTarget := wire
	malformedTarget.Targets = append([]viewTargetWire(nil), wire.Targets...)
	malformedTarget.Targets[0].Resolved.LocalPrincipal = "agent:injected"
	malformedTargetBytes, _ := json.Marshal(malformedTarget)
	badDigest := wire
	badDigest.Artifacts = append([]viewArtifactOfferWire(nil), wire.Artifacts...)
	badDigest.Artifacts[0].Digest = "sha256:not-a-digest"
	badDigestBytes, _ := json.Marshal(badDigest)
	wrongReplyTarget := wire
	wrongReplyTarget.ReplyTarget = &targetWire{Self: true}
	wrongReplyTargetBytes, _ := json.Marshal(wrongReplyTarget)

	cases := map[string][]byte{
		"leading whitespace": append([]byte(" "), canonical...),
		"trailing value":     append(append([]byte(nil), canonical...), []byte("{}")...),
		"duplicate key": bytes.Replace(canonical, []byte(`"schema_version":3`),
			[]byte(`"schema_version":3,"schema_version":3`), 1),
		"unknown top field": bytes.Replace(canonical, []byte(`{"schema_version":3`),
			[]byte(`{"schema_version":3,"unknown":true`), 1),
		"unknown nested field": bytes.Replace(canonical, []byte(`"binding":{`),
			[]byte(`"binding":{"unknown":true,`), 1),
		"duplicate typed handle": duplicateBytes,
		"mixed target authority": malformedTargetBytes,
		"malformed digest":       badDigestBytes,
		"local reply target":     wrongReplyTargetBytes,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseViewAuthorityCanonicalJSON(data, fixture.attachment); err == nil {
				t.Fatalf("ParseViewAuthorityCanonicalJSON(%s) unexpectedly succeeded", data)
			}
		})
	}
}

func TestParseAgentViewCanonicalJSONRoundTripsAndBindsPrivateAuthority(t *testing.T) {
	fixture := newViewParserFixture(t)
	parsed, err := ParseAgentViewCanonicalJSON(fixture.public.CanonicalJSON(), fixture.authority)
	if err != nil {
		t.Fatalf("ParseAgentViewCanonicalJSON() error = %v", err)
	}
	if parsed.Handle() != fixture.public.Handle() || !bytes.Equal(parsed.CanonicalJSON(), fixture.public.CanonicalJSON()) {
		t.Fatal("parsed Agent View differs from original")
	}
	mutated := parsed.CanonicalJSON()
	mutated[0] = '['
	if parsed.CanonicalJSON()[0] != '{' {
		t.Fatal("parsed Agent View retained mutable caller bytes")
	}

	otherAttachment := mustAttachment(t, "attachment:other-view", fixture.attachment.Principal(), true)
	otherAuthority := mustView(t, MachineViewSpec{Attachment: otherAttachment,
		Consequences: []Consequence{ConsequenceCreateHandlings}})
	if _, err := ParseAgentViewCanonicalJSON(fixture.public.CanonicalJSON(), otherAuthority); err == nil {
		t.Fatal("public View unexpectedly parsed against different private authority")
	}
}

func TestParseAgentViewCanonicalJSONRejectsNoncanonicalAndDivergentProjection(t *testing.T) {
	fixture := newViewParserFixture(t)
	canonical := fixture.public.CanonicalJSON()
	var wire agentViewWire
	if err := json.Unmarshal(canonical, &wire); err != nil {
		t.Fatal(err)
	}

	wrongArtifact := wire
	wrongArtifact.Current = cloneAgentViewCurrent(wire.Current)
	wrongArtifact.Current.Facts.Artifacts[0].Digest = Sum([]byte("other bytes")).String()
	wrongArtifactBytes, _ := json.Marshal(wrongArtifact)
	wrongReference := wire
	wrongReference.References = append([]agentViewReferenceWire(nil), wire.References...)
	wrongReference.References[0].Facts.Key = "another-playbook"
	wrongReferenceBytes, _ := json.Marshal(wrongReference)
	wrongOutcome := wire
	wrongOutcome.References = cloneAgentViewReferences(wire.References)
	wrongOutcome.References[0].Facts.TerminalOutcomes.Completed++
	wrongOutcomeBytes, _ := json.Marshal(wrongOutcome)
	wrongShape := wire
	wrongShape.AllowedIntents = append([]agentViewIntentShapeWire(nil), wire.AllowedIntents...)
	wrongShape.AllowedIntents[0].Artifacts = "none"
	wrongShapeBytes, _ := json.Marshal(wrongShape)
	duplicateArtifact := wire
	duplicateArtifact.Current = cloneAgentViewCurrent(wire.Current)
	duplicateArtifact.Current.Facts.Artifacts = append(duplicateArtifact.Current.Facts.Artifacts,
		duplicateArtifact.Current.Facts.Artifacts[0])
	duplicateArtifactBytes, _ := json.Marshal(duplicateArtifact)
	wrongReplyTarget := wire
	wrongReplyTarget.Current = cloneAgentViewCurrent(wire.Current)
	wrongReplyTarget.Current.Facts.ReplyTarget = "self"
	wrongReplyTargetBytes, _ := json.Marshal(wrongReplyTarget)

	cases := map[string][]byte{
		"leading whitespace": append([]byte("\n"), canonical...),
		"trailing value":     append(append([]byte(nil), canonical...), []byte("null")...),
		"duplicate key": bytes.Replace(canonical, []byte(`"schema":"mnemon.agent.view"`),
			[]byte(`"schema":"mnemon.agent.view","schema":"mnemon.agent.view"`), 1),
		"private field": bytes.Replace(canonical, []byte(`{"schema":`),
			[]byte(`{"source_principal":"agent:injected","schema":`), 1),
		"unknown nested field": bytes.Replace(canonical, []byte(`"facts":{`),
			[]byte(`"facts":{"handling_id":"injected",`), 1),
		"artifact divergence":     wrongArtifactBytes,
		"Reference divergence":    wrongReferenceBytes,
		"outcome divergence":      wrongOutcomeBytes,
		"shape divergence":        wrongShapeBytes,
		"duplicate handle":        duplicateArtifactBytes,
		"reply target divergence": wrongReplyTargetBytes,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAgentViewCanonicalJSON(data, fixture.authority); err == nil {
				t.Fatalf("ParseAgentViewCanonicalJSON(%s) unexpectedly succeeded", data)
			}
		})
	}
}

func cloneAgentViewReferences(source []agentViewReferenceWire) []agentViewReferenceWire {
	clone := append([]agentViewReferenceWire(nil), source...)
	for index := range clone {
		if source[index].Facts.TerminalOutcomes == nil {
			continue
		}
		outcomes := *source[index].Facts.TerminalOutcomes
		clone[index].Facts.TerminalOutcomes = &outcomes
	}
	return clone
}

func cloneAgentViewCurrent(source *agentViewCurrentWire) *agentViewCurrentWire {
	clone := *source
	clone.Facts.Artifacts = append([]agentViewArtifactWire(nil), source.Facts.Artifacts...)
	return &clone
}

func TestViewParsersEnforceCanonicalByteBounds(t *testing.T) {
	fixture := newViewParserFixture(t)
	private := []byte(`{"schema_version":3,"source_principal":"agent:parse","may_initiate":true,"padding":"` +
		strings.Repeat("x", MaxViewCanonicalBytes) + `"}`)
	if _, err := ParseViewAuthorityCanonicalJSON(private, fixture.attachment); !errors.Is(err, ErrLimit) {
		t.Fatalf("private byte bound error = %v, want ErrLimit", err)
	}
	public := []byte(`{"schema":"mnemon.agent.view","version":4,"view":"view:public","padding":"` +
		strings.Repeat("x", MaxAgentViewCanonicalBytes) + `"}`)
	if _, err := ParseAgentViewCanonicalJSON(public, fixture.authority); !errors.Is(err, ErrLimit) {
		t.Fatalf("public byte bound error = %v, want ErrLimit", err)
	}
}

func FuzzParseViewAuthorityCanonicalJSON(f *testing.F) {
	attachment, authority, _ := minimalParserFixture()
	f.Add(authority.CanonicalJSON())
	f.Add([]byte(`{"schema_version":3}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		view, err := ParseViewAuthorityCanonicalJSON(data, attachment)
		if err != nil {
			return
		}
		if !bytes.Equal(view.CanonicalJSON(), data) || view.Digest() != Sum(data) || view.Attachment() != attachment {
			t.Fatal("successful private parse did not preserve exact authenticated value")
		}
		if _, err := ParseViewAuthorityCanonicalJSON(view.CanonicalJSON(), attachment); err != nil {
			t.Fatalf("reparse error = %v", err)
		}
	})
}

func FuzzParseAgentViewCanonicalJSON(f *testing.F) {
	_, authority, public := minimalParserFixture()
	f.Add(public.CanonicalJSON())
	f.Add([]byte(`{"schema":"mnemon.agent.view","version":4}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		view, err := ParseAgentViewCanonicalJSON(data, authority)
		if err != nil {
			return
		}
		if !bytes.Equal(view.CanonicalJSON(), data) || view.Handle().IsZero() {
			t.Fatal("successful public parse did not preserve exact safe value")
		}
		if _, err := ParseAgentViewCanonicalJSON(view.CanonicalJSON(), authority); err != nil {
			t.Fatalf("reparse error = %v", err)
		}
	})
}

func minimalParserFixture() (Attachment, ViewAuthority, AgentView) {
	principal, _ := NewAgentPrincipalID("agent:fuzz")
	attachmentID, _ := NewAttachmentID("attachment:fuzz")
	attachment, _ := NewAttachment(attachmentID, principal, true, testTime, testTime.Add(time.Minute))
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	authority, _ := NewViewAuthority(MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{self}})
	handle, _ := NewOpaqueHandle("view:fuzz")
	public, _ := NewAgentView(AgentViewSpec{Handle: handle, Authority: authority})
	return attachment, authority, public
}
