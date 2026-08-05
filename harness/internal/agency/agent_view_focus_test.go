package agency

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestAgentViewProjectsRelatedEvidenceWithoutWritableSubjectAuthority(t *testing.T) {
	principal := mustPrincipal(t, "agent:focus")
	attachment := mustAttachment(t, "attachment:focus", principal, true)
	current := mustHandle(t, "subject:focus-current")
	related := mustHandle(t, "related:focus-result")
	artifact := mustHandle(t, "artifact:focus-result")
	authority := mustView(t, MachineViewSpec{
		Attachment:   attachment,
		Consequences: []Consequence{ConsequenceAdvanceHandling},
		Subjects: []SubjectBinding{
			mustSubject(t, current, "handling:focus", "event:focus-root", "focus-root", 3),
		},
		Artifacts: []ViewArtifactOffer{mustViewOffer(t, artifact, "review result")},
		Provenance: []ProvenanceOffer{
			mustProvenance(t, current, "event:focus-root", "focus-root"),
			mustProvenance(t, related, "event:focus-result", "focus-result"),
		},
	})
	view, err := NewAgentView(AgentViewSpec{
		Handle: mustHandle(t, "view:focus"), Authority: authority,
		Current: &AgentViewCurrentSpec{Subject: current, ReplyTo: current, Kind: mustLabel(t, "work.request"),
			Payload: mustPayload(t, "review this change")},
		Related: []AgentViewRelatedSpec{{Event: related, Relation: AgentViewRelationCorrelation,
			Kind: mustLabel(t, "review.response"), Payload: mustPayload(t, "accepted with evidence"),
			Artifacts: []OpaqueHandle{artifact}}},
		Outstanding: AgentViewOutstanding{OpenTotal: 2, RelatedTotal: 1, RelatedProjected: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire agentViewWire
	if err := json.Unmarshal(view.CanonicalJSON(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Current == nil || wire.Current.Facts.ReplyTo != current.String() ||
		len(wire.RelatedOpen) != 1 || wire.RelatedOpen[0].Facts.Event != related.String() ||
		wire.RelatedOpen[0].Facts.Relation != "correlation" ||
		wire.RelatedOpen[0].Semantic.Payload != "accepted with evidence" ||
		wire.Outstanding.OpenTotal != 2 || wire.Outstanding.RelatedProjected != 1 ||
		wire.Outstanding.Truncated {
		t.Fatalf("focus projection = %#v", wire)
	}
	if _, err := ParseAgentViewCanonicalJSON(view.CanonicalJSON(), authority); err != nil {
		t.Fatalf("ParseAgentViewCanonicalJSON() error = %v", err)
	}
	cite, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "work.progress"),
		Consequence: ConsequenceAdvanceHandling, SubjectHandling: current,
		CausationHandles: []OpaqueHandle{related}})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindIntent(BoundIntentSpec{Intent: cite,
		OperationKey: mustOperation(t, "operation:focus-cite"), View: authority})
	if err != nil || len(bound.Causation()) != 1 || bound.Causation()[0].IsZero() {
		t.Fatalf("related provenance citation = %#v, %v", bound.Causation(), err)
	}

	intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "review.illegal-progress"),
		Consequence: ConsequenceAdvanceHandling, SubjectHandling: related})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "operation:focus-illegal"), View: authority}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("related Event as subject error = %v, want ErrInvariant", err)
	}
}

func TestAgentViewAlwaysProjectsExplicitZeroReferenceOutcomes(t *testing.T) {
	principal := mustPrincipal(t, "agent:zero-outcomes")
	attachment := mustAttachment(t, "attachment:zero-outcomes", principal, true)
	head := mustHandle(t, "reference:zero-outcomes")
	authority := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceSupersedeReference},
		References: []ReferenceExpectation{
			mustReference(t, head, "guide-zero", "event:guide-zero", "guide-zero"),
		}})
	view, err := NewAgentView(AgentViewSpec{Handle: mustHandle(t, "view:zero-outcomes"),
		Authority: authority, References: []AgentViewReferenceSpec{{
			Head: head, State: AgentViewReferenceStateRetracted,
		}}})
	if err != nil {
		t.Fatal(err)
	}
	var wire agentViewWire
	if err := json.Unmarshal(view.CanonicalJSON(), &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.References) != 1 || wire.References[0].Facts.TerminalOutcomes == nil ||
		*wire.References[0].Facts.TerminalOutcomes != (agentViewTerminalOutcomesWire{}) {
		t.Fatalf("zero terminal outcomes were not explicit: %#v", wire.References)
	}
}

func TestAgentViewRejectsDivergentOutstandingProjection(t *testing.T) {
	principal := mustPrincipal(t, "agent:focus-counts")
	attachment := mustAttachment(t, "attachment:focus-counts", principal, true)
	view := AgentViewSpec{Handle: mustHandle(t, "view:focus-counts"),
		Authority:   mustView(t, MachineViewSpec{Attachment: attachment}),
		Outstanding: AgentViewOutstanding{OpenTotal: 1, RelatedTotal: 1, RelatedProjected: 0}}
	if _, err := NewAgentView(view); !errors.Is(err, ErrInvariant) {
		t.Fatalf("divergent outstanding error = %v, want ErrInvariant", err)
	}
}
