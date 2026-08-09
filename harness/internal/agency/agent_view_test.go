package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestAgentViewProjectsOnlyBoundedPublicWorld(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:public-view", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	aliasRef := mustAliasTarget(t, "target:assistant")
	alias, _ := ResolveLocalTarget(aliasRef, mustPrincipal(t, "agent:assistant"))
	currentHandle := mustHandle(t, "subject:current")
	currentArtifact := mustHandle(t, "artifact:current")
	activeArtifact := mustHandle(t, "artifact:active-reference")
	activeHead := mustHandle(t, "reference:active")
	retractedHead := mustHandle(t, "reference:retracted")

	authority := mustView(t, MachineViewSpec{
		Attachment: attachment,
		ReplyTo:    currentHandle,
		Consequences: []Consequence{
			ConsequenceCreateHandlings, ConsequenceAdvanceHandling,
			ConsequenceResolveCompleted, ConsequenceSupersedeReference, ConsequenceRetractReference,
		},
		Subjects: []SubjectBinding{
			mustSubject(t, currentHandle, "handling:private", "event:private-head", "private-head", 9),
		},
		References: []ReferenceExpectation{
			mustReference(t, activeHead, "knowledge-active", "event:active-head", "active-head"),
			mustReference(t, retractedHead, "knowledge-retracted", "event:retracted-head", "retracted-head"),
		},
		Targets: []ResolvedTarget{alias, self},
		Artifacts: []ViewArtifactOffer{
			mustViewOffer(t, activeArtifact, "active reference bytes"),
			mustViewOffer(t, currentArtifact, "current bytes"),
		},
		Provenance: []ProvenanceOffer{
			mustProvenance(t, mustHandle(t, "cause:prior"), "event:prior", "prior"),
			mustProvenance(t, currentHandle, "event:private-head", "private-head"),
		},
	})

	view, err := NewAgentView(AgentViewSpec{
		Handle:    mustHandle(t, "view:opaque"),
		Authority: authority,
		Current: &AgentViewCurrentSpec{
			Subject: currentHandle, ReplyTo: currentHandle, Kind: mustLabel(t, "custom.agent.signal"),
			Payload:   mustPayload(t, "Inspect the bounded material and choose one allowed effect."),
			Artifacts: []OpaqueHandle{currentArtifact},
		},
		References: []AgentViewReferenceSpec{
			{Head: retractedHead, State: AgentViewReferenceStateRetracted},
			{Head: activeHead, State: AgentViewReferenceStateActive, Artifact: activeArtifact},
		},
	})
	if err != nil {
		t.Fatalf("NewAgentView() error = %v", err)
	}
	assertAgentViewProjection(t, view, currentHandle)
	assertAgentViewHidesAuthority(t, view, authority, attachment, principal)

	first := view.CanonicalJSON()
	first[0] = '['
	if view.CanonicalJSON()[0] != '{' {
		t.Fatal("CanonicalJSON returned mutable backing storage")
	}
}

func assertAgentViewProjection(t *testing.T, view AgentView, currentHandle OpaqueHandle) {
	t.Helper()
	var wire agentViewWire
	if err := json.Unmarshal(view.CanonicalJSON(), &wire); err != nil {
		t.Fatalf("json.Unmarshal(AgentView) error = %v", err)
	}
	if wire.Schema != AgentViewSchema || wire.Version != AgentViewVersion || wire.View != "view:opaque" {
		t.Fatalf("Agent View envelope = %#v", wire)
	}
	if wire.Current == nil || wire.Current.Facts.Handle != currentHandle.String() ||
		wire.Current.Facts.ReplyRequired ||
		wire.Current.Semantic.Kind != "custom.agent.signal" || len(wire.Current.Facts.Artifacts) != 1 {
		t.Fatalf("current projection = %#v", wire.Current)
	}
	if len(wire.References) != 2 || wire.References[0].Facts.State != "active" ||
		wire.References[0].Facts.Artifact == nil || wire.References[1].Facts.State != "retracted" ||
		wire.References[1].Facts.Artifact != nil {
		t.Fatalf("Reference projections = %#v", wire.References)
	}
	if got := strings.Join(wire.Targets, ","); got != "self,target:assistant" {
		t.Fatalf("target aliases = %q", got)
	}
	if len(wire.AllowedIntents) != 5 || wire.AllowedIntents[0].Consequence > wire.AllowedIntents[1].Consequence {
		t.Fatalf("allowed Intent shapes = %#v", wire.AllowedIntents)
	}
	if got := strings.Join(wire.Provenance, ","); got != "cause:prior,subject:current" {
		t.Fatalf("provenance handles = %q", got)
	}
	assertNoPrivateViewKeys(t, view.CanonicalJSON())
}

func assertAgentViewHidesAuthority(t *testing.T, view AgentView, authority ViewAuthority,
	attachment Attachment, principal AgentPrincipalID,
) {
	t.Helper()
	for _, privateValue := range []string{
		principal.String(), attachment.ID().String(), "handling:private", "event:private-head",
		authority.Digest().String(),
	} {
		if bytes.Contains(view.CanonicalJSON(), []byte(privateValue)) {
			t.Fatalf("Agent View exposes private value %q: %s", privateValue, view.CanonicalJSON())
		}
	}
}

func TestAgentViewRejectsProjectionThatDivergesFromAuthority(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:projection-check", principal, true)
	currentHandle := mustHandle(t, "subject:current")
	artifactHandle := mustHandle(t, "artifact:offered")
	viewHandle := mustHandle(t, "view:projection-check")
	authority := mustView(t, MachineViewSpec{
		Attachment:   attachment,
		ReplyTo:      currentHandle,
		Consequences: []Consequence{ConsequenceAdvanceHandling},
		Subjects: []SubjectBinding{
			mustSubject(t, currentHandle, "handling:private", "event:head", "head", 3),
		},
		Artifacts: []ViewArtifactOffer{mustViewOffer(t, artifactHandle, "bytes")},
		Provenance: []ProvenanceOffer{mustProvenance(t, currentHandle,
			"event:head", "head")},
	})

	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: authority}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing current error = %v, want ErrInvariant", err)
	}
	current := &AgentViewCurrentSpec{Subject: currentHandle, ReplyTo: currentHandle,
		Kind: mustLabel(t, "custom.agent.signal")}
	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: authority,
		Current: current}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("hidden Artifact error = %v, want ErrInvariant", err)
	}
	current.Artifacts = []OpaqueHandle{artifactHandle}
	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: authority,
		Current: current}); err != nil {
		t.Fatalf("exact projection error = %v", err)
	}

	referenceHead := mustHandle(t, "reference:head")
	referenceAuthority := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceSupersedeReference},
		References: []ReferenceExpectation{
			mustReference(t, referenceHead, "knowledge-entry", "event:reference", "reference"),
		},
	})
	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: referenceAuthority}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing Reference error = %v, want ErrInvariant", err)
	}
	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: referenceAuthority,
		References: []AgentViewReferenceSpec{{Head: referenceHead, State: AgentViewReferenceStateActive}},
	}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("active Reference without Artifact error = %v, want ErrInvariant", err)
	}
	if _, err := NewAgentView(AgentViewSpec{Handle: viewHandle, Authority: referenceAuthority,
		References: []AgentViewReferenceSpec{{Head: referenceHead, State: AgentViewReferenceStateRetracted}},
	}); err != nil {
		t.Fatalf("retracted Reference projection error = %v", err)
	}
}

func TestAgentViewRejectsNegativeOrUnsealedReferenceOutcomes(t *testing.T) {
	principal := mustPrincipal(t, "agent:outcome-projection")
	attachment := mustAttachment(t, "attachment:outcome-projection", principal, true)
	head := mustHandle(t, "reference:outcome-head")
	reference := mustReference(t, head, "outcome-guide", "event:outcome-head", "outcome-head")
	reference.outcomes = AgentViewTerminalOutcomes{Completed: 1}
	authority := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceSupersedeReference},
		References:   []ReferenceExpectation{reference}})
	view := AgentViewSpec{Handle: mustHandle(t, "view:outcome-projection"), Authority: authority,
		References: []AgentViewReferenceSpec{{Head: head, State: AgentViewReferenceStateRetracted}}}
	if _, err := NewAgentView(view); !errors.Is(err, ErrInvariant) {
		t.Fatalf("unsealed outcome error = %v, want ErrInvariant", err)
	}
	view.References[0].TerminalOutcomes = AgentViewTerminalOutcomes{Completed: -1}
	if _, err := NewAgentView(view); err == nil {
		t.Fatal("negative Reference outcome unexpectedly projected")
	}
	view.References[0].TerminalOutcomes = AgentViewTerminalOutcomes{Completed: 1}
	if _, err := NewAgentView(view); err != nil {
		t.Fatalf("exact outcome projection error = %v", err)
	}
}

func TestAgentViewHasByteAndApproximateTokenRegressionBudget(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:budget", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	current := mustHandle(t, "subject:current")
	artifact := mustHandle(t, "artifact:current")
	authority := mustView(t, MachineViewSpec{
		Attachment: attachment,
		ReplyTo:    current,
		Consequences: []Consequence{
			ConsequenceCreateHandlings, ConsequenceAdvanceHandling, ConsequenceResolveCompleted,
			ConsequenceResolveDeclined, ConsequenceResolveUnresolved, ConsequencePublishReference,
		},
		Subjects: []SubjectBinding{mustSubject(t, current, "handling:private", "event:head", "head", 1)},
		Targets:  []ResolvedTarget{self}, Artifacts: []ViewArtifactOffer{mustViewOffer(t, artifact, "bytes")},
		Provenance: []ProvenanceOffer{mustProvenance(t, current, "event:head", "head")},
	})
	view, err := NewAgentView(AgentViewSpec{Handle: mustHandle(t, "view:budget"), Authority: authority,
		Current: &AgentViewCurrentSpec{Subject: current, ReplyTo: current, Kind: mustLabel(t, "custom.agent.signal"),
			Payload: mustPayload(t, strings.Repeat("bounded context. ", 32)), Artifacts: []OpaqueHandle{artifact}},
	})
	if err != nil {
		t.Fatalf("NewAgentView() error = %v", err)
	}
	bytesUsed := len(view.CanonicalJSON())
	approximateTokens := (bytesUsed + 3) / 4
	if bytesUsed > 4<<10 || approximateTokens > 1024 {
		t.Fatalf("representative View uses %d bytes (~%d tokens), want <=4096 bytes and <=1024 tokens",
			bytesUsed, approximateTokens)
	}
}

func TestAgentViewTerminalOutcomeProjectionHasConstantTokenBound(t *testing.T) {
	principal := mustPrincipal(t, "agent:outcome-budget")
	attachment := mustAttachment(t, "attachment:outcome-budget", principal, true)
	references := make([]ReferenceExpectation, 0, MaxAgentViewReferences)
	public := make([]AgentViewReferenceSpec, 0, MaxAgentViewReferences)
	maximum := AgentViewTerminalOutcomes{Completed: 1<<63 - 1, Declined: 1<<63 - 1,
		Unresolved: 1<<63 - 1}
	for index := 0; index < MaxAgentViewReferences; index++ {
		handle := mustHandle(t, fmt.Sprintf("reference:outcome:%02d", index))
		reference := mustReference(t, handle, fmt.Sprintf("guide-%02d", index),
			fmt.Sprintf("event:outcome:%02d", index), fmt.Sprintf("outcome-%02d", index))
		reference.outcomes = maximum
		references = append(references, reference)
		public = append(public, AgentViewReferenceSpec{Head: handle,
			State: AgentViewReferenceStateRetracted, TerminalOutcomes: maximum})
	}
	authority := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceSupersedeReference}, References: references})
	view, err := NewAgentView(AgentViewSpec{Handle: mustHandle(t, "view:outcome-budget"),
		Authority: authority, References: public})
	if err != nil {
		t.Fatal(err)
	}
	bytesUsed := len(view.CanonicalJSON())
	if bytesUsed > 4<<10 || bytesUsed > MaxAgentViewCanonicalBytes {
		t.Fatalf("maximum outcome projection uses %d bytes, want <=4096", bytesUsed)
	}
}

func TestAgentViewMaximumReferenceAndPayloadShapeRemainsReadable(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:large-public-view", principal, true)
	current := mustHandle(t, "subject:current")
	artifacts := make([]ViewArtifactOffer, 0, MaxAgentViewReferences+2*MaxArtifactInputs)
	references := make([]ReferenceExpectation, 0, MaxAgentViewReferences)
	publicReferences := make([]AgentViewReferenceSpec, 0, MaxAgentViewReferences)
	for index := 0; index < MaxAgentViewReferences; index++ {
		head := mustHandle(t, fmt.Sprintf("reference:%02d:%s", index, strings.Repeat("h", 120)))
		artifact := mustHandle(t, fmt.Sprintf("artifact:%02d:%s", index, strings.Repeat("a", 120)))
		artifacts = append(artifacts, mustViewOffer(t, artifact, fmt.Sprintf("artifact-%d", index)))
		references = append(references, mustReference(t, head,
			fmt.Sprintf("knowledge-%02d-%s", index, strings.Repeat("k", 120)),
			fmt.Sprintf("event:reference:%02d", index), fmt.Sprintf("reference-%d", index)))
		publicReferences = append(publicReferences, AgentViewReferenceSpec{
			Head: head, State: AgentViewReferenceStateActive, Artifact: artifact,
		})
	}
	currentArtifacts := make([]OpaqueHandle, 0, MaxArtifactInputs)
	relatedArtifacts := make([]OpaqueHandle, 0, MaxArtifactInputs)
	for index := 0; index < MaxArtifactInputs; index++ {
		currentArtifact := mustHandle(t, fmt.Sprintf("artifact:current:%02d", index))
		relatedArtifact := mustHandle(t, fmt.Sprintf("artifact:related:%02d", index))
		artifacts = append(artifacts, mustViewOffer(t, currentArtifact, fmt.Sprintf("current-%d", index)),
			mustViewOffer(t, relatedArtifact, fmt.Sprintf("related-%d", index)))
		currentArtifacts = append(currentArtifacts, currentArtifact)
		relatedArtifacts = append(relatedArtifacts, relatedArtifact)
	}
	relatedEvent := mustHandle(t, "related:maximum")
	self, err := ResolveLocalTarget(SelfTarget(), principal)
	if err != nil {
		t.Fatal(err)
	}
	targets := []ResolvedTarget{self}
	var replyTarget TargetRef
	for index := 0; index < 8; index++ { // R7 freezes eight active Peer routes.
		requested := mustAliasTarget(t, longToken("target", index, MaxOpaqueHandleBytes))
		if index == 0 {
			replyTarget = requested
		}
		remote := mustHandle(t, longToken("remote", index, MaxOpaqueHandleBytes))
		resolved, err := ResolveRemoteTarget(requested,
			mustRoute(t, longToken("route", index, MaxOpaqueHandleBytes)), remote)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, resolved)
	}
	authority := mustView(t, MachineViewSpec{
		Attachment: attachment, ReplyTo: current, ReplyTarget: replyTarget,
		ReplyDelivery: mustDeliveryID(t, "delivery:maximum-view"),
		Consequences:  []Consequence{ConsequenceAdvanceHandling},
		Subjects:      []SubjectBinding{mustSubject(t, current, "handling:private", "event:head", "head", 1)},
		References:    references, Artifacts: artifacts, Targets: targets,
		Provenance: []ProvenanceOffer{mustProvenance(t, current, "event:head", "head"),
			mustProvenance(t, relatedEvent, "event:related", "related")},
	})
	view, err := NewAgentView(AgentViewSpec{
		Handle: mustHandle(t, "view:large"), Authority: authority,
		Current: &AgentViewCurrentSpec{Subject: current, ReplyTo: current, Kind: mustLabel(t, "custom.agent.signal"),
			Payload:   mustPayload(t, strings.Repeat("p", MaxSemanticPayloadBytes)),
			Artifacts: currentArtifacts},
		Related: []AgentViewRelatedSpec{{Event: relatedEvent,
			Relation: AgentViewRelationCorrelation, Kind: mustLabel(t, "custom.agent.related"),
			Payload: mustPayload(t, ""), Artifacts: relatedArtifacts}},
		Outstanding: AgentViewOutstanding{OpenTotal: 2, RelatedTotal: 1,
			RelatedProjected: 1},
		References: publicReferences,
	})
	if err != nil {
		t.Fatalf("maximum accepted public View became unreadable: %v", err)
	}
	if size := len(view.CanonicalJSON()); size > MaxAgentViewCanonicalBytes {
		t.Fatalf("maximum accepted public View bytes = %d, want <= %d",
			size, MaxAgentViewCanonicalBytes)
	}
}

func TestAgentViewFailsClosedAboveCanonicalByteLimit(t *testing.T) {
	principal := mustPrincipal(t, "agent:view-last-line-bound")
	attachment := mustAttachment(t, "attachment:view-last-line-bound", principal, true)
	current := mustHandle(t, "subject:view-last-line-bound")
	authority := mustView(t, MachineViewSpec{Attachment: attachment,
		ReplyTo:      current,
		Consequences: []Consequence{ConsequenceAdvanceHandling},
		Subjects: []SubjectBinding{mustSubject(t, current, "handling:last-line",
			"event:last-line", "last-line", 1)},
		Provenance: []ProvenanceOffer{mustProvenance(t, current,
			"event:last-line", "last-line")}})
	// SemanticPayload values are normally constructor-validated. An internal
	// invariant violation must still be caught by the final canonical envelope
	// bound rather than escaping as an oversized projection.
	invalidPayload := SemanticPayload{value: strings.Repeat("p", MaxAgentViewCanonicalBytes)}
	_, err := NewAgentView(AgentViewSpec{Handle: mustHandle(t, "view:last-line-bound"),
		Authority: authority, Current: &AgentViewCurrentSpec{Subject: current,
			ReplyTo: current, Kind: mustLabel(t, "custom.agent.signal"), Payload: invalidPayload}})
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized Agent View error = %v, want ErrLimit", err)
	}
}

func assertNoPrivateViewKeys(t *testing.T, data []byte) {
	t.Helper()
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	forbidden := map[string]bool{
		"principal": true, "source_principal": true, "attachment": true, "attachment_id": true,
		"operation_key": true, "handling_id": true, "event_id": true, "fence": true,
		"route": true, "peer_color": true, "transcript": true, "credential": true,
		"view_digest": true, "read_set": true,
	}
	var visit func(any)
	visit = func(item any) {
		switch typed := item.(type) {
		case map[string]any:
			for key, child := range typed {
				if forbidden[key] {
					t.Fatalf("Agent View exposes private field %q: %s", key, data)
				}
				visit(child)
			}
		case []any:
			for _, child := range typed {
				visit(child)
			}
		}
	}
	visit(value)
}
