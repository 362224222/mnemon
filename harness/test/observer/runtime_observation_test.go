package observer

import (
	"slices"
	"testing"
)

func factEvidenceInput(fact factRecord) Fact {
	return Fact{Kind: fact.Kind, Causes: slices.Clone(fact.Causes), References: References{
		Event: fact.Refs.Event, EventDigest: fact.Refs.EventDigest, Handling: fact.Refs.Handling,
	}, Fields: FactFields{
		SemanticKind: fact.Facts.SemanticKind, Consequence: fact.Facts.Consequence,
		Outcome: fact.Facts.Outcome, State: fact.Facts.State, Phase: fact.Facts.Phase,
		Action: fact.Facts.Action, Code: fact.Facts.Code, Count: fact.Facts.Count,
		AttemptCount: fact.Facts.AttemptCount, SuccessCount: fact.Facts.SuccessCount,
		ToolErrorCount: fact.Facts.ToolErrorCount, InvalidCount: fact.Facts.InvalidCount,
		BatchedCount: fact.Facts.BatchedCount, PreferenceBefore: fact.Facts.PreferenceBefore,
		PreferenceAfter: fact.Facts.PreferenceAfter, Result: fact.Facts.Result,
		Round: fact.Facts.Round, SampleSize: fact.Facts.SampleSize, Alpha: fact.Facts.Alpha,
		VotesA: fact.Facts.VotesA, VotesB: fact.Facts.VotesB,
		MarginBefore: fact.Facts.MarginBefore, MarginAfter: fact.Facts.MarginAfter,
		Authenticated: fact.Facts.Authenticated, Recolored: fact.Facts.Recolored,
		HasCurrent: fact.Facts.HasCurrent, ReplyRequired: fact.Facts.ReplyRequired,
		OpenTotal: fact.Facts.OpenTotal, RelatedTotal: fact.Facts.RelatedTotal,
		RelatedProjected: fact.Facts.RelatedProjected, Truncated: fact.Facts.Truncated,
	}}
}

func TestRuntimeObservationEvidenceIsClosed(t *testing.T) {
	attempts, successes, toolErrors, invalidResults, batched, count := 2, 1, 1, 0, 0, 1
	domain := Fact{Kind: "runtime.domain.operation", Fields: FactFields{
		Action: "mutation", AttemptCount: &attempts, SuccessCount: &successes,
		ToolErrorCount: &toolErrors, InvalidCount: &invalidResults, BatchedCount: &batched,
	}}
	if err := validateKindEvidence(domain, 1); err != nil {
		t.Fatalf("valid domain observation: %v", err)
	}
	domain.Causes = []string{"trace:not-allowed"}
	if err := validateKindEvidence(domain, 1); err == nil {
		t.Fatal("domain observation accepted a causal edge")
	}
	domain.Causes = nil
	domain.Fields.ToolErrorCount = &attempts
	if err := validateKindEvidence(domain, 1); err == nil {
		t.Fatal("domain observation accepted unbalanced outcome classes")
	}

	denial := Fact{Kind: "runtime.intent.denied", Fields: FactFields{
		Action: "submit", Code: "authentication_failed", Count: &count,
	}}
	if err := validateKindEvidence(denial, 2); err != nil {
		t.Fatalf("valid Intent denial: %v", err)
	}
	denial.Fields.Code = "provider-prose"
	if err := validateKindEvidence(denial, 2); err == nil {
		t.Fatal("Intent denial accepted an open diagnostic class")
	}
}

func TestRuntimeViewEvidenceRequiresConsistentStructuralMetadata(t *testing.T) {
	hasCurrent, replyRequired, truncated := true, true, true
	openTotal, relatedTotal, relatedProjected := 1, 65, 1
	view := Fact{Kind: "runtime.view.received", Fields: FactFields{
		Action: "current", HasCurrent: &hasCurrent, ReplyRequired: &replyRequired,
		OpenTotal: &openTotal, RelatedTotal: &relatedTotal,
		RelatedProjected: &relatedProjected, Truncated: &truncated,
	}}
	if err := validateKindEvidence(view, 1); err != nil {
		t.Fatalf("valid Agent View structure: %v", err)
	}
	view.Fields.ReplyRequired = nil
	if err := validateKindEvidence(view, 1); err == nil {
		t.Fatal("current Agent View omitted reply-required structure")
	}
	view.Fields.ReplyRequired = &replyRequired
	*view.Fields.RelatedProjected = 2
	if err := validateKindEvidence(view, 1); err == nil {
		t.Fatal("Agent View exceeded the related projection bound")
	}
}

func TestFactEvidenceInputCarriesClosedDomainOutcomes(t *testing.T) {
	attempts, successes, toolErrors, invalid, batched := 4, 1, 1, 1, 1
	fact := factRecord{Sequence: 1, Kind: "runtime.domain.operation", Facts: factsWire{
		Action: "read", AttemptCount: &attempts, SuccessCount: &successes,
		ToolErrorCount: &toolErrors, InvalidCount: &invalid, BatchedCount: &batched,
	}}
	projected := factEvidenceInput(fact)
	if projected.Fields.ToolErrorCount == nil || *projected.Fields.ToolErrorCount != 1 ||
		projected.Fields.InvalidCount == nil || *projected.Fields.InvalidCount != 1 ||
		projected.Fields.BatchedCount == nil || *projected.Fields.BatchedCount != 1 {
		t.Fatalf("domain outcome mapping = %#v", projected.Fields)
	}
	if err := validateKindEvidence(projected, fact.Sequence); err != nil {
		t.Fatalf("mapped domain observation: %v", err)
	}
}
