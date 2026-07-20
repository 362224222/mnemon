package model

import (
	"errors"
	"reflect"
	"testing"
)

func TestTeamworkActionPolicyFiltersContextsInCanonicalOrder(t *testing.T) {
	t.Parallel()
	policy := testTeamworkActionPolicy(t)
	if policy.AssetRevision() != Sum([]byte("Teamwork Action policy")) ||
		len(policy.Entries()) != TeamworkActionCount {
		t.Fatalf("policy identity = (%s, %d)", policy.AssetRevision(), len(policy.Entries()))
	}
	tests := []struct {
		name     string
		contexts []TeamworkActionContext
		want     []OperationKind
	}{
		{name: "empty", want: []OperationKind{}},
		{name: "contextless initiation", contexts: []TeamworkActionContext{TeamworkActionContextNone},
			want: []OperationKind{OperationTeamworkOffer}},
		{name: "reviewer offered", contexts: []TeamworkActionContext{TeamworkActionContextReviewerOffered},
			want: []OperationKind{OperationTeamworkAccept, OperationTeamworkDecline}},
		{name: "reviewer active", contexts: []TeamworkActionContext{TeamworkActionContextReviewerActive},
			want: []OperationKind{OperationTeamworkOffer, OperationTeamworkDeliver}},
		{name: "overlapping reviewer contexts deduplicate actions", contexts: []TeamworkActionContext{
			TeamworkActionContextReviewerRework, TeamworkActionContextReviewerActive},
			want: []OperationKind{OperationTeamworkOffer, OperationTeamworkDeliver}},
		{name: "home delivered iteration one", contexts: []TeamworkActionContext{
			TeamworkActionContextHomeNonterminal, TeamworkActionContextHomeDelivered,
			TeamworkActionContextHomeDeliveredIteration1},
			want: []OperationKind{OperationTeamworkRework, OperationTeamworkClose, OperationTeamworkCancel}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := policy.ActionsForContexts(test.contexts)
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ActionsForContexts() = (%v, %v), want %v", got, err, test.want)
			}
		})
	}
	entry, ok := policy.Operation(OperationTeamworkDeliver)
	if !ok || entry.Ordinal() != 3 || entry.EventType() != EventReviewDeliveryReady ||
		entry.MaxResults() != 1 || !entry.AllowsContext(TeamworkActionContextParentResume) {
		t.Fatalf("deliver entry = %#v, present=%t", entry, ok)
	}
}

func TestTeamworkActionPolicyIsImmutableAndZeroSafe(t *testing.T) {
	t.Parallel()
	entries := testTeamworkActionPolicySpecs()
	policy, err := NewTeamworkActionPolicy(TeamworkActionPolicySpec{
		AssetRevision: Sum([]byte("Teamwork Action policy")), Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	entries[0].AllowedContexts[0] = TeamworkActionContextHomeNonterminal
	returned := policy.Entries()
	returned[0] = TeamworkActionPolicyEntry{}
	contexts := policy.Entries()[0].AllowedContexts()
	contexts[0] = TeamworkActionContextHomeNonterminal
	offer, ok := policy.Operation(OperationTeamworkOffer)
	if !ok || offer.AllowedContexts()[0] != TeamworkActionContextNone {
		t.Fatal("policy retained caller-owned context or entry storage")
	}

	var zero TeamworkActionPolicy
	if !zero.AssetRevision().IsZero() || zero.Entries() != nil {
		t.Fatalf("zero policy = %#v", zero)
	}
	if _, ok := zero.Operation(OperationTeamworkOffer); ok {
		t.Fatal("zero policy resolved an operation")
	}
	if _, err := zero.ActionsForContexts(nil); !errors.Is(err, ErrInvariant) {
		t.Fatalf("zero ActionsForContexts() error = %v", err)
	}
	var zeroEntry TeamworkActionPolicyEntry
	if zeroEntry.AllowedContexts() != nil || zeroEntry.AllowsContext(TeamworkActionContextNone) {
		t.Fatalf("zero entry = %#v", zeroEntry)
	}
}

func TestTeamworkActionPolicyRejectsIncompleteOrAmbiguousProjection(t *testing.T) {
	t.Parallel()
	assertInvalidTeamworkActionPolicy(t, "zero revision",
		func(spec *TeamworkActionPolicySpec) { spec.AssetRevision = Digest{} })
	assertInvalidTeamworkActionPolicy(t, "missing entry",
		func(spec *TeamworkActionPolicySpec) { spec.Entries = spec.Entries[:6] })
	assertInvalidTeamworkActionPolicy(t, "extra entry", func(spec *TeamworkActionPolicySpec) {
		spec.Entries = append(spec.Entries, spec.Entries[0])
	})
	assertInvalidTeamworkActionPolicy(t, "ordinal gap",
		func(spec *TeamworkActionPolicySpec) { spec.Entries[1].Ordinal = 2 })
	assertInvalidTeamworkActionPolicy(t, "reordered ordinals", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0], spec.Entries[1] = spec.Entries[1], spec.Entries[0]
	})
	assertInvalidTeamworkActionPolicy(t, "unknown operation", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].OperationKind = OperationKind("teamwork.unknown")
	})
	assertInvalidTeamworkActionPolicy(t, "resolve operation", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].OperationKind = OperationResolveRetry
	})
	assertInvalidTeamworkActionPolicy(t, "duplicate operation", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[1].OperationKind = spec.Entries[0].OperationKind
	})
	assertInvalidTeamworkActionPolicy(t, "unknown Event", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].EventType = EventType("review.unknown")
	})
	assertInvalidTeamworkActionPolicy(t, "controller Event", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].EventType = EventReviewAccepted
	})
	assertInvalidTeamworkActionPolicy(t, "duplicate Event", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[1].EventType = spec.Entries[0].EventType
	})
	assertInvalidTeamworkActionPolicy(t, "empty contexts", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].AllowedContexts = nil
	})
	assertInvalidTeamworkActionPolicy(t, "unknown context", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].AllowedContexts[0] = TeamworkActionContext("unknown")
	})
	assertInvalidTeamworkActionPolicy(t, "duplicate context", func(spec *TeamworkActionPolicySpec) {
		spec.Entries[0].AllowedContexts = append(spec.Entries[0].AllowedContexts,
			spec.Entries[0].AllowedContexts[0])
	})
	assertInvalidTeamworkActionPolicy(t, "zero results",
		func(spec *TeamworkActionPolicySpec) { spec.Entries[0].MaxResults = 0 })
	assertInvalidTeamworkActionPolicy(t, "excess results",
		func(spec *TeamworkActionPolicySpec) { spec.Entries[0].MaxResults = MaxChildWorks + 1 })

	policy := testTeamworkActionPolicy(t)
	if _, err := policy.ActionsForContexts([]TeamworkActionContext{
		TeamworkActionContextReviewerOffered, TeamworkActionContextReviewerOffered,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate filter context error = %v", err)
	}
	if _, err := policy.ActionsForContexts([]TeamworkActionContext{"unknown"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown filter context error = %v", err)
	}
	if _, err := policy.ActionsForContexts([]TeamworkActionContext{
		TeamworkActionContextNone, TeamworkActionContextReviewerOffered,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mixed contextless filter error = %v", err)
	}
}

func TestTeamworkActionPolicyPreservesJoinedEventBindingAndAssetOrdinal(t *testing.T) {
	t.Parallel()
	specs := testTeamworkActionPolicySpecs()
	specs[1], specs[2] = specs[2], specs[1]
	specs[1].Ordinal, specs[2].Ordinal = 1, 2
	policy, err := NewTeamworkActionPolicy(TeamworkActionPolicySpec{
		AssetRevision: Sum([]byte("permuted Action ordinals")), Entries: specs})
	if err != nil {
		t.Fatal(err)
	}
	actions, err := policy.ActionsForContexts([]TeamworkActionContext{TeamworkActionContextReviewerOffered})
	if err != nil || !reflect.DeepEqual(actions,
		[]OperationKind{OperationTeamworkDecline, OperationTeamworkAccept}) {
		t.Fatalf("permuted ActionsForContexts() = (%v, %v)", actions, err)
	}
	decline, declineOK := policy.Operation(OperationTeamworkDecline)
	accept, acceptOK := policy.Operation(OperationTeamworkAccept)
	if !declineOK || !acceptOK || decline.EventType() != EventReviewDeclineRequested ||
		accept.EventType() != EventReviewAcceptRequested {
		t.Fatalf("derived Event bindings = (%#v, %#v)", decline, accept)
	}
}

func TestTeamworkActionPolicyPreservesAssetOwnedContextsAndResultBounds(t *testing.T) {
	t.Parallel()
	specs := testTeamworkActionPolicySpecs()
	specs[0].MaxResults = 2
	specs[1].AllowedContexts = []TeamworkActionContext{TeamworkActionContextHomeDelivered}
	policy, err := NewTeamworkActionPolicy(TeamworkActionPolicySpec{
		AssetRevision: Sum([]byte("asset-owned Action policy")), Entries: specs})
	if err != nil {
		t.Fatal(err)
	}
	offer, ok := policy.Operation(OperationTeamworkOffer)
	if !ok || offer.MaxResults() != 2 {
		t.Fatalf("offer entry = %#v, present=%t", offer, ok)
	}
	actions, err := policy.ActionsForContexts([]TeamworkActionContext{TeamworkActionContextHomeDelivered})
	if err != nil || !reflect.DeepEqual(actions,
		[]OperationKind{OperationTeamworkAccept, OperationTeamworkClose}) {
		t.Fatalf("ActionsForContexts() = (%v, %v)", actions, err)
	}
}

func assertInvalidTeamworkActionPolicy(t *testing.T, name string,
	mutate func(*TeamworkActionPolicySpec),
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		spec := TeamworkActionPolicySpec{AssetRevision: Sum([]byte("revision")),
			Entries: cloneTeamworkActionPolicySpecs(testTeamworkActionPolicySpecs())}
		mutate(&spec)
		if policy, err := NewTeamworkActionPolicy(spec); err == nil || !policy.AssetRevision().IsZero() {
			t.Fatalf("NewTeamworkActionPolicy() = (%#v, %v)", policy, err)
		}
	})
}

func testTeamworkActionPolicy(t testing.TB) TeamworkActionPolicy {
	t.Helper()
	policy, err := NewTeamworkActionPolicy(TeamworkActionPolicySpec{
		AssetRevision: Sum([]byte("Teamwork Action policy")), Entries: testTeamworkActionPolicySpecs()})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testTeamworkActionPolicySpecs() []TeamworkActionPolicyEntrySpec {
	return []TeamworkActionPolicyEntrySpec{
		{Ordinal: 0, OperationKind: OperationTeamworkOffer,
			EventType: EventReviewOffered,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextNone,
				TeamworkActionContextReviewerActive, TeamworkActionContextReviewerRework}, MaxResults: MaxChildWorks},
		{Ordinal: 1, OperationKind: OperationTeamworkAccept,
			EventType:       EventReviewAcceptRequested,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextReviewerOffered}, MaxResults: 1},
		{Ordinal: 2, OperationKind: OperationTeamworkDecline,
			EventType:       EventReviewDeclineRequested,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextReviewerOffered}, MaxResults: 1},
		{Ordinal: 3, OperationKind: OperationTeamworkDeliver,
			EventType: EventReviewDeliveryReady,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextReviewerActive,
				TeamworkActionContextReviewerRework, TeamworkActionContextParentResume}, MaxResults: 1},
		{Ordinal: 4, OperationKind: OperationTeamworkRework,
			EventType:       EventReviewReworkRequested,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextHomeDeliveredIteration1}, MaxResults: 1},
		{Ordinal: 5, OperationKind: OperationTeamworkClose,
			EventType:       EventReviewClosed,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextHomeDelivered}, MaxResults: 1},
		{Ordinal: 6, OperationKind: OperationTeamworkCancel,
			EventType:       EventReviewCancelled,
			AllowedContexts: []TeamworkActionContext{TeamworkActionContextHomeNonterminal}, MaxResults: 1},
	}
}

func cloneTeamworkActionPolicySpecs(source []TeamworkActionPolicyEntrySpec) []TeamworkActionPolicyEntrySpec {
	result := append([]TeamworkActionPolicyEntrySpec(nil), source...)
	for index := range result {
		result[index].AllowedContexts = append([]TeamworkActionContext(nil), source[index].AllowedContexts...)
	}
	return result
}
