package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestPeerInboxSemanticPlanBindsSnapshotAndDefensivelyCopiesResponses(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-frozen-plan", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-frozen-plan", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-frozen-plan-worker",
		readyAt.Add(time.Second))
	decisionAt := readyAt.Add(2 * time.Second)
	policy := teamworkPlanForClaim(t, claim, decisionAt)
	spec := peerInboxSemanticStorePlanSpec(t, policy)
	plan, err := NewPeerInboxSemanticPlan(claim, decisionAt, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := plan.Responses()[0]
	wantWork, hasWork := plan.Work()
	spec.Responses[0] = PeerInboxSemanticResponseIntentSpec{}
	if spec.Work != nil {
		spec.Work.NextVersion++
	}
	copy := plan.Responses()
	copy[0] = PeerInboxSemanticResponseIntent{}
	if got := plan.Responses(); len(got) != 1 || got[0].EventType() != want.EventType() ||
		got[0].Payload().String() != want.Payload().String() || got[0].Cause() != want.Cause() {
		t.Fatalf("immutable responses = %#v, want %#v", got, want)
	}
	if got, ok := plan.Work(); ok != hasWork || ok && got.NextVersion() != wantWork.NextVersion() {
		t.Fatalf("immutable Work = (%#v,%t), want (%#v,%t)", got, ok, wantWork, hasWork)
	}
	if plan.inboxID != claim.InboxID() || plan.attempt != claim.Fence().Attempt() ||
		plan.snapshotDigest != claim.SnapshotDigest() || !plan.DecisionAt().Equal(decisionAt) {
		t.Fatalf("plan binding = %#v, claim = %#v", plan, claim)
	}
}

func TestPeerInboxSemanticPlanRejectsRetryAndForgedEffects(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-plan-invalid", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-plan-invalid", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-plan-invalid-worker",
		readyAt.Add(time.Second))
	decisionAt := readyAt.Add(2 * time.Second)
	valid := peerInboxSemanticStorePlanSpec(t, teamworkPlanForClaim(t, claim, decisionAt))

	cases := []struct {
		name string
		edit func(*PeerInboxSemanticPlanSpec)
	}{
		{"retry disposition", func(spec *PeerInboxSemanticPlanSpec) {
			spec.Disposition = PeerInboxSemanticDisposition("retry")
		}},
		{"accepted diagnostic", func(spec *PeerInboxSemanticPlanSpec) {
			spec.Diagnostic = "forged"
		}},
		{"response authority", func(spec *PeerInboxSemanticPlanSpec) {
			spec.Responses[0].EventType = model.EventReviewOffered
		}},
		{"work predecessor", func(spec *PeerInboxSemanticPlanSpec) {
			if spec.Work == nil {
				spec.Work = &PeerInboxSemanticWorkIntentSpec{}
			}
			spec.Work.ExpectedVersion++
		}},
		{"work response ordinal", func(spec *PeerInboxSemanticPlanSpec) {
			spec.Work.ResponseOrdinal = 2
		}},
		{"handling response ordinal", func(spec *PeerInboxSemanticPlanSpec) {
			if spec.Handling == nil {
				spec.Handling = &PeerInboxSemanticHandlingIntentSpec{
					Source:          PeerInboxSemanticFromImportedEvent,
					WorkRef:         claim.ImportedEvent().Scope().WorkRef(),
					LocalRole:       model.WorkRoleInitiator,
					SourceEventType: claim.ImportedEvent().Type()}
			}
			spec.Handling.Source = PeerInboxSemanticFromLocalResponse
			spec.Handling.ResponseOrdinal = 2
		}},
		{"settlement disposition", func(spec *PeerInboxSemanticPlanSpec) {
			spec.Settlement = &PeerInboxSemanticHandlingSettlementSpec{
				WorkRef:       claim.ImportedEvent().Scope().WorkRef(),
				SourceEventID: claim.ImportedEvent().ID(), Disposition: "forged"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			spec.Responses = append([]PeerInboxSemanticResponseIntentSpec(nil), valid.Responses...)
			if valid.Work != nil {
				copy := *valid.Work
				spec.Work = &copy
			}
			if valid.Handling != nil {
				copy := *valid.Handling
				spec.Handling = &copy
			}
			if valid.Settlement != nil {
				copy := *valid.Settlement
				spec.Settlement = &copy
			}
			test.edit(&spec)
			if _, err := NewPeerInboxSemanticPlan(claim, decisionAt, spec); !errors.Is(err, ErrPeerInboxSemanticInput) {
				t.Fatalf("NewPeerInboxSemanticPlan() error = %v", err)
			}
		})
	}
}

func TestPeerInboxSemanticRequestDigestBindsFrozenPlanProjection(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-plan-request-digest", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-plan-request-digest", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-plan-digest-worker",
		readyAt.Add(time.Second))
	decisionAt := readyAt.Add(2 * time.Second)
	plan := peerInboxSemanticStorePlan(t, claim, decisionAt,
		teamworkPlanForClaim(t, claim, decisionAt))
	baseline, err := peerInboxSemanticCommitRequestDigest(claim.Fence(), plan,
		LocalAdmissionScope{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := model.NewJSON([]byte(`{"changed":true}`))
	variants := []struct {
		name   string
		mutate func(*PeerInboxSemanticPlan)
	}{
		{"decision time", func(value *PeerInboxSemanticPlan) { value.decisionAt = value.decisionAt.Add(1) }},
		{"disposition", func(value *PeerInboxSemanticPlan) { value.disposition = PeerInboxSemanticReceiptOnly }},
		{"diagnostic", func(value *PeerInboxSemanticPlan) { value.diagnostic = "changed" }},
		{"work", func(value *PeerInboxSemanticPlan) { value.work.spec.NextVersion++ }},
		{"handling", func(value *PeerInboxSemanticPlan) {
			value.handling = PeerInboxSemanticHandlingIntent{spec: PeerInboxSemanticHandlingIntentSpec{Source: PeerInboxSemanticFromImportedEvent,
				WorkRef:         claim.ImportedEvent().Scope().WorkRef(),
				LocalRole:       model.WorkRoleInitiator,
				SourceEventType: claim.ImportedEvent().Type()}}
			value.hasHandling = true
		}},
		{"settlement", func(value *PeerInboxSemanticPlan) {
			value.settlement = PeerInboxSemanticHandlingSettlement{spec: PeerInboxSemanticHandlingSettlementSpec{WorkRef: claim.ImportedEvent().Scope().WorkRef(),
				SourceEventID: claim.ImportedEvent().ID(),
				Disposition:   PeerInboxSemanticSupersededCancelled}}
			value.hasSettlement = true
		}},
		{"response", func(value *PeerInboxSemanticPlan) {
			value.responses = append([]PeerInboxSemanticResponseIntent(nil), value.responses...)
			value.responses[0].spec.Payload = payload
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			candidate := plan
			variant.mutate(&candidate)
			digest, err := peerInboxSemanticCommitRequestDigest(claim.Fence(), candidate,
				LocalAdmissionScope{}, nil)
			if err != nil || digest == baseline {
				t.Fatalf("plan variant digest = (%s,%v), baseline %s", digest, err, baseline)
			}
		})
	}
}

func TestPeerInboxSemanticReplayRejectsDifferentFrozenPlan(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-plan-replay", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-plan-replay", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-plan-replay-worker",
		readyAt.Add(time.Second))
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
	if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
		spec.Plan.DecisionAt()); err != nil {
		t.Fatal(err)
	}
	spec.Plan.disposition = PeerInboxSemanticReceiptOnly
	if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
		spec.Plan.DecisionAt().Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("different frozen plan replay error = %v", err)
	}
}

func peerInboxSemanticStorePlan(t *testing.T, claim PeerInboxSemanticClaim,
	at time.Time, policy teamwork.ImportPlan,
) PeerInboxSemanticPlan {
	t.Helper()
	plan, err := NewPeerInboxSemanticPlan(claim, at, peerInboxSemanticStorePlanSpec(t, policy))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func peerInboxSemanticRequestDigestPlan(fence PeerInboxSemanticFence,
	at time.Time,
) PeerInboxSemanticPlan {
	return PeerInboxSemanticPlan{inboxID: fence.inboxID, attempt: fence.attempt,
		snapshotDigest: fence.snapshotDigest, decisionAt: at,
		disposition: PeerInboxSemanticApply}
}

func peerInboxSemanticRequestDigestFence(t *testing.T, at time.Time) PeerInboxSemanticFence {
	t.Helper()
	inboxID, err := model.ParseInboxID("inbox-semantic-decision-request")
	if err != nil {
		t.Fatal(err)
	}
	fence := PeerInboxSemanticFence{inboxID: inboxID,
		leaseOwner: "semantic-decision-worker", leaseUntil: at.Add(time.Minute), attempt: 3,
		snapshotDigest: model.Sum([]byte("semantic-decision-snapshot"))}
	copy(fence.semanticNonce[:], model.Sum([]byte("semantic-decision-nonce")).Bytes())
	return fence
}

func teamworkPlanForClaim(t *testing.T, claim PeerInboxSemanticClaim,
	at time.Time,
) teamwork.ImportPlan {
	t.Helper()
	facts := make([]teamwork.ImportEventFact, len(claim.CausalEvents()))
	for index, event := range claim.CausalEvents() {
		fact, err := teamwork.NewImportEventFact(event)
		if err != nil {
			t.Fatal(err)
		}
		facts[index] = fact
	}
	var current *model.ReviewWork
	if value, ok := claim.CurrentWork(); ok {
		current = &value
	}
	local := claim.ImportedEvent().Audience().Peers()
	plan, err := teamwork.PlanImportedEvent(teamwork.ImportPlanSpec{LocalPeerID: local[0],
		Event: claim.ImportedEvent(), Current: current, Facts: facts, Now: at})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func peerInboxSemanticStorePlanSpec(t *testing.T,
	policy teamwork.ImportPlan,
) PeerInboxSemanticPlanSpec {
	t.Helper()
	disposition, ok := map[teamwork.ImportDisposition]PeerInboxSemanticDisposition{
		teamwork.ImportApply:       PeerInboxSemanticApply,
		teamwork.ImportReject:      PeerInboxSemanticReject,
		teamwork.ImportConflict:    PeerInboxSemanticConflict,
		teamwork.ImportReceiptOnly: PeerInboxSemanticReceiptOnly,
	}[policy.Disposition()]
	if !ok {
		t.Fatalf("policy disposition %q is not terminal", policy.Disposition())
	}
	spec := PeerInboxSemanticPlanSpec{Disposition: disposition,
		Diagnostic: policy.Diagnostic()}
	if intent, exists := policy.Work(); exists {
		source := peerInboxSemanticStoreEffectSource(t, string(intent.Source()))
		spec.Work = &PeerInboxSemanticWorkIntentSpec{Source: source,
			ResponseOrdinal: intent.ResponseOrdinal(), WorkRef: intent.WorkRef(),
			ChannelID: intent.ChannelID(), Participants: intent.Participants(),
			ExpectedVersion: intent.ExpectedVersion(), ExpectedState: intent.ExpectedState(),
			ExpectedIteration: intent.ExpectedIteration(), NextVersion: intent.NextVersion(),
			NextState: intent.NextState(), NextIteration: intent.NextIteration(),
			DeadlineUnixNano: intent.DeadlineUnixNano(), StateData: intent.StateData(),
			ObservedAtUnixNano: intent.ObservedAtUnixNano()}
	}
	if intent, exists := policy.Handling(); exists {
		spec.Handling = &PeerInboxSemanticHandlingIntentSpec{
			Source:          peerInboxSemanticStoreEffectSource(t, string(intent.Source())),
			ResponseOrdinal: intent.ResponseOrdinal(), WorkRef: intent.WorkRef(),
			LocalRole: intent.LocalRole(), SourceEventType: intent.SourceEventType()}
	}
	if settlement, exists := policy.Settlement(); exists {
		spec.Settlement = &PeerInboxSemanticHandlingSettlementSpec{
			WorkRef: settlement.WorkRef(), SourceEventID: settlement.SourceEventID(),
			Disposition: PeerInboxSemanticSettlementDisposition(settlement.Disposition())}
	}
	for _, response := range policy.Responses() {
		spec.Responses = append(spec.Responses, PeerInboxSemanticResponseIntentSpec{
			EventType: response.EventType(), Payload: response.Payload(), Cause: response.Cause()})
	}
	return spec
}

func peerInboxSemanticStoreSettlement(t *testing.T,
	policy teamwork.ImportPlan,
) PeerInboxSemanticHandlingSettlement {
	t.Helper()
	settlement, ok := policy.Settlement()
	if !ok {
		t.Fatalf("policy plan has no Handling settlement: %#v", policy)
	}
	return PeerInboxSemanticHandlingSettlement{spec: PeerInboxSemanticHandlingSettlementSpec{
		WorkRef: settlement.WorkRef(), SourceEventID: settlement.SourceEventID(),
		Disposition: PeerInboxSemanticSettlementDisposition(settlement.Disposition())}}
}

func peerInboxSemanticStoreEffectSource(t *testing.T,
	source string,
) PeerInboxSemanticEffectSource {
	t.Helper()
	value := PeerInboxSemanticEffectSource(source)
	if !value.Valid() {
		t.Fatalf("invalid policy effect source %q", source)
	}
	return value
}
