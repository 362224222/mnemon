package store

import (
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestDeriveCurrentActionsFailsClosedWithoutManagedPolicy(t *testing.T) {
	if contexts := deriveCurrentActionContexts(model.CurrentRole("unknown"), "", "", 0, false); len(contexts) != 0 {
		t.Fatalf("non-exact current contexts = %v", contexts)
	}
	if actions, err := deriveCurrentActions(model.TeamworkActionPolicy{}, model.CurrentRole("unknown"),
		"", "", 0, false); !errors.Is(err, ErrCurrentReadInvariant) || actions != nil {
		t.Fatalf("zero policy actions = (%v, %v)", actions, err)
	}
}

func TestDeriveCurrentActionsUsesExactParticipantStateAndSourceEvent(t *testing.T) {
	now := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	policy := acceptanceActionPolicy(t, model.MaxChildWorks)
	home, _ := model.ParsePeerID("peer-current-policy-home")
	reviewer, _ := model.ParsePeerID("peer-current-policy-reviewer")
	channel, _ := model.ParseChannelID("channel-current-policy")
	workID, _ := model.ParseWorkID("work-current-policy")
	ref, _ := model.NewWorkRef(home, workID)
	participants, _ := model.NewParticipantSnapshot(channel, 2, home, reviewer)

	offered := currentPolicyEvent(t, now, channel, ref, home, reviewer,
		model.EventReviewOffered, "event-current-policy-offered",
		`{"content":"review","deadline":"2026-07-17T16:00:00Z","iteration":1,"work_version":1}`)
	work := currentPolicyWork(t, ref, channel, participants, offered, model.WorkOffered, 1, 1,
		now.Add(24*time.Hour).UnixNano())
	offeredFacts, _ := decodeClosedEventPayload(offered)
	if exact, err := currentWorkIsExactSource(offered, work, offeredFacts); err != nil || !exact {
		t.Fatalf("offered current binding = (%t, %v)", exact, err)
	}
	if got := mustDeriveCurrentActions(t, policy, model.CurrentReviewer, offered, work, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkDecline, model.OperationResolveRetry}) {
		t.Fatalf("reviewer OFFERED actions = %v", got)
	}
	if got := mustDeriveCurrentActions(t, policy, model.CurrentReviewer, offered, work, false); !sameOperationKinds(got,
		currentResolutionActions()) {
		t.Fatalf("stale actions = %v", got)
	}
	accepted := currentPolicyEvent(t, now.Add(time.Second), channel, ref, home, reviewer,
		model.EventReviewAccepted, "event-current-policy-accepted", `{"iteration":1,"work_version":1}`)
	activeWork := currentPolicyWork(t, ref, channel, participants, accepted, model.WorkActive, 2, 1,
		now.Add(24*time.Hour).UnixNano())
	acceptedFacts, _ := decodeClosedEventPayload(accepted)
	if exact, err := currentWorkIsExactSource(accepted, activeWork, acceptedFacts); err != nil || !exact {
		t.Fatalf("accepted current binding = (%t, %v)", exact, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*model.ReviewWorkSpec)
	}{
		{"version", func(spec *model.ReviewWorkSpec) { spec.Version++ }},
		{"state data", func(spec *model.ReviewWorkSpec) {
			spec.StateData, _ = model.NewJSON([]byte(`{"forged":true}`))
		}},
		{"updated time", func(spec *model.ReviewWorkSpec) {
			spec.UpdatedAt = spec.UpdatedAt.Add(time.Nanosecond)
		}},
	} {
		t.Run("reject "+test.name+" drift", func(t *testing.T) {
			spec := activeWork.Spec()
			test.mutate(&spec)
			drifted, err := model.NewReviewWork(spec)
			if err != nil {
				t.Fatal(err)
			}
			if exact, err := currentWorkIsExactSource(accepted, drifted, acceptedFacts); err == nil || exact {
				t.Fatalf("drifted current binding = (%t,%v)", exact, err)
			}
		})
	}
	if got := mustDeriveCurrentActions(t, policy, model.CurrentReviewer, accepted, activeWork, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver, model.OperationResolveRetry}) {
		t.Fatalf("reviewer ACTIVE actions = %v", got)
	}

	delivered := currentPolicyEvent(t, now.Add(2*time.Second), channel, ref, home, reviewer,
		model.EventReviewDelivered, "event-current-policy-delivered", `{"iteration":1,"work_version":3}`)
	deliveredWork := currentPolicyWork(t, ref, channel, participants, delivered, model.WorkDelivered, 4, 1,
		now.Add(24*time.Hour).UnixNano())
	deliveredFacts, _ := decodeClosedEventPayload(delivered)
	if exact, err := currentWorkIsExactSource(delivered, deliveredWork, deliveredFacts); err != nil || !exact {
		t.Fatalf("delivered current binding = (%t, %v)", exact, err)
	}
	if got := mustDeriveCurrentActions(t, policy, model.CurrentInitiator, delivered, deliveredWork, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkRework, model.OperationTeamworkClose,
			model.OperationTeamworkCancel, model.OperationResolveRetry}) {
		t.Fatalf("initiator DELIVERED actions = %v", got)
	}
	rework := currentPolicyEvent(t, now.Add(3*time.Second), channel, ref, home, reviewer,
		model.EventReviewReworkRequested, "event-current-policy-rework",
		`{"content":"correct this","iteration":1,"work_version":4}`)
	reworkWork := currentPolicyWork(t, ref, channel, participants, rework, model.WorkRework, 5, 2,
		now.Add(24*time.Hour).UnixNano())
	reworkFacts, _ := decodeClosedEventPayload(rework)
	if exact, err := currentWorkIsExactSource(rework, reworkWork, reworkFacts); err != nil || !exact {
		t.Fatalf("rework current binding = (%t, %v)", exact, err)
	}
}

func TestDeriveCurrentActionsPreservesManagedPolicyOrder(t *testing.T) {
	operations := acceptanceActionOperations()
	operations[1], operations[2] = operations[2], operations[1]
	policy := acceptanceActionPolicyForOperations(t, model.MaxChildWorks, operations, model.Digest{})
	actions, err := deriveCurrentActions(policy, model.CurrentReviewer, model.EventReviewOffered,
		model.WorkOffered, 1, true)
	want := []model.OperationKind{model.OperationTeamworkDecline, model.OperationTeamworkAccept,
		model.OperationResolveRetry}
	if err != nil || !sameOperationKinds(actions, want) {
		t.Fatalf("policy-ordered actions = (%v, %v), want %v", actions, err, want)
	}
}

func TestValidateStoredCurrentWorkProjectionUsesFrozenActionAuthority(t *testing.T) {
	now := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	home, _ := model.ParsePeerID("peer-current-replay-home")
	reviewer, _ := model.ParsePeerID("peer-current-replay-reviewer")
	channel, _ := model.ParseChannelID("channel-current-replay")
	workID, _ := model.ParseWorkID("work-current-replay")
	ref, _ := model.NewWorkRef(home, workID)
	participants, _ := model.NewParticipantSnapshot(channel, 2, home, reviewer)
	deadline := now.Add(24 * time.Hour).UnixNano()
	policy := acceptanceActionPolicy(t, model.MaxChildWorks)

	offered := currentPolicyEvent(t, now, channel, ref, home, reviewer,
		model.EventReviewOffered, "event-current-replay-offered",
		`{"content":"review","deadline":"2026-07-17T16:00:00Z","iteration":1,"work_version":1}`)
	offeredWork := currentPolicyWork(t, ref, channel, participants, offered, model.WorkOffered, 1, 1, deadline)
	brief := mustCurrentBrief(t, "review", deadline)
	offeredProjection := mustCurrentPolicyProjection(t, offered, offeredWork, model.CurrentReviewer,
		brief, []model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkDecline,
			model.OperationResolveRetry})
	if err := validateStoredCurrentWorkProjection(policy, offeredProjection, offered, offered, offeredWork,
		model.CurrentReviewer, brief, offered.ID(), offered.AcceptedAt()); err != nil {
		t.Fatalf("canonical stored projection = %v", err)
	}
	operations := acceptanceActionOperations()
	operations[1], operations[2] = operations[2], operations[1]
	reordered := acceptanceActionPolicyForOperations(t, model.MaxChildWorks, operations,
		policy.AssetRevision())
	if err := validateStoredCurrentWorkProjection(reordered, offeredProjection, offered, offered, offeredWork,
		model.CurrentReviewer, brief, offered.ID(), offered.AcceptedAt()); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("same-revision policy drift error = %v", err)
	}
	downgraded := mustCurrentPolicyProjection(t, offered, offeredWork, model.CurrentReviewer,
		brief, currentResolutionActions())
	if err := validateStoredCurrentWorkProjection(policy, downgraded, offered, offered, offeredWork,
		model.CurrentReviewer, brief, offered.ID(), offered.AcceptedAt()); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("exact projection downgrade error = %v", err)
	}

	firstDelivery := currentPolicyEvent(t, now.Add(2*time.Second), channel, ref, home, reviewer,
		model.EventReviewDelivered, "event-current-replay-first-delivery",
		`{"iteration":1,"work_version":3}`)
	secondDeliveryEvent := currentPolicyEvent(t, now.Add(4*time.Second), channel, ref, home, reviewer,
		model.EventReviewDelivered, "event-current-replay-second-delivery",
		`{"iteration":2,"work_version":5}`)
	secondDelivery := currentPolicyWork(t, ref, channel, participants, secondDeliveryEvent,
		model.WorkDelivered, 6, 2, deadline)
	staleProjection := mustCurrentPolicyProjection(t, firstDelivery, secondDelivery, model.CurrentInitiator,
		brief, currentResolutionActions())
	if err := validateStoredCurrentWorkProjection(policy, staleProjection, firstDelivery, secondDeliveryEvent,
		secondDelivery, model.CurrentInitiator, brief, secondDelivery.UpdatedBy(),
		secondDelivery.UpdatedAt()); err != nil {
		t.Fatalf("frozen fail-closed projection replay = %v", err)
	}
	elevatedProjection := mustCurrentPolicyProjection(t, firstDelivery, secondDelivery,
		model.CurrentInitiator, brief, []model.OperationKind{model.OperationTeamworkClose,
			model.OperationTeamworkCancel, model.OperationResolveRetry})
	if err := validateStoredCurrentWorkProjection(policy, elevatedProjection, firstDelivery, secondDeliveryEvent,
		secondDelivery, model.CurrentInitiator, brief, secondDelivery.UpdatedBy(),
		secondDelivery.UpdatedAt()); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("stale projection elevation error = %v", err)
	}
	if err := validateStoredCurrentWorkProjection(policy, staleProjection, firstDelivery, secondDeliveryEvent,
		offeredWork, model.CurrentInitiator, brief, secondDelivery.UpdatedBy(),
		secondDelivery.UpdatedAt()); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("future frozen Work error = %v", err)
	}
	if err := validateStoredCurrentWorkProjection(policy, downgraded, offered, firstDelivery,
		offeredWork, model.CurrentReviewer, brief, firstDelivery.ID(),
		firstDelivery.AcceptedAt()); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("forged unrelated updater error = %v", err)
	}
}

func mustDeriveCurrentActions(t testing.TB, policy model.TeamworkActionPolicy,
	role model.CurrentRole, event model.Event, work model.ReviewWork, exactUpdate bool,
) []model.OperationKind {
	t.Helper()
	actions, err := deriveCurrentActions(policy, role, event.Type(), work.State(), work.Iteration(), exactUpdate)
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func mustCurrentBrief(t testing.TB, content string, deadline int64) model.CurrentBrief {
	t.Helper()
	brief, err := model.NewCurrentBrief(model.CurrentBriefSpec{Content: content,
		DeadlineUnixNano: deadline})
	if err != nil {
		t.Fatal(err)
	}
	return brief
}

func mustCurrentPolicyProjection(t testing.TB, event model.Event, work model.ReviewWork,
	role model.CurrentRole, brief model.CurrentBrief, actions []model.OperationKind,
) model.CurrentProjection {
	t.Helper()
	currentEvent, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: event.Key(), Digest: event.Digest(),
		Type: event.Type(), WorkRef: event.Scope().WorkRef(), Summary: event.Summary(), Payload: event.Payload(),
		AcceptedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: work.Ref(), Version: work.Version(),
		Iteration: work.Iteration(), DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(),
		StateData: work.StateData(), LocalRole: role, Brief: brief})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: currentEvent,
		ActionWork: currentWork, AllowedActions: actions})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}
