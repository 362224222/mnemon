package model

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCurrentProjectionAndReceiptAreCanonicalImmutableBindings(t *testing.T) {
	projection := currentTestProjection(t, []OperationKind{
		OperationResolveRetry, OperationTeamworkDecline, OperationTeamworkAccept,
	})
	actions := projection.AllowedActions()
	wantActions := []OperationKind{OperationTeamworkAccept, OperationTeamworkDecline, OperationResolveRetry}
	if !equalCurrentActions(actions, wantActions) {
		t.Fatalf("canonical allowed actions = %v, want %v", actions, wantActions)
	}
	actions[0] = OperationTeamworkCancel
	if projection.AllowedActions()[0] != OperationTeamworkAccept {
		t.Fatal("allowed actions getter mutated projection")
	}

	runID, _ := ParseRunID("run-current-one")
	handlingID, _ := ParseHandlingID("handling-current-one")
	readAt := time.Date(2026, 7, 16, 14, 0, 1, 123, time.UTC)
	receipt, err := NewCurrentReadReceipt(CurrentReadReceiptSpec{
		RunID: runID, ProfileID: TeamworkProfileID(), HandlingID: handlingID,
		HandlingAttempt: 2, Projection: projection, ReadAt: readAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.SourceEvent() != projection.SourceEvent().Key() ||
		receipt.ActionWork() != projection.ActionWork().Ref() ||
		receipt.ActionWorkVersion() != projection.ActionWork().Version() ||
		receipt.ProjectionDigest() != projection.Digest() ||
		!equalCurrentActions(receipt.AllowedActions(), wantActions) {
		t.Fatalf("receipt does not bind projection: %#v", receipt)
	}
	if bytes.Contains(receipt.CanonicalJSON().Bytes(), []byte("claim")) ||
		bytes.Contains(receipt.CanonicalJSON().Bytes(), []byte("fence")) ||
		bytes.Contains(receipt.CanonicalJSON().Bytes(), []byte("secret")) ||
		bytes.Contains(receipt.CanonicalJSON().Bytes(), []byte(`"role"`)) {
		t.Fatalf("receipt leaked claim authority: %s", receipt.CanonicalJSON().String())
	}
	parsed, err := ParseCurrentReadReceipt(receipt.CanonicalJSON().Bytes())
	if err != nil || parsed.CanonicalJSON().String() != receipt.CanonicalJSON().String() ||
		parsed.Projection().CanonicalJSON().String() != projection.CanonicalJSON().String() {
		t.Fatalf("ParseCurrentReadReceipt() = (%#v, %v)", parsed, err)
	}
}

func TestCurrentReadReceiptRejectsNoncanonicalOrConflictingEvidence(t *testing.T) {
	projection := currentTestProjection(t, []OperationKind{OperationTeamworkAccept, OperationResolveRetry})
	runID, _ := ParseRunID("run-current-parse")
	handlingID, _ := ParseHandlingID("handling-current-parse")
	receipt, err := NewCurrentReadReceipt(CurrentReadReceiptSpec{
		RunID: runID, ProfileID: TeamworkProfileID(), HandlingID: handlingID,
		HandlingAttempt: 1, Projection: projection,
		ReadAt: time.Date(2026, 7, 16, 14, 1, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := receipt.CanonicalJSON().String()
	tests := []struct {
		name string
		raw  string
	}{
		{"whitespace", " " + raw},
		{"unknown field", strings.Replace(raw, `{"action_work":`, `{"unknown":true,"action_work":`, 1)},
		{"binding drift", strings.Replace(raw, `"action_work_version":1`, `"action_work_version":2`, 1)},
		{"projection digest drift", strings.Replace(raw, receipt.ProjectionDigest().String(), Sum([]byte("other")).String(), 1)},
		{"action order", strings.Replace(raw,
			`"allowed_actions":["teamwork.accept","agent.resolve.retry"]`,
			`"allowed_actions":["agent.resolve.retry","teamwork.accept"]`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseCurrentReadReceipt([]byte(test.raw)); err == nil {
				t.Fatalf("ParseCurrentReadReceipt accepted %s", test.raw)
			}
		})
	}
}

func TestCurrentProjectionRejectsInvalidScopeCollectionsAndBudget(t *testing.T) {
	valid := currentTestProjection(t, []OperationKind{OperationTeamworkAccept})
	event := valid.SourceEvent()
	work := valid.ActionWork()

	otherWorkID, _ := ParseWorkID("work-current-other")
	otherRef, _ := NewWorkRef(work.Ref().HomePeerID(), otherWorkID)
	otherWork, _ := NewCurrentWork(CurrentWorkSpec{Ref: otherRef, Version: work.Version(),
		Iteration: work.Iteration(), DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(),
		StateData: work.StateData(), LocalRole: work.LocalRole()})
	if _, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: event, ActionWork: otherWork,
		AllowedActions: []OperationKind{OperationTeamworkAccept}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("scope mismatch error = %v", err)
	}
	if _, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: event, ActionWork: work,
		AllowedActions: []OperationKind{OperationTeamworkAccept, OperationTeamworkAccept}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate action error = %v", err)
	}
	if _, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: event, ActionWork: work,
		AllowedActions: []OperationKind{"teamwork.unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown action error = %v", err)
	}

	largePayload, err := NewJSON([]byte(`{"content":"` + strings.Repeat("x", DefaultCurrentJSONBytes) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	largeEvent, err := NewCurrentEvent(CurrentEventSpec{Key: event.Key(), Digest: event.Digest(), Type: event.Type(),
		WorkRef: event.WorkRef(), Summary: event.Summary(), Payload: largePayload,
		ArtifactRefs: event.ArtifactRefs(), AcceptedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: largeEvent, ActionWork: work,
		AllowedActions: []OperationKind{OperationTeamworkAccept}}); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversize projection error = %v", err)
	}
}

func currentTestProjection(t *testing.T, actions []OperationKind) CurrentProjection {
	t.Helper()
	home, _ := ParsePeerID("peer-current-home")
	origin, _ := ParsePeerID("peer-current-origin")
	epoch, _ := ParseOriginEpoch("epoch-current-origin")
	eventID, _ := ParseEventID("event-current-source")
	key, _ := NewEventKey(origin, epoch, eventID)
	workID, _ := ParseWorkID("work-current-source")
	workRef, _ := NewWorkRef(home, workID)
	payload, _ := NewJSON([]byte(`{"content":"review this","deadline":"2026-07-17T14:00:00Z","iteration":1,"work_version":1}`))
	root, _ := ParseDigest(Sum([]byte("current-artifact")).String())
	artifact, _ := NewCurrentArtifactRef(root)
	acceptedAt := time.Date(2026, 7, 16, 14, 0, 0, 0, time.UTC)
	event, err := NewCurrentEvent(CurrentEventSpec{Key: key, Digest: Sum([]byte("event-current-digest")),
		Type: EventReviewOffered, WorkRef: workRef, Summary: "Review this change", Payload: payload,
		ArtifactRefs: []CurrentArtifactRef{artifact}, AcceptedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := NewJSON([]byte(`{"content":"review this"}`))
	work, err := NewCurrentWork(CurrentWorkSpec{Ref: workRef, Version: 1, Iteration: 1,
		DeadlineUnixNano: acceptedAt.Add(24 * time.Hour).UnixNano(), State: WorkOffered,
		StateData: state, LocalRole: CurrentReviewer})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: event,
		ActionWork: work, AllowedActions: actions})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}
