package event

import (
	"errors"
	"testing"
	"time"
)

func TestControllerCandidateSetAndOutcomeEnumAreClosed(t *testing.T) {
	t.Parallel()

	rejected, err := NewAcceptRejectedDecision("accept-race-lost")
	if err != nil {
		t.Fatalf("NewAcceptRejectedDecision() error = %v", err)
	}
	outcome, err := NewOutcomeDecision(OutcomeConflicted, "work-conflict", "decision-a")
	if err != nil {
		t.Fatalf("NewOutcomeDecision() error = %v", err)
	}
	decisions := []ControllerCandidate{
		AcceptedDecision{}, rejected, DeliveredDecision{}, DeclinedDecision{}, ExpiredDecision{}, outcome,
	}
	if len(decisions) != 6 {
		t.Fatalf("closed controller candidate count = %d", len(decisions))
	}
	for _, status := range []OutcomeStatus{OutcomeAccepted, OutcomeRejected, OutcomeConflicted} {
		if !status.Valid() {
			t.Fatalf("OutcomeStatus(%q).Valid() = false", status)
		}
	}
	if OutcomeStatus("completed").Valid() {
		t.Fatal("unknown outcome status was accepted")
	}
	if _, err := NewOutcomeDecision("completed", "code", "decision"); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("unknown outcome error = %v", err)
	}
}

func TestControllerZeroValueCannotBypassValidatedFields(t *testing.T) {
	t.Parallel()

	stamp := AdmissionStamp{workVersion: 1, iteration: 1, deadlineUnixNano: time.Now().UnixNano()}
	if _, err := (AcceptRejectedDecision{}).draft(stamp, time.Now()); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("zero accept rejection error = %v", err)
	}
	if _, err := (OutcomeDecision{}).draft(stamp, time.Now()); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("zero outcome error = %v", err)
	}
}

func TestExpiryUsesTrustedClockAtExactDeadline(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 16, 1, 0, 0, 99, time.UTC)
	stamp := AdmissionStamp{workVersion: 3, iteration: 1, deadlineUnixNano: deadline.UnixNano()}
	if _, err := (ExpiredDecision{}).draft(stamp, deadline.Add(-time.Nanosecond)); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("early expiry error = %v", err)
	}
	draft, err := (ExpiredDecision{}).draft(stamp, deadline)
	if err != nil {
		t.Fatalf("boundary expiry error = %v", err)
	}
	if draft.eventType != "review.expired" || draft.deadlineUnixNano != deadline.UnixNano() {
		t.Fatalf("expiry draft = %#v", draft)
	}
}
