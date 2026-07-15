package event

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgentCandidateConstructorsEnforceClosedInputSchemas(t *testing.T) {
	t.Parallel()

	valid := []AgentCandidate{}
	offer, err := NewOfferCandidate("review this", 0)
	if err != nil {
		t.Fatalf("NewOfferCandidate() error = %v", err)
	}
	valid = append(valid, offer)
	accept, _ := NewAcceptCandidate("")
	decline, _ := NewDeclineCandidate("not available")
	deliver, _ := NewDeliverCandidate("no defects")
	rework, _ := NewReworkCandidate("cover the race")
	closeCandidate, _ := NewCloseCandidate("")
	cancel, _ := NewCancelCandidate("superseded")
	valid = append(valid, accept, decline, deliver, rework, closeCandidate, cancel)
	if len(valid) != 7 {
		t.Fatalf("closed Agent candidate count = %d", len(valid))
	}

	for _, build := range []func() error{
		func() error { _, err := NewOfferCandidate("", 0); return err },
		func() error { _, err := NewOfferCandidate("ok", time.Minute); return err },
		func() error { _, err := NewDeclineCandidate(""); return err },
		func() error { _, err := NewDeliverCandidate(""); return err },
		func() error { _, err := NewReworkCandidate(""); return err },
		func() error { _, err := NewCancelCandidate(""); return err },
	} {
		if err := build(); !errors.Is(err, ErrInvalidCandidate) {
			t.Fatalf("invalid candidate error = %v", err)
		}
	}
}

func TestAgentCandidateRejectsOversizeAndForgedZeroValue(t *testing.T) {
	t.Parallel()

	if _, err := NewDeliverCandidate(strings.Repeat("x", model.MaxContentBytes+1)); !errors.Is(err, model.ErrLimit) {
		t.Fatalf("oversize error = %v", err)
	}
	if _, err := (OfferCandidate{}).draft(AdmissionStamp{}, time.Now()); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("zero literal draft error = %v", err)
	}
}

func TestOfferDeadlineDefaultsAndUsesTrustedClock(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 1, 2, 3, 4, time.UTC)
	candidate, _ := NewOfferCandidate("review", 0)
	draft, err := candidate.draft(AdmissionStamp{workVersion: 1, iteration: 1}, now)
	if err != nil {
		t.Fatalf("draft() error = %v", err)
	}
	if got, want := draft.deadlineUnixNano, now.Add(DefaultOfferDeadline).UnixNano(); got != want {
		t.Fatalf("deadline = %d, want %d", got, want)
	}
}

func TestOfferDeadlineRejectsUnixNanosecondOverflow(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, int64(model.MaxSQLiteInteger)-int64(MaximumOfferDeadline)+1).UTC()
	candidate, _ := NewOfferCandidate("review", MaximumOfferDeadline)
	if _, err := candidate.draft(AdmissionStamp{workVersion: 1, iteration: 1}, now); !errors.Is(err, ErrInvalidStamp) {
		t.Fatalf("overflow error = %v", err)
	}
}
