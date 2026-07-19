package event

import (
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ControllerCandidate is the closed set of mnemond-only decisions. It is
// intentionally disjoint from AgentCandidate so Agent actions cannot claim a
// home decision, timer expiry, or semantic receipt outcome.
type ControllerCandidate interface {
	controllerCandidate()
	draft(AdmissionStamp, time.Time) (eventDraft, error)
}

type AcceptedDecision struct{}

func (AcceptedDecision) controllerCandidate() {}
func (AcceptedDecision) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	return eventDraft{eventType: model.EventReviewAccepted, summary: "Review accepted",
		payload:          decisionPayload{stamp.workVersion, stamp.iteration},
		deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type AcceptRejectedDecision struct{ diagnosticCode string }

func NewAcceptRejectedDecision(diagnosticCode string) (AcceptRejectedDecision, error) {
	if err := validateToken("accept rejection diagnostic", diagnosticCode); err != nil {
		return AcceptRejectedDecision{}, err
	}
	return AcceptRejectedDecision{diagnosticCode}, nil
}

func (AcceptRejectedDecision) controllerCandidate() {}
func (decision AcceptRejectedDecision) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateToken("accept rejection diagnostic", decision.diagnosticCode); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{eventType: model.EventReviewAcceptRejected, summary: "Review acceptance rejected",
		payload:          diagnosticPayload{decision.diagnosticCode, stamp.workVersion, stamp.iteration},
		deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type DeliveredDecision struct{}

func (DeliveredDecision) controllerCandidate() {}
func (DeliveredDecision) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	return eventDraft{eventType: model.EventReviewDelivered, summary: "Review delivered",
		payload:          decisionPayload{stamp.workVersion, stamp.iteration},
		deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type DeclinedDecision struct{}

func (DeclinedDecision) controllerCandidate() {}
func (DeclinedDecision) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	return eventDraft{eventType: model.EventReviewDeclined, summary: "Review declined",
		payload:          decisionPayload{stamp.workVersion, stamp.iteration},
		deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type ExpiredDecision struct{}

func (ExpiredDecision) controllerCandidate() {}
func (ExpiredDecision) draft(stamp AdmissionStamp, now time.Time) (eventDraft, error) {
	if stamp.deadlineUnixNano <= 0 {
		return eventDraft{}, candidateError("Work deadline", model.ErrInvalid, "is required for expiry")
	}
	if now.UnixNano() < stamp.deadlineUnixNano {
		return eventDraft{}, candidateError("Work deadline", model.ErrInvariant,
			"trusted clock has not reached the deadline")
	}
	payload := expiryPayload{formatTimestamp(stamp.deadlineUnixNano), stamp.workVersion, stamp.iteration}
	return eventDraft{eventType: model.EventReviewExpired, summary: "Review expired",
		payload: payload, deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type OutcomeStatus string

const (
	OutcomeAccepted   OutcomeStatus = "accepted"
	OutcomeRejected   OutcomeStatus = "rejected"
	OutcomeConflicted OutcomeStatus = "conflicted"
)

func (status OutcomeStatus) Valid() bool {
	return status == OutcomeAccepted || status == OutcomeRejected || status == OutcomeConflicted
}

type OutcomeDecision struct {
	status         OutcomeStatus
	diagnosticCode string
	decisionRef    string
}

func NewOutcomeDecision(status OutcomeStatus, diagnosticCode, decisionRef string) (OutcomeDecision, error) {
	if !status.Valid() {
		return OutcomeDecision{}, candidateError("outcome status", model.ErrInvalid, "unknown closed enum value")
	}
	if err := validateToken("outcome diagnostic", diagnosticCode); err != nil {
		return OutcomeDecision{}, err
	}
	if err := validateToken("outcome decision ref", decisionRef); err != nil {
		return OutcomeDecision{}, err
	}
	return OutcomeDecision{status, diagnosticCode, decisionRef}, nil
}

func (OutcomeDecision) controllerCandidate() {}
func (decision OutcomeDecision) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if !decision.status.Valid() {
		return eventDraft{}, candidateError("outcome status", model.ErrInvalid, "unknown closed enum value")
	}
	if err := validateToken("outcome diagnostic", decision.diagnosticCode); err != nil {
		return eventDraft{}, err
	}
	if err := validateToken("outcome decision ref", decision.decisionRef); err != nil {
		return eventDraft{}, err
	}
	payload := outcomePayload{decision.status, decision.diagnosticCode, decision.decisionRef,
		stamp.workVersion, stamp.iteration}
	return eventDraft{eventType: model.EventReviewOutcome, summary: "Review outcome",
		payload: payload, deadlineUnixNano: stamp.deadlineUnixNano}, nil
}

type decisionPayload struct {
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type diagnosticPayload struct {
	DiagnosticCode string `json:"diagnostic_code"`
	WorkVersion    uint64 `json:"work_version"`
	Iteration      uint8  `json:"iteration"`
}

type expiryPayload struct {
	Deadline    string `json:"deadline"`
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type outcomePayload struct {
	Status         OutcomeStatus `json:"status"`
	DiagnosticCode string        `json:"diagnostic_code"`
	DecisionRef    string        `json:"decision_ref"`
	WorkVersion    uint64        `json:"work_version"`
	Iteration      uint8         `json:"iteration"`
}
