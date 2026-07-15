package event

import (
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

const (
	DefaultOfferDeadline = teamwork.DefaultOfferDeadline
	MinimumOfferDeadline = teamwork.MinimumOfferDeadline
	MaximumOfferDeadline = teamwork.MaximumOfferDeadline
)

// AgentCandidate is the closed set of semantic actions accepted from a
// managed Agent. Its private marker prevents generic Event implementations.
type AgentCandidate interface {
	agentCandidate()
	draft(AdmissionStamp, time.Time) (eventDraft, error)
}

type OfferCandidate struct {
	content  string
	deadline time.Duration
}

func NewOfferCandidate(content string, deadline time.Duration) (OfferCandidate, error) {
	if err := validateContent("offer content", content, true); err != nil {
		return OfferCandidate{}, err
	}
	if err := validateDeadline(deadline); err != nil {
		return OfferCandidate{}, err
	}
	return OfferCandidate{content, deadline}, nil
}

func (OfferCandidate) agentCandidate() {}
func (candidate OfferCandidate) draft(stamp AdmissionStamp, now time.Time) (eventDraft, error) {
	if err := validateContent("offer content", candidate.content, true); err != nil {
		return eventDraft{}, err
	}
	if err := validateDeadline(candidate.deadline); err != nil {
		return eventDraft{}, err
	}
	duration := candidate.deadline
	if duration == 0 {
		duration = DefaultOfferDeadline
	}
	nowUnixNano := now.UnixNano()
	if nowUnixNano > int64(model.MaxSQLiteInteger)-int64(duration) {
		return eventDraft{}, stampError("trusted clock plus offer deadline overflows Unix nanoseconds")
	}
	deadline := nowUnixNano + int64(duration)
	payload := offerPayload{candidate.content, formatTimestamp(deadline), stamp.workVersion, stamp.iteration}
	return eventDraft{model.EventReviewOffered, "Review offered", payload, true, false, deadline}, nil
}

type AcceptCandidate struct{ note string }

func NewAcceptCandidate(note string) (AcceptCandidate, error) {
	if err := validateContent("accept note", note, false); err != nil {
		return AcceptCandidate{}, err
	}
	return AcceptCandidate{note}, nil
}

func (AcceptCandidate) agentCandidate() {}
func (candidate AcceptCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("accept note", candidate.note, false); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewAcceptRequested, "Review acceptance requested",
		notePayload{candidate.note, stamp.workVersion, stamp.iteration}, false, true, stamp.deadlineUnixNano}, nil
}

type DeclineCandidate struct{ reason string }

func NewDeclineCandidate(reason string) (DeclineCandidate, error) {
	if err := validateContent("decline reason", reason, true); err != nil {
		return DeclineCandidate{}, err
	}
	return DeclineCandidate{reason}, nil
}

func (DeclineCandidate) agentCandidate() {}
func (candidate DeclineCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("decline reason", candidate.reason, true); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewDeclineRequested, "Review decline requested",
		contentPayload{candidate.reason, stamp.workVersion, stamp.iteration}, false, true, stamp.deadlineUnixNano}, nil
}

type DeliverCandidate struct{ summary string }

func NewDeliverCandidate(summary string) (DeliverCandidate, error) {
	if err := validateContent("delivery summary", summary, true); err != nil {
		return DeliverCandidate{}, err
	}
	return DeliverCandidate{summary}, nil
}

func (DeliverCandidate) agentCandidate() {}
func (candidate DeliverCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("delivery summary", candidate.summary, true); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewDeliveryReady, "Review delivery ready",
		contentPayload{candidate.summary, stamp.workVersion, stamp.iteration}, true, true, stamp.deadlineUnixNano}, nil
}

type ReworkCandidate struct{ correction string }

func NewReworkCandidate(correction string) (ReworkCandidate, error) {
	if err := validateContent("rework correction", correction, true); err != nil {
		return ReworkCandidate{}, err
	}
	return ReworkCandidate{correction}, nil
}

func (ReworkCandidate) agentCandidate() {}
func (candidate ReworkCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("rework correction", candidate.correction, true); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewReworkRequested, "Review rework requested",
		contentPayload{candidate.correction, stamp.workVersion, stamp.iteration}, true, true, stamp.deadlineUnixNano}, nil
}

type CloseCandidate struct{ note string }

func NewCloseCandidate(note string) (CloseCandidate, error) {
	if err := validateContent("close note", note, false); err != nil {
		return CloseCandidate{}, err
	}
	return CloseCandidate{note}, nil
}

func (CloseCandidate) agentCandidate() {}
func (candidate CloseCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("close note", candidate.note, false); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewClosed, "Review closed",
		notePayload{candidate.note, stamp.workVersion, stamp.iteration}, false, true, stamp.deadlineUnixNano}, nil
}

type CancelCandidate struct{ reason string }

func NewCancelCandidate(reason string) (CancelCandidate, error) {
	if err := validateContent("cancel reason", reason, true); err != nil {
		return CancelCandidate{}, err
	}
	return CancelCandidate{reason}, nil
}

func (CancelCandidate) agentCandidate() {}
func (candidate CancelCandidate) draft(stamp AdmissionStamp, _ time.Time) (eventDraft, error) {
	if err := validateContent("cancel reason", candidate.reason, true); err != nil {
		return eventDraft{}, err
	}
	return eventDraft{model.EventReviewCancelled, "Review cancelled",
		contentPayload{candidate.reason, stamp.workVersion, stamp.iteration}, false, true, stamp.deadlineUnixNano}, nil
}

func validateDeadline(value time.Duration) error {
	if value != 0 && (value < MinimumOfferDeadline || value > MaximumOfferDeadline) {
		return candidateError("offer deadline", model.ErrInvalid,
			"must be zero/default or within %s..%s", MinimumOfferDeadline, MaximumOfferDeadline)
	}
	return nil
}

type offerPayload struct {
	Content     string `json:"content"`
	Deadline    string `json:"deadline"`
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type contentPayload struct {
	Content     string `json:"content"`
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type notePayload struct {
	Note        string `json:"note"`
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}
