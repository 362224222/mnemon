package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type closedPayloadFacts struct {
	WorkVersion      uint64
	Iteration        uint8
	DeadlineUnixNano int64
}

type versionPayload struct {
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

func decodeClosedEventPayload(event model.Event) (closedPayloadFacts, error) {
	var version versionPayload
	var deadline string
	switch event.Type() {
	case model.EventReviewOffered:
		value := struct {
			Content  string `json:"content"`
			Deadline string `json:"deadline"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil || value.Content == "" {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version, deadline = value.versionPayload, value.Deadline
	case model.EventReviewAcceptRequested, model.EventReviewClosed:
		value := struct {
			Note string `json:"note"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version = value.versionPayload
	case model.EventReviewDeclineRequested, model.EventReviewDeliveryReady,
		model.EventReviewReworkRequested, model.EventReviewCancelled:
		value := struct {
			Content string `json:"content"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil || value.Content == "" {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version = value.versionPayload
	case model.EventReviewAccepted, model.EventReviewDelivered, model.EventReviewDeclined:
		if err := decodeExactPayload(event, &version); err != nil {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
	case model.EventReviewAcceptRejected:
		value := struct {
			DiagnosticCode string `json:"diagnostic_code"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil || value.DiagnosticCode == "" {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version = value.versionPayload
	case model.EventReviewExpired:
		value := struct {
			Deadline string `json:"deadline"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version, deadline = value.versionPayload, value.Deadline
	case model.EventReviewOutcome:
		value := struct {
			Status         string `json:"status"`
			DiagnosticCode string `json:"diagnostic_code"`
			DecisionRef    string `json:"decision_ref"`
			versionPayload
		}{}
		if err := decodeExactPayload(event, &value); err != nil ||
			(value.Status != "accepted" && value.Status != "rejected" && value.Status != "conflicted") ||
			value.DiagnosticCode == "" || value.DecisionRef == "" {
			return closedPayloadFacts{}, closedPayloadError(err)
		}
		version = value.versionPayload
	default:
		return closedPayloadFacts{}, errors.New("commit local acceptance: Event payload type is outside closed Teamwork schema")
	}
	if version.WorkVersion == 0 || version.Iteration < 1 || version.Iteration > 2 {
		return closedPayloadFacts{}, errors.New("commit local acceptance: payload lacks frozen Work version/iteration")
	}
	facts := closedPayloadFacts{WorkVersion: version.WorkVersion, Iteration: version.Iteration}
	if deadline != "" {
		parsed, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil || parsed.UnixNano() <= 0 || parsed.UTC().Format(time.RFC3339Nano) != deadline {
			return closedPayloadFacts{}, errors.New("commit local acceptance: payload deadline is not canonical UTC")
		}
		facts.DeadlineUnixNano = parsed.UnixNano()
	}
	return facts, nil
}

func decodeExactPayload(event model.Event, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(event.Payload().Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func closedPayloadError(err error) error {
	if err == nil {
		return errors.New("commit local acceptance: required payload content is empty")
	}
	return fmt.Errorf("commit local acceptance: closed Event payload: %w", err)
}
