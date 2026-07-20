package store

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestValidateOperationEventsClosesControllerBypass(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	allowed := []struct {
		typeValue model.EventType
		payload   string
	}{
		{model.EventReviewAccepted, `{"iteration":1,"work_version":1}`},
		{model.EventReviewAcceptRejected, `{"diagnostic_code":"race","iteration":1,"work_version":1}`},
		{model.EventReviewDelivered, `{"iteration":1,"work_version":2}`},
		{model.EventReviewDeclined, `{"iteration":1,"work_version":1}`},
		{model.EventReviewExpired, `{"deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`},
		{model.EventReviewOutcome, `{"decision_ref":"d","diagnostic_code":"fallback","iteration":1,"status":"accepted","work_version":1}`},
	}
	for _, test := range allowed {
		event := eventWithCodecCause(t, newCodecEvent(t, test.typeValue, test.payload, now))
		if err := validateOperationEvents(nil, []model.Event{event}, false); err != nil {
			t.Errorf("controller %s error = %v", test.typeValue, err)
		}
	}
	for _, forbidden := range []model.EventType{
		model.EventReviewOffered, model.EventReviewAcceptRequested, model.EventReviewDeclineRequested,
		model.EventReviewDeliveryReady, model.EventReviewReworkRequested, model.EventReviewClosed,
		model.EventReviewCancelled,
	} {
		event := eventWithCodecCause(t, newCodecEvent(t, forbidden, `{"iteration":1,"work_version":1}`, now))
		if err := validateOperationEvents(nil, []model.Event{event}, false); err == nil {
			t.Errorf("controller accepted %s", forbidden)
		}
	}
	valid := eventWithCodecCause(t, newCodecEvent(t, model.EventReviewAccepted, `{"iteration":1,"work_version":1}`, now))
	if err := validateOperationEvents(nil, []model.Event{valid, valid}, false); err == nil {
		t.Fatal("controller accepted a batch")
	}
}

func TestValidateOperationEventsRequiresExactActionAndExpandedSemantics(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	offer := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	authority := &LocalOperationAuthority{Kind: model.OperationTeamworkOffer}
	if err := validateOperationEvents(authority, []model.Event{offer}, false); err != nil {
		t.Fatalf("offer validation error = %v", err)
	}
	changed := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"different","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	if err := validateOperationEvents(authority, []model.Event{offer, changed}, false); err == nil {
		t.Fatal("expanded offer content drift was accepted")
	}
	expired := eventWithCodecCause(t, newCodecEvent(t, model.EventReviewExpired,
		`{"deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now))
	cancel := &LocalOperationAuthority{Kind: model.OperationTeamworkCancel}
	if err := validateOperationEvents(cancel, []model.Event{expired}, false); err == nil {
		t.Fatal("cancel operation was committed as expiry success")
	}
}

func eventWithCodecCause(t *testing.T, event model.Event) model.Event {
	t.Helper()
	causePeer, _ := model.ParsePeerID("peer-cause")
	causeEpoch, _ := model.ParseOriginEpoch("epoch-cause")
	causeID, _ := model.ParseEventID("event-cause")
	cause, _ := model.NewEventKey(causePeer, causeEpoch, causeID)
	spec := event.Spec()
	spec.CausedBy = []model.EventKey{cause}
	result, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
