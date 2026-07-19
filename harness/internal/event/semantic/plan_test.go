package semantic

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestPlanPolicyProjectsOfferIntoCompleteStoreSpec(t *testing.T) {
	fixture := newSemanticPlanFixture(t)
	policy, err := planPolicy(policySnapshot{local: fixture.local,
		imported: fixture.offer, decisionAt: fixture.at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := storePlanSpec(policy)
	if err != nil {
		t.Fatal(err)
	}
	assertOfferStorePlanShape(t, spec)
	assertOfferStoreWork(t, fixture, spec.Work)
	assertOfferStoreHandlingAndResponse(t, fixture, spec)
}

func assertOfferStorePlanShape(t *testing.T, spec store.PeerInboxSemanticPlanSpec) {
	t.Helper()
	if spec.Disposition != store.PeerInboxSemanticApply || spec.Diagnostic != "" ||
		spec.Work == nil || spec.Handling == nil || spec.Settlement != nil ||
		len(spec.Responses) != 1 {
		t.Fatalf("projected Store plan shape = %#v", spec)
	}
}

func assertOfferStoreWork(t *testing.T, fixture semanticPlanFixture,
	work *store.PeerInboxSemanticWorkIntentSpec,
) {
	t.Helper()
	if work.Source != store.PeerInboxSemanticFromImportedEvent || work.ResponseOrdinal != 0 ||
		work.WorkRef != fixture.offer.Scope().WorkRef() ||
		work.ChannelID != fixture.offer.Scope().ChannelID() || work.ExpectedVersion != 0 ||
		work.ExpectedState != "" || work.ExpectedIteration != 0 || work.NextVersion != 1 ||
		work.NextState != model.WorkOffered || work.NextIteration != 1 ||
		work.DeadlineUnixNano != fixture.deadline.UnixNano() ||
		work.ObservedAtUnixNano != fixture.offer.AcceptedAt().UnixNano() ||
		work.StateData.String() != fixture.offer.Payload().String() {
		t.Fatalf("projected Work intent = %#v", work)
	}
}

func assertOfferStoreHandlingAndResponse(t *testing.T, fixture semanticPlanFixture,
	spec store.PeerInboxSemanticPlanSpec,
) {
	t.Helper()
	handling, response := spec.Handling, spec.Responses[0]
	if handling.Source != store.PeerInboxSemanticFromImportedEvent ||
		handling.WorkRef != spec.Work.WorkRef || handling.LocalRole != model.WorkRoleReviewer ||
		handling.SourceEventType != model.EventReviewOffered ||
		response.EventType != model.EventReviewOutcome || response.Cause != fixture.offer.Key() ||
		response.Payload.IsZero() {
		t.Fatalf("projected Handling/response = (%#v,%#v)", handling, response)
	}
}

func TestPlanPolicyKeepsRetryOutsideTerminalStorePlan(t *testing.T) {
	fixture := newSemanticPlanFixture(t)
	payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
	accepted := fixture.event(t, model.EventReviewAccepted, payload, "accepted")
	policy, err := planPolicy(policySnapshot{local: fixture.local, imported: accepted,
		decisionAt: fixture.at.Add(time.Second)})
	if err != nil || policy.Disposition() != teamwork.ImportRetry ||
		policy.Diagnostic() != "missing_work" {
		t.Fatalf("retry policy = (%s,%q,%v)", policy.Disposition(), policy.Diagnostic(), err)
	}
	if _, err := storePlanSpec(policy); err == nil {
		t.Fatal("retry policy was projected as a terminal Store plan")
	}
	if _, err := planPolicy(policySnapshot{local: fixture.local, imported: fixture.offer,
		causalEvents: []model.Event{{}}, decisionAt: fixture.at}); err == nil {
		t.Fatal("zero causal Event was accepted")
	}
}

func TestPlanResultZeroAndRetryCannotExposeTerminalPlan(t *testing.T) {
	if _, ok := (PlanResult{}).Plan(); ok {
		t.Fatal("zero result exposed a terminal plan")
	}
	retry := PlanResult{retry: true, retryDiagnostic: "missing_work"}
	if _, ok := retry.Plan(); ok {
		t.Fatal("retry result exposed a terminal plan")
	}
	if diagnostic, ok := retry.RetryDiagnostic(); !ok || diagnostic != "missing_work" {
		t.Fatalf("retry diagnostic = (%q,%t)", diagnostic, ok)
	}
	if _, ok := (PlanResult{terminal: true}).Plan(); !ok {
		t.Fatal("terminal result did not expose its plan")
	}
}

func TestStorePlanClosedSetMappingsAreExhaustive(t *testing.T) {
	dispositionCases := []struct {
		input teamwork.ImportDisposition
		want  store.PeerInboxSemanticDisposition
	}{{teamwork.ImportApply, store.PeerInboxSemanticApply},
		{teamwork.ImportReject, store.PeerInboxSemanticReject},
		{teamwork.ImportConflict, store.PeerInboxSemanticConflict},
		{teamwork.ImportReceiptOnly, store.PeerInboxSemanticReceiptOnly}}
	for _, test := range dispositionCases {
		if got, err := storeDisposition(test.input); err != nil || got != test.want {
			t.Fatalf("disposition %q = (%q,%v), want %q", test.input, got, err, test.want)
		}
	}
	workCases := []struct {
		input teamwork.ImportWorkSource
		want  store.PeerInboxSemanticEffectSource
	}{{teamwork.ImportWorkFromEvent, store.PeerInboxSemanticFromImportedEvent},
		{teamwork.ImportWorkFromResponse, store.PeerInboxSemanticFromLocalResponse}}
	for _, test := range workCases {
		if got, err := storeWorkSource(test.input); err != nil || got != test.want {
			t.Fatalf("Work source %q = (%q,%v), want %q", test.input, got, err, test.want)
		}
	}
	handlingCases := []struct {
		input teamwork.ImportHandlingSource
		want  store.PeerInboxSemanticEffectSource
	}{{teamwork.ImportHandlingFromEvent, store.PeerInboxSemanticFromImportedEvent},
		{teamwork.ImportHandlingFromResponse, store.PeerInboxSemanticFromLocalResponse}}
	for _, test := range handlingCases {
		if got, err := storeHandlingSource(test.input); err != nil || got != test.want {
			t.Fatalf("Handling source %q = (%q,%v), want %q", test.input, got, err, test.want)
		}
	}
	settlementCases := []struct {
		input string
		want  store.PeerInboxSemanticSettlementDisposition
	}{{"superseded_cancelled", store.PeerInboxSemanticSupersededCancelled},
		{"superseded_expired", store.PeerInboxSemanticSupersededExpired}}
	for _, test := range settlementCases {
		if got, err := storeSettlementDisposition(test.input); err != nil || got != test.want {
			t.Fatalf("settlement %q = (%q,%v), want %q", test.input, got, err, test.want)
		}
	}
	if _, err := storeWorkSource(teamwork.ImportWorkSource("unknown")); err == nil {
		t.Fatal("unknown Work source was accepted")
	}
	if _, err := storeDisposition(teamwork.ImportRetry); err == nil {
		t.Fatal("retry disposition was accepted as terminal")
	}
	if _, err := storeHandlingSource(teamwork.ImportHandlingSource("unknown")); err == nil {
		t.Fatal("unknown Handling source was accepted")
	}
	if _, err := storeSettlementDisposition("unknown"); err == nil {
		t.Fatal("unknown settlement disposition was accepted")
	}
}

type semanticPlanFixture struct {
	local    model.PeerID
	home     model.PeerID
	epoch    model.OriginEpoch
	channel  model.ChannelID
	work     model.WorkRef
	head     model.RecordHead
	at       time.Time
	deadline time.Time
	offer    model.Event
}

func newSemanticPlanFixture(t *testing.T) semanticPlanFixture {
	t.Helper()
	local, _ := model.ParsePeerID("peer-semantic-local")
	home, _ := model.ParsePeerID("peer-semantic-home")
	epoch, _ := model.ParseOriginEpoch("epoch-semantic-home")
	channel, _ := model.ParseChannelID("channel-semantic")
	workID, _ := model.ParseWorkID("work-semantic")
	work, _ := model.NewWorkRef(home, workID)
	head, _ := model.NewRecordHead(1, model.Sum([]byte("semantic-head")))
	at := time.Date(2026, time.July, 19, 8, 0, 0, 0, time.UTC)
	fixture := semanticPlanFixture{local: local, home: home, epoch: epoch,
		channel: channel, work: work, head: head, at: at, deadline: at.Add(time.Hour)}
	payload, _ := model.JSONFrom(struct {
		Content     string `json:"content"`
		Deadline    string `json:"deadline"`
		Iteration   uint8  `json:"iteration"`
		WorkVersion uint64 `json:"work_version"`
	}{"review", fixture.deadline.Format(time.RFC3339Nano), 1, 1})
	fixture.offer = fixture.event(t, model.EventReviewOffered, payload, "offer")
	return fixture
}

func (fixture semanticPlanFixture) event(t *testing.T, eventType model.EventType,
	payload model.JSON, suffix string,
) model.Event {
	t.Helper()
	scope, err := model.NewEventScope(fixture.channel, fixture.home, fixture.epoch,
		1, 1, fixture.head, fixture.head, fixture.work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{fixture.local})
	id, _ := model.ParseEventID("event-semantic-" + suffix)
	var causes []model.EventKey
	if eventType.RequiresAdmissionCausality() {
		causeEpoch, _ := model.ParseOriginEpoch("epoch-semantic-cause")
		causeID, _ := model.ParseEventID("event-semantic-cause-" + suffix)
		cause, _ := model.NewEventKey(fixture.local, causeEpoch, causeID)
		causes = []model.EventKey{cause}
	}
	event, err := model.NewEvent(model.EventSpec{ID: id, Scope: scope,
		Source: model.EventSourceImported, ActorPrincipal: "principal-semantic-home",
		Type: eventType, Audience: audience, Summary: "semantic plan fixture", Payload: payload,
		CausedBy:  causes,
		CreatedAt: fixture.at, AcceptedAt: fixture.at})
	if err != nil {
		t.Fatal(err)
	}
	return event
}
