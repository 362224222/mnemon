package teamwork

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPlanImportedEventCoversAllClosedTeamworkEvents(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "remote-offered")
	offeredWork := f.work(t, model.WorkOffered, 1, 1, offered)
	localOffered := f.asSource(t, offered, model.EventSourceLocal)
	localOfferedWork := f.work(t, model.WorkOffered, 1, 1, localOffered)
	localAccept := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "local-accept")
	localDecline := f.event(t, model.EventSourceLocal, model.EventReviewDeclineRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"no","iteration":1,"work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "local-decline")
	accepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localAccept.Key()}, nil,
		f.base.Add(2*time.Minute), "remote-accepted")
	activeWork := f.work(t, model.WorkActive, 2, 1, accepted)
	localAccepted := f.asSource(t, accepted, model.EventSourceLocal)
	localActiveWork := f.work(t, model.WorkActive, 2, 1, localAccepted)
	resultRoot := importArtifactRef(t, "review-result", model.ArtifactProduced)
	resultRootTwo := importArtifactRef(t, "review-result-two", model.ArtifactProduced)
	referencedResultRoot := importArtifactRef(t, "review-result", model.ArtifactReferenced)
	referencedResultRootTwo := importArtifactRef(t, "review-result-two", model.ArtifactReferenced)
	localDelivery := f.event(t, model.EventSourceLocal, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"result","iteration":1,"work_version":2}`,
		[]model.EventKey{accepted.Key()}, []model.ArtifactRef{resultRootTwo, resultRoot},
		f.base.Add(3*time.Minute), "local-delivery")
	delivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{referencedResultRoot, referencedResultRootTwo},
		f.base.Add(4*time.Minute), "remote-delivered")
	deliveredWork := f.work(t, model.WorkDelivered, 3, 1, delivered)
	wrongUsageDelivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{resultRoot, resultRootTwo},
		f.base.Add(4*time.Minute), "remote-delivered-wrong-usage")
	substitutedDelivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{referencedResultRoot,
			importArtifactRef(t, "other-review-result", model.ArtifactReferenced)},
		f.base.Add(4*time.Minute), "remote-delivered-substituted-root")
	omittedDelivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{referencedResultRoot},
		f.base.Add(4*time.Minute), "remote-delivered-omitted-root")
	lateLocalAccept := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"late","work_version":2}`,
		[]model.EventKey{accepted.Key()}, nil, f.base.Add(3*time.Minute), "late-local-accept")
	lateAcceptRejected := f.event(t, model.EventSourceImported, model.EventReviewAcceptRejected, f.home,
		[]model.PeerID{f.reviewer}, `{"diagnostic_code":"stale_request","iteration":1,"work_version":2}`,
		[]model.EventKey{lateLocalAccept.Key()}, nil, f.base.Add(4*time.Minute), "late-accept-rejected")

	tests := []struct {
		name             string
		local            model.PeerID
		event            model.Event
		current          *model.ReviewWork
		facts            []ImportEventFact
		wantDisposition  ImportDisposition
		wantState        model.WorkState
		wantVersion      uint64
		wantResponse     model.EventType
		wantHandling     bool
		wantHandlingRole model.WorkRole
		wantSettlement   bool
	}{
		{
			name: "review.offered creates reviewer mirror and handling", local: f.reviewer, event: offered,
			wantDisposition: ImportApply, wantState: model.WorkOffered, wantVersion: 1,
			wantResponse: model.EventReviewOutcome, wantHandling: true, wantHandlingRole: model.WorkRoleReviewer,
		},
		{
			name: "review.accept.requested advances local home", local: f.home,
			event: f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
				[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
				[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "remote-accept-request"),
			current: importWorkPointer(localOfferedWork), facts: []ImportEventFact{importFact(t, localOffered)},
			wantDisposition: ImportApply, wantState: model.WorkActive, wantVersion: 2,
			wantResponse: model.EventReviewAccepted,
		},
		{
			name: "review.decline.requested advances local home", local: f.home,
			event: f.event(t, model.EventSourceImported, model.EventReviewDeclineRequested, f.reviewer,
				[]model.PeerID{f.home}, `{"content":"no","iteration":1,"work_version":1}`,
				[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "remote-decline-request"),
			current: importWorkPointer(localOfferedWork), facts: []ImportEventFact{importFact(t, localOffered)},
			wantDisposition: ImportApply, wantState: model.WorkDeclined, wantVersion: 2,
			wantResponse: model.EventReviewDeclined,
		},
		{
			name: "review.delivery.ready advances home and creates initiator handling", local: f.home,
			event: f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
				[]model.PeerID{f.home}, `{"content":"result","iteration":1,"work_version":2}`,
				[]model.EventKey{accepted.Key()}, []model.ArtifactRef{resultRoot},
				f.base.Add(3*time.Minute), "remote-delivery-ready"),
			current: importWorkPointer(localActiveWork), facts: []ImportEventFact{importFact(t, localAccepted)},
			wantDisposition: ImportApply, wantState: model.WorkDelivered, wantVersion: 3,
			wantResponse: model.EventReviewDelivered, wantHandling: true, wantHandlingRole: model.WorkRoleInitiator,
		},
		{
			name: "review.accepted advances reviewer mirror", local: f.reviewer, event: accepted,
			current: importWorkPointer(offeredWork), facts: []ImportEventFact{importFact(t, localAccept)},
			wantDisposition: ImportApply, wantState: model.WorkActive, wantVersion: 2,
			wantResponse: model.EventReviewOutcome, wantHandling: true, wantHandlingRole: model.WorkRoleReviewer,
		},
		{
			name: "review.accept_rejected is non-mutating", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewAcceptRejected, f.home,
				[]model.PeerID{f.reviewer}, `{"diagnostic_code":"stale_request","iteration":1,"work_version":1}`,
				[]model.EventKey{localAccept.Key()}, nil, f.base.Add(2*time.Minute), "remote-accept-rejected"),
			current: importWorkPointer(offeredWork), facts: []ImportEventFact{importFact(t, localAccept)},
			wantDisposition: ImportApply, wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.accept_rejected rejects non-T0 tuple", local: f.reviewer,
			event: lateAcceptRejected, current: importWorkPointer(activeWork),
			facts:           []ImportEventFact{importFact(t, lateLocalAccept)},
			wantDisposition: ImportConflict, wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.delivered advances reviewer mirror without handling", local: f.reviewer, event: delivered,
			current: importWorkPointer(activeWork), facts: []ImportEventFact{importFact(t, localDelivery)},
			wantDisposition: ImportApply, wantState: model.WorkDelivered, wantVersion: 3,
			wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.delivered rejects produced role instead of referenced", local: f.reviewer,
			event: wrongUsageDelivered, current: importWorkPointer(activeWork),
			facts:           []ImportEventFact{importFact(t, localDelivery)},
			wantDisposition: ImportConflict, wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.delivered rejects Artifact root substitution", local: f.reviewer,
			event: substitutedDelivered, current: importWorkPointer(activeWork),
			facts:           []ImportEventFact{importFact(t, localDelivery)},
			wantDisposition: ImportConflict, wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.delivered rejects Artifact root omission", local: f.reviewer,
			event: omittedDelivered, current: importWorkPointer(activeWork),
			facts:           []ImportEventFact{importFact(t, localDelivery)},
			wantDisposition: ImportConflict, wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.rework_requested advances mirror and creates handling", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewReworkRequested, f.home,
				[]model.PeerID{f.reviewer}, `{"content":"fix","iteration":1,"work_version":3}`,
				[]model.EventKey{delivered.Key()}, nil, f.base.Add(5*time.Minute), "remote-rework"),
			current: importWorkPointer(deliveredWork), facts: []ImportEventFact{importFact(t, delivered)},
			wantDisposition: ImportApply, wantState: model.WorkRework, wantVersion: 4,
			wantResponse: model.EventReviewOutcome, wantHandling: true, wantHandlingRole: model.WorkRoleReviewer,
		},
		{
			name: "review.closed terminates mirror", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewClosed, f.home,
				[]model.PeerID{f.reviewer}, `{"iteration":1,"note":"done","work_version":3}`,
				[]model.EventKey{delivered.Key()}, nil, f.base.Add(5*time.Minute), "remote-closed"),
			current: importWorkPointer(deliveredWork), facts: []ImportEventFact{importFact(t, delivered)},
			wantDisposition: ImportApply, wantState: model.WorkClosed, wantVersion: 4,
			wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.declined terminates mirror", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewDeclined, f.home,
				[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localDecline.Key()}, nil,
				f.base.Add(2*time.Minute), "remote-declined"),
			current: importWorkPointer(offeredWork), facts: []ImportEventFact{importFact(t, localDecline)},
			wantDisposition: ImportApply, wantState: model.WorkDeclined, wantVersion: 2,
			wantResponse: model.EventReviewOutcome,
		},
		{
			name: "review.cancelled settles superseded handling", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewCancelled, f.home,
				[]model.PeerID{f.reviewer}, `{"content":"stop","iteration":1,"work_version":2}`,
				[]model.EventKey{accepted.Key()}, nil, f.base.Add(5*time.Minute), "remote-cancelled"),
			current: importWorkPointer(activeWork), facts: []ImportEventFact{importFact(t, accepted)},
			wantDisposition: ImportApply, wantState: model.WorkCancelled, wantVersion: 3,
			wantResponse: model.EventReviewOutcome, wantSettlement: true,
		},
		{
			name: "review.expired trusts home acceptance time and settles handling", local: f.reviewer,
			event: f.event(t, model.EventSourceImported, model.EventReviewExpired, f.home,
				[]model.PeerID{f.reviewer}, f.expiryPayload(2, 1), []model.EventKey{accepted.Key()}, nil,
				f.deadline, "remote-expired"),
			current: importWorkPointer(activeWork), facts: []ImportEventFact{importFact(t, accepted)},
			wantDisposition: ImportApply, wantState: model.WorkExpired, wantVersion: 3,
			wantResponse: model.EventReviewOutcome, wantSettlement: true,
		},
		{
			name: "review.outcome is receipt only and stops the loop", local: f.home,
			event: f.event(t, model.EventSourceImported, model.EventReviewOutcome, f.reviewer,
				[]model.PeerID{f.home}, outcomePayload("rejected", "remote_rejected", accepted.ID().String(), 1, 1),
				[]model.EventKey{accepted.Key()}, nil, f.base.Add(3*time.Minute), "remote-outcome"),
			current: importWorkPointer(f.work(t, model.WorkActive, 2, 1,
				f.asSource(t, accepted, model.EventSourceLocal))),
			facts:           []ImportEventFact{importFact(t, f.asSource(t, accepted, model.EventSourceLocal))},
			wantDisposition: ImportReceiptOnly,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanImportedEvent(ImportPlanSpec{
				LocalPeerID: test.local, Event: test.event, Current: test.current, Facts: test.facts,
				Now: f.base.Add(10 * time.Minute),
			})
			if err != nil {
				t.Fatalf("PlanImportedEvent() error = %v", err)
			}
			if plan.Disposition() != test.wantDisposition {
				t.Fatalf("Disposition() = %q, want %q (diagnostic %q)", plan.Disposition(), test.wantDisposition, plan.Diagnostic())
			}
			work, hasWork := plan.Work()
			if test.wantState == "" {
				if hasWork {
					t.Fatalf("unexpected Work intent %#v", work)
				}
			} else if !hasWork || work.NextState() != test.wantState || work.NextVersion() != test.wantVersion {
				t.Fatalf("Work intent = (%t,%s,v%d), want (%s,v%d)", hasWork, work.NextState(), work.NextVersion(), test.wantState, test.wantVersion)
			}
			handling, hasHandling := plan.Handling()
			if hasHandling != test.wantHandling || (hasHandling && handling.LocalRole() != test.wantHandlingRole) {
				t.Fatalf("Handling() = (%t,%s), want (%t,%s)", hasHandling, handling.LocalRole(), test.wantHandling, test.wantHandlingRole)
			}
			_, hasSettlement := plan.Settlement()
			if hasSettlement != test.wantSettlement {
				t.Fatalf("Settlement() = %t, want %t", hasSettlement, test.wantSettlement)
			}
			responses := plan.Responses()
			if test.wantResponse == "" {
				if len(responses) != 0 {
					t.Fatalf("responses = %v, want none", importResponseTypes(responses))
				}
			} else if len(responses) != 1 || responses[0].EventType() != test.wantResponse {
				t.Fatalf("responses = %v, want [%s]", importResponseTypes(responses), test.wantResponse)
			}
		})
	}
}

func TestPlanImportedParticipantDeadlineRaceIsOrderedAndSingleOutcome(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceLocal, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "local-deadline-offer")
	current := f.work(t, model.WorkOffered, 1, 1, offered)
	request := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "deadline-accept-request")
	fact := importFact(t, offered)

	tests := []struct {
		name       string
		now        time.Time
		want       ImportDisposition
		responses  []model.EventType
		wantState  model.WorkState
		wantStatus model.InboxStatus
	}{
		{"before", f.deadline.Add(-time.Nanosecond), ImportApply,
			[]model.EventType{model.EventReviewAccepted}, model.WorkActive, model.InboxAccepted},
		{"equal", f.deadline, ImportReject,
			[]model.EventType{model.EventReviewExpired, model.EventReviewAcceptRejected}, model.WorkExpired, model.InboxRejected},
		{"after", f.deadline.Add(time.Nanosecond), ImportReject,
			[]model.EventType{model.EventReviewExpired, model.EventReviewAcceptRejected}, model.WorkExpired, model.InboxRejected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanImportedEvent(ImportPlanSpec{f.home, request, &current, []ImportEventFact{fact}, test.now})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Disposition() != test.want || plan.InboxStatus() != test.wantStatus {
				t.Fatalf("plan = (%s,%s,%s), want (%s,%s)", plan.Disposition(), plan.InboxStatus(), plan.Diagnostic(), test.want, test.wantStatus)
			}
			responses := plan.Responses()
			if !reflect.DeepEqual(importResponseTypes(responses), test.responses) {
				t.Fatalf("response order = %v, want %v", importResponseTypes(responses), test.responses)
			}
			work, ok := plan.Work()
			if !ok || work.NextState() != test.wantState || work.Source() != ImportWorkFromResponse || work.ResponseOrdinal() != 0 {
				t.Fatalf("Work intent = (%t,%s,%s,%d)", ok, work.NextState(), work.Source(), work.ResponseOrdinal())
			}
			if len(responses) == 2 {
				if responses[0].Cause() != offered.Key() || responses[1].Cause() != request.Key() ||
					!strings.Contains(responses[1].Payload().String(), `"diagnostic_code":"work_expired"`) {
					t.Fatalf("deadline causes/payload = (%v,%v,%s)", responses[0].Cause(), responses[1].Cause(), responses[1].Payload().String())
				}
			}
		})
	}

	accepted := f.event(t, model.EventSourceLocal, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), nil, nil,
		f.base.Add(time.Minute), "deadline-active")
	active := f.work(t, model.WorkActive, 2, 1, accepted)
	equalRaces := []struct {
		name    string
		event   model.Event
		current model.ReviewWork
		fact    ImportEventFact
	}{
		{
			name: "decline",
			event: f.event(t, model.EventSourceImported, model.EventReviewDeclineRequested, f.reviewer,
				[]model.PeerID{f.home}, `{"content":"no","iteration":1,"work_version":1}`,
				[]model.EventKey{offered.Key()}, nil, f.base.Add(2*time.Minute), "deadline-decline"),
			current: current, fact: fact,
		},
		{
			name: "delivery",
			event: f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
				[]model.PeerID{f.home}, `{"content":"ready","iteration":1,"work_version":2}`,
				[]model.EventKey{accepted.Key()}, nil, f.base.Add(2*time.Minute), "deadline-delivery"),
			current: active, fact: importFact(t, accepted),
		},
	}
	for _, test := range equalRaces {
		t.Run(test.name+" equality", func(t *testing.T) {
			plan, err := PlanImportedEvent(ImportPlanSpec{f.home, test.event, &test.current,
				[]ImportEventFact{test.fact}, f.deadline})
			if err != nil {
				t.Fatal(err)
			}
			if got, want := importResponseTypes(plan.Responses()),
				[]model.EventType{model.EventReviewExpired, model.EventReviewOutcome}; !reflect.DeepEqual(got, want) {
				t.Fatalf("response order = %v, want %v", got, want)
			}
			if !strings.Contains(plan.Responses()[1].Payload().String(), `"status":"rejected"`) {
				t.Fatalf("correlated fallback = %s", plan.Responses()[1].Payload().String())
			}
		})
	}
}

func TestPlanImportedEventClassifiesStaleOutOfOrderAndConflicts(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceLocal, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "classification-offered")
	accepted := f.event(t, model.EventSourceLocal, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), nil, nil, f.base.Add(time.Minute), "classification-accepted")
	active := f.work(t, model.WorkActive, 2, 1, accepted)
	offeredWork := f.work(t, model.WorkOffered, 1, 1, offered)
	other := parsePeer(t, "peer-other")

	stale := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"again","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(2*time.Minute), "stale-request")
	futureAccepted := f.event(t, model.EventSourceLocal, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), nil, nil, f.base.Add(2*time.Minute), "future-accepted")
	outOfOrder := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"future","iteration":1,"work_version":2}`,
		[]model.EventKey{futureAccepted.Key()}, nil, f.base.Add(3*time.Minute), "out-of-order")
	wrongReviewer := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, other,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"spoof","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "wrong-reviewer")
	wrongAudience := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home, other}, `{"iteration":1,"note":"wide","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "wrong-audience")
	wrongIteration := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"wrong iteration","iteration":2,"work_version":2}`,
		[]model.EventKey{accepted.Key()}, nil, f.base.Add(2*time.Minute), "wrong-iteration")
	wrongCause := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"wrong cause","iteration":1,"work_version":2}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(2*time.Minute), "wrong-cause")
	missingCause := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"missing cause","iteration":1,"work_version":2}`,
		[]model.EventKey{accepted.Key()}, nil, f.base.Add(2*time.Minute), "missing-cause")
	olderIterationTwo := f.event(t, model.EventSourceLocal, model.EventReviewReworkRequested, f.home,
		[]model.PeerID{f.reviewer}, `{"content":"older","iteration":1,"work_version":3}`,
		nil, nil, f.base.Add(2*time.Minute), "older-iteration-two")
	versionFiveUpdate := f.event(t, model.EventSourceLocal, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(4, 1), nil, nil,
		f.base.Add(3*time.Minute), "version-five-update")
	versionFive := f.work(t, model.WorkDelivered, 5, 1, versionFiveUpdate)
	crossVersionIteration := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"old","iteration":2,"work_version":4}`,
		[]model.EventKey{olderIterationTwo.Key()}, nil, f.base.Add(4*time.Minute), "cross-version-iteration")
	otherScope := newTestWorkInChannel(t, "channel-alpha", "other-work", "peer-home",
		model.WorkOffered, 1, 1, f.deadline.UnixNano())

	tests := []struct {
		name       string
		event      model.Event
		current    *model.ReviewWork
		facts      []ImportEventFact
		want       ImportDisposition
		diagnostic string
		response   model.EventType
	}{
		{"stale known accept", stale, &active, []ImportEventFact{importFact(t, offered)}, ImportReject, "stale_request", model.EventReviewAcceptRejected},
		{"missing Work", outOfOrder, nil, []ImportEventFact{importFact(t, futureAccepted)}, ImportRetry, "missing_work", ""},
		{"local Work behind", outOfOrder, &offeredWork, []ImportEventFact{importFact(t, futureAccepted)}, ImportRetry, "local_work_behind", ""},
		{"wrong reviewer", wrongReviewer, &offeredWork, []ImportEventFact{importFact(t, offered)}, ImportConflict, "participant_conflict", ""},
		{"wrong audience", wrongAudience, &offeredWork, []ImportEventFact{importFact(t, offered)}, ImportConflict, "invalid_audience", ""},
		{"wrong Work scope", wrongCause, &otherScope, []ImportEventFact{importFact(t, offered)}, ImportConflict, "participant_conflict", ""},
		{"same version wrong iteration", wrongIteration, &active, []ImportEventFact{importFact(t, accepted)}, ImportConflict, "iteration_conflict", ""},
		{"known wrong predecessor", wrongCause, &active, []ImportEventFact{importFact(t, offered)}, ImportConflict, "predecessor_conflict", model.EventReviewOutcome},
		{"missing predecessor fact", missingCause, &active, nil, ImportRetry, "missing_cause", ""},
		{"version dominates iteration when classifying stale", crossVersionIteration, &versionFive,
			[]ImportEventFact{importFact(t, olderIterationTwo)}, ImportReject, "stale_request", model.EventReviewOutcome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanImportedEvent(ImportPlanSpec{f.home, test.event, test.current, test.facts, f.base.Add(10 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Disposition() != test.want || plan.Diagnostic() != test.diagnostic {
				t.Fatalf("plan = (%s,%s), want (%s,%s)", plan.Disposition(), plan.Diagnostic(), test.want, test.diagnostic)
			}
			responses := plan.Responses()
			if test.response == "" && len(responses) != 0 {
				t.Fatalf("unexpected responses %v", importResponseTypes(responses))
			}
			if test.response != "" && (len(responses) != 1 || responses[0].EventType() != test.response) {
				t.Fatalf("responses = %v, want %s", importResponseTypes(responses), test.response)
			}
		})
	}
}

func TestPlanImportedHomeDecisionDoesNotOrderIndependentPeerClocks(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "skew-offered")
	current := f.work(t, model.WorkOffered, 1, 1, offered)
	request := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(6*time.Hour), "skew-local-request")
	decision := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{request.Key()}, nil,
		f.base.Add(time.Minute), "skew-home-decision")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.reviewer, decision, &current,
		[]ImportEventFact{importFact(t, request)}, f.base.Add(2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportApply {
		t.Fatalf("independent Peer clock skew changed causality: (%s,%s)", plan.Disposition(), plan.Diagnostic())
	}
}

func TestPlanImportedStaleRequiresExactHistoricalCauseAuthority(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offeredLocal := f.event(t, model.EventSourceLocal, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "stale-authority-offered")
	acceptedLocal := f.event(t, model.EventSourceLocal, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), nil, nil,
		f.base.Add(time.Minute), "stale-authority-accepted")
	activeHome := f.work(t, model.WorkActive, 2, 1, acceptedLocal)
	badParticipant := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"old","work_version":1}`,
		[]model.EventKey{acceptedLocal.Key()}, nil, f.base.Add(2*time.Minute), "stale-bad-participant")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.home, badParticipant, &activeHome,
		[]ImportEventFact{importFact(t, acceptedLocal)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportConflict || plan.Diagnostic() != "predecessor_conflict" ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome {
		t.Fatalf("wrong historical participant cause = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()))
	}
	missingParticipant := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"old","work_version":1}`,
		[]model.EventKey{offeredLocal.Key()}, nil, f.base.Add(2*time.Minute), "stale-missing-participant")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, missingParticipant, &activeHome, nil, f.base.Add(3 * time.Minute)})
	if err != nil || plan.Disposition() != ImportRetry || plan.Diagnostic() != "missing_cause" {
		t.Fatalf("missing historical participant cause = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), err)
	}

	offeredRemote := f.asSource(t, offeredLocal, model.EventSourceImported)
	localRequest := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offeredRemote.Key()}, nil, f.base.Add(time.Minute), "stale-home-request")
	currentAccepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localRequest.Key()}, nil,
		f.base.Add(2*time.Minute), "stale-home-current")
	activeMirror := f.work(t, model.WorkActive, 2, 1, currentAccepted)
	staleDecision := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localRequest.Key()}, nil,
		f.base.Add(3*time.Minute), "stale-home-decision")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, staleDecision, &activeMirror,
		[]ImportEventFact{importFact(t, localRequest)}, f.base.Add(4 * time.Minute)})
	if err != nil || plan.Disposition() != ImportReject || plan.Diagnostic() != "stale_home_event" ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome ||
		!strings.Contains(plan.Responses()[0].Payload().String(), `"status":"rejected"`) {
		t.Fatalf("valid stale home decision = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), err)
	}
	wrongRequest := f.event(t, model.EventSourceLocal, model.EventReviewDeclineRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"no","iteration":1,"work_version":1}`,
		[]model.EventKey{offeredRemote.Key()}, nil, f.base.Add(time.Minute), "stale-home-wrong-request")
	wrongDecision := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{wrongRequest.Key()}, nil,
		f.base.Add(3*time.Minute), "stale-home-wrong-decision")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, wrongDecision, &activeMirror,
		[]ImportEventFact{importFact(t, wrongRequest)}, f.base.Add(4 * time.Minute)})
	if err != nil || plan.Disposition() != ImportConflict || plan.Diagnostic() != "predecessor_conflict" ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome ||
		!strings.Contains(plan.Responses()[0].Payload().String(), `"status":"conflicted"`) {
		t.Fatalf("wrong stale home cause = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), err)
	}
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, staleDecision, &activeMirror, nil, f.base.Add(4 * time.Minute)})
	if err != nil || plan.Disposition() != ImportRetry || plan.Diagnostic() != "missing_cause" || len(plan.Responses()) != 0 {
		t.Fatalf("missing stale home cause = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), err)
	}
}

func TestImportedStaleEventsStillEnforceEventPolicy(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "stale-policy-offered")
	localAccept := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "stale-policy-local-accept")
	accepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localAccept.Key()}, nil,
		f.base.Add(2*time.Minute), "stale-policy-current-accepted")
	activeMirror := f.work(t, model.WorkActive, 2, 1, accepted)
	root := importArtifactRef(t, "stale-policy-root", model.ArtifactReferenced)

	staleAcceptedWithRoot := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{localAccept.Key()},
		[]model.ArtifactRef{root}, f.base.Add(3*time.Minute), "stale-policy-accepted-root")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.reviewer, staleAcceptedWithRoot, &activeMirror,
		[]ImportEventFact{importFact(t, localAccept)}, f.base.Add(4 * time.Minute)})
	assertImportConflictOutcome(t, plan, err, "artifact_conflict")

	wrongDeadlineExpired := f.event(t, model.EventSourceImported, model.EventReviewExpired, f.home,
		[]model.PeerID{f.reviewer}, f.expiryPayloadAt(f.deadline.Add(time.Hour), 1, 1),
		[]model.EventKey{offered.Key()}, nil, f.base.Add(3*time.Minute), "stale-policy-expired-deadline")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, wrongDeadlineExpired, &activeMirror,
		[]ImportEventFact{importFact(t, offered)}, f.base.Add(4 * time.Minute)})
	assertImportConflictOutcome(t, plan, err, "deadline_conflict")

	homeOffered := f.asSource(t, offered, model.EventSourceLocal)
	homeAccepted := f.asSource(t, accepted, model.EventSourceLocal)
	activeHome := f.work(t, model.WorkActive, 2, 1, homeAccepted)
	staleAcceptWithRoot := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"old","work_version":1}`,
		[]model.EventKey{homeOffered.Key()}, []model.ArtifactRef{root},
		f.base.Add(3*time.Minute), "stale-policy-request-root")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, staleAcceptWithRoot, &activeHome,
		[]ImportEventFact{importFact(t, homeOffered)}, f.base.Add(4 * time.Minute)})
	assertImportConflictOutcome(t, plan, err, "artifact_conflict")

	produced := importArtifactRef(t, "stale-delivery-root", model.ArtifactProduced)
	localDelivery := f.event(t, model.EventSourceLocal, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"result","iteration":1,"work_version":2}`,
		[]model.EventKey{accepted.Key()}, []model.ArtifactRef{produced},
		f.base.Add(3*time.Minute), "stale-policy-local-delivery")
	currentDelivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{importArtifactRef(t, "stale-delivery-root", model.ArtifactReferenced)},
		f.base.Add(4*time.Minute), "stale-policy-current-delivered")
	deliveredMirror := f.work(t, model.WorkDelivered, 3, 1, currentDelivered)
	staleDeliveredProduced := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), []model.EventKey{localDelivery.Key()},
		[]model.ArtifactRef{produced}, f.base.Add(5*time.Minute), "stale-policy-delivered-produced")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, staleDeliveredProduced, &deliveredMirror,
		[]ImportEventFact{importFact(t, localDelivery)}, f.base.Add(6 * time.Minute)})
	assertImportConflictOutcome(t, plan, err, "artifact_conflict")
}

func TestImportedHomeTransitionIgnoresSameOriginClockRollbackForOrdering(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	currentTime := f.base.Add(5 * time.Minute)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, currentTime, "clock-rollback-offered")
	current := f.work(t, model.WorkOffered, 1, 1, offered)
	request := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, currentTime.Add(time.Minute), "clock-rollback-request")
	rolledBackTime := currentTime.Add(-time.Minute)
	accepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{request.Key()}, nil,
		rolledBackTime, "clock-rollback-accepted")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.reviewer, accepted, &current,
		[]ImportEventFact{importFact(t, request)}, f.base.Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	work, ok := plan.Work()
	if plan.Disposition() != ImportApply || !ok || work.NextState() != model.WorkActive ||
		work.ObservedAtUnixNano() != rolledBackTime.UnixNano() {
		t.Fatalf("clock rollback plan = (%s,%t,%s,%d)",
			plan.Disposition(), ok, work.NextState(), work.ObservedAtUnixNano())
	}
}

func TestImportedHistoricalFactUsesClosedWorkSuccessors(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	tests := []struct {
		name      string
		eventType model.EventType
		payload   string
		version   uint64
		iteration uint8
		valid     bool
	}{
		{"offered establishes one", model.EventReviewOffered, f.offerPayload(1, 1), 1, 1, true},
		{"accepted establishes two", model.EventReviewAccepted, versionPayload(1, 1), 2, 1, true},
		{"first delivery establishes three", model.EventReviewDelivered, versionPayload(2, 1), 3, 1, true},
		{"rework establishes four iteration two", model.EventReviewReworkRequested,
			`{"content":"fix","iteration":1,"work_version":3}`, 4, 2, true},
		{"second delivery establishes five", model.EventReviewDelivered, versionPayload(4, 2), 5, 2, true},
		{"receipt does not establish Work", model.EventReviewAcceptRejected,
			`{"diagnostic_code":"stale","iteration":1,"work_version":1}`, 0, 0, false},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := f.event(t, model.EventSourceLocal, test.eventType, f.home, []model.PeerID{f.reviewer},
				test.payload, nil, nil, f.base.Add(time.Duration(index)*time.Minute), fmt.Sprintf("successor-%d", index))
			version, iteration, valid := factEstablishedWork(importFact(t, event))
			if version != test.version || iteration != test.iteration || valid != test.valid {
				t.Fatalf("factEstablishedWork() = (%d,%d,%t), want (%d,%d,%t)",
					version, iteration, valid, test.version, test.iteration, test.valid)
			}
		})
	}
}

func TestImportedSemanticConflictReceiptsStopAtAuthorityBoundary(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "conflict-receipt-offered")
	current := f.work(t, model.WorkOffered, 1, 1, offered)
	request := f.event(t, model.EventSourceLocal, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "conflict-receipt-request")
	dueAccepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1), []model.EventKey{request.Key()}, nil,
		f.deadline, "conflict-receipt-due-accepted")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.reviewer, dueAccepted, &current,
		[]ImportEventFact{importFact(t, request)}, f.base.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	responses := plan.Responses()
	if plan.Disposition() != ImportConflict || len(responses) != 1 ||
		responses[0].EventType() != model.EventReviewOutcome || responses[0].Cause() != dueAccepted.Key() ||
		!strings.Contains(responses[0].Payload().String(), `"status":"conflicted"`) ||
		!strings.Contains(responses[0].Payload().String(), `"decision_ref":"`+dueAccepted.ID().String()+`"`) {
		t.Fatalf("semantic conflict receipt = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), importResponseTypes(responses))
	}

	noCauseRequest := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"yes","work_version":1}`,
		nil, nil, f.base.Add(time.Minute), "conflict-receipt-no-cause-request")
	homeOffered := f.asSource(t, offered, model.EventSourceLocal)
	homeCurrent := f.work(t, model.WorkOffered, 1, 1, homeOffered)
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, noCauseRequest, &homeCurrent, nil, f.base.Add(2 * time.Minute)})
	if err != nil || plan.Disposition() != ImportConflict || plan.Diagnostic() != "cause_conflict" ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome {
		t.Fatalf("participant cause-cardinality conflict = (%s,%s,%v,%v)",
			plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()), err)
	}

	twoCauseAccepted := f.event(t, model.EventSourceImported, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(1, 1),
		[]model.EventKey{request.Key(), offered.Key()}, nil, f.base.Add(2*time.Minute),
		"conflict-receipt-two-cause-accepted")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, twoCauseAccepted, &current, nil, f.base.Add(3 * time.Minute)})
	if err != nil || plan.Disposition() != ImportConflict || plan.Diagnostic() != "cause_conflict" ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome {
		t.Fatalf("home cause-cardinality conflict = (%s,%s,%v,%v)",
			plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()), err)
	}

	other := parsePeer(t, "peer-authority-other")
	wrongReviewer := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, other,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"spoof","work_version":1}`,
		[]model.EventKey{offered.Key()}, nil, f.base.Add(time.Minute), "conflict-receipt-wrong-reviewer")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, wrongReviewer, &homeCurrent,
		[]ImportEventFact{importFact(t, homeOffered)}, f.base.Add(2 * time.Minute)})
	if err != nil || plan.Disposition() != ImportConflict || len(plan.Responses()) != 0 {
		t.Fatalf("authority conflict emitted response = (%s,%s,%v,%v)",
			plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()), err)
	}
}

func TestPlanImportedEventRejectsTerminalAndVersionExhaustion(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	cancelled := f.event(t, model.EventSourceLocal, model.EventReviewCancelled, f.home,
		[]model.PeerID{f.reviewer}, `{"content":"stop","iteration":1,"work_version":2}`,
		nil, nil, f.base.Add(time.Minute), "local-cancelled")
	terminal := f.work(t, model.WorkCancelled, 3, 1, cancelled)
	request := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"late","iteration":1,"work_version":3}`,
		[]model.EventKey{cancelled.Key()}, nil, f.base.Add(2*time.Minute), "terminal-request")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.home, request, &terminal,
		[]ImportEventFact{importFact(t, cancelled)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportReject || len(plan.Responses()) != 1 ||
		plan.Responses()[0].EventType() != model.EventReviewOutcome {
		t.Fatalf("terminal plan = (%s,%v,%s)", plan.Disposition(), importResponseTypes(plan.Responses()), plan.Diagnostic())
	}
	terminalAccept := f.event(t, model.EventSourceImported, model.EventReviewAcceptRequested, f.reviewer,
		[]model.PeerID{f.home}, `{"iteration":1,"note":"too late","work_version":3}`,
		[]model.EventKey{cancelled.Key()}, nil, f.base.Add(2*time.Minute), "terminal-accept-request")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, terminalAccept, &terminal,
		[]ImportEventFact{importFact(t, cancelled)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportReject || len(plan.Responses()) != 1 ||
		plan.Responses()[0].EventType() != model.EventReviewOutcome {
		t.Fatalf("terminal accept plan = (%s,%v,%s)", plan.Disposition(),
			importResponseTypes(plan.Responses()), plan.Diagnostic())
	}

	maxUpdate := f.event(t, model.EventSourceLocal, model.EventReviewAccepted, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(model.MaxSQLiteInteger-1, 1), nil, nil,
		f.base.Add(time.Minute), "max-update")
	maxWork := f.work(t, model.WorkActive, model.MaxSQLiteInteger, 1, maxUpdate)
	maxRequest := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, fmt.Sprintf(`{"content":"max","iteration":1,"work_version":%d}`, model.MaxSQLiteInteger),
		[]model.EventKey{maxUpdate.Key()}, nil, f.base.Add(2*time.Minute), "max-request")
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, maxRequest, &maxWork,
		[]ImportEventFact{importFact(t, maxUpdate)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportConflict || plan.Diagnostic() != "work_version_exhausted" ||
		len(plan.Responses()) != 0 {
		t.Fatalf("max-version plan = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(),
			importResponseTypes(plan.Responses()))
	}

	invalidTerminalTuple := f.event(t, model.EventSourceImported, model.EventReviewDeliveryReady, f.reviewer,
		[]model.PeerID{f.home}, `{"content":"impossible","iteration":1,"work_version":4}`,
		[]model.EventKey{cancelled.Key()}, nil, f.base.Add(2*time.Minute), "invalid-terminal-tuple")
	terminalV4 := f.work(t, model.WorkCancelled, 4, 1, cancelled)
	plan, err = PlanImportedEvent(ImportPlanSpec{f.home, invalidTerminalTuple, &terminalV4,
		[]ImportEventFact{importFact(t, cancelled)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Disposition() != ImportReject || plan.Diagnostic() != "stale_request" ||
		len(plan.Responses()) != 0 {
		t.Fatalf("invalid terminal tuple plan = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(),
			importResponseTypes(plan.Responses()))
	}
}

func TestPlanImportedCancellationSettlesOnlyActionableHandling(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	delivered := f.event(t, model.EventSourceImported, model.EventReviewDelivered, f.home,
		[]model.PeerID{f.reviewer}, versionPayload(2, 1), nil, nil,
		f.base.Add(time.Minute), "cancel-delivered-current")
	current := f.work(t, model.WorkDelivered, 3, 1, delivered)
	cancelled := f.event(t, model.EventSourceImported, model.EventReviewCancelled, f.home,
		[]model.PeerID{f.reviewer}, `{"content":"stop after delivery","iteration":1,"work_version":3}`,
		[]model.EventKey{delivered.Key()}, nil, f.base.Add(2*time.Minute), "cancel-after-delivery")
	plan, err := PlanImportedEvent(ImportPlanSpec{f.reviewer, cancelled, &current,
		[]ImportEventFact{importFact(t, delivered)}, f.base.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	work, hasWork := plan.Work()
	if plan.Disposition() != ImportApply || !hasWork || work.NextState() != model.WorkCancelled ||
		len(plan.Responses()) != 1 {
		t.Fatalf("delivered cancellation plan = (%s,%t,%s,%v,%s)", plan.Disposition(),
			hasWork, work.NextState(), importResponseTypes(plan.Responses()), plan.Diagnostic())
	}
	if settlement, ok := plan.Settlement(); ok {
		t.Fatalf("delivered cancellation settled nonexistent handling: %+v", settlement)
	}
}

func TestPlanImportedEventStrictPayloadAndInputValidation(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil, nil, f.base, "strict-offered")
	current := f.work(t, model.WorkOffered, 1, 1, offered)
	malformed := []struct {
		name      string
		eventType model.EventType
		payload   string
	}{
		{"unknown field", model.EventReviewAccepted, `{"extra":true,"iteration":1,"work_version":1}`},
		{"missing field", model.EventReviewAccepted, `{"work_version":1}`},
		{"empty required content", model.EventReviewDeliveryReady, `{"content":"","iteration":1,"work_version":1}`},
		{"noncanonical UTC deadline", model.EventReviewExpired, `{"deadline":"2026-07-19T01:00:00+00:00","iteration":1,"work_version":1}`},
		{"unknown outcome status", model.EventReviewOutcome, outcomePayload("unknown", "x", offered.ID().String(), 1, 1)},
		{"version beyond SQLite", model.EventReviewAccepted, fmt.Sprintf(`{"iteration":1,"work_version":%d}`, uint64(model.MaxSQLiteInteger)+1)},
	}
	for index, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			origin, audience := f.home, []model.PeerID{f.reviewer}
			if test.eventType.ParticipantInput() || test.eventType == model.EventReviewOutcome {
				origin, audience = f.reviewer, []model.PeerID{f.home}
			}
			event := f.event(t, model.EventSourceImported, test.eventType, origin, audience,
				test.payload, nil, nil, f.base.Add(time.Duration(index+1)*time.Minute), fmt.Sprintf("malformed-%d", index))
			local := f.reviewer
			if origin == f.reviewer {
				local = f.home
			}
			plan, err := PlanImportedEvent(ImportPlanSpec{local, event, &current, nil, f.base.Add(10 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Disposition() != ImportConflict || plan.Diagnostic() != "invalid_payload" ||
				len(plan.Responses()) != 0 {
				t.Fatalf("plan = (%s,%s,%v)", plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()))
			}
		})
	}

	_, err := PlanImportedEvent(ImportPlanSpec{})
	if !errors.Is(err, ErrInvalidImport) {
		t.Fatalf("zero input error = %v, want ErrInvalidImport", err)
	}
	localEvent := f.asSource(t, offered, model.EventSourceLocal)
	_, err = PlanImportedEvent(ImportPlanSpec{f.reviewer, localEvent, nil, nil, f.base})
	if !errors.Is(err, ErrInvalidImport) {
		t.Fatalf("local source error = %v, want ErrInvalidImport", err)
	}
	if _, err := NewImportEventFact(model.Event{}); !errors.Is(err, ErrInvalidImport) {
		t.Fatalf("zero fact error = %v, want ErrInvalidImport", err)
	}
}

func TestImportPlanIsDeterministicAccessorOnlyAndDefensive(t *testing.T) {
	t.Parallel()

	f := newImportFixture(t)
	offered := f.event(t, model.EventSourceImported, model.EventReviewOffered, f.home,
		[]model.PeerID{f.reviewer}, f.offerPayload(1, 1), nil,
		[]model.ArtifactRef{importArtifactRef(t, "offer-root", model.ArtifactProduced)}, f.base, "deterministic-offered")
	spec := ImportPlanSpec{f.reviewer, offered, nil, nil, f.base.Add(time.Minute)}
	first, err := PlanImportedEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanImportedEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	if importPlanSignature(first) != importPlanSignature(second) {
		t.Fatalf("same facts produced different plans:\n%s\n%s", importPlanSignature(first), importPlanSignature(second))
	}

	responses := first.Responses()
	responses[0] = LocalResponseIntent{}
	if got := first.Responses()[0].EventType(); got != model.EventReviewOutcome {
		t.Fatalf("Responses() exposed storage: %s", got)
	}
	payloadBytes := first.Responses()[0].Payload().Bytes()
	payloadBytes[0] = '['
	if !strings.HasPrefix(first.Responses()[0].Payload().String(), "{") {
		t.Fatal("Payload() exposed mutable bytes")
	}
	fact := importFact(t, offered)
	audience := fact.Audience()
	audience[0] = f.home
	if fact.Audience()[0] != f.reviewer {
		t.Fatal("fact Audience() exposed storage")
	}
	artifacts := fact.Artifacts()
	artifacts[0] = model.ArtifactRef{}
	if fact.Artifacts()[0].RootDigest().IsZero() {
		t.Fatal("fact Artifacts() exposed storage")
	}

	for _, disposition := range []ImportDisposition{ImportApply, ImportRetry, ImportReject, ImportConflict, ImportReceiptOnly} {
		if !disposition.Valid() {
			t.Fatalf("closed disposition %q is invalid", disposition)
		}
	}
	if ImportDisposition("forward").Valid() {
		t.Fatal("unknown disposition accepted")
	}
	responseType := reflect.TypeOf(LocalResponseIntent{})
	for _, forbidden := range []string{"id", "sequence", "signature", "publication", "receipt"} {
		for index := 0; index < responseType.NumField(); index++ {
			if strings.Contains(strings.ToLower(responseType.Field(index).Name), forbidden) {
				t.Fatalf("LocalResponseIntent contains authority field %q", responseType.Field(index).Name)
			}
		}
	}
}

type importFixture struct {
	home     model.PeerID
	reviewer model.PeerID
	channel  model.ChannelID
	workRef  model.WorkRef
	base     time.Time
	deadline time.Time
}

func newImportFixture(t *testing.T) importFixture {
	t.Helper()
	home := parsePeer(t, "peer-home")
	reviewer := parsePeer(t, "peer-home-reviewer")
	workID, err := model.ParseWorkID("work-import")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 19, 1, 0, 0, 123, time.UTC)
	return importFixture{home, reviewer, parseChannel(t, "channel-alpha"), ref, base, base.Add(time.Hour)}
}

func (f importFixture) offerPayload(version uint64, iteration uint8) string {
	return fmt.Sprintf(`{"content":"review this","deadline":%q,"iteration":%d,"work_version":%d}`,
		f.deadline.Format(time.RFC3339Nano), iteration, version)
}

func (f importFixture) expiryPayload(version uint64, iteration uint8) string {
	return f.expiryPayloadAt(f.deadline, version, iteration)
}

func (f importFixture) expiryPayloadAt(deadline time.Time, version uint64, iteration uint8) string {
	return fmt.Sprintf(`{"deadline":%q,"iteration":%d,"work_version":%d}`,
		deadline.Format(time.RFC3339Nano), iteration, version)
}

func (f importFixture) event(t *testing.T, source model.EventSource, eventType model.EventType,
	origin model.PeerID, audiencePeers []model.PeerID, payloadText string, causes []model.EventKey,
	artifacts []model.ArtifactRef, acceptedAt time.Time, idText string,
) model.Event {
	t.Helper()
	epoch, err := model.ParseOriginEpoch("epoch-" + origin.String())
	if err != nil {
		t.Fatal(err)
	}
	originHead, err := model.NewRecordHead(2, model.Sum([]byte("member-"+origin.String())))
	if err != nil {
		t.Fatal(err)
	}
	rosterHead, err := model.NewRecordHead(3, model.Sum([]byte("roster")))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := model.NewEventScope(f.channel, origin, epoch, 1, 1, originHead, rosterHead, f.workRef)
	if err != nil {
		t.Fatal(err)
	}
	audience, err := model.NewAudience(audiencePeers)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewJSON([]byte(payloadText))
	if err != nil {
		t.Fatalf("payload %s: %v", payloadText, err)
	}
	id, err := model.ParseEventID(idText)
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.NewEvent(model.EventSpec{
		ID: id, Scope: scope, Source: source, ActorPrincipal: "principal", Type: eventType,
		Audience: audience, Summary: "summary", Payload: payload, Artifacts: artifacts, CausedBy: causes,
		CreatedAt: acceptedAt, AcceptedAt: acceptedAt,
	})
	if err != nil {
		t.Fatalf("NewEvent(%s): %v", eventType, err)
	}
	return event
}

func (f importFixture) asSource(t *testing.T, event model.Event, source model.EventSource) model.Event {
	t.Helper()
	spec := event.Spec()
	spec.Source = source
	rebuilt, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return rebuilt
}

func (f importFixture) work(t *testing.T, state model.WorkState, version uint64, iteration uint8,
	updatedBy model.Event,
) model.ReviewWork {
	t.Helper()
	participants, err := model.NewParticipantSnapshot(f.channel, 3, f.home, f.reviewer)
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{
		Ref: f.workRef, ChannelID: f.channel, Participants: participants, Version: version,
		Iteration: iteration, DeadlineUnixNano: f.deadline.UnixNano(), State: state,
		StateData: updatedBy.Payload(), UpdatedBy: updatedBy.ID(), UpdatedAt: updatedBy.AcceptedAt(),
	})
	if err != nil {
		t.Fatalf("NewReviewWork(%s,v%d,i%d): %v", state, version, iteration, err)
	}
	return work
}

func importFact(t *testing.T, event model.Event) ImportEventFact {
	t.Helper()
	fact, err := NewImportEventFact(event)
	if err != nil {
		t.Fatalf("NewImportEventFact(%s): %v", event.Type(), err)
	}
	return fact
}

func importWorkPointer(work model.ReviewWork) *model.ReviewWork { return &work }

func importArtifactRef(t *testing.T, value string, role model.ArtifactRole) model.ArtifactRef {
	t.Helper()
	ref, err := model.NewArtifactRef(model.Sum([]byte(value)), role)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func versionPayload(version uint64, iteration uint8) string {
	return fmt.Sprintf(`{"iteration":%d,"work_version":%d}`, iteration, version)
}

func outcomePayload(status, diagnostic, decision string, version uint64, iteration uint8) string {
	return fmt.Sprintf(`{"decision_ref":%q,"diagnostic_code":%q,"iteration":%d,"status":%q,"work_version":%d}`,
		decision, diagnostic, iteration, status, version)
}

func importResponseTypes(responses []LocalResponseIntent) []model.EventType {
	result := make([]model.EventType, len(responses))
	for index := range responses {
		result[index] = responses[index].EventType()
	}
	return result
}

func assertImportConflictOutcome(t *testing.T, plan ImportPlan, err error, diagnostic string) {
	t.Helper()
	if err != nil || plan.Disposition() != ImportConflict || plan.Diagnostic() != diagnostic ||
		len(plan.Responses()) != 1 || plan.Responses()[0].EventType() != model.EventReviewOutcome ||
		!strings.Contains(plan.Responses()[0].Payload().String(), `"status":"conflicted"`) {
		t.Fatalf("conflict outcome = (%s,%s,%v,%v)",
			plan.Disposition(), plan.Diagnostic(), importResponseTypes(plan.Responses()), err)
	}
}

func importPlanSignature(plan ImportPlan) string {
	work, hasWork := plan.Work()
	handling, hasHandling := plan.Handling()
	settlement, hasSettlement := plan.Settlement()
	parts := []string{string(plan.Disposition()), string(plan.InboxStatus()), plan.Diagnostic(),
		fmt.Sprintf("work=%t:%s:%d:%s:%d", hasWork, work.Source(), work.ResponseOrdinal(), work.NextState(), work.NextVersion()),
		fmt.Sprintf("handling=%t:%s:%d:%s", hasHandling, handling.Source(), handling.ResponseOrdinal(), handling.LocalRole()),
		fmt.Sprintf("settlement=%t:%s:%s", hasSettlement, settlement.SourceEventID().String(), settlement.Disposition())}
	for _, response := range plan.Responses() {
		parts = append(parts, string(response.EventType())+":"+response.Payload().String()+":"+response.Cause().EventID().String())
	}
	return strings.Join(parts, "|")
}
