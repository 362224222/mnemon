package agent

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func runTeamworkActionExecutorOffersExplicitReviewer(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	action := executorAction(t, "offer", false, "architecture goal", "30m", "reviewer-0", nil)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	response, controlErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: TeamworkActionRequest{Action: "offer", Channel: "alpha", To: "reviewer-0",
			Deadline: "30m", Content: "architecture goal"},
		Action: action, Reservation: reservation, At: fixture.at})
	if controlErr != nil {
		t.Fatalf("ExecuteTeamwork(offer) error = %v", controlErr)
	}
	if response.Action != "teamwork.offer" || response.Replayed || response.Handling != nil ||
		len(response.Results) != 1 || len(fixture.backend.committed.items) != 1 ||
		fixture.backend.commitAt != fixture.clock.now {
		t.Fatalf("offer response = %#v", response)
	}
	if fixture.selector.last.ChannelAlias != "alpha" ||
		fixture.selector.last.ParticipantSelector != "reviewer-0" || fixture.artifacts.calls != 1 {
		t.Fatalf("selector/artifact calls = %#v / %d", fixture.selector.last, fixture.artifacts.calls)
	}
	item := fixture.backend.committed.items[0]
	eventValue := item.Publication.Event()
	assertExecutorOfferItem(t, fixture, reservation, item)
	assertExecutorOfferScope(t, fixture, eventValue)
	if len(eventValue.CausedBy()) != 0 {
		t.Fatal("contextless offer claimed causality")
	}
}

func assertExecutorOfferItem(t *testing.T, fixture *executorFixture,
	reservation store.ManagedOperationReservation, item store.LocalAcceptanceItem,
) {
	t.Helper()
	eventValue := item.Publication.Event()
	work := item.Work.Work
	wantReviewer := fixture.reviewers[0].PeerID()
	wantWorkID, wantEventID, _ := derivedOfferIDs(reservation.Operation.ID(), 0)
	if eventValue.ID() != wantEventID || eventValue.Type() != model.EventReviewOffered ||
		eventValue.Audience().Len() != 1 || !eventValue.Audience().Contains(wantReviewer) ||
		work.Participants().ReviewerPeerID() != wantReviewer ||
		work.Ref().HomePeerID() != fixture.scope.node.PeerID() || work.Ref().WorkID() != wantWorkID ||
		work.State() != model.WorkOffered || work.Version() != 1 ||
		work.DeadlineUnixNano() != fixture.at.Add(30*time.Minute).UnixNano() {
		t.Fatalf("offer item = Event %#v Work %#v", eventValue, work)
	}
}

func assertExecutorOfferScope(t *testing.T, fixture *executorFixture, eventValue model.Event) {
	t.Helper()
	scope := eventValue.Scope()
	if scope.OriginPeerID() != fixture.scope.node.PeerID() ||
		scope.OriginSequence() != fixture.scope.firstOriginSequence ||
		scope.ChannelSequence() != fixture.scope.firstChannelSequence ||
		scope.OriginMember() != fixture.scope.originMember ||
		scope.PublicationRoster() != fixture.scope.publicationRoster ||
		eventValue.ActorPrincipal() != fixture.profile.Principal() ||
		!eventValue.AcceptedAt().Equal(fixture.at) {
		t.Fatalf("offer item server scope = %#v", scope)
	}
}
