package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type executorOfferCase struct {
	name     string
	selector string
	count    int
}

func runTeamworkActionExecutorOffersExplicitAutoAndCanonicalTeam(t *testing.T) {
	t.Parallel()
	for _, test := range executorOfferCases() {
		t.Run(test.name, func(t *testing.T) { runExecutorOfferCase(t, test) })
	}
}

func executorOfferCases() []executorOfferCase {
	tests := []executorOfferCase{
		{name: "explicit", selector: "reviewer-0", count: 1},
		{name: "auto", selector: AgentParticipantAuto, count: 1},
	}
	for count := 1; count <= model.MaxChildWorks; count++ {
		tests = append(tests, executorOfferCase{name: fmt.Sprintf("team-%d", count),
			selector: AgentParticipantTeam, count: count})
	}
	return tests
}

func runExecutorOfferCase(t *testing.T, test executorOfferCase) {
	t.Helper()
	fixture := newExecutorFixture(t, test.count)
	reservation := executorReservation(t, fixture, model.OperationTeamworkOffer, model.ReviewWork{}, false)
	action := executorAction(t, "offer", false, "architecture goal", "30m", test.selector, nil)
	response, controlErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: TeamworkActionRequest{Action: "offer", Channel: "alpha", To: test.selector,
			Deadline: "30m", Content: "architecture goal"},
		Action: action, Reservation: reservation, At: fixture.at})
	if controlErr != nil {
		t.Fatalf("ExecuteTeamwork(offer) error = %v", controlErr)
	}
	if response.Action != "teamwork.offer" || response.Replayed || response.Handling != nil ||
		len(response.Results) != test.count || len(fixture.backend.committed.items) != test.count ||
		fixture.backend.commitAt != fixture.clock.now {
		t.Fatalf("offer response = %#v", response)
	}
	if fixture.selector.last.ChannelAlias != "alpha" ||
		fixture.selector.last.ParticipantSelector != test.selector || fixture.artifacts.calls != 1 {
		t.Fatalf("selector/artifact calls = %#v / %d", fixture.selector.last, fixture.artifacts.calls)
	}
	assertExecutorOfferItems(t, fixture, reservation)
}

func assertExecutorOfferItems(t *testing.T, fixture *executorFixture,
	reservation store.ManagedOperationReservation,
) {
	t.Helper()
	payloads := make([]string, len(fixture.backend.committed.items))
	for index, item := range fixture.backend.committed.items {
		eventValue := item.Publication.Event()
		assertExecutorOfferItem(t, fixture, reservation, index, item)
		assertExecutorOfferScope(t, fixture, index, eventValue)
		if len(eventValue.CausedBy()) != 0 {
			t.Fatal("contextless offer claimed causality")
		}
		payloads[index] = eventValue.Payload().String()
	}
	for index := 1; index < len(payloads); index++ {
		if payloads[index] != payloads[0] {
			t.Fatalf("team item %d changed shared payload", index)
		}
	}
}

func assertExecutorOfferItem(t *testing.T, fixture *executorFixture,
	reservation store.ManagedOperationReservation, index int, item store.LocalAcceptanceItem,
) {
	t.Helper()
	eventValue := item.Publication.Event()
	work := item.Work.Work
	wantReviewer := fixture.reviewers[index].PeerID()
	wantWorkID, wantEventID, _ := derivedOfferIDs(reservation.Operation.ID(), uint8(index))
	if eventValue.ID() != wantEventID || eventValue.Type() != model.EventReviewOffered ||
		eventValue.Audience().Len() != 1 || !eventValue.Audience().Contains(wantReviewer) ||
		work.Participants().ReviewerPeerID() != wantReviewer ||
		work.Ref().HomePeerID() != fixture.scope.node.PeerID() || work.Ref().WorkID() != wantWorkID ||
		work.State() != model.WorkOffered || work.Version() != 1 ||
		work.DeadlineUnixNano() != fixture.at.Add(30*time.Minute).UnixNano() {
		t.Fatalf("offer item[%d] = Event %#v Work %#v", index, eventValue, work)
	}
}

func assertExecutorOfferScope(t *testing.T, fixture *executorFixture,
	index int, eventValue model.Event,
) {
	t.Helper()
	scope := eventValue.Scope()
	if scope.OriginPeerID() != fixture.scope.node.PeerID() ||
		scope.OriginSequence() != fixture.scope.firstOriginSequence+uint64(index) ||
		scope.ChannelSequence() != fixture.scope.firstChannelSequence+uint64(index) ||
		scope.OriginMember() != fixture.scope.originMember ||
		scope.PublicationRoster() != fixture.scope.publicationRoster ||
		eventValue.ActorPrincipal() != fixture.profile.Principal() ||
		!eventValue.AcceptedAt().Equal(fixture.at) {
		t.Fatalf("offer item[%d] server scope = %#v", index, scope)
	}
}
