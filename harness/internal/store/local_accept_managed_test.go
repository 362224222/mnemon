package store

import (
	"context"
	"strings"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCommitManagedAcceptanceCompletesContextlessRunAndReplays(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	reservation := reserveManagedOfferForAcceptance(t, fixture, "managed-initiate", fixture.now)
	authority := localAuthority(t, fixture, reservation.Operation)
	lowLevel := fixture.offer(t, &authority, "managed-initiate", fixture.reviewers, nil, nil)
	spec := ManagedAcceptanceSpec{Scope: lowLevel.Scope, Items: lowLevel.Items, Operation: authority}
	committedAt := fixture.now.Add(time.Second)
	result, err := fixture.store.CommitManagedAcceptance(context.Background(), spec, committedAt)
	if err != nil || result.Replayed {
		t.Fatalf("CommitManagedAcceptance() = (%#v, %v)", result, err)
	}
	for _, binding := range []string{`"event_type":"review.offered"`, `"state":"OFFERED"`, `"version":1`} {
		if !strings.Contains(result.Receipt.String(), binding) {
			t.Fatalf("durable operation receipt lacks %s: %s", binding, result.Receipt)
		}
	}
	assertManagedAcceptanceRun(t, fixture.store, reservation.Run.ID(), result.Receipt)
	if _, hasHandling := reservation.Run.HandlingID(); hasHandling {
		t.Fatal("contextless managed Run unexpectedly has a Handling")
	}

	replay, err := fixture.store.CommitManagedAcceptance(context.Background(),
		ManagedAcceptanceSpec{Operation: authority}, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != result.Receipt.String() {
		t.Fatalf("managed response-loss replay = (%#v, %v)", replay, err)
	}
}

func TestCommitManagedAcceptanceCompletesClaimWithExactCurrentAction(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	initial := reserveManagedOfferForAcceptance(t, fixture, "managed-current-source", fixture.now)
	initialAuthority := localAuthority(t, fixture, initial.Operation)
	initialSpec := fixture.offer(t, &initialAuthority, "managed-current-source", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitManagedAcceptance(context.Background(), ManagedAcceptanceSpec{
		Scope: initialSpec.Scope, Items: initialSpec.Items, Operation: initialAuthority,
	}, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	sourceEvent := initialSpec.Items[0].Publication.Event()
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-managed-cancel", sourceEvent.ID(), 1,
		claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-managed-cancel", "token-managed-cancel", claimAt)
	readAt := claimAt.Add(time.Second)
	current, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), currentReadSpec(
		fixture, claim.Run.ID(), "token-managed-cancel", readAt))
	if err != nil {
		t.Fatal(err)
	}
	if !sameOperationKinds(current.Projection.AllowedActions(),
		[]model.OperationKind{model.OperationTeamworkCancel, model.OperationResolveRetry}) {
		t.Fatalf("current actions = %v", current.Projection.AllowedActions())
	}

	operationAt := readAt.Add(time.Second)
	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), ManagedOperationSpec{
		Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("key-managed-cancel")),
		RequestDigest: model.Sum([]byte("request-managed-cancel")), Kind: model.OperationTeamworkCancel,
		LeaseOwner: "server-managed-cancel", At: operationAt, LeaseUntil: operationAt.Add(time.Minute),
		ClaimContextHash: model.Sum([]byte("token-managed-cancel")), HasClaimContext: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := operationAt.Add(time.Second)
	managedSpec := managedCancelAcceptance(t, fixture, reservation, current, acceptedAt)
	result, err := fixture.store.CommitManagedAcceptance(context.Background(), managedSpec,
		acceptedAt.Add(time.Second))
	if err != nil || result.Replayed {
		t.Fatalf("managed cancel acceptance = (%#v, %v)", result, err)
	}
	for _, binding := range []string{`"event_type":"review.cancelled"`, `"state":"CANCELLED"`, `"version":2`} {
		if !strings.Contains(result.Receipt.String(), binding) {
			t.Fatalf("cancel receipt lacks frozen Work result %s: %s", binding, result.Receipt)
		}
	}

	handling, err := readAgentHandling(context.Background(), fixture.store.db, claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingCompleted ||
		handling.LastDisposition() != "teamwork_action" || handling.ClaimOwner() != "" {
		t.Fatalf("completed Handling = (%#v, %v)", handling, err)
	}
	outcome, ok := handling.OutcomeEventID()
	if !ok || outcome != managedSpec.Items[0].Publication.Event().ID() {
		t.Fatalf("Handling outcome Event = %s, %v", outcome, ok)
	}
	assertManagedAcceptanceRun(t, fixture.store, claim.Run.ID(), result.Receipt)
	durable, err := fixture.store.GetReviewWork(context.Background(), current.Receipt.ActionWork())
	if err != nil || durable.State() != model.WorkCancelled || durable.Version() != 2 {
		t.Fatalf("cancelled Work = (%#v, %v)", durable, err)
	}

	replay, err := fixture.store.CommitManagedAcceptance(context.Background(),
		ManagedAcceptanceSpec{Operation: managedSpec.Operation}, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != result.Receipt.String() {
		t.Fatalf("context action replay = (%#v, %v)", replay, err)
	}
}

func TestCommitManagedAcceptanceRollsBackHandlingAndRunOnLateFailure(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	initial := reserveManagedOfferForAcceptance(t, fixture, "managed-rollback-source", fixture.now)
	initialAuthority := localAuthority(t, fixture, initial.Operation)
	initialSpec := fixture.offer(t, &initialAuthority, "managed-rollback-source", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitManagedAcceptance(context.Background(), ManagedAcceptanceSpec{
		Scope: initialSpec.Scope, Items: initialSpec.Items, Operation: initialAuthority,
	}, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	source := initialSpec.Items[0].Publication.Event()
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-managed-rollback", source.ID(), 1, claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-managed-rollback", "token-managed-rollback", claimAt)
	readAt := claimAt.Add(time.Second)
	current, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), currentReadSpec(
		fixture, claim.Run.ID(), "token-managed-rollback", readAt))
	if err != nil {
		t.Fatal(err)
	}
	operationAt := readAt.Add(time.Second)
	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), ManagedOperationSpec{
		Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("key-managed-rollback")),
		RequestDigest: model.Sum([]byte("request-managed-rollback")), Kind: model.OperationTeamworkCancel,
		LeaseOwner: "server-managed-rollback", At: operationAt, LeaseUntil: operationAt.Add(time.Minute),
		ClaimContextHash: model.Sum([]byte("token-managed-rollback")), HasClaimContext: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := managedCancelAcceptance(t, fixture, reservation, current, operationAt.Add(time.Second))
	workSpec := spec.Items[0].Work.Work.Spec()
	workSpec.StateData, _ = model.NewJSON([]byte(`{"tampered":true}`))
	tampered, _ := model.NewReviewWork(workSpec)
	spec.Items[0].Work.Work = tampered
	if _, err := fixture.store.CommitManagedAcceptance(context.Background(), spec,
		operationAt.Add(2*time.Second)); err == nil {
		t.Fatal("tampered managed acceptance succeeded")
	}
	handling, _ := readAgentHandling(context.Background(), fixture.store.db, claim.Handling.ID())
	run, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
	operation, _ := readOperationByID(context.Background(), fixture.store.db, reservation.Operation.ID())
	if handling.Status() != model.HandlingClaimed || run.Status() != model.AgentRunRunning ||
		operation.Status() != model.OperationStarted {
		t.Fatalf("late failure left partial authority: handling=%s run=%s operation=%s",
			handling.Status(), run.Status(), operation.Status())
	}
}

func reserveManagedOfferForAcceptance(t *testing.T, fixture *acceptanceFixture, suffix string,
	at time.Time,
) ManagedOperationReservation {
	t.Helper()
	result, err := fixture.store.ReserveManagedOperation(context.Background(), ManagedOperationSpec{
		Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("key-" + suffix)),
		RequestDigest: model.Sum([]byte("request-" + suffix)), Kind: model.OperationTeamworkOffer,
		LeaseOwner: "server-" + suffix, At: at, LeaseUntil: at.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func localAuthority(t testing.TB, fixture *acceptanceFixture,
	operation model.Operation,
) LocalOperationAuthority {
	t.Helper()
	return *mustLocalOperationAuthority(t, operation, fixture.policy)
}

func managedCancelAcceptance(t *testing.T, fixture *acceptanceFixture,
	reservation ManagedOperationReservation, current AgentCurrentReadResult,
	acceptedAt time.Time,
) ManagedAcceptanceSpec {
	t.Helper()
	work, err := fixture.store.GetReviewWork(context.Background(), current.Receipt.ActionWork())
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{work.Participants().ReviewerPeerID()})
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(), work.ChannelID(), audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, _ := scope.EventScope(0, work.Ref())
	eventID, _ := model.ParseEventID("event-managed-cancel-result")
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: scope.Node(), Profile: scope.Profile(), EventID: eventID, ChannelID: work.ChannelID(),
		WorkRef: work.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: work.Version(), Iteration: work.Iteration(),
		WorkDeadlineUnixNano: work.DeadlineUnixNano(), CausedBy: []model.EventKey{current.Receipt.SourceEvent()},
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{acceptedAt}, signer)
	candidate, _ := eventpkg.NewCancelCandidate("superseded")
	bundle, err := factory.AdmitAgent(context.Background(), stamp, candidate)
	if err != nil {
		t.Fatal(err)
	}
	nextSpec := work.Spec()
	nextSpec.Version++
	nextSpec.State = model.WorkCancelled
	nextSpec.StateData = bundle.Event().Payload()
	nextSpec.UpdatedBy = bundle.Event().ID()
	nextSpec.UpdatedAt = bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, work.Version(), work.State())
	if err != nil {
		t.Fatal(err)
	}
	authority := localAuthority(t, fixture, reservation.Operation)
	return ManagedAcceptanceSpec{Scope: scope, Operation: authority,
		Items: []LocalAcceptanceItem{{Publication: bundle.Publication(), Work: &mutation}}}
}

func assertManagedAcceptanceRun(t *testing.T, st *Store, runID model.RunID, receipt model.JSON) {
	t.Helper()
	run, err := readAgentRun(context.Background(), st.db, runID)
	if err != nil || run.Status() != model.AgentRunOutcomeAccepted {
		t.Fatalf("completed managed Run = (%#v, %v)", run, err)
	}
	outcome, outcomeOK := run.OutcomeReceipt()
	_, completionOK := run.CompletionReceipt()
	if !outcomeOK || completionOK || outcome.String() != receipt.String() {
		t.Fatalf("managed Run receipts = outcome (%s,%v) completion present %v, want independent outcome %s",
			outcome, outcomeOK, completionOK, receipt)
	}
}
