package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerInboxSemanticOutcomeBindsExactRejectedOperation(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "receipt-operation-link",
		model.EventReviewCancelled)
	claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
	installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
		fixture.source.AcceptedAt().Add(500*time.Millisecond))
	operation := reservePeerInboxSemanticSettlementOperation(t, fixture, claim, token, "receipt-link")
	tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
	advancePeerInboxSemanticSettlementWork(t, tx, fixture)
	if err := settlePeerInboxSemanticHandling(context.Background(), tx,
		fixture.settlement, fixture.settleAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	settledRun, err := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	outcome, ok := settledRun.OutcomeReceipt()
	linked, linkErr := peerInboxSemanticHandlingOutcomeOperationID(outcome)
	if !ok || linkErr != nil || linked != operation.ID() {
		t.Fatalf("settled outcome operation link = (%s, %v, %v)", linked.String(), ok, linkErr)
	}
	missingLink, err := peerInboxSemanticHandlingOutcomeReceipt(fixture.settlement,
		fixture.handling, claim.Run, model.OperationID{}, fixture.settleAt)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, `DROP TRIGGER agent_runs_evidence_once`)
	mustExec(t, fixture.store, `UPDATE agent_runs SET outcome_receipt_json=? WHERE run_id=?`,
		missingLink.Bytes(), claim.Run.ID().String())
	validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
	err = validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
		fixture.settlement)
	_ = validateTx.Rollback()
	if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("missing Operation link validation error = %v", err)
	}
}

func TestCommitPeerInboxSemanticReceiptOnlyAcceptsClosedOutcomeWithoutResponse(t *testing.T) {
	ctx := context.Background()
	fixture := newPeerInboxFixture(t, "semantic-closed-receipt", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	request, initial, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-closed-receipt", 1, 1)
	accepted := commitPeerInboxSemanticPublication(t, fixture, request, fixture.at,
		"semantic-closed-accept", fixture.at.Add(2*time.Second)).Responses[0].Event()
	active, err := fixture.store.GetReviewWork(ctx, initial.Ref())
	if err != nil || active.State() != model.WorkActive || active.Version() != 2 {
		t.Fatalf("active Work = (%#v,%v)", active, err)
	}
	delivery := peerInboxSemanticRemoteDeliveryReady(t, fixture, active, accepted,
		2, 2, fixture.at.Add(3*time.Second))
	delivered := commitPeerInboxSemanticPublication(t, fixture, delivery,
		fixture.at.Add(4*time.Second), "semantic-closed-deliver",
		fixture.at.Add(6*time.Second)).Responses[0].Event()
	deliveredWork, err := fixture.store.GetReviewWork(ctx, initial.Ref())
	if err != nil || deliveredWork.State() != model.WorkDelivered || deliveredWork.Version() != 3 {
		t.Fatalf("delivered Work = (%#v,%v)", deliveredWork, err)
	}
	closed := commitPeerInboxSemanticLocalClose(t, fixture, deliveredWork, delivered,
		fixture.at.Add(7*time.Second))
	outcome := peerInboxSemanticRemoteOutcome(t, fixture, closed, deliveredWork.Version(),
		deliveredWork.Iteration(), 3, 3, fixture.at.Add(8*time.Second))
	result := commitPeerInboxSemanticPublication(t, fixture, outcome, fixture.at.Add(9*time.Second),
		"semantic-closed-outcome", fixture.at.Add(11*time.Second))
	if result.Status != model.InboxAccepted || len(result.Responses) != 0 {
		t.Fatalf("closed outcome receipt-only result = %#v", result)
	}
	assertPeerInboxSemanticTerminalProjection(t, fixture.store, result.InboxID,
		model.InboxAccepted, outcome.Event().ID(), model.EventID{})
}

type committedPeerInboxSemanticPublication struct {
	InboxID   model.InboxID
	Status    model.InboxStatus
	Responses []model.SignedPublication
}

func commitPeerInboxSemanticPublication(t *testing.T, fixture peerInboxFixture,
	publication model.SignedPublication, putAt time.Time, owner string, commitAt time.Time,
) committedPeerInboxSemanticPublication {
	t.Helper()
	put := fixture.put(t, publication, putAt)
	readyAt := putAt.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, owner, readyAt.Add(time.Nanosecond))
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, commitAt)
	result, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec, commitAt)
	if err != nil || !result.Changed() || result.Replayed() {
		t.Fatalf("commit Peer Inbox semantic publication = (%#v,%v)", result, err)
	}
	return committedPeerInboxSemanticPublication{InboxID: put.InboxID,
		Status: result.Status(), Responses: spec.Responses}
}

func peerInboxSemanticRemoteDeliveryReady(t *testing.T, fixture peerInboxFixture,
	current model.ReviewWork, cause model.Event, originSequence, channelSequence uint64,
	at time.Time,
) model.SignedPublication {
	t.Helper()
	scope := peerInboxSemanticRemoteScope(t, fixture, current.Ref(), originSequence, channelSequence)
	audience, _ := model.NewAudience([]model.PeerID{fixture.channel.Owner().PeerID()})
	payload, _ := model.JSONFrom(struct {
		Content     string `json:"content"`
		Iteration   uint8  `json:"iteration"`
		WorkVersion uint64 `json:"work_version"`
	}{"closed receipt delivery", current.Iteration(), current.Version()})
	id, _ := model.ParseEventID("event-inbox-closed-receipt-delivery")
	return fixture.signEvent(t, model.EventSpec{ID: id, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-remote",
		Type: model.EventReviewDeliveryReady, Audience: audience,
		Summary: "semantic closed receipt delivery", Payload: payload,
		CausedBy:  []model.EventKey{cause.Key()},
		CreatedAt: at, AcceptedAt: at})
}

func commitPeerInboxSemanticLocalClose(t *testing.T, fixture peerInboxFixture,
	current model.ReviewWork, cause model.Event, at time.Time,
) model.Event {
	t.Helper()
	local := fixture.channel.Owner()
	scope := peerInboxSemanticLocalScope(t, fixture, current.Ref(), 300, 300)
	audience, _ := model.NewAudience([]model.PeerID{fixture.remote.Identity().PeerID()})
	payload, _ := model.JSONFrom(struct {
		Iteration   uint8  `json:"iteration"`
		Note        string `json:"note"`
		WorkVersion uint64 `json:"work_version"`
	}{current.Iteration(), "", current.Version()})
	id, _ := model.ParseEventID("event-inbox-closed-receipt-close")
	publication := fixture.signEventAs(t, model.EventSpec{ID: id, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-local",
		Type: model.EventReviewClosed, Audience: audience, Summary: "semantic closed receipt close",
		Payload: payload, CausedBy: []model.EventKey{cause.Key()},
		CreatedAt: at, AcceptedAt: at}, local)
	spec := current.Spec()
	spec.Version = current.Version() + 1
	spec.State = model.WorkClosed
	spec.StateData = publication.Event().Payload()
	spec.UpdatedBy = publication.Event().ID()
	spec.UpdatedAt = at
	closed, err := model.NewReviewWork(spec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(closed, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if err := applyWorkMutation(context.Background(), tx, mutation, publication.Event()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return publication.Event()
}

func peerInboxSemanticRemoteOutcome(t *testing.T, fixture peerInboxFixture, cause model.Event,
	version uint64, iteration uint8, originSequence, channelSequence uint64, at time.Time,
) model.SignedPublication {
	t.Helper()
	scope := peerInboxSemanticRemoteScope(t, fixture, cause.Scope().WorkRef(), originSequence,
		channelSequence)
	audience, _ := model.NewAudience([]model.PeerID{fixture.channel.Owner().PeerID()})
	payload, _ := model.JSONFrom(struct {
		DecisionRef    string `json:"decision_ref"`
		DiagnosticCode string `json:"diagnostic_code"`
		Iteration      uint8  `json:"iteration"`
		Status         string `json:"status"`
		WorkVersion    uint64 `json:"work_version"`
	}{cause.ID().String(), "applied", iteration, "accepted", version})
	id, _ := model.ParseEventID("event-inbox-closed-receipt-outcome")
	return fixture.signEvent(t, model.EventSpec{ID: id, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-remote",
		Type: model.EventReviewOutcome, Audience: audience,
		Summary: "semantic closed receipt outcome", Payload: payload,
		CausedBy:  []model.EventKey{cause.Key()},
		CreatedAt: at, AcceptedAt: at})
}

func peerInboxSemanticRemoteScope(t *testing.T, fixture peerInboxFixture, ref model.WorkRef,
	originSequence, channelSequence uint64,
) model.EventScope {
	t.Helper()
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(),
		fixture.remote.Identity().PeerID(), fixture.remote.Identity().OriginEpoch(),
		originSequence, channelSequence, fixture.remote.Member().Head(),
		fixture.channel.Roster().Head(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func peerInboxSemanticLocalScope(t *testing.T, fixture peerInboxFixture, ref model.WorkRef,
	originSequence, channelSequence uint64,
) model.EventScope {
	t.Helper()
	local := fixture.channel.Owner()
	member, ok := fixture.channel.Roster().CurrentMember(local.PeerID())
	if !ok {
		t.Fatal("local member missing from fixture roster")
	}
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(), local.PeerID(),
		local.OriginEpoch(), originSequence, channelSequence, member.Head(),
		fixture.channel.Roster().Head(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}
