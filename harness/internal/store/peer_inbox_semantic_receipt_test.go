package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerInboxSemanticOutcomeBindsExactRejectedOperation(t *testing.T) {
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
