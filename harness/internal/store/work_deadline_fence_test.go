package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type deadlineCompetingFixture struct {
	fixture       *acceptanceFixture
	current       model.ReviewWork
	candidate     WorkDeadlineCandidate
	prepared      WorkExpiryPreparation
	item          LocalAcceptanceItem
	operationSpec ManagedOperationSpec
	operation     model.Operation
}

func TestCommitWorkExpiryFencesCompetingOperationLeaseDrift(t *testing.T) {
	test := newDeadlineCompetingFixture(t, "lease-drift")
	reclaim := test.operationSpec
	reclaim.LeaseOwner = "deadline-operation-reclaimed"
	reclaim.At = test.operationSpec.LeaseUntil
	reclaim.LeaseUntil = reclaim.At.Add(time.Minute)
	reclaimed, err := test.fixture.store.ReserveManagedOperation(context.Background(), reclaim)
	if err != nil || !reclaimed.Acquired || reclaimed.Operation.LeaseOwner() != reclaim.LeaseOwner {
		t.Fatalf("reclaim competing Operation = (%#v, %v)", reclaimed, err)
	}
	if _, err := test.fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: test.prepared, Expiry: test.item}, test.current.Deadline()); !errors.Is(err, ErrWorkDeadlineStale) {
		t.Fatalf("operation TOCTOU error = %v", err)
	}
	assertUnexpiredDeadlineWork(t, test.fixture.store, test.current.Ref(), test.current.Version())
	assertOperationStatus(t, test.fixture.store, reclaimed.Operation.ID(), model.OperationStarted)

	reprepared, err := test.fixture.store.PrepareWorkExpiry(context.Background(), test.candidate,
		test.current.Deadline())
	if err != nil || !reprepared.operation.found || reprepared.EventID() != test.prepared.EventID() {
		t.Fatalf("reprepare competing fence = (%#v, %v)", reprepared.operation, err)
	}
	frozenLease, _ := reprepared.operation.operation.LeaseUntil()
	if !frozenLease.Before(reprepared.AcceptedAt()) {
		t.Fatalf("test Operation lease %s is not expired at preparation %s",
			frozenLease, reprepared.AcceptedAt())
	}
}

func TestCommitWorkExpiryAtomicallyRejectsExactCompetingOperation(t *testing.T) {
	test := newDeadlineCompetingFixture(t, "atomic-rejection")
	mustExec(t, test.fixture.store, `CREATE TRIGGER test_timer_expiry_rejection_failure
		BEFORE UPDATE ON operations WHEN OLD.operation_id='`+test.operation.ID().String()+`'
		AND NEW.status='rejected' BEGIN SELECT RAISE(ABORT,'injected expiry rejection failure'); END`)
	if _, err := test.fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: test.prepared, Expiry: test.item}, test.current.Deadline()); err == nil {
		t.Fatal("injected operation rejection failure did not abort expiry")
	}
	assertUnexpiredDeadlineWork(t, test.fixture.store, test.current.Ref(), test.current.Version())
	assertOperationStatus(t, test.fixture.store, test.operation.ID(), model.OperationStarted)
	mustExec(t, test.fixture.store, "DROP TRIGGER test_timer_expiry_rejection_failure")

	result, err := test.fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: test.prepared, Expiry: test.item}, test.current.Deadline().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	operationID, receipt, ok := result.RejectedOperation()
	wantReceipt, _ := buildWorkExpiredReceipt(test.operation.ID())
	if !ok || operationID != test.operation.ID() || receipt.String() != wantReceipt.String() {
		t.Fatalf("expiry rejection = %s %s %t", operationID, receipt, ok)
	}
	assertCommittedWorkExpiry(t, test.fixture.store, test.current, test.prepared.EventID(), "pending", "")
	assertRejectedDeadlineOperation(t, test.fixture.store, operationID, wantReceipt)
}

func newDeadlineCompetingFixture(t *testing.T, suffix string) deadlineCompetingFixture {
	t.Helper()
	fixture, events := newAgentClaimFixture(t, 1, "deadline-competing-"+suffix)
	source, err := readCurrentSourceEvent(context.Background(), fixture.store.db, events[0])
	if err != nil {
		t.Fatal(err)
	}
	current, err := fixture.store.GetReviewWork(context.Background(), source.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-deadline-"+suffix, events[0], 1, claimAt, claimAt, 0)
	token := "token-deadline-" + suffix
	claim := claimCurrent(t, fixture, "owner-deadline-"+suffix, token, claimAt)
	read, err := fixture.store.FinalizeAgentCurrentRead(context.Background(),
		currentReadSpec(fixture, claim.Run.ID(), token, claimAt.Add(time.Second)))
	if err != nil || read.Receipt.ActionWork() != current.Ref() ||
		read.Receipt.ActionWorkVersion() != current.Version() {
		t.Fatalf("managed current = (%#v, %v)", read, err)
	}
	operationSpec := ManagedOperationSpec{Profile: fixture.profile,
		ClientKeyHash: model.Sum([]byte("deadline-competing-key-" + suffix)),
		RequestDigest: model.Sum([]byte("deadline-competing-request-" + suffix)),
		Kind:          model.OperationTeamworkCancel, LeaseOwner: "deadline-operation-first",
		At: claimAt.Add(2 * time.Second), LeaseUntil: claimAt.Add(time.Minute),
		ClaimContextHash: model.Sum([]byte(token)), HasClaimContext: true}
	reserved, err := fixture.store.ReserveManagedOperation(context.Background(), operationSpec)
	if err != nil || !reserved.Acquired || reserved.Operation.Status() != model.OperationStarted {
		t.Fatalf("reserve competing Operation = (%#v, %v)", reserved, err)
	}
	candidate := onlyWorkDeadlineCandidate(t, fixture.store, current.Deadline())
	prepared, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
	if err != nil || !prepared.operation.found || prepared.operation.operation.ID() != reserved.Operation.ID() {
		t.Fatalf("prepared competing fence = (%#v, %v)", prepared.operation, err)
	}
	return deadlineCompetingFixture{fixture, current, candidate, prepared,
		preparedExpiryItem(t, fixture, prepared, prepared.EventID()), operationSpec, reserved.Operation}
}

func assertRejectedDeadlineOperation(t *testing.T, store *Store, operation model.OperationID,
	want model.JSON,
) {
	t.Helper()
	var status string
	var stored []byte
	if err := store.db.QueryRow(`SELECT status,result_json FROM operations WHERE operation_id=?`,
		operation.String()).Scan(&status, &stored); err != nil ||
		status != string(model.OperationRejected) || string(stored) != want.String() {
		t.Fatalf("terminal competing Operation = %q %s, err=%v", status, stored, err)
	}
}
