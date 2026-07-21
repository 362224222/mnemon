package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestResolveDeadlineWinnerCommitsAtomicEvidenceAndReplaysAfterRestart(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "atomic")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"atomic", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "atomic")

	result, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec, deadline)
	if err != nil || result.Replayed {
		t.Fatalf("ResolveDeadlineWinner() = (%#v, %v)", result, err)
	}
	wantReceipt := `{"code":"work_expired","message":"Work deadline reached before action commit",` +
		`"operation_id":"` + operation.ID().String() + `","replayed":false,"retryable":false,` +
		`"schema_version":1,"status":"error"}`
	if result.Receipt.String() != wantReceipt || !strings.Contains(result.Receipt.String(), `"code":"work_expired"`) {
		t.Fatalf("deadline receipt = %s", result.Receipt.String())
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
	assertAcceptanceHeads(t, fixture.store, 3, 2)

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	replaySpec := DeadlineResolutionSpec{Action: *authority}
	replaySpec.Action.LeaseOwner = "different-replay-owner"
	replay, err := restarted.ResolveDeadlineWinner(context.Background(), replaySpec, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != result.Receipt.String() {
		t.Fatalf("restart replay = (%#v, %v)", replay, err)
	}
	assertDeadlineWinner(t, restarted, current.Ref(), operation.ID(), current.Version()+1, 1)
}

func TestResolveDeadlineWinnerCommitsOverdueWorkAfterRestart(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "overdue-restart")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"overdue-restart", model.OperationTeamworkCancel)
	trustedNow := current.Deadline().Add(time.Minute)
	spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, trustedNow, "overdue-restart")

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	if _, err := restarted.ResolveDeadlineWinner(context.Background(), spec, trustedNow); err != nil {
		t.Fatalf("overdue resolution after restart error = %v", err)
	}
	assertDeadlineWinner(t, restarted, current.Ref(), operation.ID(), current.Version()+1, 1)
}

func TestResolveDeadlineWinnerBeatsConcurrentHomeAction(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "race")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"race", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	expiry := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "race")
	cancel := admittedCancelAction(t, fixture, current, authority, deadline, "race")

	start := make(chan struct{})
	var wait sync.WaitGroup
	var expiryResult DeadlineResolutionResult
	var expiryErr, cancelErr error
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		expiryResult, expiryErr = fixture.store.ResolveDeadlineWinner(context.Background(), expiry, deadline)
	}()
	go func() {
		defer wait.Done()
		<-start
		_, cancelErr = fixture.store.CommitLocalAcceptance(context.Background(), cancel, deadline)
	}()
	close(start)
	wait.Wait()

	if expiryErr != nil || expiryResult.Replayed {
		t.Fatalf("expiry race result = (%#v, %v)", expiryResult, expiryErr)
	}
	if cancelErr == nil {
		t.Fatal("competing cancellation committed at the deadline")
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
	var cancelled int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM events WHERE event_type='review.cancelled'").Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != 0 {
		t.Fatalf("cancelled Event count = %d, want 0", cancelled)
	}
}

func TestResolveDeadlineWinnerCannotBeBypassedByPreDeadlineEventTime(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "predeadline-event")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"predeadline-event", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	cancel := admittedCancelAction(t, fixture, current, authority, deadline.Add(-time.Nanosecond),
		"predeadline-event")

	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), cancel, deadline); !errors.Is(err, ErrDeadlineResolution) {
		t.Fatalf("pre-deadline Event at deadline commit error = %v", err)
	}
	assertDeadlineRollback(t, fixture.store, current.Ref(), operation.ID())
	var cancellations int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM events WHERE event_type='review.cancelled'").
		Scan(&cancellations); err != nil {
		t.Fatal(err)
	}
	if cancellations != 0 {
		t.Fatalf("pre-deadline cancellation Event count = %d, want 0", cancellations)
	}

	expiry := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline,
		"predeadline-event")
	result, err := fixture.store.ResolveDeadlineWinner(context.Background(), expiry, deadline)
	if err != nil || result.Replayed {
		t.Fatalf("deadline recovery = (%#v, %v)", result, err)
	}
	if !strings.Contains(result.Receipt.String(), `"code":"work_expired"`) {
		t.Fatalf("deadline recovery receipt = %s", result.Receipt)
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
}

func TestResolveDeadlineWinnerConvergesConcurrentRetries(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "concurrent-retry")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"concurrent-retry", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "concurrent-retry")

	start := make(chan struct{})
	results := make([]DeadlineResolutionResult, 2)
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index := range results {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results[index], errs[index] = fixture.store.ResolveDeadlineWinner(context.Background(), spec, deadline)
		}()
	}
	close(start)
	wait.Wait()

	replayed := 0
	for index := range results {
		if errs[index] != nil {
			t.Fatalf("concurrent resolution %d error = %v", index, errs[index])
		}
		if results[index].Receipt.String() != results[0].Receipt.String() {
			t.Fatalf("concurrent receipts differ: %s / %s", results[0].Receipt, results[index].Receipt)
		}
		if results[index].Replayed {
			replayed++
		}
	}
	if replayed != 1 {
		t.Fatalf("concurrent replay count = %d, want 1", replayed)
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
}

func TestResolveDeadlineWinnerRollsBackEveryEffectOnLateFailure(t *testing.T) {
	t.Parallel()
	fixture, current := newDeadlineWorkFixture(t, "rollback")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
		"rollback", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "rollback")
	eventID := spec.Expiry.Publication.Event().ID().String()
	mustExec(t, fixture.store, `CREATE TRIGGER test_deadline_delivery_failure
		BEFORE INSERT ON peer_deliveries WHEN NEW.event_id='`+eventID+`'
		BEGIN SELECT RAISE(ABORT, 'injected deadline delivery failure'); END`)

	if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec, deadline); err == nil {
		t.Fatal("injected late failure did not abort deadline resolution")
	}
	assertDeadlineRollback(t, fixture.store, current.Ref(), operation.ID())
	assertAcceptanceHeads(t, fixture.store, 2, 1)
	mustExec(t, fixture.store, "DROP TRIGGER test_deadline_delivery_failure")

	if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec, deadline); err != nil {
		t.Fatalf("retry after rollback error = %v", err)
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
}

func TestResolveDeadlineWinnerReconcilesNestedDerivationInSameTransaction(t *testing.T) {
	t.Parallel()
	fixture := newDerivationDispositionFixture(t, false)
	current, err := fixture.store.GetReviewWork(context.Background(), fixture.children[1])
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, "UPDATE node SET next_origin_seq=42 WHERE singleton=1")
	operation, authority, contextHash := reserveDeadlineAction(t, fixture.acceptanceFixture, current,
		"nested", model.OperationTeamworkCancel)
	deadline := current.Deadline()
	spec := admittedDeadlineResolution(t, fixture.acceptanceFixture, current, authority, contextHash, deadline, "nested")

	if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec, deadline); err != nil {
		t.Fatal(err)
	}
	assertDeadlineWinner(t, fixture.store, current.Ref(), operation.ID(), current.Version()+1, 1)
	handling := fixture.handling(t)
	if handling.Status() != model.HandlingPending || handling.LastDisposition() != "" ||
		handling.EventID() != spec.Expiry.Publication.Event().ID() {
		t.Fatalf("nested expiry disposition = %#v", handling)
	}
	assertDispositionHandlingPins(t, fixture, 1)
}

func TestResolveDeadlineWinnerRejectsEarlyForgedAndContextlessInputsWithoutWrites(t *testing.T) {
	t.Parallel()
	t.Run("trusted time before admitted expiry", func(t *testing.T) {
		fixture, current := newDeadlineWorkFixture(t, "early")
		operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
			"early", model.OperationTeamworkCancel)
		deadline := current.Deadline()
		spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "early")
		if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec,
			deadline.Add(-time.Nanosecond)); !errors.Is(err, ErrDeadlineResolution) {
			t.Fatalf("early trusted time error = %v", err)
		}
		assertDeadlineRollback(t, fixture.store, current.Ref(), operation.ID())
	})

	t.Run("forged controller signature", func(t *testing.T) {
		fixture, current := newDeadlineWorkFixture(t, "forged")
		operation, authority, contextHash := reserveDeadlineAction(t, fixture, current,
			"forged", model.OperationTeamworkCancel)
		deadline := current.Deadline()
		spec := admittedDeadlineResolution(t, fixture, current, authority, contextHash, deadline, "forged")
		body := spec.Expiry.Publication.Body()
		spec.Expiry.Publication, _ = model.AttachSignature(body, make([]byte, ed25519.SignatureSize))
		if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec,
			deadline); !errors.Is(err, ErrPublicationInvalid) {
			t.Fatalf("forged signature error = %v", err)
		}
		assertDeadlineRollback(t, fixture.store, current.Ref(), operation.ID())
	})

	t.Run("contextless operation", func(t *testing.T) {
		fixture, current := newDeadlineWorkFixture(t, "contextless")
		operation, authority, _ := reserveDeadlineAction(t, fixture, current,
			"contextless", model.OperationTeamworkOffer)
		deadline := current.Deadline()
		spec := admittedDeadlineResolution(t, fixture, current, authority,
			model.Sum([]byte("not-operation-context")), deadline, "contextless")
		if _, err := fixture.store.ResolveDeadlineWinner(context.Background(), spec,
			deadline); !errors.Is(err, ErrDeadlineResolution) {
			t.Fatalf("contextless operation error = %v", err)
		}
		assertDeadlineRollback(t, fixture.store, current.Ref(), operation.ID())
	})
}

func newDeadlineWorkFixture(t *testing.T, suffix string) (*acceptanceFixture, model.ReviewWork) {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "deadline-work-"+suffix, nil)
	offer := fixture.offer(t, authority, "deadline-work-"+suffix, fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), offer, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	current, err := fixture.store.GetReviewWork(context.Background(), offer.Items[0].Work.Work.Ref())
	if err != nil {
		t.Fatal(err)
	}
	return fixture, current
}

func reserveDeadlineAction(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	suffix string, kind model.OperationKind,
) (model.Operation, *LocalOperationAuthority, model.Digest) {
	t.Helper()
	runID, _ := model.ParseRunID("run-deadline-" + suffix)
	started := current.Deadline().Add(-time.Minute)
	mustExec(t, fixture.store, `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES(?,?,'{}','test',?,'{}','{}','running',?)`, runID.String(), model.TeamworkProfileID().String(),
		string(fixture.profile.Runtime()), storeTime(started))
	operationID, _ := model.ParseOperationID("operation-deadline-" + suffix)
	contextHash := model.Sum([]byte("context-deadline-" + suffix))
	leaseUntil := current.Deadline().Add(10 * time.Minute)
	var operationContext *model.Digest
	if kind != model.OperationTeamworkOffer {
		operationContext = &contextHash
	}
	operation, err := model.NewOperation(model.OperationSpec{
		ID: operationID, ProfileID: model.TeamworkProfileID(), AgentRunID: runID,
		ClientKeyHash: model.Sum([]byte("key-deadline-" + suffix)), ContextHash: operationContext, Kind: kind,
		RequestDigest: model.Sum([]byte("request-deadline-" + suffix)), Status: model.OperationStarted,
		LeaseOwner: "owner-deadline-" + suffix, LeaseUntil: &leaseUntil, CreatedAt: started,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(context.Background(), operation, started.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	authority := &LocalOperationAuthority{operation.ID(), operation.Kind(), operation.RequestDigest(), operation.LeaseOwner()}
	return operation, authority, contextHash
}

func admittedDeadlineResolution(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	authority *LocalOperationAuthority, contextHash model.Digest, acceptedAt time.Time, suffix string,
) DeadlineResolutionSpec {
	t.Helper()
	scope, stamp := deadlineAdmissionStamp(t, fixture, current, acceptedAt, "event-deadline-expired-"+suffix)
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{acceptedAt}, signer)
	bundle, err := factory.AdmitController(context.Background(), stamp, eventpkg.ExpiredDecision{})
	if err != nil {
		t.Fatal(err)
	}
	nextSpec := current.Spec()
	nextSpec.Version++
	nextSpec.State = model.WorkExpired
	nextSpec.StateData = bundle.Event().Payload()
	nextSpec.UpdatedBy = bundle.Event().ID()
	nextSpec.UpdatedAt = acceptedAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}
	return DeadlineResolutionSpec{Scope: scope,
		Expiry: LocalAcceptanceItem{Publication: bundle.Publication(), Work: &mutation},
		Action: *authority, ContextHash: contextHash}
}

func admittedCancelAction(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	authority *LocalOperationAuthority, acceptedAt time.Time, suffix string,
) LocalAcceptanceSpec {
	t.Helper()
	scope, stamp := deadlineAdmissionStamp(t, fixture, current, acceptedAt, "event-deadline-cancel-"+suffix)
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{acceptedAt}, signer)
	candidate, _ := eventpkg.NewCancelCandidate("cancel at deadline")
	bundle, err := factory.AdmitAgent(context.Background(), stamp, candidate)
	if err != nil {
		t.Fatal(err)
	}
	nextSpec := current.Spec()
	nextSpec.Version++
	nextSpec.State = model.WorkCancelled
	nextSpec.StateData = bundle.Event().Payload()
	nextSpec.UpdatedBy = bundle.Event().ID()
	nextSpec.UpdatedAt = acceptedAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}
	return LocalAcceptanceSpec{Scope: scope,
		Items: []LocalAcceptanceItem{{Publication: bundle.Publication(), Work: &mutation}}, Operation: authority}
}

func deadlineAdmissionStamp(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	acceptedAt time.Time, eventText string,
) (LocalAdmissionScope, eventpkg.AdmissionStamp) {
	t.Helper()
	audience, _ := model.NewAudience([]model.PeerID{current.Participants().ReviewerPeerID()})
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(), current.ChannelID(), audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, _ := scope.EventScope(0, current.Ref())
	var originText, epochText string
	if err := fixture.store.db.QueryRow("SELECT origin_peer_id,origin_epoch FROM events WHERE event_id=?",
		current.UpdatedBy().String()).Scan(&originText, &epochText); err != nil {
		t.Fatal(err)
	}
	origin, _ := model.ParsePeerID(originText)
	epoch, _ := model.ParseOriginEpoch(epochText)
	cause, _ := model.NewEventKey(origin, epoch, current.UpdatedBy())
	eventID, _ := model.ParseEventID(eventText)
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: scope.Node(), Profile: scope.Profile(), EventID: eventID, ChannelID: current.ChannelID(),
		WorkRef: current.Ref(), OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
		OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: current.Version(), Iteration: current.Iteration(), WorkDeadlineUnixNano: current.DeadlineUnixNano(),
		CausedBy: []model.EventKey{cause},
	})
	if err != nil {
		t.Fatal(err)
	}
	return scope, stamp
}

func assertDeadlineWinner(t *testing.T, store *Store, ref model.WorkRef,
	operation model.OperationID, wantVersion uint64, wantExpiryEvents int,
) {
	t.Helper()
	work, err := store.GetReviewWork(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if work.State() != model.WorkExpired || work.Version() != wantVersion {
		t.Fatalf("deadline Work = %s version %d", work.State(), work.Version())
	}
	var status string
	var result []byte
	if err := store.db.QueryRow("SELECT status,result_json FROM operations WHERE operation_id=?",
		operation.String()).Scan(&status, &result); err != nil {
		t.Fatal(err)
	}
	wantReceipt, _ := buildWorkExpiredReceipt(operation)
	if status != string(model.OperationRejected) || string(result) != wantReceipt.String() {
		t.Fatalf("deadline operation = status %q result %s", status, string(result))
	}
	var events, publications, deliveries int
	if err := store.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM events WHERE event_type='review.expired'),
		(SELECT COUNT(*) FROM gossip_publications WHERE event_id IN
			(SELECT event_id FROM events WHERE event_type='review.expired')),
		(SELECT COUNT(*) FROM peer_deliveries WHERE event_id IN
			(SELECT event_id FROM events WHERE event_type='review.expired'))`).Scan(&events, &publications, &deliveries); err != nil {
		t.Fatal(err)
	}
	if events != wantExpiryEvents || publications != wantExpiryEvents || deliveries != wantExpiryEvents {
		t.Fatalf("deadline evidence = events %d publications %d deliveries %d", events, publications, deliveries)
	}
}

func assertDeadlineRollback(t *testing.T, store *Store, ref model.WorkRef, operation model.OperationID) {
	t.Helper()
	work, err := store.GetReviewWork(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if work.State() != model.WorkOffered || work.Version() != 1 {
		t.Fatalf("rolled-back Work = %s version %d", work.State(), work.Version())
	}
	assertOperationStatus(t, store, operation, model.OperationStarted)
	var expiryEvents int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM events WHERE event_type='review.expired'").Scan(&expiryEvents); err != nil {
		t.Fatal(err)
	}
	if expiryEvents != 0 {
		t.Fatalf("rolled-back expiry Event count = %d", expiryEvents)
	}
}
