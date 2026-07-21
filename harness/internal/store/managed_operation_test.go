package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReserveManagedOperationBindsExactCurrentAndFencesOneStartedAction(t *testing.T) {
	t.Parallel()
	fixture := newManagedContextFixture(t, "authority",
		model.OperationTeamworkAccept, model.OperationTeamworkDecline)
	spec := fixture.spec("authority-key", "authority-request", model.OperationTeamworkAccept)

	first, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil || first.Replayed || !first.Acquired || !first.HasHandling {
		t.Fatalf("first managed reserve = (%#v, %v)", first, err)
	}
	wantID, _ := managedOperationID(fixture.profile.ID(), spec.ClientKeyHash)
	contextHash, hasContext := first.Operation.ContextHash()
	if first.Operation.ID() != wantID || first.Operation.AgentRunID() != fixture.claim.Run.ID() ||
		first.Run.ID() != fixture.claim.Run.ID() || first.Handling.ID() != fixture.claim.Handling.ID() ||
		!hasContext || contextHash != fixture.tokenHash {
		t.Fatalf("managed reserve did not derive exact authority: %#v", first)
	}

	replay, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil || !replay.Replayed || !replay.Acquired || replay.Operation.ID() != first.Operation.ID() {
		t.Fatalf("same owner replay = (%#v, %v)", replay, err)
	}

	contender := spec
	contender.LeaseOwner = "server-contender"
	contender.At = spec.At.Add(time.Second)
	contender.LeaseUntil = contender.At.Add(time.Minute)
	pending, err := fixture.store.ReserveManagedOperation(context.Background(), contender)
	if !errors.Is(err, ErrOperationPending) || pending.Operation.ID() != first.Operation.ID() {
		t.Fatalf("active lease contender = (%#v, %v)", pending, err)
	}

	changedRequest := spec
	changedRequest.RequestDigest = model.Sum([]byte("changed-authority-request"))
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), changedRequest); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("started request mismatch error = %v", err)
	}

	notAllowed := fixture.spec("not-allowed-key", "not-allowed-request", model.OperationTeamworkDeliver)
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), notAllowed); !errors.Is(err, ErrManagedActionNotAllowed) {
		t.Fatalf("unlisted current action error = %v", err)
	}

	second := fixture.spec("second-key", "second-request", model.OperationTeamworkDecline)
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), second); !errors.Is(err, ErrOperationPending) {
		t.Fatalf("second started context action error = %v", err)
	}
	var operations int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM operations WHERE context_hash=? AND status='started'`,
		fixture.tokenHash.Bytes()).Scan(&operations); err != nil || operations != 1 {
		t.Fatalf("started context operation count = %d, err=%v", operations, err)
	}

	reclaim := spec
	reclaim.LeaseOwner = "server-reclaimer"
	reclaim.At = spec.LeaseUntil
	reclaim.LeaseUntil = reclaim.At.Add(time.Minute)
	reclaimed, err := fixture.store.ReserveManagedOperation(context.Background(), reclaim)
	if err != nil || !reclaimed.Replayed || !reclaimed.Acquired ||
		reclaimed.Operation.LeaseOwner() != reclaim.LeaseOwner {
		t.Fatalf("expired operation lease reclaim = (%#v, %v)", reclaimed, err)
	}
}

func TestReserveManagedOperationRejectsStaleContextAndCrossRunTakeover(t *testing.T) {
	t.Parallel()
	t.Run("wrong context", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "wrong-context", model.OperationTeamworkAccept)
		spec := fixture.spec("wrong-context-key", "wrong-context-request", model.OperationTeamworkAccept)
		spec.ClaimContextHash = model.Sum([]byte("another-token"))
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("wrong context error = %v", err)
		}
	})

	t.Run("lease boundary", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "lease-boundary", model.OperationTeamworkAccept)
		claimLease, _ := fixture.claim.Run.LeaseUntil()
		spec := fixture.spec("lease-key", "lease-request", model.OperationTeamworkAccept)
		spec.At, spec.LeaseUntil = claimLease, claimLease.Add(time.Minute)
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("expired context boundary error = %v", err)
		}
	})

	t.Run("current not read", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "unread")
		at := fixture.now.Add(time.Minute)
		insertClaimHandling(t, fixture.store, "handling-unread", events[0], 1, at, at, 0)
		claim := claimCurrent(t, fixture, "owner-unread", "token-unread", at)
		spec := ManagedOperationSpec{Profile: fixture.profile,
			ClientKeyHash: model.Sum([]byte("unread-key")), RequestDigest: model.Sum([]byte("unread-request")),
			Kind: model.OperationTeamworkAccept, LeaseOwner: "server-unread", At: at.Add(time.Second),
			LeaseUntil: at.Add(time.Minute), ClaimContextHash: model.Sum([]byte("token-unread")), HasClaimContext: true}
		if claim.Run.ID().IsZero() {
			t.Fatal("claim fixture has no Run")
		}
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("unread current error = %v", err)
		}
	})

	t.Run("stale attempt", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "stale-attempt", model.OperationTeamworkAccept)
		oldHash := fixture.tokenHash
		lease, _ := fixture.claim.Run.LeaseUntil()
		probe := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: lease}
		if _, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil {
			t.Fatal(err)
		}
		retryAt := lease.Add(5 * time.Second)
		second := claimCurrent(t, fixture.acceptanceFixture, "owner-stale-attempt-two", "token-stale-attempt-two", retryAt)
		if second.Run.ID() == fixture.claim.Run.ID() {
			t.Fatal("new attempt reused old Run")
		}
		spec := fixture.spec("stale-attempt-key", "stale-attempt-request", model.OperationTeamworkAccept)
		spec.At, spec.LeaseUntil, spec.ClaimContextHash = retryAt, retryAt.Add(time.Minute), oldHash
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("old attempt context error = %v", err)
		}
	})

	t.Run("runtime drift", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "runtime-drift", model.OperationTeamworkAccept)
		mustExec(t, fixture.store, `DROP TRIGGER agent_runs_creation_identity_immutable`)
		if _, err := fixture.store.db.Exec("UPDATE agent_runs SET runtime_kind='claude-cli' WHERE run_id=?",
			fixture.claim.Run.ID().String()); err != nil {
			t.Fatal(err)
		}
		spec := fixture.spec("runtime-drift-key", "runtime-drift-request", model.OperationTeamworkAccept)
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("runtime-drift context error = %v", err)
		}
	})

	t.Run("started operation cannot cross Run", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "cross-run", model.OperationTeamworkAccept)
		spec := fixture.spec("cross-run-key", "cross-run-request", model.OperationTeamworkAccept)
		forgedRun, _ := model.ParseRunID("run-cross-run-unrelated")
		mustExec(t, fixture.store, `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
			launcher_diagnostic_json,runtime_ids_json,status,started_at)
			VALUES(?,?,'{}','test',?,'{}','{}','running',?)`, forgedRun.String(), fixture.profile.ID().String(),
			string(fixture.profile.Runtime()), storeTime(spec.At.Add(-time.Second)))
		operationID, _ := managedOperationID(fixture.profile.ID(), spec.ClientKeyHash)
		forged, err := newManagedStartedOperation(operationID, fixture.profile.ID(), forgedRun,
			spec.ClientKeyHash, &fixture.tokenHash, spec.Kind, spec.RequestDigest,
			spec.LeaseOwner, spec.At, spec.LeaseUntil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ReserveOperation(context.Background(), forged, spec.At); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrOperationMismatch) {
			t.Fatalf("cross-Run started replay error = %v", err)
		}
	})
}

func TestReserveManagedOperationTerminalReplayPrecedesContextAndActivation(t *testing.T) {
	t.Parallel()
	fixture := newManagedContextFixture(t, "terminal", model.OperationTeamworkAccept)
	spec := fixture.spec("terminal-key", "terminal-request", model.OperationTeamworkAccept)
	reserved, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	receipt := mustManagedOperationRejectionReceipt(t, reserved.Operation.ID(), "work_conflict",
		"current Work changed before admission")
	rejectedAt := spec.At.Add(time.Second)
	rejected, err := fixture.store.RejectOperation(context.Background(), reserved.Operation.ID(),
		spec.LeaseOwner, rejectedAt, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec("UPDATE profiles SET enabled=0 WHERE profile_id=?",
		fixture.profile.ID().String()); err != nil {
		t.Fatal(err)
	}

	replaySpec := spec
	replaySpec.At = spec.At.Add(10 * time.Minute)
	replaySpec.LeaseUntil = replaySpec.At.Add(time.Minute)
	replayed, err := fixture.store.ReserveManagedOperation(context.Background(), replaySpec)
	if err != nil || !replayed.Replayed || replayed.Acquired || replayed.HasHandling ||
		replayed.Operation.Status() != model.OperationRejected ||
		replayed.Operation.AgentRunID() != rejected.Operation.AgentRunID() {
		t.Fatalf("terminal replay with stale exact context = (%#v, %v)", replayed, err)
	}

	mismatch := replaySpec
	mismatch.RequestDigest = model.Sum([]byte("terminal-request-changed"))
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), mismatch); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("terminal digest mismatch error = %v", err)
	}
	missingContext := replaySpec
	missingContext.ClaimContextHash, missingContext.HasClaimContext = model.Digest{}, false
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), missingContext); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("terminal context presence mismatch error = %v", err)
	}
}

func TestReserveManagedOperationContextlessOfferCreatesOneRunAndSurvivesRestart(t *testing.T) {
	t.Parallel()
	fixture := newAcceptanceFixture(t, 1)
	at := fixture.now.Add(time.Minute)
	spec := ManagedOperationSpec{Profile: fixture.profile,
		ClientKeyHash: model.Sum([]byte("initiate-key")), RequestDigest: model.Sum([]byte("initiate-request")),
		Kind: model.OperationTeamworkOffer, LeaseOwner: "server-initiate", At: at,
		LeaseUntil: at.Add(2 * time.Minute)}
	first, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil || first.Replayed || !first.Acquired || first.HasHandling || first.Run.ID().IsZero() {
		t.Fatalf("contextless initiate = (%#v, %v)", first, err)
	}
	if _, hasHandling := first.Run.HandlingID(); hasHandling {
		t.Fatal("contextless initiate Run has Handling authority")
	}
	wantCause, _ := managedInitiateCause(first.Operation.ID())
	if first.Run.Cause().String() != wantCause.String() || first.Operation.AgentRunID() != first.Run.ID() {
		t.Fatalf("initiate Run evidence = %#v", first.Run)
	}
	assertManagedOperationCounts(t, fixture.store, spec.ClientKeyHash, 1, 1)

	missingContext := spec
	missingContext.ClientKeyHash = model.Sum([]byte("missing-context-key"))
	missingContext.RequestDigest = model.Sum([]byte("missing-context-request"))
	missingContext.Kind = model.OperationTeamworkAccept
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), missingContext); !errors.Is(err, ErrManagedContextRequired) {
		t.Fatalf("contextless non-offer error = %v", err)
	}
	assertManagedOperationCounts(t, fixture.store, missingContext.ClientKeyHash, 0, 1)

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	retry := spec
	retry.At = at.Add(time.Second)
	retry.LeaseUntil = at.Add(3 * time.Minute)
	replayed, err := restarted.ReserveManagedOperation(context.Background(), retry)
	if err != nil || !replayed.Replayed || !replayed.Acquired ||
		replayed.Operation.ID() != first.Operation.ID() || replayed.Run.ID() != first.Run.ID() {
		t.Fatalf("restart started replay = (%#v, %v)", replayed, err)
	}
	assertManagedOperationCounts(t, restarted, spec.ClientKeyHash, 1, 1)
}

func TestReserveManagedOperationContextlessConcurrencyIsSingleEffect(t *testing.T) {
	t.Parallel()
	fixture := newAcceptanceFixture(t, 1)
	at := fixture.now.Add(time.Minute)
	spec := ManagedOperationSpec{Profile: fixture.profile,
		ClientKeyHash: model.Sum([]byte("concurrent-initiate-key")),
		RequestDigest: model.Sum([]byte("concurrent-initiate-request")), Kind: model.OperationTeamworkOffer,
		LeaseOwner: "server-concurrent-initiate", At: at, LeaseUntil: at.Add(time.Minute)}

	start := make(chan struct{})
	results := make(chan ManagedOperationReservation, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent managed reserve error = %v", err)
		}
	}
	var operationID model.OperationID
	replays := 0
	for result := range results {
		if !result.Acquired || result.Run.ID().IsZero() {
			t.Fatalf("concurrent reserve did not acquire: %#v", result)
		}
		if operationID.IsZero() {
			operationID = result.Operation.ID()
		} else if result.Operation.ID() != operationID {
			t.Fatalf("concurrent reserve IDs differ: %s != %s", result.Operation.ID().String(), operationID.String())
		}
		if result.Replayed {
			replays++
		}
	}
	if replays != 1 {
		t.Fatalf("concurrent replay count = %d, want 1", replays)
	}
	assertManagedOperationCounts(t, fixture.store, spec.ClientKeyHash, 1, 1)
}

func TestReserveManagedOperationContextlessOfferRejectsActiveClaim(t *testing.T) {
	t.Parallel()
	fixture := newManagedContextFixture(t, "active-claim", model.OperationTeamworkAccept)
	spec := fixture.spec("active-claim-offer-key", "active-claim-offer-request", model.OperationTeamworkOffer)
	spec.HasClaimContext = false
	spec.ClaimContextHash = model.Digest{}
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), spec); !errors.Is(err, ErrManagedContextRequired) {
		t.Fatalf("contextless offer with active claim error = %v", err)
	}
}

type managedContextFixture struct {
	*acceptanceFixture
	claim     AgentClaimResult
	tokenHash model.Digest
	at        time.Time
}

func newManagedContextFixture(t *testing.T, suffix string,
	allowed ...model.OperationKind,
) *managedContextFixture {
	t.Helper()
	fixture, events := newAgentClaimFixture(t, 1, "managed-"+suffix)
	at := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-managed-"+suffix, events[0], 1, at, at, 0)
	token := "token-managed-" + suffix
	claim := claimCurrent(t, fixture, "owner-managed-"+suffix, token, at)
	writeManagedCurrentReceipt(t, fixture.store, claim, allowed...)
	return &managedContextFixture{acceptanceFixture: fixture, claim: claim,
		tokenHash: model.Sum([]byte(token)), at: at.Add(time.Second)}
}

func (fixture *managedContextFixture) spec(key, request string,
	kind model.OperationKind,
) ManagedOperationSpec {
	return ManagedOperationSpec{Profile: fixture.profile, ClientKeyHash: model.Sum([]byte(key)),
		RequestDigest: model.Sum([]byte(request)), Kind: kind, LeaseOwner: "server-" + key,
		At: fixture.at, LeaseUntil: fixture.at.Add(2 * time.Minute),
		ClaimContextHash: fixture.tokenHash, HasClaimContext: true}
}

func writeManagedCurrentReceipt(t *testing.T, st *Store, claim AgentClaimResult,
	allowed ...model.OperationKind,
) {
	t.Helper()
	origin, _ := model.ParsePeerID("peer-managed-current-origin")
	epoch, _ := model.ParseOriginEpoch("epoch-managed-current-origin")
	eventKey, err := model.NewEventKey(origin, epoch, claim.Handling.EventID())
	if err != nil {
		t.Fatal(err)
	}
	home, _ := model.ParsePeerID("peer-managed-current-home")
	workID, _ := model.ParseWorkID("work-" + claim.Handling.ID().String())
	workRef, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := claim.Run.StartedAt().Add(24 * time.Hour)
	payload, _ := model.JSONFrom(struct {
		Content     string `json:"content"`
		Deadline    string `json:"deadline"`
		Iteration   uint8  `json:"iteration"`
		WorkVersion uint64 `json:"work_version"`
	}{"managed current", deadline.UTC().Format(time.RFC3339Nano), 1, 1})
	event, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: eventKey,
		Digest: model.Sum([]byte("event-" + claim.Handling.EventID().String())),
		Type:   model.EventReviewOffered, WorkRef: workRef, Summary: "Managed current",
		Payload: payload, AcceptedAt: claim.Run.StartedAt()})
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: workRef, Version: 1, Iteration: 1,
		DeadlineUnixNano: deadline.UnixNano(), State: model.WorkOffered,
		StateData: payload, LocalRole: model.CurrentReviewer})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: event,
		ActionWork: work, AllowedActions: allowed})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: claim.Run.ID(),
		ProfileID: claim.Run.ProfileID(), HandlingID: claim.Handling.ID(),
		HandlingAttempt: claim.Handling.Attempts(), Projection: projection,
		ReadAt: claim.Run.StartedAt().Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := st.db.Exec(`UPDATE agent_runs SET current_read_receipt_json=?
		WHERE run_id=? AND current_read_receipt_json IS NULL`, receipt.CanonicalJSON().Bytes(), claim.Run.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "test current receipt"); err != nil {
		t.Fatal(err)
	}
}

func assertManagedOperationCounts(t *testing.T, st *Store, key model.Digest,
	wantOperations, wantInitiateRuns int,
) {
	t.Helper()
	var operations, runs int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM operations WHERE client_key_hash=?", key.Bytes()).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE handling_id IS NULL AND launcher='external-operation'`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if operations != wantOperations || runs != wantInitiateRuns {
		t.Fatalf("managed durable counts = operations %d runs %d, want %d/%d",
			operations, runs, wantOperations, wantInitiateRuns)
	}
}
