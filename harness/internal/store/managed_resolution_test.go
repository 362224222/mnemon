package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCommitManagedResolutionAtomicallyTransitionsClosedDecisions(t *testing.T) {
	tests := []struct {
		name        string
		kind        model.OperationKind
		content     string
		handling    model.HandlingStatus
		disposition string
		run         model.AgentRunStatus
	}{
		{"no action", model.OperationResolveNoAction, "not actionable for this Agent",
			model.HandlingCompleted, "no_action", model.AgentRunOutcomeAccepted},
		{"reject", model.OperationResolveReject, "the Event targets a different responsibility",
			model.HandlingRejected, "reject", model.AgentRunRejected},
		{"retry", model.OperationResolveRetry, "retry after the dependency is ready",
			model.HandlingPending, "retry", model.AgentRunRequeued},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedResolutionFixture(t, strings.ReplaceAll(test.name, " ", "-"),
				test.kind, test.content)
			beforeEvents := countManagedResolutionEvents(t, fixture.store)
			spec := fixture.resolveSpec()
			// The Store must derive these from Operation.context_hash, not trust
			// the convenience projections returned by reservation.
			spec.Reservation.Run = model.AgentRun{}
			spec.Reservation.Handling = model.Handling{}
			spec.Reservation.HasHandling = false

			result, err := fixture.store.CommitManagedResolution(context.Background(), spec)
			if err != nil || result.Replayed || result.Operation.Status() != model.OperationCommitted {
				t.Fatalf("CommitManagedResolution() = (%#v, %v)", result, err)
			}
			assertManagedResolutionReceipt(t, result.Receipt, fixture, test.handling)
			assertManagedResolutionRows(t, fixture, result, test.handling, test.disposition, test.run)
			if got := countManagedResolutionEvents(t, fixture.store); got != beforeEvents {
				t.Fatalf("resolution created domain Events: before=%d after=%d", beforeEvents, got)
			}
			if test.kind == model.OperationResolveRetry {
				wantRetry := spec.At.Add(time.Duration(model.DefaultHandlingBudget().Spec().RetryInitialSeconds) * time.Second)
				handling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
				if err != nil || !handling.AvailableAt().Equal(wantRetry) {
					t.Fatalf("explicit retry backoff = (%s, %v), want %s",
						handling.AvailableAt(), err, wantRetry)
				}
				probe := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
					ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: wantRetry.Add(-time.Nanosecond)}
				if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimWaiting {
					t.Fatalf("retry-before-boundary probe = (%q, %v)", status, err)
				}
				probe.At = wantRetry
				if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimActionable {
					t.Fatalf("retry-boundary probe = (%q, %v)", status, err)
				}
			}
		})
	}
}

func TestCommitManagedResolutionTerminalReplaySurvivesRestart(t *testing.T) {
	fixture := newManagedResolutionFixture(t, "restart", model.OperationResolveNoAction,
		"no local action remains")
	spec := fixture.resolveSpec()
	first, err := fixture.store.CommitManagedResolution(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted

	replaySpec := spec
	replaySpec.At = spec.At.Add(10 * time.Minute)
	replay, err := restarted.CommitManagedResolution(context.Background(), replaySpec)
	if err != nil || !replay.Replayed || replay.Receipt.String() != first.Receipt.String() ||
		replay.Operation.AgentRunID() != first.Operation.AgentRunID() {
		t.Fatalf("restart terminal replay = (%#v, %v)", replay, err)
	}
	assertManagedResolutionRows(t, fixture, replay, model.HandlingCompleted,
		"no_action", model.AgentRunOutcomeAccepted)

	mismatch := replaySpec
	mismatch.Content = "different content behind the same operation"
	if _, err := restarted.CommitManagedResolution(context.Background(), mismatch); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("terminal content mismatch error = %v", err)
	}
}

func TestCommitManagedResolutionTerminalReplayRejectsLifecycleDrift(t *testing.T) {
	fixture := newManagedResolutionFixture(t, "terminal-drift", model.OperationResolveNoAction,
		"nothing remains actionable")
	spec := fixture.resolveSpec()
	if _, err := fixture.store.CommitManagedResolution(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_handlings SET status='rejected',
		last_disposition='reject' WHERE handling_id=?`, fixture.claim.Handling.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.CommitManagedResolution(context.Background(), spec); !errors.Is(err, ErrManagedResolutionInvariant) {
		t.Fatalf("terminal lifecycle drift error = %v", err)
	}
}

func TestCommitManagedResolutionConcurrentReplayIsSingleEffect(t *testing.T) {
	fixture := newManagedResolutionFixture(t, "concurrent", model.OperationResolveReject,
		"this obligation is invalid for the current Agent")
	spec := fixture.resolveSpec()
	start := make(chan struct{})
	results := make(chan ManagedResolutionResult, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := fixture.store.CommitManagedResolution(context.Background(), spec)
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
			t.Fatalf("concurrent resolution error = %v", err)
		}
	}
	var receipt string
	replays := map[bool]int{}
	for result := range results {
		replays[result.Replayed]++
		if receipt == "" {
			receipt = result.Receipt.String()
		} else if receipt != result.Receipt.String() {
			t.Fatal("concurrent resolution returned different receipts")
		}
	}
	if replays[false] != 1 || replays[true] != 1 {
		t.Fatalf("concurrent resolution replay counts = %#v", replays)
	}
	var committed int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM operations
		WHERE operation_id=? AND status='committed'`, fixture.reservation.Operation.ID().String()).Scan(&committed); err != nil || committed != 1 {
		t.Fatalf("committed resolution count = %d, err=%v", committed, err)
	}
}

func TestCommitManagedResolutionRejectsStaleExpiredAndFutureEvidenceAtomically(t *testing.T) {
	t.Run("Work version changed", func(t *testing.T) {
		fixture := newManagedResolutionFixture(t, "work-stale", model.OperationResolveNoAction, "stale Work")
		work := fixture.current.ActionWork()
		if _, err := fixture.store.db.Exec(`UPDATE works SET version=version+1,updated_at=?
			WHERE home_peer_id=? AND work_id=?`, storeTime(fixture.resolveAt),
			work.HomePeerID().String(), work.WorkID().String()); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.CommitManagedResolution(context.Background(), fixture.resolveSpec()); !errors.Is(err, ErrManagedResolutionStale) {
			t.Fatalf("stale Work error = %v", err)
		}
		assertManagedResolutionUnchanged(t, fixture)
	})

	t.Run("operation lease boundary", func(t *testing.T) {
		fixture := newManagedResolutionFixture(t, "lease-expired", model.OperationResolveRetry, "retry")
		lease, _ := fixture.reservation.Operation.LeaseUntil()
		spec := fixture.resolveSpec()
		spec.At = lease
		if _, err := fixture.store.CommitManagedResolution(context.Background(), spec); !errors.Is(err, ErrOperationFence) {
			t.Fatalf("operation lease boundary error = %v", err)
		}
		assertManagedResolutionUnchanged(t, fixture)
	})

	t.Run("future Work evidence", func(t *testing.T) {
		fixture := newManagedResolutionFixture(t, "future", model.OperationResolveRetry, "retry")
		work := fixture.current.ActionWork()
		future := fixture.resolveAt.Add(time.Hour)
		if _, err := fixture.store.db.Exec(`UPDATE works SET updated_at=?
			WHERE home_peer_id=? AND work_id=?`, storeTime(future),
			work.HomePeerID().String(), work.WorkID().String()); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.CommitManagedResolution(context.Background(), fixture.resolveSpec()); !errors.Is(err, ErrManagedResolutionInvariant) {
			t.Fatalf("future Work evidence error = %v", err)
		}
		assertManagedResolutionUnchanged(t, fixture)
	})

	t.Run("old attempt", func(t *testing.T) {
		fixture := newManagedResolutionFixture(t, "old-attempt", model.OperationResolveReject, "old attempt")
		newFence := model.Sum([]byte("new-attempt-fence"))
		newLease := fixture.resolveAt.Add(10 * time.Minute)
		newRun, _ := model.ParseRunID("run-managed-resolution-new-attempt")
		if _, err := fixture.store.db.Exec(`UPDATE agent_handlings SET claim_owner=?,claim_token_hash=?,
			lease_until=?,attempts=attempts+1,last_disposition='claimed',updated_at=? WHERE handling_id=?`,
			"owner-managed-resolution-new-attempt", newFence.Bytes(), storeTime(newLease),
			storeTime(fixture.resolveAt), fixture.claim.Handling.ID().String()); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,handling_id,cause_json,
			handling_attempt,claim_fence_hash,lease_until,launcher,runtime_kind,launcher_diagnostic_json,
			runtime_ids_json,status,started_at) VALUES(?,?,?,'{"kind":"new_attempt"}',2,?,?,'external',?,
			'{}','{}','running',?)`, newRun.String(), fixture.profile.ID().String(),
			fixture.claim.Handling.ID().String(), newFence.Bytes(), storeTime(newLease),
			string(fixture.profile.Runtime()), storeTime(fixture.resolveAt)); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.CommitManagedResolution(context.Background(), fixture.resolveSpec()); !errors.Is(err, ErrManagedContextStale) {
			t.Fatalf("old attempt error = %v", err)
		}
		var status string
		if err := fixture.store.db.QueryRow("SELECT status FROM operations WHERE operation_id=?",
			fixture.reservation.Operation.ID().String()).Scan(&status); err != nil || status != "started" {
			t.Fatalf("old attempt operation status = %q, err=%v", status, err)
		}
		var outcome, completion []byte
		if err := fixture.store.db.QueryRow(`SELECT outcome_receipt_json,completion_receipt_json
			FROM agent_runs WHERE run_id=?`, fixture.claim.Run.ID().String()).Scan(&outcome, &completion); err != nil {
			t.Fatal(err)
		}
		if len(outcome) != 0 || len(completion) != 0 {
			t.Fatal("old attempt wrote Run resolution evidence")
		}
	})
}

type managedResolutionFixture struct {
	*acceptanceFixture
	claim       AgentClaimResult
	current     model.CurrentReadReceipt
	reservation ManagedOperationReservation
	content     string
	resolveAt   time.Time
}

func newManagedResolutionFixture(t *testing.T, suffix string, kind model.OperationKind,
	content string,
) *managedResolutionFixture {
	t.Helper()
	fixture, events := newAgentClaimFixture(t, 1, "resolution-"+suffix)
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-resolution-"+suffix, events[0], 1,
		claimAt, claimAt, 0)
	token := "token-resolution-" + suffix
	claim := claimCurrent(t, fixture, "owner-resolution-"+suffix, token, claimAt)
	readAt := claimAt.Add(time.Second)
	current := installManagedResolutionCurrent(t, fixture, claim,
		model.Sum([]byte(token)), readAt)
	contextHash := model.Sum([]byte(token))
	requestDigest, err := ManagedResolutionRequestDigest(contextHash, kind, content)
	if err != nil {
		t.Fatal(err)
	}
	reserveAt := readAt.Add(time.Second)
	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), ManagedOperationSpec{
		Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("key-resolution-" + suffix)),
		RequestDigest: requestDigest, Kind: kind, LeaseOwner: "server-resolution-" + suffix,
		At: reserveAt, LeaseUntil: reserveAt.Add(2 * time.Minute),
		ClaimContextHash: contextHash, HasClaimContext: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &managedResolutionFixture{acceptanceFixture: fixture, claim: claim,
		current: current, reservation: reservation, content: content,
		resolveAt: reserveAt.Add(time.Second)}
}

func (fixture *managedResolutionFixture) resolveSpec() ManagedResolutionSpec {
	return ManagedResolutionSpec{Reservation: fixture.reservation, Content: fixture.content,
		At: fixture.resolveAt}
}

func installManagedResolutionCurrent(t *testing.T, fixture *acceptanceFixture,
	claim AgentClaimResult, token model.Digest, readAt time.Time,
) model.CurrentReadReceipt {
	t.Helper()
	source, err := readCurrentSourceEvent(context.Background(), fixture.store.db,
		claim.Handling.EventID())
	if err != nil {
		t.Fatal(err)
	}
	work, err := fixture.store.GetReviewWork(context.Background(), source.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	role, err := localCurrentRole(fixture.node.PeerID(), work)
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: source.Key(), Digest: source.Digest(),
		Type: source.Type(), WorkRef: source.Scope().WorkRef(), Summary: source.Summary(),
		Payload: source.Payload(), AcceptedAt: source.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: work.Ref(), Version: work.Version(),
		Iteration: work.Iteration(), DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(),
		StateData: work.StateData(), LocalRole: role})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: event,
		ActionWork: currentWork, AllowedActions: []model.OperationKind{
			model.OperationResolveNoAction, model.OperationResolveRetry, model.OperationResolveReject,
		}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: claim.Run.ID(),
		ProfileID: claim.Run.ProfileID(), HandlingID: claim.Handling.ID(),
		HandlingAttempt: claim.Handling.Attempts(), Projection: projection, ReadAt: readAt})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := writeCurrentReadEvidence(context.Background(), tx, claim.Run, claim.Handling,
		token, receipt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func assertManagedResolutionReceipt(t *testing.T, receipt model.JSON,
	fixture *managedResolutionFixture, handling model.HandlingStatus,
) {
	t.Helper()
	var envelope struct {
		Action   model.OperationKind `json:"action"`
		Handling struct {
			Status string `json:"status"`
		} `json:"handling"`
		OperationID string `json:"operation_id"`
		Receipt     string `json:"receipt"`
		Replayed    bool   `json:"replayed"`
		Results     []any  `json:"results"`
		Status      string `json:"status"`
	}
	if err := json.Unmarshal(receipt.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	wantReceiptStatus := map[model.HandlingStatus]string{
		model.HandlingCompleted: "completed", model.HandlingPending: "requeued",
		model.HandlingRejected: "rejected",
	}[handling]
	if envelope.Action != fixture.reservation.Operation.Kind() || envelope.Handling.Status != wantReceiptStatus ||
		envelope.OperationID != fixture.reservation.Operation.ID().String() || envelope.Replayed ||
		envelope.Status != "resolved" || len(envelope.Results) != 0 {
		t.Fatalf("resolution envelope = %#v", envelope)
	}
	if !strings.Contains(envelope.Receipt, fixture.content) ||
		!strings.Contains(envelope.Receipt, fixture.current.SourceEvent().EventID().String()) ||
		!strings.Contains(envelope.Receipt, `"action_work_version":1`) {
		t.Fatalf("resolution evidence does not bind content/current: %s", envelope.Receipt)
	}
	for _, forbidden := range []string{fixture.claim.Handling.ID().String(),
		fixture.claim.Run.ID().String(), "token-resolution"} {
		if strings.Contains(receipt.String(), forbidden) {
			t.Fatalf("resolution receipt leaked managed authority %q: %s", forbidden, receipt.String())
		}
	}
}

func assertManagedResolutionRows(t *testing.T, fixture *managedResolutionFixture,
	result ManagedResolutionResult, handlingStatus model.HandlingStatus, disposition string,
	runStatus model.AgentRunStatus,
) {
	t.Helper()
	handling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
	if err != nil || handling.Status() != handlingStatus || handling.LastDisposition() != disposition {
		t.Fatalf("durable Handling = (%#v, %v)", handling, err)
	}
	if _, claimed := handling.ClaimTokenHash(); claimed {
		t.Fatal("resolved Handling retained claim")
	}
	run, err := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
	if err != nil || run.Status() != runStatus {
		t.Fatalf("durable AgentRun = (%#v, %v)", run, err)
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	completion, hasCompletion := run.CompletionReceipt()
	finished, hasFinished := run.FinishedAt()
	if !hasOutcome || !hasCompletion || outcome.String() != result.Receipt.String() ||
		completion.String() != result.Receipt.String() || !hasFinished || finished.After(fixture.resolveAt) {
		t.Fatalf("AgentRun resolution evidence = outcome %t completion %t finished %s",
			hasOutcome, hasCompletion, finished)
	}
	stored, err := readOperationByID(context.Background(), fixture.store.db,
		fixture.reservation.Operation.ID())
	if err != nil || stored.Status() != model.OperationCommitted {
		t.Fatalf("durable Operation = (%#v, %v)", stored, err)
	}
	receipt, ok := stored.Result()
	if !ok || receipt.String() != result.Receipt.String() {
		t.Fatal("Operation result differs from Run receipts")
	}
}

func assertManagedResolutionUnchanged(t *testing.T, fixture *managedResolutionFixture) {
	t.Helper()
	operation, err := readOperationByID(context.Background(), fixture.store.db,
		fixture.reservation.Operation.ID())
	if err != nil || operation.Status() != model.OperationStarted {
		t.Fatalf("failed resolution Operation = (%#v, %v)", operation, err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingClaimed || handling.LastDisposition() != "read" {
		t.Fatalf("failed resolution Handling = (%#v, %v)", handling, err)
	}
	run, err := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
	if err != nil || run.Status() != model.AgentRunRunning {
		t.Fatalf("failed resolution AgentRun = (%#v, %v)", run, err)
	}
	if _, ok := run.OutcomeReceipt(); ok {
		t.Fatal("failed resolution wrote outcome receipt")
	}
	if _, ok := run.CompletionReceipt(); ok {
		t.Fatal("failed resolution wrote completion receipt")
	}
}

func countManagedResolutionEvents(t *testing.T, st *Store) int {
	t.Helper()
	var count int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	return count
}
