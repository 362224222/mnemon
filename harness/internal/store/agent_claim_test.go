package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgentClaimProbeAndDeterministicSelection(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 3, "queue")
	base := fixture.now.Add(time.Minute)
	oldHigh := insertClaimHandling(t, fixture.store, "handling-high-old", events[0], 20,
		base.Add(-20*time.Second), base.Add(-30*time.Second), 0)
	insertClaimHandling(t, fixture.store, "handling-low", events[1], 10,
		base.Add(-40*time.Second), base.Add(-50*time.Second), 0)
	insertClaimHandling(t, fixture.store, "handling-high-new", events[2], 20,
		base.Add(-10*time.Second), base.Add(-40*time.Second), 0)

	probe := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: base}
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimActionable {
		t.Fatalf("ProbeAgentClaim() = (%q, %v), want actionable", status, err)
	}
	result := claimCurrent(t, fixture, "owner-queue", "token-queue", base)
	if result.Status != AgentClaimActionable || result.Handling.ID() != oldHigh.ID() {
		t.Fatalf("ClaimAgentCurrent() selected (%q, %s), want %s", result.Status,
			result.Handling.ID().String(), oldHigh.ID().String())
	}
	if result.Handling.Attempts() != 1 || result.Handling.ClaimOwner() != "owner-queue" {
		t.Fatalf("claimed Handling snapshot = %#v", result.Handling)
	}
	runHandling, ok := result.Run.HandlingID()
	runFence, hasFence := result.Run.ClaimFenceHash()
	runLease, hasLease := result.Run.LeaseUntil()
	if !ok || runHandling != oldHigh.ID() || result.Run.HandlingAttempt() != 1 ||
		!hasFence || runFence != model.Sum([]byte("token-queue")) || !hasLease ||
		!runLease.Equal(base.Add(5*time.Minute)) || result.Run.Runtime() != fixture.profile.Runtime() ||
		result.Run.Status() != model.AgentRunRunning || result.Run.Launcher() != "external" {
		t.Fatalf("AgentRun claim snapshot = %#v", result.Run)
	}
	if got, err := fixture.store.GetAgentRun(context.Background(), result.Run.ID()); err != nil || got.ID() != result.Run.ID() {
		t.Fatalf("GetAgentRun() = (%#v, %v)", got, err)
	}
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimBusy {
		t.Fatalf("ProbeAgentClaim() after claim = (%q, %v), want busy", status, err)
	}
}

func TestAgentClaimNoneWaitingAndDueBoundary(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 1, "states")
	at := fixture.now.Add(time.Minute)
	probe := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: at}
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimNone {
		t.Fatalf("empty ProbeAgentClaim() = (%q, %v)", status, err)
	}
	insertClaimHandling(t, fixture.store, "handling-future", events[0], 1, at.Add(time.Second), at, 0)
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimWaiting {
		t.Fatalf("future ProbeAgentClaim() = (%q, %v)", status, err)
	}
	probe.At = at.Add(time.Second)
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimActionable {
		t.Fatalf("due-boundary ProbeAgentClaim() = (%q, %v)", status, err)
	}
}

func TestAgentClaimConcurrentLoserIsBusy(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 1, "concurrent")
	at := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-concurrent", events[0], 1, at, at, 0)

	start := make(chan struct{})
	results := make(chan AgentClaimResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := fixture.store.ClaimAgentCurrent(context.Background(), AgentClaimSpec{
				ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
				ClaimOwner:     "owner-concurrent-" + string(rune('a'+index)),
				ClaimTokenHash: model.Sum([]byte("token-concurrent-" + string(rune('a'+index)))),
				At:             at, LeaseUntil: at.Add(5 * time.Minute),
			})
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ClaimAgentCurrent() error = %v", err)
		}
	}
	counts := map[AgentClaimStatus]int{}
	for result := range results {
		counts[result.Status]++
	}
	if counts[AgentClaimActionable] != 1 || counts[AgentClaimBusy] != 1 {
		t.Fatalf("concurrent status counts = %#v", counts)
	}
	var runs int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM agent_runs WHERE handling_id='handling-concurrent'").Scan(&runs); err != nil || runs != 1 {
		t.Fatalf("handling AgentRun count = %d, err=%v", runs, err)
	}
	handlingID, _ := model.ParseHandlingID("handling-concurrent")
	handling, err := fixture.store.GetAgentHandling(context.Background(), handlingID)
	if err != nil || handling.Attempts() != 1 || handling.Status() != model.HandlingClaimed {
		t.Fatalf("concurrent durable Handling = (%#v, %v)", handling, err)
	}
}

func TestAgentClaimLeaseRecoveryBackoffAndRestart(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 1, "restart")
	at := fixture.now.Add(time.Minute)
	handling := insertClaimHandling(t, fixture.store, "handling-restart-claim", events[0], 1, at, at, 0)
	first := claimCurrent(t, fixture, "owner-first", "token-first", at)
	firstLease, _ := first.Run.LeaseUntil()

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted

	boundary := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: firstLease}
	if status, err := restarted.ProbeAgentClaim(context.Background(), boundary); err != nil || status != AgentClaimWaiting {
		t.Fatalf("expiry-boundary ProbeAgentClaim() = (%q, %v), want waiting backoff", status, err)
	}
	requeued, err := restarted.GetAgentHandling(context.Background(), handling.ID())
	if err != nil || requeued.Status() != model.HandlingPending || requeued.Attempts() != 1 ||
		!requeued.AvailableAt().Equal(firstLease.Add(5*time.Second)) {
		t.Fatalf("requeued Handling = (%#v, %v)", requeued, err)
	}
	oldRun, err := restarted.GetAgentRun(context.Background(), first.Run.ID())
	if err != nil || oldRun.Status() != model.AgentRunRequeued {
		t.Fatalf("expired AgentRun = (%#v, %v)", oldRun, err)
	}

	retryAt := firstLease.Add(5 * time.Second)
	boundary.At = retryAt
	if status, err := restarted.ProbeAgentClaim(context.Background(), boundary); err != nil || status != AgentClaimActionable {
		t.Fatalf("retry-boundary ProbeAgentClaim() = (%q, %v)", status, err)
	}
	second := claimCurrent(t, fixture, "owner-second", "token-second", retryAt)
	if second.Handling.ID() != handling.ID() || second.Handling.Attempts() != 2 || second.Run.ID() == first.Run.ID() {
		t.Fatalf("second claim did not create a new attempt/Run: %#v", second)
	}
}

func TestAgentClaimAttemptBudgetDiesAndFencesStartedOperation(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 1, "dead")
	setClaimBudget(t, fixture.store, 1)
	at := fixture.now.Add(time.Minute)
	handling := insertClaimHandling(t, fixture.store, "handling-dead", events[0], 1, at, at, 0)
	first := claimCurrent(t, fixture, "owner-dead", "token-dead", at)

	operationKey := model.Sum([]byte("operation-key-expiring"))
	operationID, err := managedOperationID(fixture.profile.ID(), operationKey)
	if err != nil {
		t.Fatal(err)
	}
	contextHash := model.Sum([]byte("token-dead"))
	operationLease := at.Add(10 * time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: fixture.profile.ID(),
		AgentRunID: first.Run.ID(), ClientKeyHash: operationKey,
		ContextHash: &contextHash, Kind: model.OperationTeamworkAccept,
		RequestDigest: model.Sum([]byte("operation-request-expiring")), Status: model.OperationStarted,
		LeaseOwner: "operation-owner", LeaseUntil: &operationLease, CreatedAt: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(context.Background(), operation, at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	lease, _ := first.Run.LeaseUntil()
	probe := AgentClaimProbeSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: lease}
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), probe); err != nil || status != AgentClaimNone {
		t.Fatalf("exhausted ProbeAgentClaim() = (%q, %v), want none", status, err)
	}
	dead, err := fixture.store.GetAgentHandling(context.Background(), handling.ID())
	if err != nil || dead.Status() != model.HandlingDead || dead.Attempts() != 1 {
		t.Fatalf("dead Handling = (%#v, %v)", dead, err)
	}
	if _, ok := dead.DeadAt(); !ok {
		t.Fatal("dead Handling has no death evidence")
	}
	deadRun, err := fixture.store.GetAgentRun(context.Background(), first.Run.ID())
	if err != nil || deadRun.Status() != model.AgentRunDead {
		t.Fatalf("dead AgentRun = (%#v, %v)", deadRun, err)
	}
	var operationStatus string
	var resultJSON []byte
	if err := fixture.store.db.QueryRow("SELECT status,result_json FROM operations WHERE operation_id=?",
		operationID.String()).Scan(&operationStatus, &resultJSON); err != nil || operationStatus != "rejected" ||
		string(resultJSON) != mustManagedOperationRejectionReceipt(t, operationID, "context_stale",
			"managed operation context expired with its Agent Runtime claim").String() {
		t.Fatalf("expired operation = (%q, %s, %v)", operationStatus, resultJSON, err)
	}
}

func TestAgentClaimFailsClosedForProfileAssetAndRunDrift(t *testing.T) {
	t.Parallel()
	t.Run("disabled Profile", func(t *testing.T) {
		st := openTestStore(t)
		node, profile := bootstrapValues(t, "peer-claim-disabled", "principal-claim-disabled", "/workspace/claim-disabled")
		if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
			t.Fatal(err)
		}
		_, err := st.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{ProfileID: profile.ID(),
			ExpectedAssetRevision: profile.ActiveAssetRevision(), At: profile.UpdatedAt().Add(time.Second)})
		if !errors.Is(err, ErrAgentClaimProfile) {
			t.Fatalf("disabled Profile error = %v", err)
		}
	})

	t.Run("asset drift", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "asset")
		at := fixture.now.Add(time.Minute)
		handling := insertClaimHandling(t, fixture.store, "handling-asset", events[0], 1, at, at, 0)
		_, err := fixture.store.ClaimAgentCurrent(context.Background(), AgentClaimSpec{
			ProfileID: fixture.profile.ID(), ExpectedAssetRevision: "asset-other",
			ClaimOwner: "owner-asset", ClaimTokenHash: model.Sum([]byte("token-asset")),
			At: at, LeaseUntil: at.Add(5 * time.Minute)})
		if !errors.Is(err, ErrAgentClaimAsset) {
			t.Fatalf("asset drift error = %v", err)
		}
		stored, readErr := fixture.store.GetAgentHandling(context.Background(), handling.ID())
		if readErr != nil || stored.Status() != model.HandlingPending || stored.Attempts() != 0 {
			t.Fatalf("asset failure mutated Handling: (%#v, %v)", stored, readErr)
		}
	})

	t.Run("busy Run mismatch", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "run-drift")
		at := fixture.now.Add(time.Minute)
		insertClaimHandling(t, fixture.store, "handling-run-drift", events[0], 1, at, at, 0)
		claimCurrent(t, fixture, "owner-run-drift", "token-run-drift", at)
		mustExec(t, fixture.store, `DROP TRIGGER agent_runs_creation_identity_immutable`)
		if _, err := fixture.store.db.Exec("UPDATE agent_runs SET runtime_kind='claude-cli' WHERE handling_id='handling-run-drift'"); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
			ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: at})
		if !errors.Is(err, ErrAgentClaimInvariant) {
			t.Fatalf("Run drift error = %v", err)
		}
	})

	t.Run("busy Run noncanonical evidence", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "run-evidence")
		at := fixture.now.Add(time.Minute)
		insertClaimHandling(t, fixture.store, "handling-run-evidence", events[0], 1, at, at, 0)
		claimCurrent(t, fixture, "owner-run-evidence", "token-run-evidence", at)
		mustExec(t, fixture.store, `DROP TRIGGER agent_runs_creation_identity_immutable`)
		if _, err := fixture.store.db.Exec("UPDATE agent_runs SET cause_json=' { }' WHERE handling_id='handling-run-evidence'"); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
			ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: at})
		if !errors.Is(err, ErrAgentClaimInvariant) {
			t.Fatalf("noncanonical Run evidence error = %v", err)
		}
	})
}

func TestAgentClaimRunInsertFailureRollsBackHandlingCAS(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 1, "rollback")
	at := fixture.now.Add(time.Minute)
	handling := insertClaimHandling(t, fixture.store, "handling-rollback", events[0], 1, at, at, 0)
	fence := model.Sum([]byte("preexisting-fence"))
	if _, err := fixture.store.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,handling_id,cause_json,
		handling_attempt,claim_fence_hash,lease_until,launcher,runtime_kind,launcher_diagnostic_json,
		runtime_ids_json,status,started_at) VALUES(?,?,?,'{"kind":"preexisting"}',1,?,?,'external',?,
		'{}','{}','running',?)`, "run-preexisting-attempt", fixture.profile.ID().String(), handling.ID().String(),
		fence.Bytes(), storeTime(at.Add(5*time.Minute)), string(fixture.profile.Runtime()), storeTime(at)); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.store.ClaimAgentCurrent(context.Background(), AgentClaimSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		ClaimOwner: "owner-rollback", ClaimTokenHash: model.Sum([]byte("token-rollback")),
		At: at, LeaseUntil: at.Add(5 * time.Minute)})
	if err == nil {
		t.Fatal("ClaimAgentCurrent() succeeded despite duplicate Handling attempt Run")
	}
	stored, readErr := fixture.store.GetAgentHandling(context.Background(), handling.ID())
	if readErr != nil || stored.Status() != model.HandlingPending || stored.Attempts() != 0 {
		t.Fatalf("failed Run insert left claimed Handling: (%#v, %v)", stored, readErr)
	}
}

func TestAgentClaimSchemaEnforcesOneOwnerAndRunPerAttempt(t *testing.T) {
	t.Parallel()
	fixture, events := newAgentClaimFixture(t, 2, "schema")
	at := fixture.now.Add(time.Minute)
	first := insertClaimHandling(t, fixture.store, "handling-schema-a", events[0], 1, at, at, 0)
	second := insertClaimHandling(t, fixture.store, "handling-schema-b", events[1], 1, at, at, 0)
	claimed := claimCurrent(t, fixture, "owner-schema", "token-schema", at)
	other := second
	if claimed.Handling.ID() == second.ID() {
		other = first
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_handlings SET status='claimed',claim_owner='other',
		claim_token_hash=?,lease_until=?,attempts=1,updated_at=? WHERE handling_id=?`,
		model.Sum([]byte("other")).Bytes(), storeTime(at.Add(5*time.Minute)), storeTime(at), other.ID().String()); err == nil {
		t.Fatal("schema admitted a second claimed Handling for one Profile")
	}
	if _, err := fixture.store.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,handling_id,cause_json,
		handling_attempt,claim_fence_hash,lease_until,launcher,runtime_kind,launcher_diagnostic_json,
		runtime_ids_json,status,started_at) SELECT 'run-duplicate-attempt',profile_id,handling_id,cause_json,
		handling_attempt,claim_fence_hash,lease_until,launcher,runtime_kind,launcher_diagnostic_json,
		runtime_ids_json,status,started_at FROM agent_runs WHERE run_id=?`, claimed.Run.ID().String()); err == nil {
		t.Fatal("schema admitted a second AgentRun for one Handling attempt")
	}
}

func newAgentClaimFixture(t *testing.T, eventCount int, suffix string) (*acceptanceFixture, []model.EventID) {
	return newAgentClaimFixtureWithContent(t, eventCount, suffix, "Review the production change")
}

func newAgentClaimFixtureWithContent(t *testing.T, eventCount int, suffix, content string,
) (*acceptanceFixture, []model.EventID) {
	t.Helper()
	fixture := newAcceptanceFixture(t, eventCount)
	_, authority := fixture.reserveOffer(t, "claim-source-"+suffix, nil)
	spec := fixture.offerWithContent(t, authority, "claim-source-"+suffix,
		fixture.reviewers, nil, nil, content)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	events := make([]model.EventID, eventCount)
	for index := range events {
		events[index], _ = model.ParseEventID("event-claim-source-" + suffix + "-" + string(rune('0'+index)))
	}
	return fixture, events
}

func insertClaimHandling(t *testing.T, st *Store, idText string, eventID model.EventID, priority int,
	availableAt, createdAt time.Time, attempts uint32,
) model.Handling {
	t.Helper()
	id, _ := model.ParseHandlingID(idText)
	handling, err := model.NewHandling(model.HandlingSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		EventID: eventID, Status: model.HandlingPending, Priority: priority, AvailableAt: availableAt,
		Attempts: attempts, CreatedAt: createdAt, UpdatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := insertAgentHandling(context.Background(), tx, handling); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return handling
}

func claimCurrent(t *testing.T, fixture *acceptanceFixture, owner, token string, at time.Time) AgentClaimResult {
	t.Helper()
	result, err := fixture.store.ClaimAgentCurrent(context.Background(), AgentClaimSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		ClaimOwner: owner, ClaimTokenHash: model.Sum([]byte(token)), At: at, LeaseUntil: at.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimAgentCurrent() error = %v", err)
	}
	return result
}

func setClaimBudget(t *testing.T, st *Store, maxAttempts int) {
	t.Helper()
	spec := model.DefaultHandlingBudget().Spec()
	spec.MaxAttempts = maxAttempts
	budget, err := model.NewHandlingBudget(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("UPDATE profiles SET handling_budget_json=? WHERE profile_id=?",
		budget.JSON().Bytes(), model.TeamworkProfileID().String()); err != nil {
		t.Fatal(err)
	}
}
