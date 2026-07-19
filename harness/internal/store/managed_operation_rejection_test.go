package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestRejectStartedManagedOperationsBindsExactOperationReceipt(t *testing.T) {
	fixture := newManagedOperationRejectionFixture(t, "exact", 0)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rejectStartedManagedOperations(context.Background(), tx, managedOperationRejectionSpec{
		RunID: fixture.runID, ProfileID: fixture.profile.ID(), ContextHash: fixture.contextHash,
		Code:    "internal",
		Message: "managed Runtime failed", At: fixture.at,
	}); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	operation := fixture.operations[0]
	stored, err := readOperationByID(context.Background(), fixture.store.db, operation.ID())
	if err != nil || stored.Status() != model.OperationRejected {
		t.Fatalf("rejected operation %s = (%#v, %v)", operation.ID().String(), stored, err)
	}
	assertManagedOperationRejectionReceipt(t, stored, "internal", "managed Runtime failed")
	otherID, _ := model.ParseOperationID("operation-rejection-other")
	result, _ := stored.Result()
	if _, err := model.ParseOperationRejectionReceipt(result.Bytes(), otherID); err == nil {
		t.Fatal("operation rejection receipt was accepted for another operation ID")
	}
}

func TestRejectOperationRequiresCanonicalReceiptBoundToExactOperation(t *testing.T) {
	fixture := newManagedOperationRejectionFixture(t, "boundary", 0)
	operation := fixture.operations[0]
	legacy, err := model.NewJSON([]byte(`{"code":"internal","status":"rejected"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		operation.LeaseOwner(), fixture.at, legacy); err == nil {
		t.Fatal("RejectOperation accepted a legacy rejection receipt")
	}
	assertStartedManagedOperations(t, fixture)

	otherID, err := model.ParseOperationID("operation-rejection-other")
	if err != nil {
		t.Fatal(err)
	}
	wrong := mustManagedOperationRejectionReceipt(t, otherID, "internal", "managed Runtime failed")
	if _, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		operation.LeaseOwner(), fixture.at, wrong); err == nil {
		t.Fatal("RejectOperation accepted a receipt bound to another operation")
	}
	assertStartedManagedOperations(t, fixture)

	want := mustManagedOperationRejectionReceipt(t, operation.ID(), "internal", "managed Runtime failed")
	rejected, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		operation.LeaseOwner(), fixture.at, want)
	if err != nil || rejected.Replayed || rejected.Operation.Status() != model.OperationRejected {
		t.Fatalf("RejectOperation canonical receipt = (%#v, %v)", rejected, err)
	}
	assertManagedOperationRejectionReceipt(t, rejected.Operation, "internal", "managed Runtime failed")
	replay, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		"stale-owner", fixture.at.Add(time.Hour), wrong)
	if err != nil || !replay.Replayed || replay.Operation.Status() != model.OperationRejected {
		t.Fatalf("RejectOperation terminal replay = (%#v, %v)", replay, err)
	}
	assertManagedOperationRejectionReceipt(t, replay.Operation, "internal", "managed Runtime failed")
}

func TestRejectOperationReplaysCommittedWinnerBeforeLiveFence(t *testing.T) {
	fixture := newManagedOperationRejectionFixture(t, "committed-replay", 0)
	operation := fixture.operations[0]
	receipt, err := model.JSONFrom(struct {
		OperationID model.OperationID `json:"operation_id"`
		Status      string            `json:"status"`
	}{OperationID: operation.ID(), Status: "committed"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.store.db.Exec(`UPDATE operations SET status='committed',
		lease_owner=NULL,lease_until=NULL,result_json=?,finished_at=?
		WHERE operation_id=? AND status='started'`, receipt.Bytes(), storeTime(fixture.at),
		operation.ID().String())
	if err != nil || exactlyOne(result) != nil {
		t.Fatalf("prepare committed winner = (%#v, %v)", result, err)
	}
	replay, err := fixture.store.RejectOperation(context.Background(), operation.ID(), "",
		time.Time{}, model.JSON{})
	stored, ok := replay.Operation.Result()
	if err != nil || !replay.Replayed || replay.Operation.Status() != model.OperationCommitted ||
		!ok || stored.String() != receipt.String() {
		t.Fatalf("committed RejectOperation winner = (%#v, %v)", replay, err)
	}
}

func TestRejectStartedManagedOperationsCASFailureRollsBackEveryReceipt(t *testing.T) {
	fixture := newManagedOperationRejectionFixture(t, "cas", 0)
	blockedID := fixture.operations[0].ID().String()
	trigger := fmt.Sprintf(`CREATE TRIGGER reject_managed_operation BEFORE UPDATE OF status ON operations
		WHEN OLD.operation_id='%s' BEGIN SELECT RAISE(IGNORE); END`, blockedID)
	if _, err := fixture.store.db.Exec(trigger); err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = rejectStartedManagedOperations(context.Background(), tx, managedOperationRejectionSpec{
		RunID: fixture.runID, ProfileID: fixture.profile.ID(), ContextHash: fixture.contextHash,
		Code:    "internal",
		Message: "managed Runtime failed", At: fixture.at,
	})
	if err == nil || !strings.Contains(err.Error(), "affected 0 rows, want 1") {
		_ = tx.Rollback()
		t.Fatalf("missing exact-row CAS failure = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertStartedManagedOperations(t, fixture)
}

func TestRejectStartedManagedOperationsRejectsFutureCreationAtomically(t *testing.T) {
	fixture := newManagedOperationRejectionFixture(t, "future", 20*time.Second)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = rejectStartedManagedOperations(context.Background(), tx, managedOperationRejectionSpec{
		RunID: fixture.runID, ProfileID: fixture.profile.ID(), ContextHash: fixture.contextHash,
		Code:    "context_stale",
		Message: "managed context expired", At: fixture.at,
	})
	if err == nil || !strings.Contains(err.Error(), "trusted time precedes") {
		_ = tx.Rollback()
		t.Fatalf("future-created operation error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertStartedManagedOperations(t, fixture)
}

func TestRejectStartedManagedOperationsRejectsAuthorityDrift(t *testing.T) {
	t.Run("multiple started operations", func(t *testing.T) {
		fixture := newManagedOperationRejectionFixture(t, "multiple", 0)
		if _, err := fixture.store.db.Exec(`DROP INDEX operations_one_started_context_idx`); err != nil {
			t.Fatal(err)
		}
		second := fixture.newOperation(t, "second", fixture.contextHash, fixture.at)
		tx, err := fixture.store.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := insertOperation(context.Background(), tx, second); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		fixture.operations = append(fixture.operations, second)
		assertManagedOperationRejectionFails(t, fixture, "want at most one")
	})

	t.Run("wrong context", func(t *testing.T) {
		fixture := newManagedOperationRejectionFixture(t, "wrong-context", 0)
		if _, err := fixture.store.db.Exec(`DROP TRIGGER operations_identity_immutable`); err != nil {
			t.Fatal(err)
		}
		wrong := model.Sum([]byte("wrong-managed-context"))
		if _, err := fixture.store.db.Exec(`UPDATE operations SET context_hash=? WHERE operation_id=?`,
			wrong.Bytes(), fixture.operations[0].ID().String()); err != nil {
			t.Fatal(err)
		}
		assertManagedOperationRejectionFails(t, fixture, "authority differs")
	})

	t.Run("wrong run", func(t *testing.T) {
		fixture := newManagedOperationRejectionFixture(t, "wrong-run", 0)
		corruptManagedOperationRun(t, fixture.store, fixture.profile,
			fixture.operations[0].ID(), "unit", fixture.at.Add(-time.Minute))
		assertManagedOperationRejectionFails(t, fixture, "authority differs")
	})

	t.Run("immutable row corruption", func(t *testing.T) {
		fixture := newManagedOperationRejectionFixture(t, "corrupt", 0)
		if _, err := fixture.store.db.Exec(`DROP TRIGGER operations_identity_immutable`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`UPDATE operations SET request_digest=x'01' WHERE operation_id=?`,
			fixture.operations[0].ID().String()); err != nil {
			t.Fatal(err)
		}
		assertManagedOperationRejectionFails(t, fixture, "reconstruct")
	})
}

func TestManagedOperationRejectionAuthorityFailureRollsBackLifecycle(t *testing.T) {
	t.Run("claim expiry", func(t *testing.T) {
		fixture := newManagedContextFixture(t, "reject-claim-drift", model.OperationTeamworkAccept)
		reservation, err := fixture.store.ReserveManagedOperation(context.Background(),
			fixture.spec("reject-claim-drift", "reject-claim-drift", model.OperationTeamworkAccept))
		if err != nil {
			t.Fatal(err)
		}
		corruptManagedOperationRun(t, fixture.store, fixture.profile,
			reservation.Operation.ID(), "claim", fixture.claim.Run.StartedAt())
		lease, _ := fixture.claim.Run.LeaseUntil()
		_, err = fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
			ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
			At: lease,
		})
		if !errors.Is(err, ErrAgentClaimInvariant) {
			t.Fatalf("claim expiry authority error = %v", err)
		}
		assertManagedRejectionLifecycleUnchanged(t, fixture.store, fixture.claim,
			reservation.Operation.ID())
	})

	t.Run("runtime failure", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "reject-runtime-drift", true,
			model.OperationResolveNoAction, "reject corrupt Runtime operation")
		corruptManagedOperationContext(t, fixture.store, fixture.reservation.Operation.ID())
		at := fixture.reserveSpec.At.Add(time.Second)
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"hook_failed"}`)
		_, err := fixture.store.FailAgentRuntime(context.Background(), runtimeFailureSpec(
			fixture.acceptanceFixture, fixture.claim, fixture.token, fixture.diagnostic,
			fixture.runtimeIDs, completion, "mandatory Hook failed", at))
		if !errors.Is(err, ErrAgentRuntimeInvariant) {
			t.Fatalf("Runtime failure authority error = %v", err)
		}
		assertManagedRejectionLifecycleUnchanged(t, fixture.store, fixture.claim,
			fixture.reservation.Operation.ID())
	})

	t.Run("startup orphan", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "reject-orphan-drift", true,
			model.OperationResolveNoAction, "reject corrupt orphan operation")
		corruptManagedOperationContext(t, fixture.store, fixture.reservation.Operation.ID())
		at := fixture.reserveSpec.At.Add(time.Second)
		receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed"}`)
		_, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), orphanSpec(
			fixture.acceptanceFixture, fixture.claim, fixture.token, receipt,
			"managed Runtime disappeared during restart", at))
		if !errors.Is(err, ErrAgentRuntimeOrphanInvariant) {
			t.Fatalf("orphan authority error = %v", err)
		}
		assertManagedRejectionLifecycleUnchanged(t, fixture.store, fixture.claim,
			fixture.reservation.Operation.ID())
	})
}

type managedOperationRejectionFixture struct {
	store       *Store
	profile     model.Profile
	runID       model.RunID
	contextHash model.Digest
	at          time.Time
	operations  []model.Operation
}

func newManagedOperationRejectionFixture(t *testing.T, suffix string,
	createdOffset time.Duration,
) managedOperationRejectionFixture {
	t.Helper()
	managed := newManagedContextFixture(t, "rejection-"+suffix, model.OperationTeamworkAccept)
	at := managed.at.Add(10 * time.Second)
	spec := managed.spec("reject-"+suffix, "request-reject-"+suffix,
		model.OperationTeamworkAccept)
	spec.At, spec.LeaseUntil = at.Add(createdOffset), at.Add(createdOffset+time.Minute)
	reserved, err := managed.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return managedOperationRejectionFixture{store: managed.store, profile: managed.profile,
		runID: managed.claim.Run.ID(), contextHash: managed.tokenHash, at: at,
		operations: []model.Operation{reserved.Operation}}
}

func assertStartedManagedOperations(t *testing.T, fixture managedOperationRejectionFixture) {
	t.Helper()
	for _, operation := range fixture.operations {
		stored, err := readOperationByID(context.Background(), fixture.store.db, operation.ID())
		if err != nil || stored.Status() != model.OperationStarted {
			t.Fatalf("operation after rollback %s = (%#v, %v)", operation.ID().String(), stored, err)
		}
	}
}

func (fixture managedOperationRejectionFixture) newOperation(t *testing.T, suffix string,
	contextHash model.Digest, at time.Time,
) model.Operation {
	t.Helper()
	key := model.Sum([]byte("key-rejection-" + suffix))
	id, err := managedOperationID(fixture.profile.ID(), key)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := newManagedStartedOperation(id, fixture.profile.ID(), fixture.runID, key,
		&contextHash, model.OperationTeamworkAccept, model.Sum([]byte("request-rejection-"+suffix)),
		"owner-rejection-"+suffix, at, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func assertManagedOperationRejectionFails(t *testing.T, fixture managedOperationRejectionFixture,
	want string,
) {
	t.Helper()
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = rejectStartedManagedOperations(context.Background(), tx, managedOperationRejectionSpec{
		RunID: fixture.runID, ProfileID: fixture.profile.ID(), ContextHash: fixture.contextHash,
		Code: "internal", Message: "managed Runtime failed", At: fixture.at,
	})
	if err == nil || !strings.Contains(err.Error(), want) {
		_ = tx.Rollback()
		t.Fatalf("managed rejection authority error = %v, want %q", err, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, operation := range fixture.operations {
		var status string
		var result, finished []byte
		if err := fixture.store.db.QueryRow(`SELECT status,result_json,finished_at FROM operations
			WHERE operation_id=?`, operation.ID().String()).Scan(&status, &result, &finished); err != nil {
			t.Fatal(err)
		}
		if status != "started" || len(result) != 0 || len(finished) != 0 {
			t.Fatalf("operation mutated after rejection failure: %s %q %q", status, result, finished)
		}
	}
}

func corruptManagedOperationContext(t *testing.T, st *Store, operationID model.OperationID) {
	t.Helper()
	if _, err := st.db.Exec(`DROP TRIGGER operations_identity_immutable`); err != nil {
		t.Fatal(err)
	}
	wrong := model.Sum([]byte("corrupt-managed-operation-context-" + operationID.String()))
	if _, err := st.db.Exec(`UPDATE operations SET context_hash=? WHERE operation_id=?`,
		wrong.Bytes(), operationID.String()); err != nil {
		t.Fatal(err)
	}
}

func corruptManagedOperationRun(t *testing.T, st *Store, profile model.Profile,
	operationID model.OperationID, suffix string, startedAt time.Time,
) {
	t.Helper()
	runID := "run-rejection-corrupt-" + suffix
	insertOperationAgentRun(t, st, profile, runID, "running", startedAt)
	if _, err := st.db.Exec(`DROP TRIGGER operations_identity_immutable`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE operations SET agent_run_id=? WHERE operation_id=?`,
		runID, operationID.String()); err != nil {
		t.Fatal(err)
	}
}

func assertManagedRejectionLifecycleUnchanged(t *testing.T, st *Store, claim AgentClaimResult,
	operationID model.OperationID,
) {
	t.Helper()
	run, err := st.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil || run.Status() != claim.Run.Status() {
		t.Fatalf("AgentRun mutated after rejection authority failure = (%#v, %v)", run, err)
	}
	handling, err := st.GetAgentHandling(context.Background(), claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingClaimed {
		t.Fatalf("Handling mutated after rejection authority failure = (%#v, %v)", handling, err)
	}
	var status string
	var result []byte
	if err := st.db.QueryRow(`SELECT status,result_json FROM operations WHERE operation_id=?`,
		operationID.String()).Scan(&status, &result); err != nil || status != "started" || len(result) != 0 {
		t.Fatalf("Operation mutated after rejection authority failure = (%q, %q, %v)", status, result, err)
	}
}

func mustManagedOperationRejectionReceipt(t testing.TB, operationID model.OperationID,
	code, message string,
) model.JSON {
	t.Helper()
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: operationID, Code: code, Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt.JSON()
}

func assertManagedOperationRejectionReceipt(t testing.TB, operation model.Operation,
	code, message string,
) {
	t.Helper()
	result, ok := operation.Result()
	if !ok {
		t.Fatalf("operation %s has no rejection receipt", operation.ID().String())
	}
	receipt, err := model.ParseOperationRejectionReceipt(result.Bytes(), operation.ID())
	if err != nil || receipt.OperationID() != operation.ID() || receipt.Code() != code ||
		receipt.Message() != message {
		t.Fatalf("operation %s rejection receipt = (%#v, %v)", operation.ID().String(), receipt, err)
	}
	want := mustManagedOperationRejectionReceipt(t, operation.ID(), code, message)
	if result.String() != want.String() {
		t.Fatalf("operation %s rejection JSON = %s, want %s",
			operation.ID().String(), result.String(), want.String())
	}
}
