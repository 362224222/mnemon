package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestPeerInboxSemanticHandlingSettlementClosesPendingAndClaimedAuthority(t *testing.T) {
	for _, test := range []struct {
		name        string
		eventType   model.EventType
		disposition string
		claimed     bool
		operation   bool
	}{
		{"pending-cancellation", model.EventReviewCancelled, "superseded_cancelled", false, false},
		{"claimed-cancellation", model.EventReviewCancelled, "superseded_cancelled", true, false},
		{"claimed-expiry-operation", model.EventReviewExpired, "superseded_expired", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerInboxSemanticHandlingSettlementFixture(t, test.name, test.eventType)
			var claim AgentClaimResult
			var token model.Digest
			var operation model.Operation
			if test.claimed {
				claim, token = claimPeerInboxSemanticSettlementHandling(t, fixture)
				if test.operation {
					installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
						fixture.source.AcceptedAt().Add(500*time.Millisecond))
					operation = reservePeerInboxSemanticSettlementOperation(t, fixture,
						claim, token, "target")
				}
			}

			unrelatedRun, unrelatedOperation := insertPeerInboxSemanticSettlementUnrelatedOperation(
				t, fixture, "unrelated")
			tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
			advancePeerInboxSemanticSettlementWork(t, tx, fixture)
			if err := settlePeerInboxSemanticHandling(context.Background(), tx,
				fixture.settlement, fixture.settleAt); err != nil {
				t.Fatalf("settlePeerInboxSemanticHandling() error = %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			handling, err := readAgentHandling(context.Background(), fixture.store.db,
				fixture.handling.ID())
			if err != nil || handling.Status() != model.HandlingCompleted ||
				handling.LastDisposition() != test.disposition || handling.ClaimOwner() != "" ||
				!handling.UpdatedAt().Equal(fixture.settleAt) {
				t.Fatalf("settled Handling = (%#v,%v)", handling, err)
			}
			if _, ok := handling.ClaimTokenHash(); ok {
				t.Fatal("settled Handling retained claim token")
			}
			if _, ok := handling.LeaseUntil(); ok {
				t.Fatal("settled Handling retained lease")
			}
			if test.claimed {
				run, err := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
				if err != nil || run.Status() != model.AgentRunOutcomeAccepted {
					t.Fatalf("settled AgentRun = (%#v,%v)", run, err)
				}
				outcome, ok := run.OutcomeReceipt()
				if !ok || len(outcome.Bytes()) > model.MaxContentBytes {
					t.Fatalf("bounded AgentRun outcome = (%t,%d)", ok, len(outcome.Bytes()))
				}
				assertPeerInboxSemanticSettlementReceipt(t, outcome, fixture, handling,
					claim.Run.ID(), model.OperationID{}, "superseded")
			}
			if test.operation {
				durable, err := readOperationByID(context.Background(), fixture.store.db,
					operation.ID())
				if err != nil || durable.Status() != model.OperationRejected {
					t.Fatalf("settled Operation = (%#v,%v)", durable, err)
				}
				receipt, ok := durable.Result()
				if !ok || len(receipt.Bytes()) > model.MaxContentBytes {
					t.Fatalf("bounded Operation receipt = (%t,%d)", ok, len(receipt.Bytes()))
				}
				assertPeerInboxSemanticSettlementReceipt(t, receipt, fixture, handling,
					claim.Run.ID(), operation.ID(), "rejected")
			}

			otherRun, err := readAgentRun(context.Background(), fixture.store.db, unrelatedRun.ID())
			if err != nil || otherRun.Status() != model.AgentRunRunning {
				t.Fatalf("unrelated Run changed = (%#v,%v)", otherRun, err)
			}
			otherOperation, err := readOperationByID(context.Background(), fixture.store.db,
				unrelatedOperation.ID())
			if err != nil || otherOperation.Status() != model.OperationStarted {
				t.Fatalf("unrelated Operation changed = (%#v,%v)", otherOperation, err)
			}

			validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
			if err := validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
				fixture.settlement); err != nil {
				t.Fatalf("terminal settlement validation = %v", err)
			}
			_ = validateTx.Rollback()
		})
	}
}

func TestPeerInboxSemanticHandlingSettlementReplaysAndPreservesPriorTerminal(t *testing.T) {
	t.Run("strict supersede replay", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "strict-replay",
			model.EventReviewCancelled)
		claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
		installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
			fixture.source.AcceptedAt().Add(500*time.Millisecond))
		operation := reservePeerInboxSemanticSettlementOperation(t, fixture, claim, token, "replay")
		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		advancePeerInboxSemanticSettlementWork(t, tx, fixture)
		if err := settlePeerInboxSemanticHandling(context.Background(), tx,
			fixture.settlement, fixture.settleAt); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		beforeHandling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		beforeRun, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
		beforeOperation, _ := readOperationByID(context.Background(), fixture.store.db, operation.ID())
		deactivatedAt := fixture.settleAt.Add(time.Second)
		deactivated, err := fixture.store.DeactivateProfile(context.Background(), fixture.profile,
			deactivatedAt)
		if err != nil {
			t.Fatal(err)
		}
		desiredSpec := deactivated.Profile.Spec()
		desiredSpec.Host = model.HostClaudeCode
		desiredSpec.Runtime = model.RuntimeClaudeCLI
		desiredSpec.Enabled = true
		desired, err := model.NewProfile(desiredSpec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ActivateProfile(context.Background(), desired,
			deactivated.Profile.UpdatedAt(), deactivatedAt.Add(time.Nanosecond)); err != nil {
			t.Fatalf("switch Profile runtime after settlement: %v", err)
		}
		validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if err := validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
			fixture.settlement); err != nil {
			t.Fatalf("historical settlement after runtime switch = %v", err)
		}
		_ = validateTx.Rollback()

		replayTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if err := settlePeerInboxSemanticHandling(context.Background(), replayTx,
			fixture.settlement, fixture.settleAt.Add(time.Hour)); err != nil {
			t.Fatalf("settlement replay error = %v", err)
		}
		if err := replayTx.Commit(); err != nil {
			t.Fatal(err)
		}
		afterHandling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		afterRun, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
		afterOperation, _ := readOperationByID(context.Background(), fixture.store.db, operation.ID())
		if !sameHandling(beforeHandling, afterHandling) ||
			beforeRun.Status() != afterRun.Status() ||
			peerInboxSemanticAgentRunOutcome(beforeRun) != peerInboxSemanticAgentRunOutcome(afterRun) ||
			beforeOperation.Status() != afterOperation.Status() ||
			peerInboxSemanticOperationResult(beforeOperation) != peerInboxSemanticOperationResult(afterOperation) {
			t.Fatal("strict replay rewrote terminal Handling, Run, or Operation evidence")
		}
	})

	t.Run("prior no-action wins", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "prior-no-action",
			model.EventReviewCancelled)
		claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
		readAt := fixture.source.AcceptedAt().Add(500 * time.Millisecond)
		installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token, readAt)
		content := "the source was already handled before the remote terminal arrived"
		request := mustManagedResolutionRequestDigest(t, fixture.profile, token,
			model.OperationResolveNoAction, content)
		reserveAt := readAt.Add(250 * time.Millisecond)
		reservation, err := fixture.store.ReserveManagedOperation(context.Background(), ManagedOperationSpec{
			Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("prior-no-action-key")),
			RequestDigest: request, Kind: model.OperationResolveNoAction,
			LeaseOwner: "prior-no-action-owner", At: reserveAt,
			LeaseUntil: reserveAt.Add(time.Minute), ClaimContextHash: token, HasClaimContext: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		resolvedAt := reserveAt.Add(250 * time.Millisecond)
		resolved, err := fixture.store.CommitManagedResolution(context.Background(), ManagedResolutionSpec{
			Reservation: reservation, Content: content, At: resolvedAt})
		if err != nil {
			t.Fatal(err)
		}
		deactivatedAt := resolvedAt.Add(100 * time.Millisecond)
		deactivated, err := fixture.store.DeactivateProfile(context.Background(), fixture.profile,
			deactivatedAt)
		if err != nil {
			t.Fatal(err)
		}
		desiredSpec := deactivated.Profile.Spec()
		desiredSpec.Host = model.HostClaudeCode
		desiredSpec.Runtime = model.RuntimeClaudeCLI
		desiredSpec.Enabled = true
		desired, err := model.NewProfile(desiredSpec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ActivateProfile(context.Background(), desired,
			deactivated.Profile.UpdatedAt(), deactivatedAt.Add(time.Nanosecond)); err != nil {
			t.Fatalf("switch Profile before prior-winner settlement: %v", err)
		}
		beforeHandling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		beforeRun, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
		if beforeRun.Runtime() != model.RuntimeCodexAppServer {
			t.Fatalf("historical prior-winner Runtime = %s", beforeRun.Runtime())
		}

		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		advancePeerInboxSemanticSettlementWork(t, tx, fixture)
		if err := settlePeerInboxSemanticHandling(context.Background(), tx,
			fixture.settlement, fixture.settleAt); err != nil {
			t.Fatalf("late remote terminal rejected prior no-action: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		afterHandling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		afterRun, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
		if !sameHandling(beforeHandling, afterHandling) ||
			peerInboxSemanticAgentRunOutcome(beforeRun) != resolved.Receipt.String() ||
			peerInboxSemanticAgentRunOutcome(afterRun) != resolved.Receipt.String() {
			t.Fatal("late remote terminal overwrote the prior no-action winner")
		}
		validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if err := validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
			fixture.settlement); err != nil {
			t.Fatalf("prior terminal read-only validation = %v", err)
		}
		_ = validateTx.Rollback()
	})
}

func TestCommitPeerInboxSemanticSettlesClaimedHandlingAtomicallyAndReplays(t *testing.T) {
	t.Run("commit, runtime switch, and restart replay", func(t *testing.T) {
		fixture := newPeerInboxSemanticSettlementCommitFixture(t, "public-commit")
		result, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
			fixture.spec, fixture.committedAt)
		if err != nil || !result.Changed() || result.Replayed() ||
			result.Status() != model.InboxAccepted ||
			result.ImportedEventID() != fixture.terminal.Event().ID() || result.Decision().IsZero() {
			t.Fatalf("CommitPeerInboxSemantic() = (%#v,%v)", result, err)
		}
		responses := result.ResponseEventIDs()
		if len(responses) != 1 || responses[0] != fixture.spec.Responses[0].Event().ID() ||
			fixture.spec.Responses[0].Event().Type() != model.EventReviewOutcome {
			t.Fatalf("local outcome responses = %#v", responses)
		}
		receipt, ok := result.ReceiptEventID()
		if !ok || receipt != responses[0] {
			t.Fatalf("terminal receipt = (%s,%t), want %s", receipt, ok, responses[0])
		}

		assertPeerInboxSemanticSettlementCommit(t, fixture, responses[0])
		assertPeerInboxSemanticTerminalProjection(t, fixture.store, fixture.inboxID,
			model.InboxAccepted, fixture.terminal.Event().ID(), responses[0])
		assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store,
			fixture.terminal.Event().ID())
		assertPeerInboxSemanticSettlementTransitionCount(t, fixture.store, 0)

		beforeHandling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		beforeRun, _ := readAgentRun(context.Background(), fixture.store.db, fixture.agent.Run.ID())
		beforeOperation, _ := readOperationByID(context.Background(), fixture.store.db,
			fixture.operation.ID())
		beforeWork, _ := readReviewWork(context.Background(), fixture.store.db, fixture.work.Ref())

		switchAt := fixture.committedAt.Add(time.Second)
		deactivated, err := fixture.store.DeactivateProfile(context.Background(), fixture.profile,
			switchAt)
		if err != nil {
			t.Fatalf("deactivate Profile after settlement: %v", err)
		}
		desiredSpec := deactivated.Profile.Spec()
		desiredSpec.Host = model.HostClaudeCode
		desiredSpec.Runtime = model.RuntimeClaudeCLI
		desiredSpec.Enabled = true
		desired, err := model.NewProfile(desiredSpec)
		if err != nil {
			t.Fatal(err)
		}
		activated, err := fixture.store.ActivateProfile(context.Background(), desired,
			deactivated.Profile.UpdatedAt(), switchAt.Add(time.Nanosecond))
		if err != nil || activated.Profile.Runtime() != model.RuntimeClaudeCLI {
			t.Fatalf("switch Profile runtime after commit = (%#v,%v)", activated, err)
		}
		if beforeRun.Runtime() != model.RuntimeCodexAppServer {
			t.Fatalf("historical AgentRun runtime = %s", beforeRun.Runtime())
		}

		path := fixture.store.Path()
		if err := fixture.store.Close(); err != nil {
			t.Fatal(err)
		}
		fixture.store = nil
		fixture.peer.store = nil
		restarted, err := OpenExisting(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.Close() })
		replay, err := restarted.CommitPeerInboxSemantic(context.Background(), fixture.spec,
			fixture.committedAt.Add(time.Hour))
		if err != nil || replay.Changed() || !replay.Replayed() ||
			replay.Decision().String() != result.Decision().String() ||
			replay.Status() != model.InboxAccepted {
			t.Fatalf("restart exact replay = (%#v,%v)", replay, err)
		}
		afterHandling, _ := readAgentHandling(context.Background(), restarted.db,
			fixture.handling.ID())
		afterRun, _ := readAgentRun(context.Background(), restarted.db, fixture.agent.Run.ID())
		afterOperation, _ := readOperationByID(context.Background(), restarted.db,
			fixture.operation.ID())
		afterWork, _ := readReviewWork(context.Background(), restarted.db, fixture.work.Ref())
		if !sameHandling(beforeHandling, afterHandling) ||
			beforeRun.Status() != afterRun.Status() || beforeRun.Runtime() != afterRun.Runtime() ||
			peerInboxSemanticAgentRunOutcome(beforeRun) != peerInboxSemanticAgentRunOutcome(afterRun) ||
			beforeOperation.Status() != afterOperation.Status() ||
			peerInboxSemanticOperationResult(beforeOperation) != peerInboxSemanticOperationResult(afterOperation) ||
			beforeWork.Version() != afterWork.Version() || beforeWork.State() != afterWork.State() ||
			beforeWork.UpdatedBy() != afterWork.UpdatedBy() ||
			!beforeWork.UpdatedAt().Equal(afterWork.UpdatedAt()) {
			t.Fatal("restart replay rewrote settlement, Work, Run, or Operation evidence")
		}
		assertPeerInboxSemanticSettlementTransitionCount(t, restarted, 0)
	})

	t.Run("terminal failure rolls back every domain effect", func(t *testing.T) {
		fixture := newPeerInboxSemanticSettlementCommitFixture(t, "public-rollback")
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_semantic_settlement_terminal
			BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status='accepted'
			BEGIN SELECT RAISE(ABORT,'injected semantic settlement terminal failure'); END`)
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), fixture.spec,
			fixture.committedAt); err == nil {
			t.Fatal("injected semantic terminal failure was accepted")
		}

		assertPeerInboxSemanticState(t, fixture.store, fixture.inboxID, "processing",
			fixture.semantic.Fence().Attempt(), "", true)
		assertPeerInboxSemanticTransitionReceipt(t, fixture.store, fixture.inboxID, "renew",
			fixture.originalFence.Attempt(), fixture.originalFence.LeaseUntil(),
			fixture.semantic.Fence().LeaseUntil())
		var undecided int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox
			WHERE inbox_id=? AND local_event_id IS NULL AND decision_json IS NULL
			AND receipt_event_id IS NULL`, fixture.inboxID.String()).Scan(&undecided); err != nil ||
			undecided != 1 {
			t.Fatalf("rolled-back Inbox terminal evidence = (%d,%v)", undecided, err)
		}
		var events, provenance int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id IN (?,?)`,
			fixture.terminal.Event().ID().String(),
			fixture.spec.Responses[0].Event().ID().String()).Scan(&events); err != nil || events != 0 {
			t.Fatalf("rolled-back terminal/response Events = (%d,%v)", events, err)
		}
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_provenance
			WHERE producer_event_id=?`, fixture.terminal.Event().ID().String()).Scan(&provenance); err != nil ||
			provenance != 0 {
			t.Fatalf("rolled-back imported provenance = (%d,%v)", provenance, err)
		}
		work, workErr := readReviewWork(context.Background(), fixture.store.db, fixture.work.Ref())
		handling, handlingErr := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		run, runErr := readAgentRun(context.Background(), fixture.store.db, fixture.agent.Run.ID())
		operation, operationErr := readOperationByID(context.Background(), fixture.store.db,
			fixture.operation.ID())
		if workErr != nil || work.Version() != fixture.work.Version() ||
			work.State() != model.WorkOffered || work.UpdatedBy() != fixture.source.ID() ||
			handlingErr != nil || handling.Status() != model.HandlingClaimed ||
			runErr != nil || run.Status() != model.AgentRunRunning ||
			operationErr != nil || operation.Status() != model.OperationStarted {
			t.Fatalf("partial rollback = Work (%#v,%v), Handling (%#v,%v), Run (%#v,%v), Operation (%#v,%v)",
				work, workErr, handling, handlingErr, run, runErr, operation, operationErr)
		}
	})
}

func TestPeerInboxSemanticHandlingSettlementFailsClosedWithoutPartialMutation(t *testing.T) {
	t.Run("read-only replay rejects pending", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "pending-replay",
			model.EventReviewCancelled)
		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		err := validatePeerInboxSemanticHandlingSettlement(context.Background(), tx,
			fixture.settlement)
		_ = tx.Rollback()
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("pending replay error = %v", err)
		}
		handling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		if handling.Status() != model.HandlingPending {
			t.Fatalf("read-only validation changed Handling to %s", handling.Status())
		}
	})

	t.Run("multiple exact runs", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "multiple-runs",
			model.EventReviewCancelled)
		claim, _ := claimPeerInboxSemanticSettlementHandling(t, fixture)
		mustExec(t, fixture.store, `DROP INDEX agent_runs_handling_generation_attempt_idx`)
		duplicateID, _ := model.ParseRunID("run-semantic-settlement-duplicate")
		handlingID, _ := claim.Run.HandlingID()
		fence, _ := claim.Run.ClaimFenceHash()
		lease, _ := claim.Run.LeaseUntil()
		duplicate, err := model.NewAgentRun(model.AgentRunSpec{ID: duplicateID,
			ProfileID: claim.Run.ProfileID(), HandlingID: &handlingID, Cause: claim.Run.Cause(),
			HandlingAttempt: claim.Run.HandlingAttempt(), HandlingRecovery: claim.Run.HandlingRecovery(),
			ClaimFenceHash: &fence, LeaseUntil: &lease, Launcher: claim.Run.Launcher(),
			Runtime: claim.Run.Runtime(), LauncherDiagnostic: claim.Run.LauncherDiagnostic(),
			RuntimeIDs: claim.Run.RuntimeIDs(), Status: model.AgentRunRunning,
			StartedAt: claim.Run.StartedAt().Add(time.Nanosecond)})
		if err != nil {
			t.Fatal(err)
		}
		insertTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if err := insertAgentRun(context.Background(), insertTx, duplicate); err != nil {
			t.Fatal(err)
		}
		if err := insertTx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertPeerInboxSemanticSettlementRollback(t, fixture, func(tx *sql.Tx) error {
			return settlePeerInboxSemanticHandling(context.Background(), tx,
				fixture.settlement, fixture.settleAt)
		})
	})

	t.Run("multiple started operations", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "multiple-operations",
			model.EventReviewCancelled)
		claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
		installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
			fixture.source.AcceptedAt().Add(500*time.Millisecond))
		reservePeerInboxSemanticSettlementOperation(t, fixture, claim, token, "first")
		mustExec(t, fixture.store, `DROP INDEX operations_one_started_context_idx`)
		second := newPeerInboxSemanticSettlementOperation(t, fixture, claim, token, "second")
		insertTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if err := insertOperation(context.Background(), insertTx, second); err != nil {
			t.Fatal(err)
		}
		if err := insertTx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertPeerInboxSemanticSettlementRollback(t, fixture, func(tx *sql.Tx) error {
			return settlePeerInboxSemanticHandling(context.Background(), tx,
				fixture.settlement, fixture.settleAt)
		})
	})

	t.Run("operation context drift", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "operation-drift",
			model.EventReviewCancelled)
		claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
		installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
			fixture.source.AcceptedAt().Add(500*time.Millisecond))
		wrong := model.Sum([]byte("wrong-claim-context"))
		reservePeerInboxSemanticSettlementOperation(t, fixture, claim, wrong, "drift")
		assertPeerInboxSemanticSettlementRollback(t, fixture, func(tx *sql.Tx) error {
			return settlePeerInboxSemanticHandling(context.Background(), tx,
				fixture.settlement, fixture.settleAt)
		})
	})

	t.Run("late Handling fence failure rolls back Run and Operation", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "late-fence-rollback",
			model.EventReviewCancelled)
		claim, token := claimPeerInboxSemanticSettlementHandling(t, fixture)
		installPeerInboxSemanticSettlementCurrent(t, fixture, claim, token,
			fixture.source.AcceptedAt().Add(500*time.Millisecond))
		operation := reservePeerInboxSemanticSettlementOperation(t, fixture, claim, token, "rollback")
		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		if _, err := tx.Exec(`CREATE TEMP TRIGGER reject_semantic_handling_settlement
			BEFORE UPDATE OF status ON agent_handlings
			WHEN NEW.status='completed' AND NEW.last_disposition='superseded_cancelled'
			BEGIN SELECT RAISE(ABORT,'injected semantic Handling failure'); END`); err != nil {
			t.Fatal(err)
		}
		advancePeerInboxSemanticSettlementWork(t, tx, fixture)
		err := settlePeerInboxSemanticHandling(context.Background(), tx,
			fixture.settlement, fixture.settleAt)
		_ = tx.Rollback()
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("late Handling failure error = %v", err)
		}
		handling, _ := readAgentHandling(context.Background(), fixture.store.db,
			fixture.handling.ID())
		run, _ := readAgentRun(context.Background(), fixture.store.db, claim.Run.ID())
		durableOperation, _ := readOperationByID(context.Background(), fixture.store.db,
			operation.ID())
		work, _ := readReviewWork(context.Background(), fixture.store.db, fixture.work.Ref())
		if handling.Status() != model.HandlingClaimed || run.Status() != model.AgentRunRunning ||
			durableOperation.Status() != model.OperationStarted || work.State() != model.WorkOffered {
			t.Fatalf("partial rollback = Handling %s Run %s Operation %s Work %s",
				handling.Status(), run.Status(), durableOperation.Status(), work.State())
		}
	})

	t.Run("tampered supersede receipt", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "receipt-drift",
			model.EventReviewCancelled)
		claim, _ := claimPeerInboxSemanticSettlementHandling(t, fixture)
		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		advancePeerInboxSemanticSettlementWork(t, tx, fixture)
		if err := settlePeerInboxSemanticHandling(context.Background(), tx,
			fixture.settlement, fixture.settleAt); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `DROP TRIGGER agent_runs_evidence_once`)
		mustExec(t, fixture.store, `UPDATE agent_runs SET outcome_receipt_json=? WHERE run_id=?`,
			[]byte(`{"status":"tampered"}`), claim.Run.ID().String())
		validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		err := validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
			fixture.settlement)
		_ = validateTx.Rollback()
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("tampered receipt validation error = %v", err)
		}
	})

	t.Run("tampered historical Run cause", func(t *testing.T) {
		fixture := newPeerInboxSemanticHandlingSettlementFixture(t, "run-cause-drift",
			model.EventReviewCancelled)
		claim, _ := claimPeerInboxSemanticSettlementHandling(t, fixture)
		tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		advancePeerInboxSemanticSettlementWork(t, tx, fixture)
		if err := settlePeerInboxSemanticHandling(context.Background(), tx,
			fixture.settlement, fixture.settleAt); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `DROP TRIGGER agent_runs_creation_identity_immutable`)
		mustExec(t, fixture.store, `UPDATE agent_runs SET cause_json='{}' WHERE run_id=?`,
			claim.Run.ID().String())
		validateTx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
		err := validatePeerInboxSemanticHandlingSettlement(context.Background(), validateTx,
			fixture.settlement)
		_ = validateTx.Rollback()
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("tampered historical Run cause error = %v", err)
		}
	})

	t.Run("foreign settlement", func(t *testing.T) {
		left := newPeerInboxSemanticHandlingSettlementFixture(t, "foreign-left",
			model.EventReviewCancelled)
		right := newPeerInboxSemanticHandlingSettlementFixture(t, "foreign-right",
			model.EventReviewCancelled)
		tx := beginPeerInboxSemanticSettlementTx(t, right.store)
		err := settlePeerInboxSemanticHandling(context.Background(), tx,
			left.settlement, right.settleAt)
		_ = tx.Rollback()
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("foreign settlement error = %v", err)
		}
	})
}

type peerInboxSemanticHandlingSettlementFixture struct {
	store        *Store
	peer         peerInboxFixture
	profile      model.Profile
	source       model.Event
	terminalWire model.SignedPublication
	terminal     model.SignedPublication
	work         model.ReviewWork
	handling     model.Handling
	settlement   PeerInboxSemanticHandlingSettlement
	settleAt     time.Time
}

type peerInboxSemanticSettlementCommitFixture struct {
	*peerInboxSemanticHandlingSettlementFixture
	agent         AgentClaimResult
	operation     model.Operation
	inboxID       model.InboxID
	originalFence PeerInboxSemanticFence
	semantic      PeerInboxSemanticClaim
	spec          CommitPeerInboxSemanticSpec
	committedAt   time.Time
}

func newPeerInboxSemanticSettlementCommitFixture(t *testing.T,
	seed string,
) *peerInboxSemanticSettlementCommitFixture {
	t.Helper()
	settlement := newPeerInboxSemanticHandlingSettlementFixture(t, seed,
		model.EventReviewCancelled)
	agent, token := claimPeerInboxSemanticSettlementHandling(t, settlement)
	installPeerInboxSemanticSettlementCurrent(t, settlement, agent, token,
		settlement.source.AcceptedAt().Add(500*time.Millisecond))
	operation := reservePeerInboxSemanticSettlementOperation(t, settlement, agent, token,
		"public-"+seed)

	reserved, err := settlement.store.ReserveOutboundChannelBaseline(context.Background(),
		ReserveOutboundChannelBaselineSpec{ChannelID: settlement.peer.channel.Channel().ID(),
			TargetPeerID: settlement.peer.remote.Identity().PeerID(),
			At:           settlement.source.AcceptedAt().Add(time.Second)})
	if err != nil {
		t.Fatalf("reserve outbound baseline: %v", err)
	}
	if _, err := settlement.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{
			AuthenticatedPeerID: settlement.peer.remote.Identity().PeerID(),
			Ack:                 ChannelDataBaselineAck(reserved.Baseline),
			At:                  settlement.source.AcceptedAt().Add(time.Second + time.Nanosecond),
		}); err != nil {
		t.Fatalf("confirm outbound baseline: %v", err)
	}

	put := settlement.peer.put(t, settlement.terminalWire, settlement.terminal.Event().AcceptedAt())
	readyAt := settlement.terminal.Event().AcceptedAt().Add(250 * time.Millisecond)
	markPeerInboxSemanticReady(t, settlement.store, put.InboxID, readyAt)
	semantic := mustClaimPeerInboxSemantic(t, settlement.store,
		"settlement-public-worker-"+seed, readyAt.Add(250*time.Millisecond))
	originalFence := semantic.Fence()
	renewAt := readyAt.Add(500 * time.Millisecond)
	renewal, err := settlement.store.RenewPeerInboxSemantic(context.Background(),
		RenewPeerInboxSemanticSpec{Fence: originalFence, At: renewAt})
	if err != nil || !renewal.Changed() || renewal.Replayed() {
		t.Fatalf("RenewPeerInboxSemantic() = (%#v,%v)", renewal, err)
	}
	assertPeerInboxSemanticTransitionReceipt(t, settlement.store, put.InboxID, "renew",
		originalFence.Attempt(), originalFence.LeaseUntil(), renewal.Fence().LeaseUntil())
	semantic.fence = renewal.Fence()
	decisionAt := renewAt.Add(250 * time.Millisecond)
	spec := peerInboxSemanticCommitSpec(t, settlement.peer, semantic, decisionAt)
	if len(spec.Responses) != 1 || spec.Responses[0].Event().Type() != model.EventReviewOutcome {
		t.Fatalf("cancellation semantic responses = %#v", spec.Responses)
	}
	committedAt := decisionAt.Add(250 * time.Millisecond)
	settlement.settleAt = committedAt
	return &peerInboxSemanticSettlementCommitFixture{
		peerInboxSemanticHandlingSettlementFixture: settlement,
		agent: agent, operation: operation, inboxID: put.InboxID,
		originalFence: originalFence, semantic: semantic, spec: spec, committedAt: committedAt,
	}
}

func assertPeerInboxSemanticSettlementCommit(t *testing.T,
	fixture *peerInboxSemanticSettlementCommitFixture, responseID model.EventID,
) {
	t.Helper()
	work, err := readReviewWork(context.Background(), fixture.store.db, fixture.work.Ref())
	if err != nil || work.Version() != fixture.work.Version()+1 ||
		work.State() != model.WorkCancelled || work.UpdatedBy() != fixture.terminal.Event().ID() ||
		!work.UpdatedAt().Equal(fixture.terminal.Event().AcceptedAt()) {
		t.Fatalf("settled Work = (%#v,%v)", work, err)
	}
	handling, err := readAgentHandling(context.Background(), fixture.store.db,
		fixture.handling.ID())
	if err != nil || handling.Status() != model.HandlingCompleted ||
		handling.LastDisposition() != "superseded_cancelled" ||
		!handling.UpdatedAt().Equal(fixture.committedAt) {
		t.Fatalf("settled Handling = (%#v,%v)", handling, err)
	}
	run, err := readAgentRun(context.Background(), fixture.store.db, fixture.agent.Run.ID())
	if err != nil || run.Status() != model.AgentRunOutcomeAccepted ||
		run.Runtime() != model.RuntimeCodexAppServer {
		t.Fatalf("settled AgentRun = (%#v,%v)", run, err)
	}
	runReceipt, ok := run.OutcomeReceipt()
	if !ok {
		t.Fatal("settled AgentRun has no outcome receipt")
	}
	assertPeerInboxSemanticSettlementReceipt(t, runReceipt,
		fixture.peerInboxSemanticHandlingSettlementFixture, handling, fixture.agent.Run.ID(),
		model.OperationID{}, "superseded")
	operation, err := readOperationByID(context.Background(), fixture.store.db,
		fixture.operation.ID())
	if err != nil || operation.Status() != model.OperationRejected {
		t.Fatalf("settled Operation = (%#v,%v)", operation, err)
	}
	operationReceipt, ok := operation.Result()
	if !ok {
		t.Fatal("settled Operation has no result receipt")
	}
	assertPeerInboxSemanticSettlementReceipt(t, operationReceipt,
		fixture.peerInboxSemanticHandlingSettlementFixture, handling, fixture.agent.Run.ID(),
		fixture.operation.ID(), "rejected")
	response, err := readCurrentSourceEvent(context.Background(), fixture.store.db, responseID)
	if err != nil || response.Type() != model.EventReviewOutcome ||
		response.Source() != model.EventSourceLocal ||
		len(response.CausedBy()) != 1 || response.CausedBy()[0] != fixture.terminal.Event().Key() {
		t.Fatalf("durable local outcome = (%#v,%v)", response, err)
	}
}

func assertPeerInboxSemanticSettlementTransitionCount(t *testing.T, store *Store, want int) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts`).
		Scan(&count); err != nil || count != want {
		t.Fatalf("semantic transition receipt count = (%d,%v), want %d", count, err, want)
	}
}

func newPeerInboxSemanticHandlingSettlementFixture(t *testing.T, seed string,
	terminalType model.EventType,
) *peerInboxSemanticHandlingSettlementFixture {
	t.Helper()
	peer := newPeerInboxFixture(t, "handling-settlement-"+seed, 0)
	node, err := readNode(context.Background(), peer.store.db)
	if err != nil {
		t.Fatal(err)
	}
	profileAt := node.UpdatedAt()
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-settlement", WorkspaceRoot: "/workspace/settlement",
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("credential-settlement-" + seed)),
		ActiveAssetRevision: node.ActiveAssetRevision(), HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: profileAt, UpdatedAt: profileAt})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, peer.store, `INSERT INTO profiles(profile_id,principal,workspace_root,host,
		runtime_kind,credential_hash,active_asset_rev,handling_budget_json,enabled,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,1,?,?)`, profile.ID().String(), profile.Principal(),
		profile.WorkspaceRoot(), string(profile.Host()), string(profile.Runtime()),
		profile.CredentialHash().Bytes(), profile.ActiveAssetRevision(), profile.HandlingBudget().Bytes(),
		storeTime(profile.CreatedAt()), storeTime(profile.UpdatedAt()))

	local := peer.channel.Owner()
	remote := peer.remote.Identity()
	workID, _ := model.ParseWorkID("work-semantic-settlement-" + seed)
	workRef, _ := model.NewWorkRef(remote.PeerID(), workID)
	sourceAt := peer.at.Add(time.Second)
	deadline := sourceAt.Add(10 * time.Minute)
	sourceScope, err := model.NewEventScope(peer.channel.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), 1, 1, peer.remote.Member().Head(), peer.channel.Roster().Head(), workRef)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{local.PeerID()})
	sourceID, _ := model.ParseEventID("event-semantic-settlement-" + seed + "-source")
	sourcePayload, _ := model.NewJSON([]byte(fmt.Sprintf(
		`{"content":"review the settlement fixture","deadline":%q,"iteration":1,"work_version":1}`,
		deadline.Format(time.RFC3339Nano))))
	sourceLocal := peer.signEvent(t, model.EventSpec{ID: sourceID, Scope: sourceScope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-settlement-home",
		Type: model.EventReviewOffered, Audience: audience, Summary: "settlement source",
		Payload: sourcePayload, CreatedAt: sourceAt, AcceptedAt: sourceAt})
	source, err := model.ProjectImportedPublication(&sourceLocal)
	if err != nil {
		t.Fatal(err)
	}
	participants, err := model.NewParticipantSnapshot(peer.channel.Channel().ID(),
		peer.channel.Roster().Head().Revision(), remote.PeerID(), local.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: workRef,
		ChannelID: peer.channel.Channel().ID(), Participants: participants, Version: 1, Iteration: 1,
		DeadlineUnixNano: deadline.UnixNano(), State: model.WorkOffered, StateData: source.Event().Payload(),
		UpdatedBy: source.Event().ID(), UpdatedAt: sourceAt})
	if err != nil {
		t.Fatal(err)
	}
	creation, _ := NewWorkCreation(work)
	installTx := beginPeerInboxSemanticSettlementTx(t, peer.store)
	if err := insertAcceptedEvent(context.Background(), installTx, source); err != nil {
		t.Fatal(err)
	}
	if err := applyWorkMutation(context.Background(), installTx, creation, source.Event()); err != nil {
		t.Fatal(err)
	}
	handlingID, err := peerInboxSemanticHandlingID(source.Event().ID())
	if err != nil {
		t.Fatal(err)
	}
	handling, err := model.NewHandling(model.HandlingSpec{ID: handlingID,
		ProfileID: model.TeamworkProfileID(), EventID: source.Event().ID(), Status: model.HandlingPending,
		AvailableAt: sourceAt, CreatedAt: sourceAt, UpdatedAt: sourceAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insertAgentHandling(context.Background(), installTx, handling); err != nil {
		t.Fatal(err)
	}
	if err := installTx.Commit(); err != nil {
		t.Fatal(err)
	}

	settleAt := sourceAt.Add(2 * time.Second)
	terminalPayload := fmt.Sprintf(`{"content":"remote cancellation","iteration":1,"work_version":1}`)
	if terminalType == model.EventReviewExpired {
		settleAt = deadline
		terminalPayload = fmt.Sprintf(`{"deadline":%q,"iteration":1,"work_version":1}`,
			deadline.Format(time.RFC3339Nano))
	}
	terminalScope, err := model.NewEventScope(peer.channel.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), 2, 2, peer.remote.Member().Head(), peer.channel.Roster().Head(), workRef)
	if err != nil {
		t.Fatal(err)
	}
	terminalID, _ := model.ParseEventID("event-semantic-settlement-" + seed + "-terminal")
	payload, _ := model.NewJSON([]byte(terminalPayload))
	terminalLocal := peer.signEvent(t, model.EventSpec{ID: terminalID, Scope: terminalScope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-settlement-home",
		Type: terminalType, Audience: audience, Summary: "settlement terminal", Payload: payload,
		CausedBy: []model.EventKey{source.Event().Key()}, CreatedAt: settleAt, AcceptedAt: settleAt})
	terminal, err := model.ProjectImportedPublication(&terminalLocal)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := teamwork.NewImportEventFact(source.Event())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := teamwork.PlanImportedEvent(teamwork.ImportPlanSpec{LocalPeerID: local.PeerID(),
		Event: terminal.Event(), Current: &work, Facts: []teamwork.ImportEventFact{fact}, Now: settleAt})
	if err != nil {
		t.Fatal(err)
	}
	storeSettlement := peerInboxSemanticStoreSettlement(t, plan)
	return &peerInboxSemanticHandlingSettlementFixture{store: peer.store, peer: peer,
		profile: profile, source: source.Event(), terminalWire: terminalLocal, terminal: terminal, work: work,
		handling: handling, settlement: storeSettlement, settleAt: settleAt}
}

func claimPeerInboxSemanticSettlementHandling(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture,
) (AgentClaimResult, model.Digest) {
	t.Helper()
	claimAt := fixture.source.AcceptedAt().Add(250 * time.Millisecond)
	token := model.Sum([]byte("claim-token-" + fixture.handling.ID().String()))
	claim, err := fixture.store.ClaimAgentCurrent(context.Background(), AgentClaimSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		ClaimOwner: "claim-owner-" + fixture.handling.ID().String(), ClaimTokenHash: token,
		At: claimAt, LeaseUntil: claimAt.Add(5 * time.Minute)})
	if err != nil || claim.Status != AgentClaimActionable {
		t.Fatalf("ClaimAgentCurrent() = (%#v,%v)", claim, err)
	}
	return claim, token
}

func newPeerInboxSemanticSettlementOperation(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture, claim AgentClaimResult,
	contextHash model.Digest, suffix string,
) model.Operation {
	t.Helper()
	operationID, _ := model.ParseOperationID("operation-semantic-settlement-" + suffix)
	createdAt := claim.Run.StartedAt().Add(750 * time.Millisecond)
	lease := fixture.work.UpdatedAt().Add(40 * time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID,
		ProfileID: fixture.profile.ID(), AgentRunID: claim.Run.ID(),
		ClientKeyHash: model.Sum([]byte("operation-key-" + suffix)), ContextHash: &contextHash,
		Kind: model.OperationTeamworkAccept, RequestDigest: model.Sum([]byte("operation-request-" + suffix)),
		Status: model.OperationStarted, LeaseOwner: "operation-owner-" + suffix,
		LeaseUntil: &lease, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func reservePeerInboxSemanticSettlementOperation(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture, claim AgentClaimResult,
	contextHash model.Digest, suffix string,
) model.Operation {
	t.Helper()
	operation := newPeerInboxSemanticSettlementOperation(t, fixture, claim, contextHash, suffix)
	reservation, err := fixture.store.ReserveOperation(context.Background(), operation,
		operation.CreatedAt())
	if err != nil || !reservation.Acquired {
		t.Fatalf("ReserveOperation() = (%#v,%v)", reservation, err)
	}
	return operation
}

func insertPeerInboxSemanticSettlementUnrelatedOperation(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture, suffix string,
) (model.AgentRun, model.Operation) {
	t.Helper()
	runID, _ := model.ParseRunID("run-semantic-settlement-" + suffix)
	startedAt := fixture.source.AcceptedAt().Add(-time.Second)
	empty, _ := model.NewJSON([]byte(`{}`))
	run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: fixture.profile.ID(),
		Cause: empty, Launcher: "external", Runtime: fixture.profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunRunning, StartedAt: startedAt})
	if err != nil {
		t.Fatal(err)
	}
	tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
	if err := insertAgentRun(context.Background(), tx, run); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	operationID, _ := model.ParseOperationID("operation-semantic-settlement-" + suffix)
	lease := fixture.work.UpdatedAt().Add(time.Hour)
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID,
		ProfileID: fixture.profile.ID(), AgentRunID: run.ID(),
		ClientKeyHash: model.Sum([]byte("operation-key-" + suffix)),
		Kind:          model.OperationTeamworkOffer, RequestDigest: model.Sum([]byte("operation-request-" + suffix)),
		Status: model.OperationStarted, LeaseOwner: "operation-owner-" + suffix,
		LeaseUntil: &lease, CreatedAt: startedAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(context.Background(), operation,
		operation.CreatedAt()); err != nil {
		t.Fatal(err)
	}
	return run, operation
}

func installPeerInboxSemanticSettlementCurrent(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture, claim AgentClaimResult,
	token model.Digest, readAt time.Time,
) {
	t.Helper()
	currentEvent, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: fixture.source.Key(),
		Digest: fixture.source.Digest(), Type: fixture.source.Type(), WorkRef: fixture.work.Ref(),
		Summary: fixture.source.Summary(), Payload: fixture.source.Payload(),
		AcceptedAt: fixture.source.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: fixture.work.Ref(),
		Version: fixture.work.Version(), Iteration: fixture.work.Iteration(),
		DeadlineUnixNano: fixture.work.DeadlineUnixNano(), State: fixture.work.State(),
		StateData: fixture.work.StateData(), LocalRole: model.CurrentReviewer})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: currentEvent,
		ActionWork: currentWork, AllowedActions: []model.OperationKind{
			model.OperationTeamworkAccept, model.OperationResolveNoAction}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: claim.Run.ID(),
		ProfileID: claim.Run.ProfileID(), HandlingID: claim.Handling.ID(),
		HandlingAttempt: claim.Handling.Attempts(), Projection: projection, ReadAt: readAt})
	if err != nil {
		t.Fatal(err)
	}
	tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
	if err := writeCurrentReadEvidence(context.Background(), tx, claim.Run, claim.Handling,
		token, receipt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func advancePeerInboxSemanticSettlementWork(t *testing.T, tx *sql.Tx,
	fixture *peerInboxSemanticHandlingSettlementFixture,
) {
	t.Helper()
	if err := insertAcceptedEvent(context.Background(), tx, fixture.terminal); err != nil {
		t.Fatal(err)
	}
	nextState := model.WorkCancelled
	if fixture.settlement.Disposition() == "superseded_expired" {
		nextState = model.WorkExpired
	}
	next, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: fixture.work.Ref(),
		ChannelID: fixture.work.ChannelID(), Participants: fixture.work.Participants(),
		Version: fixture.work.Version() + 1, Iteration: fixture.work.Iteration(),
		DeadlineUnixNano: fixture.work.DeadlineUnixNano(), State: nextState,
		StateData: fixture.terminal.Event().Payload(), UpdatedBy: fixture.terminal.Event().ID(),
		UpdatedAt: fixture.terminal.Event().AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	transition, err := NewWorkTransition(next, fixture.work.Version(), fixture.work.State())
	if err != nil {
		t.Fatal(err)
	}
	if err := applyWorkMutation(context.Background(), tx, transition, fixture.terminal.Event()); err != nil {
		t.Fatal(err)
	}
}

func assertPeerInboxSemanticSettlementReceipt(t *testing.T, receipt model.JSON,
	fixture *peerInboxSemanticHandlingSettlementFixture, handling model.Handling,
	runID model.RunID, operationID model.OperationID, wantStatus string,
) {
	t.Helper()
	var envelope struct {
		Code          string `json:"code"`
		Disposition   string `json:"disposition"`
		HandlingID    string `json:"handling_id"`
		OperationID   string `json:"operation_id"`
		RunID         string `json:"run_id"`
		SchemaVersion int    `json:"schema_version"`
		SettledAt     string `json:"settled_at"`
		SourceEventID string `json:"source_event_id"`
		Status        string `json:"status"`
		WorkRef       struct {
			HomePeerID string `json:"home_peer_id"`
			WorkID     string `json:"work_id"`
		} `json:"work_ref"`
	}
	if err := json.Unmarshal(receipt.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	wantCode := ""
	if !operationID.IsZero() {
		wantCode = peerInboxSemanticHandlingOperationCode
	}
	if envelope.Code != wantCode ||
		envelope.Disposition != string(fixture.settlement.Disposition()) ||
		envelope.HandlingID != handling.ID().String() ||
		envelope.OperationID != operationID.String() || envelope.RunID != runID.String() ||
		envelope.SchemaVersion != model.SchemaVersion || envelope.SettledAt != storeTime(fixture.settleAt) ||
		envelope.SourceEventID != fixture.source.ID().String() || envelope.Status != wantStatus ||
		envelope.WorkRef.HomePeerID != fixture.work.Ref().HomePeerID().String() ||
		envelope.WorkRef.WorkID != fixture.work.Ref().WorkID().String() {
		t.Fatalf("settlement receipt = %#v", envelope)
	}
}

func assertPeerInboxSemanticSettlementRollback(t *testing.T,
	fixture *peerInboxSemanticHandlingSettlementFixture, call func(*sql.Tx) error,
) {
	t.Helper()
	tx := beginPeerInboxSemanticSettlementTx(t, fixture.store)
	advancePeerInboxSemanticSettlementWork(t, tx, fixture)
	err := call(tx)
	_ = tx.Rollback()
	if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("fail-closed settlement error = %v", err)
	}
	handling, readErr := readAgentHandling(context.Background(), fixture.store.db,
		fixture.handling.ID())
	if readErr != nil || handling.Status() != model.HandlingClaimed {
		t.Fatalf("failed settlement Handling = (%#v,%v)", handling, readErr)
	}
	work, readErr := readReviewWork(context.Background(), fixture.store.db, fixture.work.Ref())
	if readErr != nil || work.State() != model.WorkOffered || work.UpdatedBy() != fixture.source.ID() {
		t.Fatalf("failed settlement Work = (%#v,%v)", work, readErr)
	}
}

func beginPeerInboxSemanticSettlementTx(t *testing.T, store *Store) *sql.Tx {
	t.Helper()
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func peerInboxSemanticAgentRunOutcome(run model.AgentRun) string {
	value, _ := run.OutcomeReceipt()
	return value.String()
}

func peerInboxSemanticOperationResult(operation model.Operation) string {
	value, _ := operation.Result()
	return value.String()
}
