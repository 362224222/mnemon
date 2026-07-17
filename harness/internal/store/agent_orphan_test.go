package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestListIncompleteManagedAgentRunsIsBoundedStableAndIndexed(t *testing.T) {
	t.Run("stable complete workset", func(t *testing.T) {
		fixture, first, firstToken, claimAt := newWakeRuntimeFixture(t, "orphan-list", 0)
		diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
		firstIDs := runtimeTestJSON(t, `{"pgid":4101,"pid":4101,"start_token":"first"}`)
		if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
			runtimeLaunchSpec(fixture, first, firstToken, diagnostic, firstIDs,
				claimAt.Add(time.Second))); err != nil {
			t.Fatal(err)
		}
		lease, _ := first.Run.LeaseUntil()
		if _, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
			ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
			At: lease}); err != nil {
			t.Fatal(err)
		}
		firstHandling, err := fixture.store.GetAgentHandling(context.Background(), first.Handling.ID())
		if err != nil {
			t.Fatal(err)
		}
		secondAt := firstHandling.AvailableAt()
		secondToken := model.Sum([]byte("runtime-token-orphan-list-second"))
		second := preclaimWake(t, fixture, secondToken, secondAt)
		secondIDs := runtimeTestJSON(t, `{"pgid":4102,"pid":4102,"start_token":"second"}`)
		if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
			runtimeLaunchSpec(fixture, second, secondToken, diagnostic, secondIDs,
				secondAt.Add(time.Second))); err != nil {
			t.Fatal(err)
		}

		runs, err := fixture.store.ListIncompleteManagedAgentRuns(context.Background())
		if err != nil || len(runs) != 2 || runs[0].ID() != first.Run.ID() || runs[1].ID() != second.Run.ID() {
			t.Fatalf("ListIncompleteManagedAgentRuns() = (%#v, %v)", runs, err)
		}
		for _, run := range runs {
			if run.Launcher() != "mnemond-wake" {
				t.Fatalf("listed launcher = %q", run.Launcher())
			}
			if _, complete := run.CompletionReceipt(); complete {
				t.Fatalf("listed completed Run %s", run.ID())
			}
		}
		var plan string
		rows, err := fixture.store.db.Query(`EXPLAIN QUERY PLAN SELECT run_id FROM agent_runs
			WHERE launcher='mnemond-wake' AND completion_receipt_json IS NULL
			AND (status IN ('starting','running') OR runtime_started_at IS NOT NULL
				OR launcher_diagnostic_json<>X'7B7D' OR runtime_ids_json<>X'7B7D'
				OR attached_at IS NOT NULL OR wake_delivered_at IS NOT NULL
				OR wake_receipt_json IS NOT NULL OR current_read_receipt_json IS NOT NULL
				OR outcome_receipt_json IS NOT NULL)
			ORDER BY started_at,run_id LIMIT ?`, MaxIncompleteManagedAgentRuns+1)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan += detail
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, "agent_runs_incomplete_managed_idx") {
			t.Fatalf("incomplete Runtime query plan = %q", plan)
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
		restartedRuns, err := restarted.ListIncompleteManagedAgentRuns(context.Background())
		if err != nil || len(restartedRuns) != 2 || restartedRuns[0].ID() != first.Run.ID() ||
			restartedRuns[1].ID() != second.Run.ID() {
			t.Fatalf("restart list = (%#v, %v)", restartedRuns, err)
		}
	})

	t.Run("overflow fails closed", func(t *testing.T) {
		fixture, _ := newAgentClaimFixture(t, 1, "orphan-overflow")
		startedAt := fixture.now.Add(time.Minute)
		tx, err := fixture.store.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for index := 0; index <= MaxIncompleteManagedAgentRuns; index++ {
			_, err := tx.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
				launcher_diagnostic_json,runtime_ids_json,status,started_at)
				VALUES(?,?,'{}','mnemond-wake',?,'{}','{}','starting',?)`,
				fmt.Sprintf("run-orphan-overflow-%03d", index), fixture.profile.ID().String(),
				string(fixture.profile.Runtime()), storeTime(startedAt))
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if runs, err := fixture.store.ListIncompleteManagedAgentRuns(context.Background()); !errors.Is(err, ErrAgentRuntimeOrphanInvariant) || runs != nil {
			t.Fatalf("overflow list = (%#v, %v)", runs, err)
		}
	})
}

func TestSettleOrphanedAgentRuntimeFailsExactActiveClaim(t *testing.T) {
	fixture := newManagedWakeRuntimeFixture(t, "orphan-active", true,
		model.OperationResolveNoAction, "orphaned before resolution")
	settledAt := fixture.reserveSpec.At.Add(time.Second)
	receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed"}`)
	spec := orphanSpec(fixture.acceptanceFixture, fixture.claim, fixture.token, receipt,
		"managed Runtime disappeared during daemon restart", settledAt)

	result, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), spec)
	if err != nil || result.Status != AgentRuntimeApplied || result.Run.Status() != model.AgentRunFailed ||
		result.Handling.Status() != model.HandlingPending ||
		result.Handling.LastDisposition() != "runtime_orphaned" ||
		result.Handling.LastError() != spec.Error {
		t.Fatalf("SettleOrphanedAgentRuntime() = (%#v, %v)", result, err)
	}
	finishedAt, hasFinished := result.Run.FinishedAt()
	completionAt, hasCompletion := result.Run.CompletionAt()
	completion, hasReceipt := result.Run.CompletionReceipt()
	if !hasFinished || !hasCompletion || !hasReceipt || !finishedAt.Equal(settledAt) ||
		!completionAt.Equal(settledAt) || completion.String() != receipt.String() ||
		result.Run.Error() != spec.Error {
		t.Fatalf("orphan completion evidence = Run %#v", result.Run)
	}
	wantAvailable := settledAt.Add(time.Duration(model.DefaultRetryInitialSeconds) * time.Second)
	if !result.Handling.AvailableAt().Equal(wantAvailable) {
		t.Fatalf("orphan retry = %s, want %s", result.Handling.AvailableAt(), wantAvailable)
	}
	operation, err := readOperationByID(context.Background(), fixture.store.db,
		fixture.reservation.Operation.ID())
	if err != nil || operation.Status() != model.OperationRejected {
		t.Fatalf("orphaned Operation = (%#v, %v)", operation, err)
	}
	operationReceipt, ok := operation.Result()
	if !ok || operationReceipt.String() != `{"code":"agent_runtime_orphaned","status":"rejected"}` {
		t.Fatalf("orphaned Operation receipt = (%s, %v)", operationReceipt, ok)
	}

	replay := spec
	replay.At = settledAt.Add(time.Minute)
	replayed, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), replay)
	if err != nil || replayed.Status != AgentRuntimeReplayed ||
		!replayed.Handling.AvailableAt().Equal(wantAvailable) {
		t.Fatalf("orphan replay = (%#v, %v)", replayed, err)
	}
	mismatch := replay
	mismatch.CompletionReceipt = runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"different"}`)
	if _, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), mismatch); !errors.Is(err, ErrAgentRuntimeOrphanInvariant) {
		t.Fatalf("changed orphan replay error = %v", err)
	}
}

func TestSettleOrphanedAgentRuntimeUsesActiveAttemptBudgetAfterLeaseInstant(t *testing.T) {
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "orphan-dead", 1)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"pgid":4201,"pid":4201,"start_token":"dead"}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			claimAt.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	lease, _ := claim.Run.LeaseUntil()
	settledAt := lease.Add(time.Second)
	receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed"}`)
	result, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(),
		orphanSpec(fixture, claim, token, receipt, "orphaned final attempt", settledAt))
	if err != nil || result.Run.Status() != model.AgentRunDead ||
		result.Handling.Status() != model.HandlingDead ||
		result.Handling.LastDisposition() != "attempt_budget_exhausted" {
		t.Fatalf("expired-instant active orphan = (%#v, %v)", result, err)
	}
	deadAt, dead := result.Handling.DeadAt()
	if !dead || !deadAt.Equal(settledAt) {
		t.Fatalf("orphan death time = (%s, %v)", deadAt, dead)
	}
}

func TestSettleOrphanedAgentRuntimeAppendsAfterLeaseSettlement(t *testing.T) {
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "orphan-lease", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"pgid":4301,"pid":4301,"start_token":"lease"}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			claimAt.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	lease, _ := claim.Run.LeaseUntil()
	if _, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		At: lease}); err != nil {
		t.Fatal(err)
	}
	beforeRun, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	beforeHandling, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
	if err != nil {
		t.Fatal(err)
	}
	finishedAt, _ := beforeRun.FinishedAt()
	receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed_after_lease"}`)
	spec := orphanSpec(fixture, claim, token, receipt,
		"managed Runtime survived beyond its claim lease", lease.Add(time.Second))
	result, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), spec)
	if err != nil || result.Status != AgentRuntimeApplied || result.Run.Status() != model.AgentRunRequeued ||
		result.Run.Error() != spec.Error || !reflect.DeepEqual(result.Handling, beforeHandling) {
		t.Fatalf("lease-settled orphan = (%#v, %v), before Handling %#v", result, err, beforeHandling)
	}
	gotFinishedAt, _ := result.Run.FinishedAt()
	if !gotFinishedAt.Equal(finishedAt) {
		t.Fatalf("lease settlement finish changed from %s to %s", finishedAt, gotFinishedAt)
	}
	completionAt, complete := result.Run.CompletionAt()
	if !complete || !completionAt.Equal(spec.At) {
		t.Fatalf("late orphan completion = (%s, %v)", completionAt, complete)
	}
	replay := spec
	replay.At = spec.At.Add(time.Minute)
	if replayed, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), replay); err != nil || replayed.Status != AgentRuntimeReplayed ||
		!reflect.DeepEqual(replayed.Handling, beforeHandling) {
		t.Fatalf("lease-settled replay = (%#v, %v)", replayed, err)
	}
}

func TestSettleOrphanedAgentRuntimePreservesSemanticWinner(t *testing.T) {
	tests := []struct {
		name    string
		kind    model.OperationKind
		content string
	}{
		{"no action", model.OperationResolveNoAction, "nothing further is required"},
		{"retry", model.OperationResolveRetry, "retry independently of Runtime exit"},
		{"reject", model.OperationResolveReject, "the request is not actionable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedWakeRuntimeFixture(t, "orphan-semantic-"+strings.ReplaceAll(test.name, " ", "-"),
				true, test.kind, test.content)
			resolvedAt := fixture.reserveSpec.At.Add(time.Second)
			resolved, err := fixture.store.CommitManagedResolution(context.Background(), ManagedResolutionSpec{
				Reservation: fixture.reservation, Content: fixture.content, At: resolvedAt})
			if err != nil {
				t.Fatal(err)
			}
			beforeRun, err := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
			if err != nil {
				t.Fatal(err)
			}
			beforeHandling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
			if err != nil {
				t.Fatal(err)
			}
			beforeFinished, _ := beforeRun.FinishedAt()
			beforeOutcome, hasOutcome := beforeRun.OutcomeReceipt()
			if !hasOutcome || beforeOutcome.String() != resolved.Receipt.String() {
				t.Fatalf("semantic outcome before orphan = (%s, %v)", beforeOutcome, hasOutcome)
			}
			receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed_after_outcome"}`)
			spec := orphanSpec(fixture.acceptanceFixture, fixture.claim, fixture.token, receipt,
				"managed Runtime exited after semantic resolution", resolvedAt.Add(time.Second))
			settled, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), spec)
			if err != nil || settled.Status != AgentRuntimeApplied ||
				settled.Run.Status() != beforeRun.Status() ||
				!reflect.DeepEqual(settled.Handling, beforeHandling) {
				t.Fatalf("semantic-settled orphan = (%#v, %v), before %#v", settled, err, beforeHandling)
			}
			afterFinished, _ := settled.Run.FinishedAt()
			afterOutcome, hasOutcome := settled.Run.OutcomeReceipt()
			if !afterFinished.Equal(beforeFinished) || !hasOutcome ||
				afterOutcome.String() != beforeOutcome.String() || settled.Run.Error() != spec.Error {
				t.Fatalf("semantic winner changed: Run %#v", settled.Run)
			}
			operation, err := readOperationByID(context.Background(), fixture.store.db,
				fixture.reservation.Operation.ID())
			operationResult, hasResult := operation.Result()
			if err != nil || operation.Status() != model.OperationCommitted || !hasResult ||
				operationResult.String() != resolved.Receipt.String() {
				t.Fatalf("semantic Operation changed = (%#v, %v)", operation, err)
			}
		})
	}
}

func TestSettleOrphanedAgentRuntimeRejectsAuthorityDriftWithoutMutation(t *testing.T) {
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "orphan-fence", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"pgid":4401,"pid":4401,"start_token":"fence"}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			claimAt.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	receipt := runtimeTestJSON(t, `{"kind":"startup_orphan","process_exit":"confirmed"}`)
	base := orphanSpec(fixture, claim, token, receipt, "orphan authority test",
		claimAt.Add(2*time.Second))
	tests := []struct {
		name string
		edit func(*AgentRuntimeOrphanSpec)
		want error
	}{
		{"asset revision", func(spec *AgentRuntimeOrphanSpec) { spec.ExpectedAssetRevision = "asset-other" }, ErrAgentRuntimeOrphanStale},
		{"Run ID", func(spec *AgentRuntimeOrphanSpec) {
			spec.RunID, _ = model.ParseRunID("run-orphan-other")
		}, ErrAgentRuntimeOrphanStale},
		{"claim fence", func(spec *AgentRuntimeOrphanSpec) {
			spec.ClaimFenceHash = model.Sum([]byte("other-fence"))
		}, ErrAgentRuntimeOrphanStale},
		{"recovery generation", func(spec *AgentRuntimeOrphanSpec) { spec.HandlingRecovery++ }, ErrAgentRuntimeOrphanStale},
		{"time before Run", func(spec *AgentRuntimeOrphanSpec) {
			spec.At = claim.Run.StartedAt().Add(-time.Nanosecond)
		}, ErrAgentRuntimeOrphanInvariant},
		{"empty receipt", func(spec *AgentRuntimeOrphanSpec) {
			spec.CompletionReceipt = runtimeTestJSON(t, `{}`)
		}, ErrAgentRuntimeOrphanInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			test.edit(&spec)
			if _, err := fixture.store.SettleOrphanedAgentRuntime(context.Background(), spec); !errors.Is(err, test.want) {
				t.Fatalf("authority error = %v, want %v", err, test.want)
			}
		})
	}
	run, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil || run.Status() != model.AgentRunRunning {
		t.Fatalf("authority failures changed Run = (%#v, %v)", run, err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingClaimed {
		t.Fatalf("authority failures changed Handling = (%#v, %v)", handling, err)
	}
}

func orphanSpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	receipt model.JSON, message string, at time.Time,
) AgentRuntimeOrphanSpec {
	return AgentRuntimeOrphanSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		CompletionReceipt: receipt, Error: message, At: at}
}
