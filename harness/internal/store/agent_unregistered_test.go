package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAbandonUnregisteredAgentRunRequeuesWithoutProcessCompletion(t *testing.T) {
	t.Parallel()
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-requeue", 0)
	before, err := fixture.store.ListIncompleteManagedAgentRuns(context.Background())
	if err != nil || len(before) != 1 || before[0].ID() != claim.Run.ID() {
		t.Fatalf("incomplete Runs before abandonment = (%#v, %v)", before, err)
	}
	settledAt := claimAt.Add(time.Second)
	spec := unregisteredRunSpec(fixture, claim, token,
		"managed Runtime launch was not registered", settledAt)
	result, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), spec)
	if err != nil || result.Status != AgentRuntimeApplied ||
		result.Run.Status() != model.AgentRunFailed || result.Run.Error() != spec.Error ||
		result.Handling.Status() != model.HandlingPending ||
		result.Handling.LastDisposition() != "runtime_unregistered" ||
		result.Handling.LastError() != spec.Error {
		t.Fatalf("AbandonUnregisteredAgentRun() = (%#v, %v)", result, err)
	}
	finishedAt, finished := result.Run.FinishedAt()
	_, runtimeStarted := result.Run.RuntimeStartedAt()
	_, completionAt := result.Run.CompletionAt()
	_, completion := result.Run.CompletionReceipt()
	if !finished || !finishedAt.Equal(settledAt) || runtimeStarted || completionAt || completion ||
		result.Run.LauncherDiagnostic().String() != `{}` || result.Run.RuntimeIDs().String() != `{}` {
		t.Fatalf("unregistered Run evidence = %#v", result.Run)
	}
	wantAvailable := settledAt.Add(time.Duration(model.DefaultRetryInitialSeconds) * time.Second)
	if !result.Handling.AvailableAt().Equal(wantAvailable) {
		t.Fatalf("retry time = %s, want %s", result.Handling.AvailableAt(), wantAvailable)
	}
	after, err := fixture.store.ListIncompleteManagedAgentRuns(context.Background())
	if err != nil || len(after) != 0 {
		t.Fatalf("incomplete Runs after abandonment = (%#v, %v)", after, err)
	}
	reapable, err := fixture.store.ListReapableAgentRunAttachments(context.Background(),
		AgentAttachmentCleanupSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: settledAt,
			Candidates: []AgentRunAttachmentCandidate{{RunID: claim.Run.ID(), TokenHash: token}}})
	if err != nil || len(reapable) != 1 || reapable[0].RunID != claim.Run.ID() {
		t.Fatalf("reapable abandoned attachment = (%#v, %v)", reapable, err)
	}
	var runBusy int
	if err := fixture.store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM agent_runs WHERE run_id=? AND (
		status IN ('starting','running','runtime_finished') OR
		(launcher='mnemond-wake' AND runtime_started_at IS NOT NULL
			AND completion_receipt_json IS NULL)))`, claim.Run.ID().String()).Scan(&runBusy); err != nil || runBusy != 0 {
		t.Fatalf("terminal unregistered Run busy predicate = (%d, %v)", runBusy, err)
	}

	replay := spec
	replay.At = settledAt.Add(time.Minute)
	replayed, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), replay)
	if err != nil || replayed.Status != AgentRuntimeReplayed ||
		!replayed.Handling.AvailableAt().Equal(wantAvailable) {
		t.Fatalf("abandonment replay = (%#v, %v)", replayed, err)
	}
	replay.Error = "changed unregistered launch diagnostic"
	if _, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), replay); !errors.Is(err,
		ErrAgentUnregisteredRunInvariant) {
		t.Fatalf("changed abandonment replay error = %v", err)
	}
}

func TestAbandonUnregisteredAgentRunAllowsDeadRecoveryWithoutFalseCompletion(t *testing.T) {
	t.Parallel()
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-dead", 1)
	settledAt := claimAt.Add(time.Second)
	result, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(),
		unregisteredRunSpec(fixture, claim, token,
			"managed Runtime launch was not registered", settledAt))
	if err != nil || result.Run.Status() != model.AgentRunDead ||
		result.Handling.Status() != model.HandlingDead ||
		result.Handling.LastDisposition() != "attempt_budget_exhausted" {
		t.Fatalf("final unregistered attempt = (%#v, %v)", result, err)
	}
	if _, complete := result.Run.CompletionReceipt(); complete {
		t.Fatal("unregistered final attempt fabricated Runtime completion")
	}
	authority, err := fixture.store.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(),
		AgentDeadRecoverySpec{Profile: authority.Profile, At: settledAt.Add(time.Second)})
	if err != nil || recovered.Recovered != 1 {
		t.Fatalf("RecoverDeadAgentHandlings() = (%#v, %v)", recovered, err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingPending || handling.RecoveryCount() != 1 ||
		handling.Attempts() != 0 {
		t.Fatalf("recovered unregistered Handling = (%#v, %v)", handling, err)
	}
}

func TestAbandonUnregisteredAgentRunReplayPreservesAdvancedHandling(t *testing.T) {
	t.Parallel()
	t.Run("next attempt claimed", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-replay-claimed", 0)
		settledAt := claimAt.Add(time.Second)
		spec := unregisteredRunSpec(fixture, claim, token,
			"managed Runtime launch was not registered", settledAt)
		settled, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		nextToken := model.Sum([]byte("runtime-token-unregistered-replay-claimed-next"))
		next := preclaimWake(t, fixture, nextToken, settled.Handling.AvailableAt())
		before, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
		if err != nil {
			t.Fatal(err)
		}
		replay := spec
		replay.At = next.Run.StartedAt().Add(time.Second)
		replayed, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), replay)
		if err != nil || replayed.Status != AgentRuntimeReplayed ||
			replayed.Run.ID() != claim.Run.ID() || !reflect.DeepEqual(replayed.Handling, before) {
			t.Fatalf("advanced abandonment replay = (%#v, %v), winner %#v", replayed, err, before)
		}
		after, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
		if err != nil || !reflect.DeepEqual(after, before) {
			t.Fatalf("advanced replay changed Handling = (%#v, %v), want %#v", after, err, before)
		}
	})

	t.Run("dead generation recovered", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-replay-recovered", 1)
		settledAt := claimAt.Add(time.Second)
		spec := unregisteredRunSpec(fixture, claim, token,
			"managed Runtime launch was not registered", settledAt)
		if _, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
		authority, err := fixture.store.ReadLocalAuthority(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		recoveryAt := settledAt.Add(time.Second)
		if recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(),
			AgentDeadRecoverySpec{Profile: authority.Profile, At: recoveryAt}); err != nil || recovered.Recovered != 1 {
			t.Fatalf("RecoverDeadAgentHandlings() = (%#v, %v)", recovered, err)
		}
		before, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
		if err != nil {
			t.Fatal(err)
		}
		replay := spec
		replay.At = recoveryAt.Add(time.Second)
		replayed, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), replay)
		if err != nil || replayed.Status != AgentRuntimeReplayed ||
			replayed.Run.ID() != claim.Run.ID() || !reflect.DeepEqual(replayed.Handling, before) {
			t.Fatalf("recovered abandonment replay = (%#v, %v), winner %#v", replayed, err, before)
		}
		after, err := fixture.store.GetAgentHandling(context.Background(), claim.Handling.ID())
		if err != nil || !reflect.DeepEqual(after, before) {
			t.Fatalf("recovered replay changed Handling = (%#v, %v), want %#v", after, err, before)
		}
	})
}

func TestAbandonUnregisteredAgentRunAndLateLaunchAreExactlyOrdered(t *testing.T) {
	t.Parallel()
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex"}`)
	runtimeIDs := runtimeTestJSON(t, `{"pid":4402}`)

	t.Run("launch wins", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-order-launch", 0)
		launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			claimAt.Add(500*time.Millisecond))
		launched, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch)
		if err != nil || launched.Status != AgentRuntimeApplied ||
			launched.Run.Status() != model.AgentRunRunning {
			t.Fatalf("RecordAgentRuntimeLaunch() = (%#v, %v)", launched, err)
		}
		if _, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(),
			unregisteredRunSpec(fixture, claim, token,
				"managed Runtime launch was not registered", claimAt.Add(time.Second))); !errors.Is(err,
			ErrAgentUnregisteredRunInvariant) {
			t.Fatalf("abandonment after launch error = %v", err)
		}
	})

	t.Run("abandonment wins but preserves late identity", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-order-abandon", 0)
		settledAt := claimAt.Add(time.Second)
		settled, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(),
			unregisteredRunSpec(fixture, claim, token,
				"managed Runtime launch was not registered", settledAt))
		if err != nil {
			t.Fatal(err)
		}
		before := settled.Handling
		launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			claimAt.Add(500*time.Millisecond))
		late, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch)
		if err != nil || late.Status != AgentRuntimeAlreadySettled ||
			late.Run.Status() != model.AgentRunFailed || !reflect.DeepEqual(late.Handling, before) {
			t.Fatalf("late launch after abandonment = (%#v, %v), Handling %#v", late, err, before)
		}
		startedAt, started := late.Run.RuntimeStartedAt()
		if !started || !startedAt.Equal(launch.At) ||
			late.Run.LauncherDiagnostic().String() != diagnostic.String() ||
			late.Run.RuntimeIDs().String() != runtimeIDs.String() {
			t.Fatalf("late launch identity = %#v", late.Run)
		}
		if _, complete := late.Run.CompletionReceipt(); complete {
			t.Fatal("late launch fabricated Runtime completion")
		}
		replayed, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch)
		if err != nil || replayed.Status != AgentRuntimeAlreadySettled ||
			replayed.Run.Status() != model.AgentRunFailed || !reflect.DeepEqual(replayed.Handling, before) {
			t.Fatalf("repeated late launch after abandonment = (%#v, %v), Handling %#v",
				replayed, err, before)
		}
	})
}

func TestAbandonUnregisteredAgentRunRejectsLaunchedPartialAndStaleAuthority(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*acceptanceFixture, AgentClaimResult, model.Digest, time.Time)
		edit   func(*AgentUnregisteredRunSpec)
		want   error
	}{
		{name: "launched", mutate: func(fixture *acceptanceFixture, claim AgentClaimResult,
			token model.Digest, at time.Time,
		) {
			_, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), runtimeLaunchSpec(fixture,
				claim, token, runtimeTestJSON(t, `{"adapter":"codex"}`),
				runtimeTestJSON(t, `{"pid":4401}`), at.Add(500*time.Millisecond)))
			if err != nil {
				t.Fatal(err)
			}
		}, want: ErrAgentUnregisteredRunInvariant},
		{name: "partial launch evidence", mutate: func(fixture *acceptanceFixture, claim AgentClaimResult,
			_ model.Digest, _ time.Time,
		) {
			if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET launcher_diagnostic_json=? WHERE run_id=?`,
				[]byte(`{"adapter":"codex"}`), claim.Run.ID().String()); err != nil {
				t.Fatal(err)
			}
		}, want: ErrAgentUnregisteredRunInvariant},
		{name: "wrong fence", edit: func(spec *AgentUnregisteredRunSpec) {
			spec.ClaimFenceHash = model.Sum([]byte("wrong-unregistered-fence"))
		}, want: ErrAgentUnregisteredRunStale},
		{name: "trusted time before Run", edit: func(spec *AgentUnregisteredRunSpec) {
			spec.At = spec.At.Add(-2 * time.Second)
		}, want: ErrAgentUnregisteredRunStale},
	} {
		t.Run(test.name, func(t *testing.T) {
			suffix := strings.ReplaceAll(test.name, " ", "-")
			fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "unregistered-"+suffix, 0)
			if test.mutate != nil {
				test.mutate(fixture, claim, token, claimAt)
			}
			spec := unregisteredRunSpec(fixture, claim, token,
				"managed Runtime launch was not registered", claimAt.Add(time.Second))
			if test.edit != nil {
				test.edit(&spec)
			}
			if _, err := fixture.store.AbandonUnregisteredAgentRun(context.Background(), spec); !errors.Is(err, test.want) {
				t.Fatalf("AbandonUnregisteredAgentRun() error = %v, want %v", err, test.want)
			}
		})
	}
}

func unregisteredRunSpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	message string, at time.Time,
) AgentUnregisteredRunSpec {
	return AgentUnregisteredRunSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(), Error: message, At: at}
}
