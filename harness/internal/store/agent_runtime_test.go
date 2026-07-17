package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgentRuntimeWakeDeliveryIsExactAndReplayable(t *testing.T) {
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "wake-evidence", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-wake-evidence"}`)
	wakeReceipt := runtimeTestJSON(t, `{"hook_id":"hook-wake-evidence","thread_id":"thread-wake-evidence","turn_id":"turn-wake-evidence"}`)
	spec := wakeDeliverySpec(fixture, claim, token, wakeReceipt, claimAt.Add(time.Second))
	emptyWake := spec
	emptyWake.WakeReceipt = runtimeTestJSON(t, `{}`)
	if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(), emptyWake); !errors.Is(err,
		ErrAgentRuntimeInput) {
		t.Fatalf("empty wake receipt error = %v", err)
	}
	if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(), spec); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("wake before Runtime launch error = %v", err)
	}
	launchSpec := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
		claimAt.Add(500*time.Millisecond))
	emptyLaunch := launchSpec
	emptyLaunch.RuntimeIDs = runtimeTestJSON(t, `{}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), emptyLaunch); !errors.Is(err,
		ErrAgentRuntimeInput) {
		t.Fatalf("empty Runtime launch IDs error = %v", err)
	}
	launched, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launchSpec)
	if err != nil || launched.Status != AgentRuntimeApplied || launched.Run.Status() != model.AgentRunRunning {
		t.Fatalf("RecordAgentRuntimeLaunch() = (%#v, %v)", launched, err)
	}
	runtimeStartedAt, runtimeStarted := launched.Run.RuntimeStartedAt()
	if !runtimeStarted || !runtimeStartedAt.Equal(launchSpec.At) {
		t.Fatalf("Runtime start evidence = (%s, %v)", runtimeStartedAt, runtimeStarted)
	}
	launchReplay := launchSpec
	launchReplay.At = launchSpec.At.Add(time.Millisecond)
	if replay, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launchReplay); err != nil ||
		replay.Status != AgentRuntimeReplayed {
		t.Fatalf("launch replay = (%#v, %v)", replay, err)
	}
	launchMismatch := launchReplay
	launchMismatch.RuntimeIDs = runtimeTestJSON(t, `{"process":"other"}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launchMismatch); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("changed launch replay error = %v", err)
	}
	launchRegressed := launchSpec
	launchRegressed.At = launchSpec.At.Add(-time.Nanosecond)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launchRegressed); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("regressed launch replay error = %v", err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET runtime_ids_json=? WHERE run_id=?`,
		runtimeTestJSON(t, `{"process":"overwritten"}`).Bytes(), claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed launched Runtime IDs to be overwritten")
	}

	first, err := fixture.store.RecordAgentWakeDelivery(context.Background(), spec)
	if err != nil || first.Status != AgentRuntimeApplied {
		t.Fatalf("RecordAgentWakeDelivery() = (%#v, %v)", first, err)
	}
	deliveredAt, delivered := first.Run.WakeDeliveredAt()
	if !delivered || !deliveredAt.Equal(spec.At) ||
		first.Run.LauncherDiagnostic().String() != diagnostic.String() ||
		first.Run.RuntimeIDs().String() != runtimeIDs.String() {
		t.Fatalf("wake evidence = %#v", first.Run)
	}
	if receipt, ok := first.Run.WakeReceipt(); !ok || receipt.String() != wakeReceipt.String() {
		t.Fatalf("wake receipt = (%s, %v)", receipt, ok)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET runtime_started_at=? WHERE run_id=?`,
		storeTime(spec.At), claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed Runtime start evidence to be overwritten")
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET wake_receipt_json=? WHERE run_id=?`,
		runtimeTestJSON(t, `{"hook_id":"overwritten"}`).Bytes(), claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed wake receipt evidence to be overwritten")
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
	replaySpec.At = spec.At.Add(time.Second)
	launchReplay.At = launchSpec.At
	if replay, err := restarted.RecordAgentRuntimeLaunch(context.Background(), launchReplay); err != nil ||
		replay.Status != AgentRuntimeReplayed {
		t.Fatalf("restart launch replay = (%#v, %v)", replay, err)
	}
	replay, err := restarted.RecordAgentWakeDelivery(context.Background(), replaySpec)
	if err != nil || replay.Status != AgentRuntimeReplayed {
		t.Fatalf("restart wake replay = (%#v, %v)", replay, err)
	}
	replayedAt, _ := replay.Run.WakeDeliveredAt()
	if !replayedAt.Equal(deliveredAt) {
		t.Fatalf("wake replay timestamp = %s, want %s", replayedAt, deliveredAt)
	}

	mismatch := replaySpec
	mismatch.WakeReceipt = runtimeTestJSON(t, `{"hook_id":"different"}`)
	if _, err := restarted.RecordAgentWakeDelivery(context.Background(), mismatch); !errors.Is(err, ErrAgentRuntimeInvariant) {
		t.Fatalf("changed wake replay error = %v", err)
	}
	array := replaySpec
	array.WakeReceipt = runtimeTestJSON(t, `[]`)
	if _, err := restarted.RecordAgentWakeDelivery(context.Background(), array); !errors.Is(err, ErrAgentRuntimeInput) {
		t.Fatalf("array wake receipt error = %v", err)
	}

	external, events := newAgentClaimFixture(t, 1, "wake-external")
	externalAt := external.now.Add(time.Minute)
	insertClaimHandling(t, external.store, "handling-wake-external", events[0], 1,
		externalAt, externalAt, 0)
	externalClaim := claimCurrent(t, external, "owner-wake-external", "token-wake-external", externalAt)
	externalSpec := wakeDeliverySpec(external, externalClaim, model.Sum([]byte("token-wake-external")),
		wakeReceipt, externalAt.Add(time.Second))
	externalLaunch := runtimeLaunchSpec(external, externalClaim,
		model.Sum([]byte("token-wake-external")), diagnostic, runtimeIDs,
		externalAt.Add(500*time.Millisecond))
	if _, err := external.store.RecordAgentRuntimeLaunch(context.Background(), externalLaunch); !errors.Is(err,
		ErrAgentRuntimeStale) {
		t.Fatalf("external Runtime launch attribution error = %v", err)
	}
	if _, err := external.store.RecordAgentWakeDelivery(context.Background(), externalSpec); !errors.Is(err, ErrAgentRuntimeStale) {
		t.Fatalf("external wake attribution error = %v", err)
	}

	lateFixture, lateClaim, lateToken, _ := newWakeRuntimeFixture(t, "wake-after-expiry", 0)
	lease, _ := lateClaim.Run.LeaseUntil()
	if result, err := lateFixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(lateFixture, lateClaim, lateToken, diagnostic, runtimeIDs,
			lateClaim.Run.StartedAt().Add(500*time.Millisecond))); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("late fixture launch = (%#v, %v)", result, err)
	}
	if _, err := lateFixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: lateFixture.profile.ID(), ExpectedAssetRevision: lateFixture.profile.ActiveAssetRevision(),
		At: lease}); err != nil {
		t.Fatal(err)
	}
	completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"late_normal_exit"}`)
	finish := runtimeFinishSpec(lateFixture, lateClaim, lateToken, completion, lease.Add(time.Second))
	if _, err := lateFixture.store.FinishAgentRuntime(context.Background(), finish); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("late normal completion without wake error = %v", err)
	}
	lateSpec := wakeDeliverySpec(lateFixture, lateClaim, lateToken, wakeReceipt,
		lease.Add(-time.Second))
	late, err := lateFixture.store.RecordAgentWakeDelivery(context.Background(), lateSpec)
	if err != nil || late.Status != AgentRuntimeAlreadySettled || late.Run.Status() != model.AgentRunRequeued {
		t.Fatalf("wake after lease settlement = (%#v, %v)", late, err)
	}
	lateReplay := lateSpec
	lateReplay.At = lease
	if replay, err := lateFixture.store.RecordAgentWakeDelivery(context.Background(), lateReplay); err != nil ||
		replay.Status != AgentRuntimeAlreadySettled || replay.Run.Status() != model.AgentRunRequeued {
		t.Fatalf("wake replay after lease settlement = (%#v, %v)", replay, err)
	}
	completed, err := lateFixture.store.FinishAgentRuntime(context.Background(), finish)
	if err != nil || completed.Status != AgentRuntimeApplied ||
		completed.Run.Status() != model.AgentRunRequeued ||
		completed.Handling.Status() != model.HandlingPending || completed.Run.Error() != "" {
		t.Fatalf("late normal completion = (%#v, %v)", completed, err)
	}
	if completionAt, ok := completed.Run.CompletionAt(); !ok || !completionAt.Equal(finish.At) {
		t.Fatalf("late normal completion time = (%s, %v)", completionAt, ok)
	}
	finish.At = finish.At.Add(time.Second)
	if replay, err := lateFixture.store.FinishAgentRuntime(context.Background(), finish); err != nil ||
		replay.Status != AgentRuntimeReplayed {
		t.Fatalf("late normal completion replay = (%#v, %v)", replay, err)
	}
	authority, err := lateFixture.store.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lateFixture.store.db.Exec(`UPDATE agent_runs SET status='outcome_accepted',finished_at=?
		WHERE profile_id=? AND launcher='test' AND status='running'`, storeTime(finish.At),
		authority.Profile.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := lateFixture.store.DeactivateProfile(context.Background(), authority.Profile,
		finish.At.Add(time.Second)); err != nil {
		t.Fatalf("deactivate after late completion error = %v", err)
	}
}

func TestAgentRuntimeFinishThenLateStartedOutcomePreservesIndependentReceipts(t *testing.T) {
	fixture := newManagedWakeRuntimeFixture(t, "runtime-first", true,
		model.OperationResolveNoAction, "runtime completed before the accepted decision")
	completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"turn_finished"}`)
	finishAt := fixture.reserveSpec.At.Add(time.Second)
	finishSpec := runtimeFinishSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
		completion, finishAt)

	finished, err := fixture.store.FinishAgentRuntime(context.Background(), finishSpec)
	if err != nil || finished.Status != AgentRuntimeApplied ||
		finished.Run.Status() != model.AgentRunRuntimeFinished ||
		finished.Handling.Status() != model.HandlingClaimed {
		t.Fatalf("FinishAgentRuntime() = (%#v, %v)", finished, err)
	}
	if _, hasOutcome := finished.Run.OutcomeReceipt(); hasOutcome {
		t.Fatal("normal Runtime finish fabricated semantic outcome")
	}
	if got, ok := finished.Run.CompletionReceipt(); !ok || got.String() != completion.String() {
		t.Fatalf("Runtime completion = (%s, %v)", got, ok)
	}
	if got, ok := finished.Run.CompletionAt(); !ok || !got.Equal(finishAt) {
		t.Fatalf("Runtime completion time = (%s, %v)", got, ok)
	}

	fresh := fixture.reserveSpec
	fresh.ClientKeyHash = model.Sum([]byte("key-runtime-first-fresh"))
	fresh.LeaseOwner = "server-runtime-first-fresh"
	fresh.At = finishAt.Add(time.Second)
	fresh.LeaseUntil = fresh.At.Add(time.Minute)
	if _, err := fixture.store.ReserveManagedOperation(context.Background(), fresh); !errors.Is(err, ErrManagedContextStale) {
		t.Fatalf("fresh operation after runtime_finished error = %v", err)
	}

	replaySpec := fixture.reserveSpec
	replaySpec.At = finishAt.Add(time.Second)
	replaySpec.LeaseUntil = replaySpec.At.Add(time.Minute)
	replayedReservation, err := fixture.store.ReserveManagedOperation(context.Background(), replaySpec)
	if err != nil || !replayedReservation.Replayed || !replayedReservation.Acquired ||
		replayedReservation.Operation.ID() != fixture.reservation.Operation.ID() {
		t.Fatalf("started operation after runtime_finished = (%#v, %v)", replayedReservation, err)
	}

	resolveAt := finishAt.Add(2 * time.Second)
	resolved, err := fixture.store.CommitManagedResolution(context.Background(), ManagedResolutionSpec{
		Reservation: replayedReservation, Content: fixture.content, At: resolveAt})
	if err != nil || resolved.Replayed {
		t.Fatalf("late exact resolution = (%#v, %v)", resolved, err)
	}
	run, err := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
	if err != nil || run.Status() != model.AgentRunOutcomeAccepted {
		t.Fatalf("late outcome Run = (%#v, %v)", run, err)
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	storedCompletion, hasCompletion := run.CompletionReceipt()
	storedFinishedAt, _ := run.FinishedAt()
	if !hasOutcome || !hasCompletion || outcome.String() != resolved.Receipt.String() ||
		storedCompletion.String() != completion.String() || outcome.String() == storedCompletion.String() ||
		!storedFinishedAt.Equal(finishAt) {
		t.Fatalf("independent receipt evidence = outcome %s completion %s finished %s",
			outcome, storedCompletion, storedFinishedAt)
	}

	finishReplay := finishSpec
	finishReplay.At = resolveAt.Add(time.Second)
	if replay, err := fixture.store.FinishAgentRuntime(context.Background(), finishReplay); err != nil || replay.Status != AgentRuntimeReplayed {
		t.Fatalf("late finish replay = (%#v, %v)", replay, err)
	}
	regressedReplay := finishSpec
	regressedReplay.At = finishAt.Add(-time.Nanosecond)
	if _, err := fixture.store.FinishAgentRuntime(context.Background(), regressedReplay); !errors.Is(err, ErrAgentRuntimeInvariant) {
		t.Fatalf("regressed finish replay error = %v", err)
	}
}

func TestAgentRuntimeFinishedEvidenceSurvivesLaterClaimExpiry(t *testing.T) {
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "finish-before-expiry", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-finish-before-expiry"}`)
	wakeReceipt := runtimeTestJSON(t, `{"hook_id":"hook-finish-before-expiry","turn_id":"turn-finish-before-expiry"}`)
	launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
		claimAt.Add(250*time.Millisecond))
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	wake := wakeDeliverySpec(fixture, claim, token, wakeReceipt,
		claimAt.Add(500*time.Millisecond))
	if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil {
		t.Fatal(err)
	}
	receipt := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"normal_exit"}`)
	finishAt := claimAt.Add(time.Second)
	finish := runtimeFinishSpec(fixture, claim, token, receipt, finishAt)
	if result, err := fixture.store.FinishAgentRuntime(context.Background(), finish); err != nil ||
		result.Status != AgentRuntimeApplied || result.Run.Status() != model.AgentRunRuntimeFinished {
		t.Fatalf("finish before expiry = (%#v, %v)", result, err)
	}
	if replay, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch); err != nil ||
		replay.Status != AgentRuntimeAlreadySettled || replay.Run.Status() != model.AgentRunRuntimeFinished {
		t.Fatalf("launch replay after Runtime finish = (%#v, %v)", replay, err)
	}
	if replay, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil ||
		replay.Status != AgentRuntimeAlreadySettled || replay.Run.Status() != model.AgentRunRuntimeFinished {
		t.Fatalf("wake replay after Runtime finish = (%#v, %v)", replay, err)
	}
	lease, _ := claim.Run.LeaseUntil()
	if _, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		At: lease}); err != nil {
		t.Fatal(err)
	}
	run, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	completionAt, hasCompletionAt := run.CompletionAt()
	storedReceipt, hasReceipt := run.CompletionReceipt()
	if err != nil || run.Status() != model.AgentRunRequeued || run.Error() != "" ||
		!hasCompletionAt || !completionAt.Equal(finishAt) || !hasReceipt ||
		storedReceipt.String() != receipt.String() {
		t.Fatalf("expired finished Run = (%#v, %v)", run, err)
	}
	finish.At = lease.Add(time.Second)
	if replay, err := fixture.store.FinishAgentRuntime(context.Background(), finish); err != nil ||
		replay.Status != AgentRuntimeReplayed {
		t.Fatalf("finished Run replay after expiry = (%#v, %v)", replay, err)
	}
}

func TestAgentRuntimeLateLaunchSurvivesLeaseSettlement(t *testing.T) {
	fixture, claim, token, _ := newWakeRuntimeFixture(t, "launch-after-expiry", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-launch-after-expiry"}`)
	lease, _ := claim.Run.LeaseUntil()
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		At: lease,
	}); err != nil || status != AgentClaimWaiting {
		t.Fatalf("settle claim before launch callback = (%q, %v)", status, err)
	}
	launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs, lease.Add(-time.Second))
	late, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch)
	if err != nil || late.Status != AgentRuntimeAlreadySettled || late.Run.Status() != model.AgentRunRequeued ||
		late.Handling.Status() != model.HandlingPending {
		t.Fatalf("late RecordAgentRuntimeLaunch() = (%#v, %v)", late, err)
	}
	if startedAt, ok := late.Run.RuntimeStartedAt(); !ok || !startedAt.Equal(launch.At) {
		t.Fatalf("late Runtime start evidence = (%s, %v)", startedAt, ok)
	}
	if replay, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch); err != nil ||
		replay.Status != AgentRuntimeAlreadySettled {
		t.Fatalf("late launch replay = (%#v, %v)", replay, err)
	}
	mismatch := launch
	mismatch.RuntimeIDs = runtimeTestJSON(t, `{"process":"other"}`)
	if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), mismatch); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("late launch mismatch error = %v", err)
	}

	tooLateFixture, tooLateClaim, tooLateToken, _ := newWakeRuntimeFixture(t, "launch-too-late", 0)
	tooLateLease, _ := tooLateClaim.Run.LeaseUntil()
	if _, err := tooLateFixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID:             tooLateFixture.profile.ID(),
		ExpectedAssetRevision: tooLateFixture.profile.ActiveAssetRevision(), At: tooLateLease,
	}); err != nil {
		t.Fatal(err)
	}
	tooLate := runtimeLaunchSpec(tooLateFixture, tooLateClaim, tooLateToken, diagnostic, runtimeIDs,
		tooLateLease.Add(time.Nanosecond))
	if _, err := tooLateFixture.store.RecordAgentRuntimeLaunch(context.Background(), tooLate); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("launch after settled finish error = %v", err)
	}
}

func TestAgentRuntimeEvidenceAtomicallySettlesExpiredClaim(t *testing.T) {
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-runtime-lease-edge"}`)
	wakeReceipt := runtimeTestJSON(t, `{"hook_id":"hook-runtime-lease-edge","turn_id":"turn-runtime-lease-edge"}`)

	t.Run("launch at lease cannot regain authority", func(t *testing.T) {
		fixture, claim, token, _ := newWakeRuntimeFixture(t, "runtime-lease-launch", 0)
		lease, _ := claim.Run.LeaseUntil()
		launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs, lease)
		settled, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch)
		if err != nil || settled.Status != AgentRuntimeAlreadySettled ||
			settled.Run.Status() != model.AgentRunRequeued ||
			settled.Handling.Status() != model.HandlingPending ||
			settled.Handling.LastDisposition() != "lease_expired" {
			t.Fatalf("launch at expired lease = (%#v, %v)", settled, err)
		}
		startedAt, started := settled.Run.RuntimeStartedAt()
		finishedAt, finished := settled.Run.FinishedAt()
		if !started || !startedAt.Equal(lease) || !finished || !finishedAt.Equal(lease) ||
			settled.Run.LauncherDiagnostic().String() != diagnostic.String() ||
			settled.Run.RuntimeIDs().String() != runtimeIDs.String() {
			t.Fatalf("late launch evidence = %#v", settled.Run)
		}
		if _, completed := settled.Run.CompletionReceipt(); completed {
			t.Fatal("lease-edge launch fabricated Runtime completion")
		}
		wantRetry := lease.Add(time.Duration(model.DefaultRetryInitialSeconds) * time.Second)
		if !settled.Handling.AvailableAt().Equal(wantRetry) {
			t.Fatalf("lease-edge retry = %s, want %s", settled.Handling.AvailableAt(), wantRetry)
		}
	})

	t.Run("wake at lease records proof but stops authority", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "runtime-lease-wake", 0)
		if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
			runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
				claimAt.Add(250*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		lease, _ := claim.Run.LeaseUntil()
		wake := wakeDeliverySpec(fixture, claim, token, wakeReceipt, lease)
		settled, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake)
		if err != nil || settled.Status != AgentRuntimeAlreadySettled ||
			settled.Run.Status() != model.AgentRunRequeued ||
			settled.Handling.LastDisposition() != "lease_expired" {
			t.Fatalf("wake at expired lease = (%#v, %v)", settled, err)
		}
		wakeAt, delivered := settled.Run.WakeDeliveredAt()
		stored, hasReceipt := settled.Run.WakeReceipt()
		if !delivered || !wakeAt.Equal(lease) || !hasReceipt || stored.String() != wakeReceipt.String() {
			t.Fatalf("lease-edge wake evidence = %#v", settled.Run)
		}
	})

	t.Run("normal completion at lease preserves lease winner", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "runtime-lease-finish", 0)
		if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
			runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
				claimAt.Add(250*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(),
			wakeDeliverySpec(fixture, claim, token, wakeReceipt,
				claimAt.Add(500*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		lease, _ := claim.Run.LeaseUntil()
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"normal_exit"}`)
		finished, err := fixture.store.FinishAgentRuntime(context.Background(),
			runtimeFinishSpec(fixture, claim, token, completion, lease))
		if err != nil || finished.Status != AgentRuntimeApplied ||
			finished.Run.Status() != model.AgentRunRequeued || finished.Run.Error() != "" ||
			finished.Handling.Status() != model.HandlingPending ||
			finished.Handling.LastDisposition() != "lease_expired" ||
			finished.Handling.LastError() != "claim lease expired" {
			t.Fatalf("completion at expired lease = (%#v, %v)", finished, err)
		}
		completionAt, hasCompletion := finished.Run.CompletionAt()
		stored, hasReceipt := finished.Run.CompletionReceipt()
		if !hasCompletion || !completionAt.Equal(lease) || !hasReceipt ||
			stored.String() != completion.String() {
			t.Fatalf("lease-edge completion evidence = %#v", finished.Run)
		}
	})

	t.Run("completion response loss replay settles expired claim", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "runtime-lease-finish-replay", true,
			model.OperationResolveNoAction, "completion succeeds before its response is lost")
		lease, _ := fixture.claim.Run.LeaseUntil()
		finishAt := fixture.reserveSpec.At.Add(time.Second)
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"normal_exit"}`)
		finishSpec := runtimeFinishSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			completion, finishAt)
		finished, err := fixture.store.FinishAgentRuntime(context.Background(), finishSpec)
		if err != nil || finished.Status != AgentRuntimeApplied ||
			finished.Run.Status() != model.AgentRunRuntimeFinished ||
			finished.Handling.Status() != model.HandlingClaimed {
			t.Fatalf("initial completion before lease = (%#v, %v)", finished, err)
		}

		finishSpec.At = lease
		replayed, err := fixture.store.FinishAgentRuntime(context.Background(), finishSpec)
		if err != nil || replayed.Status != AgentRuntimeReplayed ||
			replayed.Run.Status() != model.AgentRunRequeued ||
			replayed.Handling.Status() != model.HandlingPending ||
			replayed.Handling.LastDisposition() != "lease_expired" ||
			replayed.Handling.LastError() != "claim lease expired" {
			t.Fatalf("completion replay at expired lease = (%#v, %v)", replayed, err)
		}
		completionAt, hasCompletion := replayed.Run.CompletionAt()
		stored, hasReceipt := replayed.Run.CompletionReceipt()
		if !hasCompletion || !completionAt.Equal(finishAt) || !hasReceipt ||
			stored.String() != completion.String() {
			t.Fatalf("completion replay changed receipt = %#v", replayed.Run)
		}
		wantRetry := lease.Add(time.Duration(model.DefaultRetryInitialSeconds) * time.Second)
		if !replayed.Handling.AvailableAt().Equal(wantRetry) {
			t.Fatalf("completion replay retry = %s, want lease-derived %s",
				replayed.Handling.AvailableAt(), wantRetry)
		}
		operation, err := readOperationByID(context.Background(), fixture.store.db,
			fixture.reservation.Operation.ID())
		if err != nil || operation.Status() != model.OperationRejected {
			t.Fatalf("completion replay Operation = (%#v, %v)", operation, err)
		}
		result, hasResult := operation.Result()
		if !hasResult || result.String() != `{"code":"claim_lease_expired","status":"rejected"}` {
			t.Fatalf("completion replay Operation receipt = (%s, %v)", result, hasResult)
		}
	})

	t.Run("failure after lease rejects operation without moving backoff", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "runtime-lease-failure", true,
			model.OperationResolveNoAction, "operation expires before Runtime failure")
		lease, _ := fixture.claim.Run.LeaseUntil()
		failureAt := lease.Add(time.Second)
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"provider_failed"}`)
		failure := runtimeFailureSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			fixture.diagnostic, fixture.runtimeIDs, completion, "provider failed after claim lease", failureAt)
		failed, err := fixture.store.FailAgentRuntime(context.Background(), failure)
		if err != nil || failed.Status != AgentRuntimeApplied ||
			failed.Run.Status() != model.AgentRunRequeued || failed.Run.Error() != failure.Error ||
			failed.Handling.Status() != model.HandlingPending ||
			failed.Handling.LastDisposition() != "lease_expired" ||
			failed.Handling.LastError() != "claim lease expired" {
			t.Fatalf("failure after expired lease = (%#v, %v)", failed, err)
		}
		wantRetry := lease.Add(time.Duration(model.DefaultRetryInitialSeconds) * time.Second)
		if !failed.Handling.AvailableAt().Equal(wantRetry) {
			t.Fatalf("late failure retry = %s, want lease-derived %s",
				failed.Handling.AvailableAt(), wantRetry)
		}
		operation, err := readOperationByID(context.Background(), fixture.store.db,
			fixture.reservation.Operation.ID())
		if err != nil || operation.Status() != model.OperationRejected {
			t.Fatalf("expired Runtime Operation = (%#v, %v)", operation, err)
		}
		result, hasResult := operation.Result()
		if !hasResult || result.String() != `{"code":"claim_lease_expired","status":"rejected"}` {
			t.Fatalf("expired Runtime Operation receipt = (%s, %v)", result, hasResult)
		}
		completionAt, completed := failed.Run.CompletionAt()
		if !completed || !completionAt.Equal(failureAt) {
			t.Fatalf("late failure completion = (%s, %v)", completionAt, completed)
		}
	})

	t.Run("final attempt completion is recoverable without restart", func(t *testing.T) {
		fixture, claim, token, claimAt := newWakeRuntimeFixture(t, "runtime-lease-dead", 1)
		if _, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
			runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
				claimAt.Add(250*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(),
			wakeDeliverySpec(fixture, claim, token, wakeReceipt,
				claimAt.Add(500*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		lease, _ := claim.Run.LeaseUntil()
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"normal_exit"}`)
		finished, err := fixture.store.FinishAgentRuntime(context.Background(),
			runtimeFinishSpec(fixture, claim, token, completion, lease))
		if err != nil || finished.Run.Status() != model.AgentRunDead ||
			finished.Handling.Status() != model.HandlingDead ||
			finished.Handling.LastDisposition() != "attempt_budget_exhausted" {
			t.Fatalf("final lease-edge completion = (%#v, %v)", finished, err)
		}
		authority, err := fixture.store.ReadLocalAuthority(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(),
			AgentDeadRecoverySpec{Profile: authority.Profile, At: lease.Add(time.Second)})
		if err != nil || recovered.Recovered != 1 {
			t.Fatalf("recover completed final lease attempt = (%#v, %v)", recovered, err)
		}
	})
}

func TestAgentRuntimeOutcomeFirstStillAcceptsWakeAndCompletionEvidence(t *testing.T) {
	t.Run("normal completion", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "outcome-first-finish", false,
			model.OperationResolveNoAction, "semantic outcome won first")
		resolveAt := fixture.reserveSpec.At.Add(time.Second)
		resolved, err := fixture.store.CommitManagedResolution(context.Background(), ManagedResolutionSpec{
			Reservation: fixture.reservation, Content: fixture.content, At: resolveAt})
		if err != nil {
			t.Fatal(err)
		}
		before, err := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
		if err != nil || before.Status() != model.AgentRunOutcomeAccepted {
			t.Fatalf("outcome-first Run = (%#v, %v)", before, err)
		}
		if _, ok := before.WakeDeliveredAt(); ok {
			t.Fatal("semantic outcome fabricated wake evidence")
		}
		if _, ok := before.CompletionReceipt(); ok {
			t.Fatal("semantic outcome fabricated Runtime completion")
		}
		semanticFinishedAt, _ := before.FinishedAt()

		wakeAt := fixture.claim.Run.StartedAt().Add(500 * time.Millisecond)
		wake := wakeDeliverySpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			fixture.wakeReceipt, wakeAt)
		wakeResult, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake)
		if err != nil || wakeResult.Status != AgentRuntimeAlreadySettled ||
			wakeResult.Run.Status() != model.AgentRunOutcomeAccepted {
			t.Fatalf("settled wake evidence = (%#v, %v)", wakeResult, err)
		}
		if replay, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil ||
			replay.Status != AgentRuntimeAlreadySettled || replay.Run.Status() != model.AgentRunOutcomeAccepted {
			t.Fatalf("settled wake replay = (%#v, %v)", replay, err)
		}
		completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"outcome_then_exit"}`)
		finish := runtimeFinishSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			completion, resolveAt.Add(2*time.Second))
		finishResult, err := fixture.store.FinishAgentRuntime(context.Background(), finish)
		if err != nil || finishResult.Status != AgentRuntimeApplied ||
			finishResult.Run.Status() != model.AgentRunOutcomeAccepted {
			t.Fatalf("settled completion evidence = (%#v, %v)", finishResult, err)
		}
		outcome, _ := finishResult.Run.OutcomeReceipt()
		storedCompletion, _ := finishResult.Run.CompletionReceipt()
		completionAt, hasCompletionAt := finishResult.Run.CompletionAt()
		finishedAt, _ := finishResult.Run.FinishedAt()
		if outcome.String() != resolved.Receipt.String() || storedCompletion.String() != completion.String() ||
			!hasCompletionAt || !completionAt.Equal(finish.At) || !finishedAt.Equal(semanticFinishedAt) {
			t.Fatalf("outcome-first evidence changed authority: outcome %s completion %s finished %s",
				outcome, storedCompletion, finishedAt)
		}
		handling, _ := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
		if handling.Status() != model.HandlingCompleted {
			t.Fatalf("completion downgraded Handling: %#v", handling)
		}
	})

	t.Run("failure completion", func(t *testing.T) {
		fixture := newManagedWakeRuntimeFixture(t, "outcome-first-failure", false,
			model.OperationResolveNoAction, "semantic outcome precedes provider failure")
		resolveAt := fixture.reserveSpec.At.Add(time.Second)
		resolved, err := fixture.store.CommitManagedResolution(context.Background(), ManagedResolutionSpec{
			Reservation: fixture.reservation, Content: fixture.content, At: resolveAt})
		if err != nil {
			t.Fatal(err)
		}
		failureReceipt := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"provider_failed"}`)
		failure := runtimeFailureSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			fixture.diagnostic, fixture.runtimeIDs, failureReceipt, "provider stream closed", resolveAt.Add(time.Second))
		failed, err := fixture.store.FailAgentRuntime(context.Background(), failure)
		if err != nil || failed.Status != AgentRuntimeApplied ||
			failed.Run.Status() != model.AgentRunOutcomeAccepted || failed.Run.Error() != failure.Error {
			t.Fatalf("settled failure evidence = (%#v, %v)", failed, err)
		}
		outcome, _ := failed.Run.OutcomeReceipt()
		completion, _ := failed.Run.CompletionReceipt()
		if outcome.String() != resolved.Receipt.String() || completion.String() != failureReceipt.String() {
			t.Fatalf("settled failure receipts = outcome %s completion %s", outcome, completion)
		}
		operation, err := readOperationByID(context.Background(), fixture.store.db,
			fixture.reservation.Operation.ID())
		if err != nil || operation.Status() != model.OperationCommitted {
			t.Fatalf("settled failure changed Operation = (%#v, %v)", operation, err)
		}
		handling, _ := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
		if handling.Status() != model.HandlingCompleted {
			t.Fatalf("settled failure downgraded Handling: %#v", handling)
		}

		wakeAt := fixture.claim.Run.StartedAt().Add(500 * time.Millisecond)
		wake := wakeDeliverySpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
			fixture.wakeReceipt, wakeAt)
		if delivered, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil ||
			delivered.Status != AgentRuntimeAlreadySettled {
			t.Fatalf("wake after settled failure = (%#v, %v)", delivered, err)
		}
		if replay, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil ||
			replay.Status != AgentRuntimeAlreadySettled {
			t.Fatalf("wake replay after settled failure = (%#v, %v)", replay, err)
		}
		mismatchedWake := wake
		mismatchedWake.WakeReceipt = runtimeTestJSON(t, `{"hook_id":"different"}`)
		if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(), mismatchedWake); !errors.Is(err,
			ErrAgentRuntimeInvariant) {
			t.Fatalf("late wake receipt mismatch error = %v", err)
		}
		afterMismatch, _ := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
		storedWake, hasWake := afterMismatch.WakeReceipt()
		if !hasWake || storedWake.String() != fixture.wakeReceipt.String() ||
			afterMismatch.LauncherDiagnostic().String() != fixture.diagnostic.String() {
			t.Fatalf("mismatched late wake overwrote evidence: %#v", afterMismatch)
		}
		failure.At = resolveAt.Add(3 * time.Second)
		if replay, err := fixture.store.FailAgentRuntime(context.Background(), failure); err != nil || replay.Status != AgentRuntimeReplayed {
			t.Fatalf("settled failure replay = (%#v, %v)", replay, err)
		}
		failure.At = resolveAt.Add(500 * time.Millisecond)
		if _, err := fixture.store.FailAgentRuntime(context.Background(), failure); !errors.Is(err, ErrAgentRuntimeInvariant) {
			t.Fatalf("regressed settled failure replay error = %v", err)
		}
	})
}

func TestAgentRuntimeFailureAtomicallyRejectsOperationAndRequeues(t *testing.T) {
	fixture := newManagedWakeRuntimeFixture(t, "failure-requeue", true,
		model.OperationResolveNoAction, "operation should be fenced by Runtime failure")
	failureAt := fixture.reserveSpec.At.Add(time.Second)
	completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"hook_failed"}`)
	spec := runtimeFailureSpec(fixture.acceptanceFixture, fixture.claim, fixture.token,
		fixture.diagnostic, fixture.runtimeIDs, completion, "mandatory Hook failed", failureAt)

	failed, err := fixture.store.FailAgentRuntime(context.Background(), spec)
	if err != nil || failed.Status != AgentRuntimeApplied || failed.Run.Status() != model.AgentRunFailed ||
		failed.Handling.Status() != model.HandlingPending {
		t.Fatalf("FailAgentRuntime() = (%#v, %v)", failed, err)
	}
	wantRetry := failureAt.Add(time.Duration(model.DefaultHandlingBudget().Spec().RetryInitialSeconds) * time.Second)
	if !failed.Handling.AvailableAt().Equal(wantRetry) || failed.Handling.LastDisposition() != "runtime_failed" ||
		failed.Handling.LastError() != spec.Error {
		t.Fatalf("failed Handling backoff = %#v, want %s", failed.Handling, wantRetry)
	}
	operation, err := readOperationByID(context.Background(), fixture.store.db,
		fixture.reservation.Operation.ID())
	if err != nil || operation.Status() != model.OperationRejected {
		t.Fatalf("failed Runtime Operation = (%#v, %v)", operation, err)
	}
	operationReceipt, _ := operation.Result()
	if operationReceipt.String() != `{"code":"agent_runtime_failed","status":"rejected"}` {
		t.Fatalf("failure Operation receipt = %s", operationReceipt)
	}

	replaySpec := spec
	replaySpec.At = failureAt.Add(time.Second)
	replay, err := fixture.store.FailAgentRuntime(context.Background(), replaySpec)
	if err != nil || replay.Status != AgentRuntimeReplayed ||
		!replay.Handling.AvailableAt().Equal(wantRetry) {
		t.Fatalf("failure replay = (%#v, %v)", replay, err)
	}
	mismatch := replaySpec
	mismatch.Error = "different failure"
	if _, err := fixture.store.FailAgentRuntime(context.Background(), mismatch); !errors.Is(err, ErrAgentRuntimeInvariant) {
		t.Fatalf("changed failure replay error = %v", err)
	}
	regressed := spec
	regressed.At = failureAt.Add(-time.Nanosecond)
	if _, err := fixture.store.FailAgentRuntime(context.Background(), regressed); !errors.Is(err, ErrAgentRuntimeInvariant) {
		t.Fatalf("regressed failure replay error = %v", err)
	}
}

func TestAgentRuntimeFinalFailureDeadRecoveryStartsNewGeneration(t *testing.T) {
	fixture, first, token, claimAt := newWakeRuntimeFixture(t, "dead-recovery", 1)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"launch"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-dead-recovery"}`)
	completion := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"launch_failed"}`)
	failureAt := claimAt.Add(time.Second)
	failure := runtimeFailureSpec(fixture, first, token, diagnostic, runtimeIDs, completion,
		"Codex app-server did not start", failureAt)
	dead, err := fixture.store.FailAgentRuntime(context.Background(), failure)
	if err != nil || dead.Status != AgentRuntimeApplied || dead.Run.Status() != model.AgentRunDead ||
		dead.Handling.Status() != model.HandlingDead {
		t.Fatalf("final Runtime failure = (%#v, %v)", dead, err)
	}
	if deadAt, ok := dead.Handling.DeadAt(); !ok || !deadAt.Equal(failureAt) {
		t.Fatalf("dead evidence = (%s, %v)", deadAt, ok)
	}
	lateWake := wakeDeliverySpec(fixture, first, token,
		runtimeTestJSON(t, `{"hook_id":"impossible-prelaunch-hook"}`),
		claimAt.Add(500*time.Millisecond))
	if _, err := fixture.store.RecordAgentWakeDelivery(context.Background(), lateWake); !errors.Is(err,
		ErrAgentRuntimeInvariant) {
		t.Fatalf("wake after pre-launch failure error = %v", err)
	}

	authority, err := fixture.store.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wrongSpec := authority.Profile.Spec()
	wrongSpec.ActiveAssetRevision = "asset-wrong-recovery"
	wrong, err := model.NewProfile(wrongSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: wrong, At: failureAt.Add(time.Second)}); !errors.Is(err, ErrAgentDeadRecoveryAuthority) {
		t.Fatalf("stale setup recovery error = %v", err)
	}

	recoveryAt := failureAt.Add(time.Second)
	recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: recoveryAt})
	if err != nil || recovered.Recovered != 1 {
		t.Fatalf("RecoverDeadAgentHandlings() = (%#v, %v)", recovered, err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), first.Handling.ID())
	if err != nil || handling.Status() != model.HandlingPending || handling.Attempts() != 0 ||
		handling.RecoveryCount() != 1 || !handling.AvailableAt().Equal(recoveryAt) ||
		handling.LastDisposition() != "setup_recovered" || handling.LastError() != "" {
		t.Fatalf("recovered Handling = (%#v, %v)", handling, err)
	}
	if _, ok := handling.DeadAt(); ok {
		t.Fatal("recovered Handling retained dead_at")
	}
	idempotent, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: recoveryAt.Add(time.Second)})
	if err != nil || idempotent.Recovered != 0 {
		t.Fatalf("recovery replay = (%#v, %v)", idempotent, err)
	}

	secondToken := model.Sum([]byte("runtime-token-dead-recovery-second"))
	second := preclaimWake(t, fixture, secondToken, recoveryAt)
	if second.Run.HandlingAttempt() != 1 || second.Run.HandlingRecovery() != 1 ||
		second.Run.ID() == first.Run.ID() {
		t.Fatalf("new recovery generation Run = %#v", second.Run)
	}
	var runs int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs WHERE handling_id=?`,
		first.Handling.ID().String()).Scan(&runs); err != nil || runs != 2 {
		t.Fatalf("recovery generation Run count = %d, %v", runs, err)
	}

	failure.At = recoveryAt.Add(time.Second)
	if replay, err := fixture.store.FailAgentRuntime(context.Background(), failure); err != nil || replay.Status != AgentRuntimeReplayed {
		t.Fatalf("historical failure replay after recovery = (%#v, %v)", replay, err)
	}
}

func TestAgentRuntimeLateFailureClosesExpiredFinalClaimBeforeRecovery(t *testing.T) {
	fixture, claim, token, _ := newWakeRuntimeFixture(t, "late-failure-dead", 1)
	lease, _ := claim.Run.LeaseUntil()
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		At: lease}); err != nil || status != AgentClaimNone {
		t.Fatalf("expire final managed claim = (%q, %v)", status, err)
	}
	before, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil || before.Status() != model.AgentRunDead {
		t.Fatalf("expired final Run = (%#v, %v)", before, err)
	}
	if _, completed := before.CompletionReceipt(); completed {
		t.Fatal("lease expiry fabricated Runtime completion")
	}
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"late-exit"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-late-failure-dead"}`)
	receipt := runtimeTestJSON(t, `{"kind":"runtime_completion","result":"failed_after_lease"}`)
	failureAt := lease.Add(time.Second)
	failure := runtimeFailureSpec(fixture, claim, token, diagnostic, runtimeIDs, receipt,
		"Runtime process failed after claim expiry", failureAt)
	settled, err := fixture.store.FailAgentRuntime(context.Background(), failure)
	if err != nil || settled.Status != AgentRuntimeApplied || settled.Run.Status() != model.AgentRunDead ||
		settled.Handling.Status() != model.HandlingDead || settled.Run.Error() != failure.Error {
		t.Fatalf("late final failure = (%#v, %v)", settled, err)
	}
	failure.At = failureAt.Add(time.Second)
	if replay, err := fixture.store.FailAgentRuntime(context.Background(), failure); err != nil ||
		replay.Status != AgentRuntimeReplayed {
		t.Fatalf("late final failure replay = (%#v, %v)", replay, err)
	}
	authority, err := fixture.store.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: lease.Add(500 * time.Millisecond)}); !errors.Is(err, ErrAgentDeadRecoveryInvariant) {
		t.Fatalf("recovery before late completion error = %v", err)
	}
	recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: failure.At.Add(time.Second)})
	if err != nil || recovered.Recovered != 1 {
		t.Fatalf("recovery after late completion = (%#v, %v)", recovered, err)
	}
}

func TestAgentDeadRecoveryWaitsForManagedRuntimeCompletion(t *testing.T) {
	fixture := newManagedResolutionFixture(t, "dead-runtime-unfinished", model.OperationResolveRetry,
		"final semantic retry while the managed Runtime is still alive")
	setClaimBudget(t, fixture.store, 1)
	authority, err := fixture.store.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixture.profile = authority.Profile
	if _, err := fixture.store.CommitManagedResolution(context.Background(), fixture.resolveSpec()); err != nil {
		t.Fatal(err)
	}
	runtimeStartedAt := fixture.claim.Run.StartedAt().Add(time.Millisecond)
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET launcher='mnemond-wake',
		runtime_started_at=?,launcher_diagnostic_json=?,runtime_ids_json=? WHERE run_id=?`,
		storeTime(runtimeStartedAt), []byte(`{"adapter":"codex"}`), []byte(`{"pid":4501}`),
		fixture.claim.Run.ID().String()); err != nil {
		t.Fatal(err)
	}
	recoveryAt := fixture.resolveAt.Add(time.Second)
	if _, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: recoveryAt}); !errors.Is(err, ErrAgentDeadRecoveryInvariant) {
		t.Fatalf("unfinished managed Runtime recovery error = %v", err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
	if err != nil || handling.Status() != model.HandlingDead || handling.RecoveryCount() != 0 {
		t.Fatalf("blocked recovery changed Handling = (%#v, %v)", handling, err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET completion_at=?,completion_receipt_json='{}'
		WHERE run_id=?`, storeTime(recoveryAt), fixture.claim.Run.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: fixture.resolveAt.Add(500 * time.Millisecond)}); !errors.Is(err,
		ErrAgentDeadRecoveryInvariant) {
		t.Fatalf("recovery before managed completion error = %v", err)
	}
	recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: authority.Profile, At: recoveryAt.Add(time.Second)})
	if err != nil || recovered.Recovered != 1 {
		t.Fatalf("completed managed Runtime recovery = (%#v, %v)", recovered, err)
	}
}

func TestAgentDeadRecoveryAcceptsBudgetResealedTerminalRun(t *testing.T) {
	fixture := newManagedResolutionFixture(t, "budget-resealed", model.OperationResolveRetry,
		"retry under the original handling budget")
	resolveSpec := fixture.resolveSpec()
	first, err := fixture.store.CommitManagedResolution(context.Background(), resolveSpec)
	if err != nil {
		t.Fatal(err)
	}
	budgetSpec := model.DefaultHandlingBudget().Spec()
	budgetSpec.MaxAttempts = 1
	budget, err := model.NewHandlingBudget(budgetSpec)
	if err != nil {
		t.Fatal(err)
	}
	upgradeAt := fixture.resolveAt.Add(time.Second)
	desiredSpec := fixture.profile.Spec()
	desiredSpec.HandlingBudget = budget.JSON()
	desiredSpec.UpdatedAt = upgradeAt
	desired, err := model.NewProfile(desiredSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET status='outcome_accepted',finished_at=?
		WHERE profile_id=? AND launcher='test' AND status='running'`, storeTime(fixture.resolveAt),
		fixture.profile.ID().String()); err != nil {
		t.Fatal(err)
	}
	upgraded, err := fixture.store.ActivateProfile(context.Background(), desired,
		fixture.profile.UpdatedAt(), upgradeAt)
	if err != nil || !upgraded.Changed {
		t.Fatalf("lower handling budget = (%#v, %v)", upgraded, err)
	}
	fixture.profile = upgraded.Profile
	sealAt := upgradeAt.Add(time.Second)
	if status, err := fixture.store.ProbeAgentClaim(context.Background(), AgentClaimProbeSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		At: sealAt}); err != nil || status != AgentClaimNone {
		t.Fatalf("seal exhausted pending Handling = (%q, %v)", status, err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), fixture.claim.Handling.ID())
	run, runErr := fixture.store.GetAgentRun(context.Background(), fixture.claim.Run.ID())
	if err != nil || runErr != nil || handling.Status() != model.HandlingDead ||
		handling.LastDisposition() != "attempt_budget_exhausted" ||
		handling.LastError() != "maximum handling attempts exhausted" ||
		run.Status() != model.AgentRunRequeued {
		t.Fatalf("budget-resealed evidence = Handling %#v (%v), Run %#v (%v)",
			handling, err, run, runErr)
	}
	replaySpec := resolveSpec
	replaySpec.At = sealAt.Add(time.Millisecond)
	replay, err := fixture.store.CommitManagedResolution(context.Background(), replaySpec)
	if err != nil || !replay.Replayed || replay.Receipt.String() != first.Receipt.String() {
		t.Fatalf("retry replay after budget reseal = (%#v, %v)", replay, err)
	}
	recovered, err := fixture.store.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{
		Profile: fixture.profile, At: sealAt.Add(time.Second)})
	if err != nil || recovered.Recovered != 1 {
		t.Fatalf("recover budget-resealed Handling = (%#v, %v)", recovered, err)
	}
	replaySpec.At = sealAt.Add(2 * time.Second)
	replay, err = fixture.store.CommitManagedResolution(context.Background(), replaySpec)
	if err != nil || !replay.Replayed || replay.Receipt.String() != first.Receipt.String() {
		t.Fatalf("retry replay after budget-reseal recovery = (%#v, %v)", replay, err)
	}
}

type managedWakeRuntimeFixture struct {
	*acceptanceFixture
	claim       AgentClaimResult
	token       model.Digest
	diagnostic  model.JSON
	runtimeIDs  model.JSON
	wakeReceipt model.JSON
	reservation ManagedOperationReservation
	reserveSpec ManagedOperationSpec
	content     string
}

func newManagedWakeRuntimeFixture(t *testing.T, suffix string, recordWake bool,
	kind model.OperationKind, content string,
) *managedWakeRuntimeFixture {
	t.Helper()
	fixture, claim, token, claimAt := newWakeRuntimeFixture(t, suffix, 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"process-`+suffix+`"}`)
	wakeReceipt := runtimeTestJSON(t, `{"hook_id":"hook-`+suffix+`","thread_id":"thread-`+suffix+`","turn_id":"turn-`+suffix+`"}`)
	launch := runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
		claimAt.Add(250*time.Millisecond))
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), launch); err != nil ||
		result.Status != AgentRuntimeApplied {
		t.Fatalf("prepare Runtime launch = (%#v, %v)", result, err)
	}
	if recordWake {
		wake := wakeDeliverySpec(fixture, claim, token, wakeReceipt,
			claimAt.Add(500*time.Millisecond))
		if result, err := fixture.store.RecordAgentWakeDelivery(context.Background(), wake); err != nil || result.Status != AgentRuntimeApplied {
			t.Fatalf("prepare wake delivery = (%#v, %v)", result, err)
		}
	}
	consumed, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), AgentAttachmentSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		AttachmentTokenHash: token, At: claimAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	readAt := claimAt.Add(2 * time.Second)
	installManagedResolutionCurrent(t, fixture, consumed, token, readAt)
	requestDigest, err := ManagedResolutionRequestDigest(token, kind, content)
	if err != nil {
		t.Fatal(err)
	}
	reserveAt := claimAt.Add(3 * time.Second)
	reserveSpec := ManagedOperationSpec{Profile: fixture.profile,
		ClientKeyHash: model.Sum([]byte("key-" + suffix)), RequestDigest: requestDigest,
		Kind: kind, LeaseOwner: "server-" + suffix, At: reserveAt,
		LeaseUntil: reserveAt.Add(2 * time.Minute), ClaimContextHash: token, HasClaimContext: true}
	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), reserveSpec)
	if err != nil {
		t.Fatal(err)
	}
	return &managedWakeRuntimeFixture{acceptanceFixture: fixture, claim: consumed, token: token,
		diagnostic: diagnostic, runtimeIDs: runtimeIDs, wakeReceipt: wakeReceipt, reservation: reservation,
		reserveSpec: reserveSpec, content: content}
}

func newWakeRuntimeFixture(t *testing.T, suffix string,
	maxAttempts int,
) (*acceptanceFixture, AgentClaimResult, model.Digest, time.Time) {
	t.Helper()
	fixture, events := newAgentClaimFixture(t, 1, "runtime-"+suffix)
	if maxAttempts > 0 {
		setClaimBudget(t, fixture.store, maxAttempts)
		authority, err := fixture.store.ReadLocalAuthority(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fixture.profile = authority.Profile
	}
	claimAt := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-runtime-"+suffix, events[0], 1,
		claimAt, claimAt, 0)
	token := model.Sum([]byte("runtime-token-" + suffix))
	claim := preclaimWake(t, fixture, token, claimAt)
	return fixture, claim, token, claimAt
}

func wakeDeliverySpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	receipt model.JSON, at time.Time,
) AgentWakeDeliverySpec {
	return AgentWakeDeliverySpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		WakeReceipt: receipt, At: at}
}

func runtimeLaunchSpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	diagnostic, runtimeIDs model.JSON, at time.Time,
) AgentRuntimeLaunchSpec {
	return AgentRuntimeLaunchSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs, At: at}
}

func runtimeFinishSpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	receipt model.JSON, at time.Time,
) AgentRuntimeFinishSpec {
	return AgentRuntimeFinishSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		CompletionReceipt: receipt, At: at}
}

func runtimeFailureSpec(fixture *acceptanceFixture, claim AgentClaimResult, token model.Digest,
	diagnostic, runtimeIDs, receipt model.JSON, message string, at time.Time,
) AgentRuntimeFailureSpec {
	return AgentRuntimeFailureSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: claim.Run.ID(),
		ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs, CompletionReceipt: receipt,
		Error: message, At: at}
}

func runtimeTestJSON(t *testing.T, raw string) model.JSON {
	t.Helper()
	value, err := model.NewJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
