package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestFinalizeAgentCurrentReadDerivesAndReplaysDurableProjection(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "current-read")
	claimAt := fixture.now.Add(2 * time.Second)
	handling := insertClaimHandling(t, fixture.store, "handling-current-read", events[0], 10,
		claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-current-read", "token-current-read", claimAt)
	readAt := claimAt.Add(time.Second)
	spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-read", readAt)

	result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
	if err != nil || result.Replayed {
		t.Fatalf("FinalizeAgentCurrentRead() = (%#v, %v)", result, err)
	}
	if result.Receipt.RunID() != claim.Run.ID() || result.Receipt.HandlingID() != handling.ID() ||
		result.Receipt.HandlingAttempt() != 1 || result.Receipt.SourceEvent().EventID() != events[0] ||
		result.Projection.ActionWork().Version() != 1 ||
		result.Projection.ActionWork().LocalRole() != model.CurrentInitiator {
		t.Fatalf("current binding = %#v", result.Receipt)
	}
	brief, ok := result.Projection.ActionWork().Brief()
	if !ok || brief.Content() != "Review the production change" ||
		brief.DeadlineUnixNano() != result.Projection.ActionWork().DeadlineUnixNano() {
		t.Fatalf("durable offered brief = (%#v, %t)", brief, ok)
	}
	wantActions := []model.OperationKind{model.OperationTeamworkCancel, model.OperationResolveRetry}
	if !sameOperationKinds(result.Projection.AllowedActions(), wantActions) {
		t.Fatalf("allowed actions = %v, want %v", result.Projection.AllowedActions(), wantActions)
	}
	if len(result.Receipt.ArtifactRefs()) != 0 {
		t.Fatalf("empty source Event invented Artifact refs: %v", result.Receipt.ArtifactRefs())
	}
	if strings.Contains(result.Receipt.CanonicalJSON().String(), "token-current-read") ||
		strings.Contains(result.Receipt.CanonicalJSON().String(), spec.ClaimTokenHash.String()) {
		t.Fatalf("current receipt leaked claim authority: %s", result.Receipt.CanonicalJSON().String())
	}
	var stored []byte
	var disposition string
	if err := fixture.store.db.QueryRow(`SELECT r.current_read_receipt_json,h.last_disposition
		FROM agent_runs r JOIN agent_handlings h ON h.handling_id=r.handling_id WHERE r.run_id=?`,
		claim.Run.ID().String()).Scan(&stored, &disposition); err != nil ||
		string(stored) != result.Receipt.CanonicalJSON().String() || disposition != "read" {
		t.Fatalf("durable current evidence = (%s, %q, %v)", stored, disposition, err)
	}
	advanceCurrentWorkAfterRead(t, fixture, events[0], readAt.Add(time.Second))

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	spec.At = readAt.Add(2 * time.Second)
	assertCurrentReplayRejectsActionPolicyDrift(t, restarted, spec)
	replay, err := restarted.FinalizeAgentCurrentRead(context.Background(), spec)
	if err != nil || !replay.Replayed ||
		replay.Receipt.CanonicalJSON().String() != result.Receipt.CanonicalJSON().String() ||
		replay.Projection.CanonicalJSON().String() != result.Projection.CanonicalJSON().String() {
		t.Fatalf("restart replay = (%#v, %v)", replay, err)
	}
	if _, err := restarted.db.Exec(`UPDATE agent_runs SET current_read_receipt_json='{}' WHERE run_id=?`,
		claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed current-read evidence rewrite")
	}
}

func assertCurrentReplayRejectsActionPolicyDrift(t testing.TB, st *Store,
	spec AgentCurrentReadSpec,
) {
	t.Helper()
	spec.ActionPolicy = acceptanceActionPolicy(t, 1)
	if _, err := st.FinalizeAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInput) {
		t.Fatalf("replay with drifted Action policy error = %v", err)
	}
}

func advanceCurrentWorkAfterRead(t *testing.T, fixture *acceptanceFixture,
	eventID model.EventID, at time.Time,
) {
	t.Helper()
	source, err := readCurrentSourceEvent(context.Background(), fixture.store.db, eventID)
	if err != nil {
		t.Fatal(err)
	}
	work, err := fixture.store.GetReviewWork(context.Background(), source.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	advanced := commitCausalAcceptedWork(t, fixture, source, work, at)
	if advanced.State() != model.WorkActive || advanced.Version() != 2 {
		t.Fatalf("advanced Work = %#v", advanced)
	}
}

func corruptCurrentWorkUpdater(t *testing.T, fixture *acceptanceFixture,
	eventID model.EventID, at time.Time,
) {
	t.Helper()
	offered, err := readCurrentSourceEvent(context.Background(), fixture.store.db, eventID)
	if err != nil {
		t.Fatal(err)
	}
	work, err := fixture.store.GetReviewWork(context.Background(), offered.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	_, publication := controllerAcceptance(t, fixture, work, offered, at)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE works SET updated_by_event=?,updated_at=?
		WHERE home_peer_id=? AND work_id=?`, publication.Event().ID().String(), storeTime(at),
		work.Ref().HomePeerID().String(), work.Ref().WorkID().String()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentCurrentReadRejectsActionPolicyRevisionDriftWithoutWrites(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "current-action-policy")
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-current-action-policy", events[0], 1,
		claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-current-action-policy",
		"token-current-action-policy", claimAt)
	spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-action-policy",
		claimAt.Add(time.Second))
	spec.ActionPolicy = model.TeamworkActionPolicy{}
	if _, err := fixture.store.PlanAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInput) {
		t.Fatalf("zero Action policy error = %v", err)
	}
	spec.ActionPolicy = acceptanceActionPolicy(t, 1)
	if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInput) {
		t.Fatalf("drifted Action policy error = %v", err)
	}
	var receipts int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, claim.Run.ID().String()).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("invalid Action policy persisted receipts = %d, err=%v", receipts, err)
	}
}

func TestAgentCurrentReadRejectsCorruptDurableWorkUpdater(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-corrupt-updater-fresh")
		claimAt := fixture.now.Add(5 * time.Second)
		corruptCurrentWorkUpdater(t, fixture, events[0], claimAt.Add(-time.Second))
		insertClaimHandling(t, fixture.store, "handling-current-corrupt-updater-fresh",
			events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-corrupt-updater-fresh",
			"token-current-corrupt-updater-fresh", claimAt)
		spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-corrupt-updater-fresh",
			claimAt.Add(time.Second))
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInvariant) ||
			!strings.Contains(err.Error(), "Work updater does not produce") {
			t.Fatalf("fresh corrupt updater error = %v", err)
		}
		assertNoCurrentReadReceipt(t, fixture.store, claim.Run.ID())
	})

	t.Run("replay", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-corrupt-updater-replay")
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-corrupt-updater-replay",
			events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-corrupt-updater-replay",
			"token-current-corrupt-updater-replay", claimAt)
		spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-corrupt-updater-replay",
			claimAt.Add(time.Second))
		original, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		corruptCurrentWorkUpdater(t, fixture, events[0], claimAt.Add(2*time.Second))
		spec.At = claimAt.Add(3 * time.Second)
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInvariant) ||
			!strings.Contains(err.Error(), "Work updater does not produce") {
			t.Fatalf("replay corrupt updater error = %v", err)
		}
		assertStoredCurrentReceipt(t, fixture.store, claim.Run.ID(), original.Receipt.CanonicalJSON())
	})
}

func TestFinalizeAgentCurrentReadIsFencedAndConcurrentReplaySafe(t *testing.T) {
	t.Run("wrong token and lease boundary", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-fence")
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-fence", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-fence", "token-current-fence", claimAt)
		wrong := currentReadSpec(fixture, claim.Run.ID(), "wrong-current-token", claimAt.Add(time.Second))
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), wrong); !errors.Is(err, ErrCurrentReadStale) {
			t.Fatalf("wrong token error = %v", err)
		}
		var receipts int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
			WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, claim.Run.ID().String()).Scan(&receipts); err != nil || receipts != 0 {
			t.Fatalf("wrong token persisted receipts = %d, err=%v", receipts, err)
		}
		lease, _ := claim.Run.LeaseUntil()
		expired := currentReadSpec(fixture, claim.Run.ID(), "token-current-fence", lease)
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), expired); !errors.Is(err, ErrCurrentReadStale) {
			t.Fatalf("lease boundary error = %v", err)
		}
	})

	t.Run("Profile projection budget", func(t *testing.T) {
		fixture, events := newAgentClaimFixtureWithContent(t, 1, "current-budget",
			strings.Repeat("x", 2048))
		budgetSpec := model.DefaultHandlingBudget().Spec()
		budgetSpec.MaxCurrentJSONBytes = 1024
		budget, err := model.NewHandlingBudget(budgetSpec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`UPDATE profiles SET handling_budget_json=? WHERE profile_id=?`,
			budget.JSON().Bytes(), fixture.profile.ID().String()); err != nil {
			t.Fatal(err)
		}
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-budget", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-budget", "token-current-budget", claimAt)
		_, err = fixture.store.FinalizeAgentCurrentRead(context.Background(),
			currentReadSpec(fixture, claim.Run.ID(), "token-current-budget", claimAt.Add(time.Second)))
		if !errors.Is(err, ErrCurrentReadTooLarge) {
			t.Fatalf("Profile budget error = %v", err)
		}
		var receipts int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
			WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, claim.Run.ID().String()).Scan(&receipts); err != nil || receipts != 0 {
			t.Fatalf("oversize current persisted receipts = %d, err=%v", receipts, err)
		}
	})

	t.Run("concurrent same claim", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-concurrent")
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-concurrent", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-concurrent", "token-current-concurrent", claimAt)
		spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-concurrent", claimAt.Add(time.Second))

		start := make(chan struct{})
		results := make(chan AgentCurrentReadResult, 2)
		errs := make(chan error, 2)
		var wait sync.WaitGroup
		for index := 0; index < 2; index++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
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
				t.Fatalf("concurrent finalize error = %v", err)
			}
		}
		var canonical string
		replayed := map[bool]int{}
		for result := range results {
			replayed[result.Replayed]++
			if canonical == "" {
				canonical = result.Receipt.CanonicalJSON().String()
			} else if result.Receipt.CanonicalJSON().String() != canonical {
				t.Fatal("concurrent finalizers returned different receipts")
			}
		}
		if replayed[false] != 1 || replayed[true] != 1 {
			t.Fatalf("concurrent replay states = %#v", replayed)
		}
	})
}

func TestFinalizeAgentCurrentReadIncludesOnlyVerifiedEventPinnedArtifacts(t *testing.T) {
	t.Run("verified and pinned", func(t *testing.T) {
		fixture, eventID, root, claim, claimAt := currentArtifactReadFixture(t, "valid")
		spec := plannedCurrentReadSpec(t, fixture.store, currentReadSpec(fixture, claim.Run.ID(),
			"token-current-artifact-valid", claimAt.Add(time.Second)))
		result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		refs := result.Receipt.ArtifactRefs()
		if len(refs) != 1 || refs[0].RootDigest() != root.RootDigest ||
			result.Receipt.SourceEvent().EventID() != eventID {
			t.Fatalf("current Artifact refs = %v", refs)
		}
	})

	t.Run("missing Event pin", func(t *testing.T) {
		fixture, eventID, root, claim, claimAt := currentArtifactReadFixture(t, "unpinned")
		spec := plannedCurrentReadSpec(t, fixture.store, currentReadSpec(fixture, claim.Run.ID(),
			"token-current-artifact-unpinned", claimAt.Add(time.Second)))
		if _, err := fixture.store.db.Exec(`DELETE FROM artifact_pins
			WHERE root_digest=? AND owner_kind='event' AND owner_id=?`, root.RootDigest.String(), eventID.String()); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
		if !errors.Is(err, ErrCurrentReadInvariant) {
			t.Fatalf("missing Event pin error = %v", err)
		}
		assertNoCurrentReadReceipt(t, fixture.store, claim.Run.ID())
	})
}

func TestAgentCurrentArtifactPlanRequiresExactViewsAndReverifiesReplay(t *testing.T) {
	fixture, eventID, root, claim, claimAt := currentArtifactReadFixture(t, "exact-view")
	base := currentReadSpec(fixture, claim.Run.ID(), "token-current-artifact-exact-view",
		claimAt.Add(time.Second))
	plan, err := fixture.store.PlanAgentCurrentRead(context.Background(), base)
	if err != nil || plan.Replay != nil || plan.RunID != claim.Run.ID() || len(plan.Artifacts) != 1 ||
		plan.Artifacts[0].Ordinal != 0 || plan.Artifacts[0].RootDigest != root.RootDigest ||
		plan.Artifacts[0].ManifestDigest != root.ManifestDigest {
		t.Fatalf("fresh materialization plan = (%#v, %v)", plan, err)
	}

	rootOnly, _ := model.NewCurrentArtifactRef(root.RootDigest)
	wrongRoot, _ := model.NewCurrentArtifactView(model.Sum([]byte("wrong-current-view-root")),
		fmt.Sprintf(".mnemon/harness/node/views/%s/0/root", claim.Run.ID().String()))
	otherRun, _ := model.ParseRunID("run-current-view-other")
	wrongRun, _ := model.NewCurrentArtifactView(root.RootDigest,
		fmt.Sprintf(".mnemon/harness/node/views/%s/0/root", otherRun.String()))
	wrongOrdinal, _ := model.NewCurrentArtifactView(root.RootDigest,
		fmt.Sprintf(".mnemon/harness/node/views/%s/1/root", claim.Run.ID().String()))
	for name, views := range map[string][]model.CurrentArtifactRef{
		"missing": nil, "unmaterialized": {rootOnly}, "wrong root": {wrongRoot},
		"wrong Run": {wrongRun}, "wrong ordinal": {wrongOrdinal},
	} {
		t.Run(name, func(t *testing.T) {
			spec := base
			spec.ArtifactViews = views
			if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec); !errors.Is(err, ErrCurrentReadInvariant) {
				t.Fatalf("mapping error = %v", err)
			}
			assertNoCurrentReadReceipt(t, fixture.store, claim.Run.ID())
		})
	}

	freshSpec := plannedCurrentReadSpec(t, fixture.store, base)
	fresh, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), freshSpec)
	if err != nil || fresh.Replayed || len(fresh.Receipt.ArtifactRefs()) != 1 {
		t.Fatalf("fresh finalization = (%#v, %v)", fresh, err)
	}
	storedPath, ok := fresh.Receipt.ArtifactRefs()[0].ViewPath()
	if !ok || !strings.Contains(storedPath, claim.Run.ID().String()+"/0/") {
		t.Fatalf("stored readonly mapping = %q, %t", storedPath, ok)
	}

	replayBase := base
	replayBase.At = base.At.Add(time.Second)
	replayPlan, err := fixture.store.PlanAgentCurrentRead(context.Background(), replayBase)
	if err != nil || replayPlan.Replay == nil || !replayPlan.Replay.Replayed ||
		len(replayPlan.Artifacts) != 1 || replayPlan.Artifacts[0] != plan.Artifacts[0] {
		t.Fatalf("replay materialization plan = (%#v, %v)", replayPlan, err)
	}
	replayBase.ArtifactViews = fresh.Receipt.ArtifactRefs()
	replayed, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), replayBase)
	if err != nil || !replayed.Replayed ||
		replayed.Receipt.CanonicalJSON().String() != fresh.Receipt.CanonicalJSON().String() {
		t.Fatalf("exact mapping replay = (%#v, %v)", replayed, err)
	}

	drift, _ := model.NewCurrentArtifactView(root.RootDigest,
		fmt.Sprintf(".mnemon/harness/node/views/%s/0/other", claim.Run.ID().String()))
	replayBase.ArtifactViews = []model.CurrentArtifactRef{drift}
	if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), replayBase); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("stored mapping drift error = %v", err)
	}
	if _, err := fixture.store.db.Exec(`DELETE FROM artifact_pins
		WHERE root_digest=? AND owner_kind='event' AND owner_id=?`, root.RootDigest.String(), eventID.String()); err != nil {
		t.Fatal(err)
	}
	replayBase.ArtifactViews = nil
	if _, err := fixture.store.PlanAgentCurrentRead(context.Background(), replayBase); !errors.Is(err, ErrCurrentReadInvariant) {
		t.Fatalf("replay missing pin error = %v", err)
	}
}

func TestAgentCurrentArtifactFinalizeHonorsProfilePathBudget(t *testing.T) {
	fixture, _, _, claim, claimAt := currentArtifactReadFixture(t, "path-budget")
	base := currentReadSpec(fixture, claim.Run.ID(), "token-current-artifact-path-budget",
		claimAt.Add(time.Second))
	plan, err := fixture.store.PlanAgentCurrentRead(context.Background(), base)
	if err != nil || len(plan.Artifacts) != 1 {
		t.Fatalf("PlanAgentCurrentRead() = (%#v, %v)", plan, err)
	}
	budgetSpec := model.DefaultHandlingBudget().Spec()
	budgetSpec.MaxCurrentPathBytes = 80
	budget, err := model.NewHandlingBudget(budgetSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE profiles SET handling_budget_json=? WHERE profile_id=?`,
		budget.JSON().Bytes(), fixture.profile.ID().String()); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf(".mnemon/harness/node/views/%s/0/%s", claim.Run.ID().String(),
		strings.Repeat("x", 40))
	if len(path) <= budgetSpec.MaxCurrentPathBytes {
		t.Fatalf("test path length = %d, budget = %d", len(path), budgetSpec.MaxCurrentPathBytes)
	}
	view, err := model.NewCurrentArtifactView(plan.Artifacts[0].RootDigest, path)
	if err != nil {
		t.Fatal(err)
	}
	base.ArtifactViews = []model.CurrentArtifactRef{view}
	if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), base); !errors.Is(err, ErrCurrentReadTooLarge) {
		t.Fatalf("Profile path budget error = %v", err)
	}
	assertNoCurrentReadReceipt(t, fixture.store, claim.Run.ID())
}

func TestAgentCurrentArtifactFinalizeIsConcurrentReplaySafe(t *testing.T) {
	fixture, _, _, claim, claimAt := currentArtifactReadFixture(t, "concurrent-view")
	spec := plannedCurrentReadSpec(t, fixture.store, currentReadSpec(fixture, claim.Run.ID(),
		"token-current-artifact-concurrent-view", claimAt.Add(time.Second)))
	start := make(chan struct{})
	results := make(chan AgentCurrentReadResult, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
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
			t.Fatalf("concurrent Artifact finalize error = %v", err)
		}
	}
	replays := map[bool]int{}
	var canonical string
	for result := range results {
		replays[result.Replayed]++
		if canonical == "" {
			canonical = result.Receipt.CanonicalJSON().String()
		} else if canonical != result.Receipt.CanonicalJSON().String() {
			t.Fatal("concurrent Artifact finalizers returned different receipts")
		}
	}
	if replays[false] != 1 || replays[true] != 1 {
		t.Fatalf("concurrent Artifact replay states = %#v", replays)
	}
}

func TestFinalizeAgentCurrentReadKeepsOfferedBriefAfterAcceptedTransition(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "current-accepted-brief", nil)
	root := verifiedRoot(t, "current-accepted-brief-root",
		`{"entries":[],"kind":"brief","total_bytes":0}`, 0)
	if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
	artifact, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	offerSpec := fixture.offer(t, authority, "current-accepted-brief", fixture.reviewers,
		[]model.ArtifactRef{artifact}, nil)
	offered := offerSpec.Items[0].Publication.Event()
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), offerSpec,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	current, err := fixture.store.GetReviewWork(context.Background(), offered.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := fixture.now.Add(3 * time.Second)
	scope, publication := controllerAcceptance(t, fixture, current, offered, acceptedAt)
	nextSpec := current.Spec()
	nextSpec.Version++
	nextSpec.State = model.WorkActive
	nextSpec.StateData = publication.Event().Payload()
	nextSpec.UpdatedBy = publication.Event().ID()
	nextSpec.UpdatedAt = acceptedAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{
		Scope: scope, Controller: true,
		Items: []LocalAcceptanceItem{{Publication: publication, Work: &mutation}},
	}, acceptedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	claimAt := acceptedAt.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-current-accepted-brief", publication.Event().ID(),
		1, claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-current-accepted-brief", "token-current-accepted-brief", claimAt)
	spec := plannedCurrentReadSpec(t, fixture.store, currentReadSpec(fixture, claim.Run.ID(),
		"token-current-accepted-brief", claimAt.Add(time.Second)))
	result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	brief, ok := result.Projection.ActionWork().Brief()
	if !ok || result.Projection.SourceEvent().Type() != model.EventReviewAccepted ||
		brief.Content() != "Review the production change" || len(brief.ArtifactRefs()) != 1 ||
		brief.ArtifactRefs()[0].RootDigest() != root.RootDigest || len(result.Receipt.ArtifactRefs()) != 1 ||
		result.Receipt.ArtifactRefs()[0].RootDigest() != root.RootDigest {
		t.Fatalf("accepted fresh turn brief/roots = (%#v, %v)", brief, result.Receipt.ArtifactRefs())
	}
}

func TestFinalizeAgentCurrentReadKeepsBriefForDerivedRework(t *testing.T) {
	fixture := newAcceptanceFixture(t, 2)
	parent, source := seedDerivationParent(t, fixture)
	contextHash := model.Sum([]byte("current-derived-rework-context"))
	_, authority := fixture.reserveOffer(t, "current-derived-rework", &contextHash)
	offerSpec := fixture.offer(t, authority, "current-derived-rework", fixture.reviewers, nil,
		[]model.EventKey{source})
	offerSpec.Derivation = &LocalDerivationParent{ChannelID: parent.ChannelID(), WorkRef: parent.Ref(),
		ExpectedVersion: parent.Version(), UpdatedByEvent: parent.UpdatedBy()}
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), offerSpec,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	offered := offerSpec.Items[0].Publication.Event()
	work, err := fixture.store.GetReviewWork(context.Background(), offered.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	node, err := readNode(context.Background(), fixture.store.db)
	if err != nil {
		t.Fatal(err)
	}
	var channelHead uint64
	if err := fixture.store.db.QueryRow(`SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, fixture.channel.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&channelHead); err != nil {
		t.Fatal(err)
	}
	reworkID, _ := model.ParseEventID("event-current-derived-rework-transition")
	reworkScope, _ := model.NewEventScope(fixture.channel, node.PeerID(), node.OriginEpoch(),
		node.NextOriginSequence(), channelHead+1, offered.Scope().OriginMember(),
		offered.Scope().PublicationRoster(), work.Ref())
	payload, _ := model.NewJSON([]byte(`{"content":"apply the correction","iteration":1,"work_version":4}`))
	reworkAt := fixture.now.Add(3 * time.Second)
	rework, err := model.NewEvent(model.EventSpec{ID: reworkID, Scope: reworkScope,
		Source: model.EventSourceLocal, ActorPrincipal: fixture.profile.Principal(),
		Type: model.EventReviewReworkRequested, Audience: offered.Audience(), Summary: "Review rework requested",
		Payload: payload, CausedBy: []model.EventKey{offered.Key()}, CreatedAt: reworkAt, AcceptedAt: reworkAt})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := model.NewPublicationBody(rework)
	publication, err := model.AttachSignature(body, make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE works SET version=5,iteration=2,state='REWORK',state_json=?,
		updated_by_event=?,updated_at=? WHERE home_peer_id=? AND work_id=?`, payload.Bytes(), rework.ID().String(),
		storeTime(reworkAt), work.Ref().HomePeerID().String(), work.Ref().WorkID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE node SET next_origin_seq=?,updated_at=? WHERE singleton=1`,
		node.NextOriginSequence()+1, storeTime(reworkAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE publication_epochs SET source_head_channel_seq=?,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, channelHead+1, storeTime(reworkAt),
		fixture.channel.String(), node.PeerID().String(), node.OriginEpoch().String()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	claimAt := reworkAt.Add(time.Second)
	insertClaimHandling(t, fixture.store, "handling-current-derived-rework", rework.ID(), 1,
		claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-current-derived-rework", "token-current-derived-rework", claimAt)
	result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(),
		currentReadSpec(fixture, claim.Run.ID(), "token-current-derived-rework", claimAt.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	brief, ok := result.Projection.ActionWork().Brief()
	if !ok || brief.Content() != "Review the production change" ||
		result.Projection.ActionWork().State() != model.WorkRework ||
		result.Projection.ActionWork().Iteration() != 2 || result.Projection.SourceEvent().Type() != model.EventReviewReworkRequested {
		t.Fatalf("derived rework fresh turn = (%#v, %#v)", brief, result.Projection.ActionWork())
	}
}

func TestCurrentDistinguishesOrdinaryDerivedWorkHandlingFromParentResume(t *testing.T) {
	fixture := newDerivationDispositionFixture(t, false)
	event := currentPolicyEvent(t, fixture.now, fixture.channel, fixture.children[0],
		fixture.node.PeerID(), fixture.reviewers[1], model.EventReviewAccepted,
		"event-derived-work-current", `{"iteration":1,"work_version":1}`)
	eventID := event.ID()
	regularID, _ := model.ParseHandlingID("handling-derived-work-ordinary")
	regular, err := model.NewHandling(model.HandlingSpec{ID: regularID,
		ProfileID: model.TeamworkProfileID(), EventID: eventID, Status: model.HandlingPending,
		AvailableAt: event.AcceptedAt(), CreatedAt: event.AcceptedAt(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if parentResume, err := handlingIsParentResume(context.Background(), fixture.store.db,
		regular, event); err != nil || parentResume {
		t.Fatalf("ordinary derived Work handling classified as parent resume: %t, %v", parentResume, err)
	}

	resumeID, err := deterministicDerivationHandlingID(fixture.operation)
	if err != nil {
		t.Fatal(err)
	}
	resume, err := model.NewHandling(model.HandlingSpec{ID: resumeID,
		ProfileID: model.TeamworkProfileID(), EventID: eventID, Status: model.HandlingPending,
		AvailableAt: event.AcceptedAt(), CreatedAt: event.AcceptedAt(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if parentResume, err := handlingIsParentResume(context.Background(), fixture.store.db,
		resume, event); err != nil || !parentResume {
		t.Fatalf("derivation resume was not recognized: %t, %v", parentResume, err)
	}
}

func currentReadSpec(fixture *acceptanceFixture, run model.RunID, token string,
	at time.Time,
) AgentCurrentReadSpec {
	return AgentCurrentReadSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: run,
		ClaimTokenHash: model.Sum([]byte(token)), At: at, ActionPolicy: fixture.policy}
}

func plannedCurrentReadSpec(t *testing.T, st *Store,
	spec AgentCurrentReadSpec,
) AgentCurrentReadSpec {
	t.Helper()
	plan, err := st.PlanAgentCurrentRead(context.Background(), spec)
	if err != nil {
		t.Fatalf("PlanAgentCurrentRead() error = %v", err)
	}
	if plan.RunID != spec.RunID {
		t.Fatalf("materialization Run = %s, want %s", plan.RunID, spec.RunID)
	}
	spec.ArtifactViews = make([]model.CurrentArtifactRef, len(plan.Artifacts))
	for index, item := range plan.Artifacts {
		if item.Ordinal != uint32(index) || item.RootDigest.IsZero() || item.ManifestDigest.IsZero() {
			t.Fatalf("materialization item %d = %#v", index, item)
		}
		path := fmt.Sprintf(".mnemon/harness/node/views/%s/%d/root", spec.RunID.String(), item.Ordinal)
		spec.ArtifactViews[index], err = model.NewCurrentArtifactView(item.RootDigest, path)
		if err != nil {
			t.Fatalf("NewCurrentArtifactView() error = %v", err)
		}
	}
	return spec
}

func assertNoCurrentReadReceipt(t *testing.T, st *Store, run model.RunID) {
	t.Helper()
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, run.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("AgentRun %s has %d current-read receipts", run, count)
	}
}

func assertStoredCurrentReceipt(t testing.TB, st *Store, run model.RunID, want model.JSON) {
	t.Helper()
	var stored []byte
	if err := st.db.QueryRow(`SELECT current_read_receipt_json FROM agent_runs WHERE run_id=?`,
		run.String()).Scan(&stored); err != nil || string(stored) != want.String() {
		t.Fatalf("stored current receipt = (%s, %v), want %s", stored, err, want.String())
	}
}

func currentArtifactReadFixture(t *testing.T, suffix string) (*acceptanceFixture,
	model.EventID, VerifiedArtifactRoot, AgentClaimResult, time.Time,
) {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "current-artifact-"+suffix, nil)
	root := verifiedRoot(t, "current-artifact-root-"+suffix,
		`{"entries":[],"kind":"current","total_bytes":0}`, 0)
	if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
	artifact, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	spec := fixture.offer(t, authority, "current-artifact-"+suffix, fixture.reviewers,
		[]model.ArtifactRef{artifact}, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID("event-current-artifact-" + suffix + "-0")
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-current-artifact-"+suffix, eventID, 1, claimAt, claimAt, 0)
	token := "token-current-artifact-" + suffix
	claim := claimCurrent(t, fixture, "owner-current-artifact-"+suffix, token, claimAt)
	return fixture, eventID, root, claim, claimAt
}

func sameOperationKinds(left, right []model.OperationKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func currentPolicyEvent(t *testing.T, at time.Time, channel model.ChannelID, ref model.WorkRef,
	home, reviewer model.PeerID, eventType model.EventType, eventText, payloadText string,
) model.Event {
	t.Helper()
	epoch, _ := model.ParseOriginEpoch("epoch-current-policy-home")
	head, _ := model.NewRecordHead(1, model.Sum([]byte("current-policy-head")))
	scope, _ := model.NewEventScope(channel, home, epoch, 1, 1, head, head, ref)
	audience, _ := model.NewAudience([]model.PeerID{reviewer})
	payload, _ := model.NewJSON([]byte(payloadText))
	eventID, _ := model.ParseEventID(eventText)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceImported,
		ActorPrincipal: "principal-current-policy", Type: eventType, Audience: audience,
		Summary: "current policy", Payload: payload, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func currentPolicyWork(t *testing.T, ref model.WorkRef, channel model.ChannelID,
	participants model.ParticipantSnapshot, event model.Event, state model.WorkState,
	version uint64, iteration uint8, deadline int64,
) model.ReviewWork {
	t.Helper()
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel,
		Participants: participants, Version: version, Iteration: iteration, DeadlineUnixNano: deadline,
		State: state, StateData: event.Payload(), UpdatedBy: event.ID(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	return work
}
