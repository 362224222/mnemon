package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestArtifactStageCleanupClaimFencesPublishAndSurvivesResponseLoss(t *testing.T) {
	st, _, base := newArtifactStageCleanupStore(t, "claim-race")
	operation := reserveArtifactStageCleanupOperation(t, st, "operation-cleanup-claim",
		"run-cleanup-claim-race", "owner-cleanup-claim", base)
	begun := beginCleanupOperationStage(t, st, operation, base)
	leaseUntil, _ := operation.LeaseUntil()

	cutoff := leaseUntil
	scanAt := cutoff.Add(time.Second)
	scan := ScanArtifactStageCleanupSpec{
		Cutoff: cutoff, At: scanAt, MaxExamined: 1,
	}
	page, candidate := claimCleanupStage(t, st, scan, begun.Fence().Owner(),
		ArtifactStageStaged)
	assertCleanupClaimPage(t, page, 1)
	assertCleanupClaimReplay(t, st, scan, candidate)

	reclaimAt := scanAt.Add(time.Second)
	_, next := reclaimCleanupOperationStage(t, st, operation,
		"run-cleanup-claim-race", "owner-cleanup-claim-reclaimed", reclaimAt)
	assertNextCleanupGeneration(t, next, candidate)

	closure, root := peerInboxArtifactEmptyTreeClosure(t, "cleanup-claimed-old", 0,
		base.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	if _, err := st.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{Fence: begun.Fence(), Capture: capture,
			Closure: closure, At: reclaimAt}); err == nil {
		t.Fatal("claimed old generation was allowed to publish")
	}

	markCleanupStage(t, st, candidate, reclaimAt.Add(time.Second), false)
	markCleanupStage(t, st, candidate, reclaimAt.Add(time.Second), true)
	assertCleanupGenerationEvidence(t, st, operation.ID(), 2, 1)
}

func beginCleanupOperationStage(t *testing.T, st *Store, operation model.Operation,
	at time.Time,
) OperationArtifactStageResult {
	t.Helper()
	leaseUntil, _ := operation.LeaseUntil()
	begun, err := st.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{
			OperationID: operation.ID(), LeaseOwner: operation.LeaseOwner(),
			LeaseUntil: leaseUntil, At: at,
		})
	if err != nil {
		t.Fatal(err)
	}
	return begun
}

func claimCleanupStage(t *testing.T, st *Store, spec ScanArtifactStageCleanupSpec,
	owner artifactdomain.StageOwner, state ArtifactStageState,
) (ArtifactStageCleanupPage, ArtifactStageCleanupCandidate) {
	t.Helper()
	page, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err != nil {
		t.Fatalf("cleanup claim = (%#v,%v)", page, err)
	}
	candidates := page.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("cleanup candidates = %#v, want one", candidates)
	}
	candidate := candidates[0]
	if candidate.Owner() != owner || candidate.State() != state {
		t.Fatalf("cleanup candidate = %#v", candidate)
	}
	return page, candidate
}

func assertCleanupClaimPage(t *testing.T, page ArtifactStageCleanupPage, examined int) {
	t.Helper()
	if page.Examined() != examined {
		t.Fatalf("cleanup page examined = %d, want %d", page.Examined(), examined)
	}
}

func assertCleanupClaimReplay(t *testing.T, st *Store,
	spec ScanArtifactStageCleanupSpec, candidate ArtifactStageCleanupCandidate,
) {
	t.Helper()
	replayed, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err != nil {
		t.Fatalf("crash-after-claim replay = (%#v,%v)", replayed, err)
	}
	candidates := replayed.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("crash-after-claim candidates = %#v, want one", candidates)
	}
	got := candidates[0]
	if got.Owner() != candidate.Owner() ||
		!got.ClaimStartedAt().Equal(candidate.ClaimStartedAt()) {
		t.Fatalf("crash-after-claim replay = %#v", replayed)
	}
}

func reclaimCleanupOperationStage(t *testing.T, st *Store,
	operation model.Operation, run, owner string, at time.Time,
) (model.Operation, OperationArtifactStageResult) {
	t.Helper()
	reclaimed := startedOperation(t, operation.ID().String(),
		"key-"+operation.ID().String(), "request-"+operation.ID().String(),
		run, owner, at, nil)
	if _, err := st.ReserveOperation(context.Background(), reclaimed, at); err != nil {
		t.Fatal(err)
	}
	return reclaimed, beginCleanupOperationStage(t, st, reclaimed, at)
}

func assertNextCleanupGeneration(t *testing.T, next OperationArtifactStageResult,
	previous ArtifactStageCleanupCandidate,
) {
	t.Helper()
	if next.State() != ArtifactStageStaged ||
		next.Fence().Owner().Generation() != previous.Owner().Generation()+1 {
		t.Fatalf("Begin after cleanup claim = %#v", next)
	}
}

func markCleanupStage(t *testing.T, st *Store,
	candidate ArtifactStageCleanupCandidate, at time.Time, replayed bool,
) {
	t.Helper()
	marked, err := st.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{Candidate: candidate, At: at})
	if err != nil || marked.Replayed() != replayed {
		t.Fatalf("MarkArtifactStageCleaned(replayed=%t) = (%#v,%v)",
			replayed, marked, err)
	}
}

func assertCleanupGenerationEvidence(t *testing.T, st *Store,
	operation model.OperationID, wantGenerations, wantCleaned int,
) {
	t.Helper()
	var generations, cleaned int
	err := st.db.QueryRow(`SELECT COUNT(*),SUM(cleaned_at IS NOT NULL)
		FROM operation_artifact_stages WHERE operation_id=?`,
		operation.String()).Scan(&generations, &cleaned)
	if err != nil || generations != wantGenerations || cleaned != wantCleaned {
		t.Fatalf("cleanup generation evidence = generations %d cleaned %d: %v",
			generations, cleaned, err)
	}
}

func TestArtifactStageCleanupUsesBoundedProcessLocalKeyset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	t.Cleanup(func() {
		if st != nil {
			_ = st.Close()
		}
	})
	node, profile := bootstrapValues(t, "peer-cleanup-cursor",
		"principal-cleanup-cursor", "/workspace/cleanup-cursor")
	_, _ = activateTestNode(t, st, node, profile)
	base := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-cleanup-cursor", "running", base)

	for index := 0; index < 3; index++ {
		id := fmt.Sprintf("operation-cleanup-%02d", index)
		operation := reserveArtifactStageCleanupOperation(t, st, id,
			"run-cleanup-cursor", "owner-"+id, base)
		leaseUntil, _ := operation.LeaseUntil()
		if _, err := st.BeginOperationArtifactStage(context.Background(),
			BeginOperationArtifactStageSpec{OperationID: operation.ID(),
				LeaseOwner: operation.LeaseOwner(), LeaseUntil: leaseUntil, At: base}); err != nil {
			t.Fatal(err)
		}
	}

	cutoff := base.Add(90 * time.Second)
	at := base.Add(3 * time.Minute)
	spec := ScanArtifactStageCleanupSpec{Cutoff: cutoff, At: at, MaxExamined: 1}
	first, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err != nil || first.Examined() != 1 || len(first.Candidates()) != 1 ||
		first.Done() || first.Next().IsZero() {
		t.Fatalf("first bounded cleanup page = (%#v,%v)", first, err)
	}
	spec.After = first.Next()
	second, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err != nil || second.Examined() != 1 || len(second.Candidates()) != 1 ||
		second.Next().IsZero() ||
		first.Candidates()[0].Owner().CanonicalID() >=
			second.Candidates()[0].Owner().CanonicalID() {
		t.Fatalf("second bounded cleanup page = (%#v,%v)", second, err)
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	restart, err := st.ScanArtifactStageCleanupCandidates(context.Background(),
		ScanArtifactStageCleanupSpec{Cutoff: cutoff, At: at, MaxExamined: 1})
	if err != nil || restart.Examined() != 1 || len(restart.Candidates()) != 1 ||
		restart.Candidates()[0].Owner() != first.Candidates()[0].Owner() ||
		!restart.Candidates()[0].ClaimStartedAt().Equal(
			first.Candidates()[0].ClaimStartedAt()) {
		t.Fatalf("restart has no durable scan cursor = (%#v,%v)", restart, err)
	}
}

func newArtifactStageCleanupStore(t *testing.T, suffix string) (*Store, model.Profile, time.Time) {
	t.Helper()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-cleanup-"+suffix,
		"principal-cleanup-"+suffix, "/workspace/cleanup-"+suffix)
	_, _ = activateTestNode(t, st, node, profile)
	base := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	runID := "run-cleanup-" + suffix
	insertOperationAgentRun(t, st, profile, runID, "running", base)
	return st, profile, base
}

func reserveArtifactStageCleanupOperation(t *testing.T, st *Store, id, run, owner string,
	at time.Time,
) model.Operation {
	t.Helper()
	operation := startedOperation(t, id, "key-"+id, "request-"+id, run, owner, at, nil)
	if _, err := st.ReserveOperation(context.Background(), operation, at); err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestArtifactStageCleanupCandidateRejectsUnclaimedForgery(t *testing.T) {
	st, _, base := newArtifactStageCleanupStore(t, "forgery")
	operation := reserveArtifactStageCleanupOperation(t, st, "operation-cleanup-forgery",
		"run-cleanup-forgery", "owner-cleanup-forgery", base)
	leaseUntil, _ := operation.LeaseUntil()
	begun, err := st.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: operation.LeaseOwner(), LeaseUntil: leaseUntil, At: base})
	if err != nil {
		t.Fatal(err)
	}
	forged := ArtifactStageCleanupCandidate{owner: begun.Fence().Owner(),
		state: ArtifactStageStaged, updatedAt: base,
		claimStartedAt: leaseUntil.Add(time.Second)}
	if _, err := st.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{Candidate: forged,
			At: leaseUntil.Add(time.Second)}); !errors.Is(err, ErrArtifactStageFence) {
		t.Fatalf("unclaimed cleanup forgery error = %v", err)
	}
}

func TestPeerInboxArtifactCleanupClaimForcesNewGeneration(t *testing.T) {
	fixture, first, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-cleanup-inbox-generation", false)
	stageAt := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: first.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	cutoff := first.Fence().LeaseUntil()
	page, err := fixture.store.ScanArtifactStageCleanupCandidates(context.Background(),
		ScanArtifactStageCleanupSpec{Cutoff: cutoff, At: cutoff.Add(time.Second),
			MaxExamined: 1})
	if err != nil || len(page.Candidates()) != 1 ||
		page.Candidates()[0].Owner() != begun.Owner() {
		t.Fatalf("Inbox cleanup claim = (%#v,%v)", page, err)
	}
	candidate := page.Candidates()[0]

	reclaimed := mustClaimPeerInboxArtifact(t, fixture.store,
		"artifact-cleanup-inbox-reclaimer", cutoff)
	next, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: reclaimed.Fence(), At: cutoff})
	if err != nil || next.Owner().Generation() != begun.Owner().Generation()+1 {
		t.Fatalf("Inbox Begin after cleanup claim = (%#v,%v)", next, err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{Fence: first.Fence(), Owner: begun.Owner(),
			Closure: closure, At: cutoff}); err == nil {
		t.Fatal("claimed old Inbox generation was allowed to publish")
	}
	if _, err := fixture.store.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{Candidate: candidate,
			At: cutoff.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactStageCleanupClaimsOnlyTerminalUnacceptedPublishing(t *testing.T) {
	t.Run("rejected operation", testRejectedOperationPublishingCleanup)
	t.Run("quarantined peer Inbox", testQuarantinedPeerInboxPublishingCleanup)
}

func testRejectedOperationPublishingCleanup(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, _ := fixture.reserveOffer(t, "cleanup-rejected-publishing", nil)
	stageAt := fixture.now.Add(-10 * time.Second)
	begun := beginCleanupOperationStage(t, fixture.store, operation, stageAt)
	closure, root, block := newArtifactSourceClosure(t,
		"cleanup-rejected-publishing", []byte("terminal unaccepted bytes"),
		stageAt.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	preparedAt := stageAt.Add(time.Second)
	if _, err := fixture.store.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{
			Fence: begun.Fence(), Capture: capture, Closure: closure, At: preparedAt,
		}); err != nil {
		t.Fatal(err)
	}
	assertNoCleanupStages(t, fixture.store, ScanArtifactStageCleanupSpec{
		Cutoff: preparedAt.Add(time.Second), At: preparedAt.Add(2 * time.Second),
		MaxExamined: 1,
	}, "started publishing")

	rejectedAt := preparedAt.Add(3 * time.Second)
	rejection, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: operation.ID(), Code: "internal",
		Message: "terminal unaccepted cleanup fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		operation.LeaseOwner(), rejectedAt, rejection.JSON()); err != nil {
		t.Fatal(err)
	}
	candidate := claimPublishingCleanupStage(t, fixture.store,
		ScanArtifactStageCleanupSpec{
			Cutoff: rejectedAt.Add(time.Second), At: rejectedAt.Add(2 * time.Second),
			MaxExamined: 1,
		}, begun.Fence().Owner(), "rejected")
	markCleanupStage(t, fixture.store, candidate, rejectedAt.Add(2*time.Second), false)
	assertArtifactStagingSweep(t, fixture.store, artifactdomain.StagingSweepSpec{
		Cutoff: rejectedAt.Add(time.Second), At: rejectedAt.Add(3 * time.Second),
		MaxRoots: 4,
	}, artifactdomain.StagingSweepResult{OwnerProjections: 1, Roots: 1, Blocks: 1},
		"rejected publishing")
	assertArtifactCleanupRows(t, fixture.store, 0, []artifactCleanupRowCheck{
		{"operation_artifact_roots", "root_digest", root.RootDigest.String()},
		{"artifact_roots", "root_digest", root.RootDigest.String()},
		{"artifact_blocks", "block_digest", block.Digest.String()},
	})
}

func testQuarantinedPeerInboxPublishingCleanup(t *testing.T) {
	fixture, claim, root, closure := newPeerInboxArtifactClosureClaim(t,
		"cleanup-quarantined-publishing", false)
	stageAt := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(), Closure: closure, At: stageAt,
		}); err != nil {
		t.Fatal(err)
	}
	assertNoCleanupStages(t, fixture.store, ScanArtifactStageCleanupSpec{
		Cutoff: stageAt.Add(time.Second), At: stageAt.Add(2 * time.Second),
		MaxExamined: 1,
	}, "waiting publishing")

	quarantinedAt := stageAt.Add(3 * time.Second)
	if _, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(),
		QuarantinePeerInboxArtifactSpec{
			Fence: claim.Fence(), Diagnostic: PeerInboxArtifactManifestInvalid,
			At: quarantinedAt,
		}); err != nil {
		t.Fatal(err)
	}
	candidate := claimPublishingCleanupStage(t, fixture.store,
		ScanArtifactStageCleanupSpec{
			Cutoff: quarantinedAt.Add(time.Second),
			At:     quarantinedAt.Add(2 * time.Second), MaxExamined: 1,
		}, begun.Owner(), "quarantined")
	cas, err := artifactdomain.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cas.RemoveStage(begun.Owner()); err != nil {
		t.Fatal(err)
	}
	markCleanupStage(t, fixture.store, candidate, quarantinedAt.Add(2*time.Second), false)
	sweepAt := claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL + time.Second)
	assertArtifactStagingSweep(t, fixture.store, artifactdomain.StagingSweepSpec{
		Cutoff: quarantinedAt.Add(time.Second), At: sweepAt, MaxRoots: 4,
	}, artifactdomain.StagingSweepResult{
		ExpiredPins: 1, OwnerProjections: 1, Roots: 1, Blocks: 1,
	}, "quarantined publishing")
	assertArtifactCleanupRows(t, fixture.store, 0, []artifactCleanupRowCheck{
		{"peer_inbox_artifact_roots", "root_digest", root.RootDigest.String()},
		{"artifact_roots", "root_digest", root.RootDigest.String()},
		{"artifact_root_blocks", "root_digest", root.RootDigest.String()},
		{"artifact_blocks", "block_digest", closure.Blocks[0].Digest.String()},
	})
}

func assertNoCleanupStages(t *testing.T, st *Store,
	spec ScanArtifactStageCleanupSpec, phase string,
) {
	t.Helper()
	page, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err != nil || len(page.Candidates()) != 0 {
		t.Fatalf("%s cleanup = (%#v,%v)", phase, page, err)
	}
}

func claimPublishingCleanupStage(t *testing.T, st *Store,
	spec ScanArtifactStageCleanupSpec, owner artifactdomain.StageOwner, phase string,
) ArtifactStageCleanupCandidate {
	t.Helper()
	page, err := st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	if err == nil && len(page.Candidates()) == 0 {
		page, err = st.ScanArtifactStageCleanupCandidates(context.Background(), spec)
	}
	if err != nil {
		t.Fatalf("%s publishing cleanup = (%#v,%v)", phase, page, err)
	}
	candidates := page.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("%s publishing cleanup candidates = %#v", phase, candidates)
	}
	candidate := candidates[0]
	if candidate.Owner() != owner || candidate.State() != ArtifactStagePublishing {
		t.Fatalf("%s publishing cleanup = %#v", phase, page)
	}
	return candidate
}

func assertArtifactStagingSweep(t *testing.T, st *Store,
	spec artifactdomain.StagingSweepSpec, want artifactdomain.StagingSweepResult,
	phase string,
) {
	t.Helper()
	got, err := st.SweepArtifactStaging(context.Background(), spec)
	if err != nil || got.ExpiredPins != want.ExpiredPins ||
		got.OwnerProjections != want.OwnerProjections || got.Roots != want.Roots ||
		got.Blocks != want.Blocks {
		t.Fatalf("%s relational sweep = (%#v,%v), want %#v", phase, got, err, want)
	}
}

type artifactCleanupRowCheck struct {
	table  string
	column string
	value  string
}

func assertArtifactCleanupRows(t *testing.T, st *Store, want int,
	checks []artifactCleanupRowCheck,
) {
	t.Helper()
	for _, check := range checks {
		var count int
		err := st.db.QueryRow(`SELECT COUNT(*) FROM `+check.table+
			` WHERE `+check.column+`=?`, check.value).Scan(&count)
		if err != nil || count != want {
			t.Fatalf("%s count = (%d,%v), want %d", check.table, count, err, want)
		}
	}
}

func TestCommittedOperationOldStagedGenerationRemainsCleanupEligible(t *testing.T) {
	state := prepareCommittedOperationOldCleanupStage(t)
	cutoff := state.reclaimAt.Add(500 * time.Millisecond)
	_, candidate := claimCleanupStage(t, state.fixture.store,
		ScanArtifactStageCleanupSpec{
			Cutoff: cutoff, At: state.reclaimAt.Add(4 * time.Second), MaxExamined: 2,
		}, state.first.Fence().Owner(), ArtifactStageStaged)
	markCleanupStage(t, state.fixture.store, candidate,
		state.reclaimAt.Add(4*time.Second), false)
	if _, err := state.fixture.store.GetVerifiedArtifactRoot(context.Background(),
		state.root.RootDigest); err != nil {
		t.Fatalf("old generation cleanup removed final root: %v", err)
	}
	assertLatestOperationArtifactStage(t, state.fixture.store, state.operation.ID(),
		ArtifactStageReady)
}

type committedOperationOldCleanupStage struct {
	fixture   *acceptanceFixture
	operation model.Operation
	first     OperationArtifactStageResult
	root      VerifiedArtifactRoot
	reclaimAt time.Time
}

func prepareCommittedOperationOldCleanupStage(
	t *testing.T,
) committedOperationOldCleanupStage {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	operation, _ := fixture.reserveOffer(t, "cleanup-committed-old-stage", nil)
	firstLease, _ := operation.LeaseUntil()
	first := beginCleanupOperationStage(t, fixture.store, operation,
		fixture.now.Add(-20*time.Second))
	reclaimAt := firstLease
	nextLease := reclaimAt.Add(time.Minute)
	reclaimed := reserveReclaimedCleanupOperation(t, fixture.store, operation,
		"cleanup-committed-reclaimer", nextLease, reclaimAt)
	second := beginCleanupOperationStage(t, fixture.store, reclaimed, reclaimAt)
	if second.Fence().Owner().Generation() != first.Fence().Owner().Generation()+1 {
		t.Fatalf("second operation stage = %#v", second)
	}
	root := publishCommittedCleanupOperationArtifact(t, fixture, operation,
		reclaimed, second, reclaimAt)
	return committedOperationOldCleanupStage{
		fixture: fixture, operation: operation, first: first, root: root,
		reclaimAt: reclaimAt,
	}
}

func reserveReclaimedCleanupOperation(t *testing.T, st *Store,
	operation model.Operation, owner string, leaseUntil, at time.Time,
) model.Operation {
	t.Helper()
	reclaimed, err := model.NewOperation(model.OperationSpec{
		ID: operation.ID(), ProfileID: operation.ProfileID(),
		AgentRunID: operation.AgentRunID(), ClientKeyHash: operation.ClientKeyHash(),
		Kind: operation.Kind(), RequestDigest: operation.RequestDigest(),
		Status: model.OperationStarted, LeaseOwner: owner, LeaseUntil: &leaseUntil,
		CreatedAt: operation.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReserveOperation(context.Background(), reclaimed, at); err != nil {
		t.Fatal(err)
	}
	return reclaimed
}

func publishCommittedCleanupOperationArtifact(t *testing.T,
	fixture *acceptanceFixture, operation, reclaimed model.Operation,
	second OperationArtifactStageResult, reclaimAt time.Time,
) VerifiedArtifactRoot {
	t.Helper()
	closure, root := peerInboxArtifactEmptyTreeClosure(t,
		"cleanup-committed-old-stage", 0, fixture.now.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	if _, err := fixture.store.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{Fence: second.Fence(), Capture: capture,
			Closure: closure, At: reclaimAt.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	ref, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	authority := &LocalOperationAuthority{
		operation.ID(), operation.Kind(), operation.RequestDigest(), reclaimed.LeaseOwner(),
	}
	acceptance := fixture.offer(t, authority, "cleanup-committed-old-stage",
		fixture.reviewers, []model.ArtifactRef{ref}, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), acceptance,
		reclaimAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	cas, err := artifactdomain.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	stage, err := cas.OpenStage(second.Fence().Owner())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.Put(root.ManifestDigest, root.Manifest.Bytes()); err != nil {
		t.Fatal(err)
	}
	published, err := RebuildArtifactClosure(context.Background(), closure,
		reclaimAt.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Publish(context.Background(), published); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.MarkOperationArtifactReady(context.Background(),
		MarkOperationArtifactReadySpec{Fence: second.Fence(),
			At: reclaimAt.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	return root
}

func assertLatestOperationArtifactStage(t *testing.T, st *Store,
	operation model.OperationID, want ArtifactStageState,
) {
	t.Helper()
	var state string
	err := st.db.QueryRow(`SELECT state FROM operation_artifact_stages
		WHERE operation_id=? ORDER BY generation DESC LIMIT 1`,
		operation.String()).Scan(&state)
	if err != nil || ArtifactStageState(state) != want {
		t.Fatalf("current committed stage = (%q,%v), want %s", state, err, want)
	}
}

func TestArtifactStagingSweepRequiresEveryOwnerGenerationCleaned(t *testing.T) {
	state := prepareArtifactGenerationGuard(t)
	_, candidate := claimCleanupStage(t, state.fixture.store,
		ScanArtifactStageCleanupSpec{
			Cutoff: state.rejectedAt.Add(time.Second),
			At:     state.rejectedAt.Add(2 * time.Second), MaxExamined: 1,
		}, state.first.Fence().Owner(), ArtifactStageStaged)
	markCleanupStage(t, state.fixture.store, candidate,
		state.rejectedAt.Add(2*time.Second), false)

	assertArtifactStagingSweep(t, state.fixture.store,
		artifactdomain.StagingSweepSpec{
			Cutoff: state.rejectedAt.Add(time.Second),
			At:     state.rejectedAt.Add(3 * time.Second), MaxRoots: 4,
		}, artifactdomain.StagingSweepResult{}, "current uncleaned generation")
	if _, err := state.fixture.store.db.Exec(`DELETE FROM operation_artifact_roots
		WHERE operation_id=? AND root_digest=?`, state.operation.ID().String(),
		state.root.RootDigest.String()); err == nil {
		t.Fatal("schema allowed old cleaned generation to delete current projection")
	}
	assertArtifactCleanupRows(t, state.fixture.store, 1, []artifactCleanupRowCheck{
		{"operation_artifact_roots", "root_digest", state.root.RootDigest.String()},
		{"artifact_roots", "root_digest", state.root.RootDigest.String()},
		{"artifact_root_blocks", "root_digest", state.root.RootDigest.String()},
		{"artifact_blocks", "block_digest", state.block.Digest.String()},
	})
}

type artifactGenerationGuard struct {
	fixture    *acceptanceFixture
	operation  model.Operation
	first      OperationArtifactStageResult
	root       VerifiedArtifactRoot
	block      VerifiedArtifactBlock
	rejectedAt time.Time
}

func prepareArtifactGenerationGuard(t *testing.T) artifactGenerationGuard {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	operation, _ := fixture.reserveOffer(t, "cleanup-generation-guard", nil)
	firstLease, _ := operation.LeaseUntil()
	first := beginCleanupOperationStage(t, fixture.store, operation,
		fixture.now.Add(-20*time.Second))
	secondLease := firstLease.Add(time.Minute)
	reclaimed := reserveReclaimedCleanupOperation(t, fixture.store, operation,
		"cleanup-generation-current", secondLease, firstLease)
	second := beginCleanupOperationStage(t, fixture.store, reclaimed, firstLease)
	closure, root, block := newArtifactSourceClosure(t,
		"cleanup-generation-guard", []byte("current generation bytes"),
		fixture.now.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	preparedAt := firstLease.Add(time.Second)
	if _, err := fixture.store.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{
			Fence: second.Fence(), Capture: capture, Closure: closure, At: preparedAt,
		}); err != nil {
		t.Fatal(err)
	}
	rejectedAt := preparedAt.Add(time.Second)
	rejection, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: operation.ID(), Code: "internal",
		Message: "generation guard fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		reclaimed.LeaseOwner(), rejectedAt, rejection.JSON()); err != nil {
		t.Fatal(err)
	}
	return artifactGenerationGuard{
		fixture: fixture, operation: operation, first: first, root: root,
		block: block, rejectedAt: rejectedAt,
	}
}
