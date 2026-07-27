package node

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactStageCleanupWorkerRemovesOnlyClaimedOwnersThenSweepsAndPrunes(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-expired", state: store.ArtifactStageStaged},
		cleanupStageSpec{id: "cleanup-publishing", state: store.ArtifactStagePublishing},
	)
	expiredOwner := fixture.owner(t, "cleanup-expired")
	expiredPath := fixture.putStage(t, expiredOwner, "expired stage")
	publishingOwner := fixture.owner(t, "cleanup-publishing")
	publishingPath := fixture.putStage(t, publishingOwner, "publishing stage")
	oldStageTime := fixture.now.Add(-2 * time.Hour)
	if err := os.Chtimes(publishingPath, oldStageTime, oldStageTime); err != nil {
		t.Fatal(err)
	}
	temp := fixture.writeOldTemp(t, "00000000000000000000000000000001")

	finalContent := []byte("accepted final object")
	finalDigest := model.Sum(finalContent)
	if _, err := fixture.cas.Put(finalDigest, finalContent); err != nil {
		t.Fatal(err)
	}
	finalPath := fixture.finalPath(finalDigest)
	if err := os.Chmod(finalPath, 0o644); err != nil {
		t.Fatal(err)
	}

	worker := fixture.worker(t, artifactStageCleanupOptions{})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPathMissing(t, expiredPath)
	assertPathPresent(t, publishingPath)
	assertPathMissing(t, temp)
	fixture.assertCleaned(t, "cleanup-expired", true)
	fixture.assertCleaned(t, "cleanup-publishing", false)
	if err := os.Chmod(finalPath, 0o600); err != nil {
		t.Fatalf("final CAS object was enumerated or removed: %v", err)
	}
	if got, err := fixture.cas.Read(finalDigest, len(finalContent)); err != nil ||
		string(got) != string(finalContent) {
		t.Fatalf("final CAS object changed = (%q, %v)", got, err)
	}
}

func TestArtifactStageCleanupWorkerReconcilesPhysicalOrphansFailClosed(t *testing.T) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-physical-relational", state: store.ArtifactStageStaged})
	relationalPath := fixture.putStage(t,
		fixture.owner(t, "cleanup-physical-relational"), "relational stage")
	orphanID, err := model.ParseOperationID("cleanup-physical-orphan")
	if err != nil {
		t.Fatal(err)
	}
	orphanOwner, err := artifact.NewOperationStageOwner(orphanID, 7)
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := fixture.putStage(t, orphanOwner, "orphan stage")
	staging := filepath.Join(fixture.cas.Root(), ".staging")
	emptyPath := filepath.Join(staging, strings.Repeat("f", 64))
	if err := os.Mkdir(emptyPath, 0o700); err != nil {
		t.Fatal(err)
	}
	diagnosticPath := filepath.Join(staging, strings.Repeat("0", 64))
	if err := os.Mkdir(diagnosticPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diagnosticPath, "unexpected"),
		[]byte("unowned"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := fixture.now.Add(-2 * time.Hour)
	for _, path := range []string{orphanPath, emptyPath, diagnosticPath} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	temp := fixture.writeOldTemp(t, "00000000000000000000000000000007")

	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 1})
	diagnostics := 0
	for range 16 {
		if err := worker.runCycle(context.Background()); err != nil {
			if !errors.Is(err, artifact.ErrCASCorruption) {
				t.Fatal(err)
			}
			diagnostics++
		}
	}
	if diagnostics == 0 {
		t.Fatal("unsafe physical stage produced no observable diagnostic")
	}
	assertPathMissing(t, relationalPath)
	assertPathMissing(t, orphanPath)
	assertPathMissing(t, emptyPath)
	assertPathPresent(t, diagnosticPath)
	assertPathMissing(t, temp)
	fixture.assertCleaned(t, "cleanup-physical-relational", true)
}

func TestArtifactStageCleanupWorkerReplaysClaimAfterCrashAndMissingDirectory(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-claimed-present", state: store.ArtifactStageStaged},
		cleanupStageSpec{id: "cleanup-claimed-missing", state: store.ArtifactStageStaged},
	)
	presentPath := fixture.putStage(t,
		fixture.owner(t, "cleanup-claimed-present"), "claimed stage")
	page := fixture.claim(t, 2)
	if page.Examined() != 2 || len(page.Candidates()) != 2 {
		t.Fatalf("pre-crash claim page = %#v", page)
	}

	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 2})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPathMissing(t, presentPath)
	fixture.assertCleaned(t, "cleanup-claimed-present", true)
	fixture.assertCleaned(t, "cleanup-claimed-missing", true)
}

func TestArtifactStageCleanupWorkerReplaysRemoveAndMarkResponseLoss(t *testing.T) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-mark-committed", state: store.ArtifactStageStaged},
		cleanupStageSpec{id: "cleanup-mark-not-sent", state: store.ArtifactStageStaged},
	)
	for _, id := range []string{"cleanup-mark-committed", "cleanup-mark-not-sent"} {
		fixture.putStage(t, fixture.owner(t, id), id)
	}
	page := fixture.claim(t, 2)
	candidates := make(map[string]store.ArtifactStageCleanupCandidate, 2)
	for _, candidate := range page.Candidates() {
		candidates[candidate.Owner().CanonicalID()] = candidate
	}
	committed := candidates["cleanup-mark-committed"]
	notSent := candidates["cleanup-mark-not-sent"]
	if committed.Owner().IsZero() || notSent.Owner().IsZero() {
		t.Fatalf("claimed cleanup candidates = %#v", page.Candidates())
	}

	if err := fixture.cas.RemoveStage(committed.Owner()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.MarkArtifactStageCleaned(context.Background(),
		store.MarkArtifactStageCleanedSpec{Candidate: committed, At: fixture.now}); err != nil {
		t.Fatal(err)
	}
	// Model a crash after the other exact remove but before its mark reached the
	// Store. The next cycle must accept the missing directory as a replay.
	if err := fixture.cas.RemoveStage(notSent.Owner()); err != nil {
		t.Fatal(err)
	}

	fixture.clock.now = fixture.now.Add(time.Second)
	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 2})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.assertCleaned(t, "cleanup-mark-committed", true)
	fixture.assertCleaned(t, "cleanup-mark-not-sent", true)
	replayed, err := fixture.store.MarkArtifactStageCleaned(context.Background(),
		store.MarkArtifactStageCleanedSpec{
			Candidate: committed, At: fixture.clock.now})
	if err != nil || !replayed.Replayed() {
		t.Fatalf("lost mark response replay = (%#v, %v)", replayed, err)
	}
}

func TestArtifactStageCleanupWorkerIsolatesUnsafeStageWithoutBlockingTempPrune(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-unsafe-a", state: store.ArtifactStageStaged},
		cleanupStageSpec{id: "cleanup-unsafe-b", state: store.ArtifactStageStaged})
	stagePath := fixture.putStage(t, fixture.owner(t, "cleanup-unsafe-a"), "unsafe stage")
	laterPath := fixture.putStage(t, fixture.owner(t, "cleanup-unsafe-b"), "later stage")
	if err := os.Chmod(stagePath, 0o755); err != nil {
		t.Fatal(err)
	}
	temp := fixture.writeOldTemp(t, "00000000000000000000000000000002")

	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 2})
	err := worker.runCycle(context.Background())
	if !errors.Is(err, artifact.ErrCASCorruption) {
		t.Fatalf("unsafe stage cleanup cycle error = %v", err)
	}
	fixture.assertCleaned(t, "cleanup-unsafe-a", false)
	fixture.assertCleaned(t, "cleanup-unsafe-b", true)
	assertPathMissing(t, laterPath)
	assertPathMissing(t, temp)

	if err := os.Chmod(stagePath, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.clock.now = fixture.now.Add(time.Second)
	for range 2 {
		if err := worker.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	fixture.assertCleaned(t, "cleanup-unsafe-a", true)
	assertPathMissing(t, stagePath)
	assertPathMissing(t, temp)
}

func TestArtifactStageCleanupWorkerPrunesTempsAfterRelationalSweepFailure(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-order", state: store.ArtifactStageStaged})
	stagePath := fixture.putStage(t, fixture.owner(t, "cleanup-order"), "ordered stage")
	temp := fixture.writeOldTemp(t, "00000000000000000000000000000006")
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	if _, err := db.Exec(
		"ALTER TABLE artifact_blocks RENAME TO test_artifact_blocks_unavailable"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	worker := fixture.worker(t, artifactStageCleanupOptions{})
	err := worker.runCycle(context.Background())
	if !errors.Is(err, artifact.ErrStagingStoreInvariant) {
		t.Fatalf("relational sweep failure = %v", err)
	}
	assertPathMissing(t, stagePath)
	fixture.assertCleaned(t, "cleanup-order", true)
	assertPathMissing(t, temp)
}

func TestArtifactStageCleanupWorkerBoundsOwnerAndTempWork(t *testing.T) {
	fixture := newArtifactStageCleanupWorkerFixture(t,
		cleanupStageSpec{id: "cleanup-bound-a", state: store.ArtifactStageStaged},
		cleanupStageSpec{id: "cleanup-bound-b", state: store.ArtifactStageStaged},
	)
	for _, id := range []string{"cleanup-bound-a", "cleanup-bound-b"} {
		fixture.putStage(t, fixture.owner(t, id), id)
	}
	fixture.writeOldTemp(t, "00000000000000000000000000000003")
	fixture.writeOldTemp(t, "00000000000000000000000000000004")

	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 1, MaxTemps: 1})
	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fixture.cleanedCount(t); got != 1 {
		t.Fatalf("cleaned owner count = %d", got)
	}
	if got := fixture.stageDirectoryCount(t); got != 1 {
		t.Fatalf("remaining physical stage count = %d", got)
	}
	if got := fixture.tempCount(t); got != 1 {
		t.Fatalf("remaining recognizable temp count = %d", got)
	}

	if candidate, err := newArtifactStageCleanupWorker(fixture.store, fixture.cas,
		fixture.clock, artifactStageCleanupOptions{
			MaxExamined: maximumArtifactStageCleanupExamined + 1,
		}); candidate != nil || !errors.Is(err, ErrArtifactStageCleanup) {
		t.Fatalf("oversized worker = (%#v, %v)", candidate, err)
	}
}

func TestArtifactStageCleanupWorkerRunsImmediatelyCancelsAndRunsOnlyOnce(t *testing.T) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	temp := fixture.writeOldTemp(t, "00000000000000000000000000000005")
	worker := fixture.worker(t, artifactStageCleanupOptions{Period: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	awaitPathMissing(t, temp)

	if err := worker.Run(context.Background()); !errors.Is(err, ErrArtifactStageCleanupRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup worker did not stop after cancellation")
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrArtifactStageCleanupRunning) {
		t.Fatalf("Run after stop error = %v", err)
	}
}

func TestArtifactStageCleanupWorkerRunReportsOwnerAndGlobalFailure(
	t *testing.T,
) {
	t.Run("owner-local failure stops worker after independent maintenance", func(t *testing.T) {
		fixture := newArtifactStageCleanupWorkerFixture(t,
			cleanupStageSpec{id: "cleanup-run-owner-local", state: store.ArtifactStageStaged})
		stagePath := fixture.putStage(t,
			fixture.owner(t, "cleanup-run-owner-local"), "unsafe")
		if err := os.Chmod(stagePath, 0o755); err != nil {
			t.Fatal(err)
		}
		temp := fixture.writeOldTemp(t, "00000000000000000000000000000008")
		worker := fixture.worker(t, artifactStageCleanupOptions{Period: time.Hour})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- worker.Run(ctx) }()
		awaitPathMissing(t, temp)
		select {
		case err := <-done:
			if !errors.Is(err, artifact.ErrCASCorruption) {
				t.Fatalf("owner-local failure = %v", err)
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("owner-local failure was not reported to supervisor")
		}
		cancel()
	})

	t.Run("global Store failure stops worker", func(t *testing.T) {
		fixture := newArtifactStageCleanupWorkerFixture(t)
		db := openArtifactStageCleanupDatabase(t, fixture.database)
		if _, err := db.Exec(
			"ALTER TABLE artifact_blocks RENAME TO test_artifact_blocks_unavailable"); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		worker := fixture.worker(t, artifactStageCleanupOptions{Period: time.Hour})
		if err := worker.Run(context.Background()); !errors.Is(
			err, artifact.ErrStagingStoreInvariant) {
			t.Fatalf("global Store failure = %v", err)
		}
	})
}

func TestOpenDaemonArtifactRuntimeComposesNodeOwnedCleanup(t *testing.T) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	cas, worker, err := openDaemonArtifactRuntime(fixture.store, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	if cas.Root() != filepath.Join(filepath.Dir(fixture.store.Path()), "objects", "sha256") ||
		worker == nil || worker.store != fixture.store || worker.cas != cas ||
		worker.clock != fixture.clock {
		t.Fatalf("Artifact runtime = (%q, %#v)", cas.Root(), worker)
	}
}

type cleanupStageSpec struct {
	id    string
	state store.ArtifactStageState
}

type artifactStageCleanupWorkerFixture struct {
	store    *store.Store
	cas      *artifact.CAS
	clock    *artifactStageCleanupClock
	database string
	now      time.Time
	identity testkit.Identity
}

type artifactStageCleanupClock struct{ now time.Time }

func (clock *artifactStageCleanupClock) Now() time.Time { return clock.now }

func newArtifactStageCleanupWorkerFixture(t *testing.T,
	stages ...cleanupStageSpec,
) *artifactStageCleanupWorkerFixture {
	t.Helper()
	root := t.TempDir()
	nodeState := filepath.Join(root, "node")
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(nodeState, "node.db")
	base := time.Date(2026, 7, 26, 8, 0, 0, 0, time.UTC)
	identity := testkit.NewIdentity(t, "artifact-stage-cleanup-node")
	st := initializeArtifactStageCleanupStore(t, database, workspace, base, identity)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	insertArtifactStageCleanupRows(t, database, base, stages)
	st, err := store.OpenExisting(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cas, err := artifact.NewCAS(filepath.Join(nodeState, "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	now := base.Add(2 * time.Hour)
	return &artifactStageCleanupWorkerFixture{
		store: st, cas: cas, clock: &artifactStageCleanupClock{now: now},
		database: database, now: now, identity: identity,
	}
}

func initializeArtifactStageCleanupStore(t *testing.T, database, workspace string,
	at time.Time, identity testkit.Identity,
) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	nodeValue, err := model.NewNode(model.NodeSpec{
		PeerID: identity.PeerID(), OriginEpoch: identity.OriginEpoch(), NextOriginSequence: 1,
		ActiveAssetRevision: "artifact-stage-cleanup-test",
		CreatedAt:           at, UpdatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{
		ID: model.TeamworkProfileID(), Principal: "principal-artifact-stage-cleanup",
		WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime:             model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("artifact-stage-cleanup-credential")),
		ActiveAssetRevision: "artifact-stage-cleanup-test",
		HandlingBudget:      model.DefaultHandlingBudget().JSON(),
		CreatedAt:           at, UpdatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
	spec := profile.Spec()
	spec.Enabled = true
	spec.UpdatedAt = at.Add(time.Second)
	enabled, err := model.NewProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActivateProfile(context.Background(), enabled,
		profile.UpdatedAt(), spec.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	return st
}

func insertArtifactStageCleanupRows(t *testing.T, database string, base time.Time,
	stages []cleanupStageSpec,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, database)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO agent_runs(
		run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		"run-artifact-stage-cleanup", model.TeamworkProfileID().String(), []byte(`{}`),
		"test", string(model.RuntimeCodexAppServer), []byte(`{}`), []byte(`{}`),
		"running", cleanupStoreTime(base)); err != nil {
		t.Fatal(err)
	}
	leaseUntil := base.Add(30 * time.Minute)
	for _, stage := range stages {
		if stage.state != store.ArtifactStageStaged &&
			stage.state != store.ArtifactStagePublishing {
			t.Fatalf("unsupported cleanup stage fixture state %q", stage.state)
		}
		if _, err := model.ParseOperationID(stage.id); err != nil {
			t.Fatal(err)
		}
		leaseOwner := "owner-" + stage.id
		if _, err := db.Exec(`INSERT INTO operations(
			operation_id,profile_id,agent_run_id,client_key_hash,kind,
			request_digest,status,lease_owner,lease_until,created_at
		) VALUES(?,?,?,?,? ,?,?,?,?,?)`,
			stage.id, model.TeamworkProfileID().String(), "run-artifact-stage-cleanup",
			model.Sum([]byte("key-"+stage.id)).Bytes(), "teamwork.offer",
			model.Sum([]byte("request-"+stage.id)).Bytes(), "started", leaseOwner,
			cleanupStoreTime(leaseUntil), cleanupStoreTime(base)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO operation_artifact_stages(
			operation_id,generation,state,lease_owner,lease_until,created_at,updated_at
		) VALUES(?,1,'staged',?,?,?,?)`, stage.id, leaseOwner,
			cleanupStoreTime(leaseUntil), cleanupStoreTime(base),
			cleanupStoreTime(base)); err != nil {
			t.Fatal(err)
		}
		if stage.state == store.ArtifactStagePublishing {
			if _, err := db.Exec(`UPDATE operation_artifact_stages
				SET state='publishing',capture_digest=?,updated_at=?
				WHERE operation_id=? AND generation=1`,
				model.Sum([]byte("capture-"+stage.id)).Bytes(),
				cleanupStoreTime(base.Add(time.Minute)), stage.id); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func openArtifactStageCleanupDatabase(t *testing.T, database string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func cleanupStoreTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func (fixture *artifactStageCleanupWorkerFixture) worker(t *testing.T,
	options artifactStageCleanupOptions,
) *artifactStageCleanupWorker {
	t.Helper()
	worker, err := newArtifactStageCleanupWorker(
		fixture.store, fixture.cas, fixture.clock, options)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func (fixture *artifactStageCleanupWorkerFixture) owner(t *testing.T,
	id string,
) artifact.StageOwner {
	t.Helper()
	operationID, err := model.ParseOperationID(id)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := artifact.NewOperationStageOwner(operationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func (fixture *artifactStageCleanupWorkerFixture) putStage(t *testing.T,
	owner artifact.StageOwner, value string,
) string {
	t.Helper()
	before := fixture.stageDirectoryNames(t)
	stage, err := fixture.cas.OpenStage(owner)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte(value)
	if _, err := stage.Put(model.Sum(content), content); err != nil {
		t.Fatal(err)
	}
	for name := range fixture.stageDirectoryNames(t) {
		if _, found := before[name]; !found {
			return filepath.Join(fixture.cas.Root(), ".staging", name)
		}
	}
	t.Fatal("staged Put created no exact owner directory")
	return ""
}

func (fixture *artifactStageCleanupWorkerFixture) stageDirectoryNames(
	t *testing.T,
) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(fixture.cas.Root(), ".staging"))
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		names[entry.Name()] = struct{}{}
	}
	return names
}

func (fixture *artifactStageCleanupWorkerFixture) stageDirectoryCount(t *testing.T) int {
	t.Helper()
	return len(fixture.stageDirectoryNames(t))
}

func (fixture *artifactStageCleanupWorkerFixture) writeOldTemp(t *testing.T,
	hexID string,
) string {
	t.Helper()
	path := filepath.Join(fixture.cas.Root(), ".tmp", "cas-"+hexID+".tmp")
	if err := os.WriteFile(path, []byte("old temp"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := fixture.now.Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	return path
}

func (fixture *artifactStageCleanupWorkerFixture) tempCount(t *testing.T) int {
	t.Helper()
	temps, err := fixture.cas.TempFiles()
	if err != nil {
		t.Fatal(err)
	}
	return len(temps)
}

func (fixture *artifactStageCleanupWorkerFixture) finalPath(digest model.Digest) string {
	value := strings.TrimPrefix(digest.String(), "sha256:")
	return filepath.Join(fixture.cas.Root(), value[:2], value)
}

func (fixture *artifactStageCleanupWorkerFixture) claim(
	t *testing.T, maximum int,
) store.ArtifactStageCleanupPage {
	t.Helper()
	page, err := fixture.store.ScanArtifactStageCleanupCandidates(context.Background(),
		store.ScanArtifactStageCleanupSpec{
			Cutoff: fixture.now.Add(-time.Hour), At: fixture.now,
			MaxExamined: maximum,
		})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func (fixture *artifactStageCleanupWorkerFixture) assertCleaned(t *testing.T,
	id string, want bool,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	var cleaned int
	if err := db.QueryRow(`SELECT cleaned_at IS NOT NULL
		FROM operation_artifact_stages WHERE operation_id=? AND generation=1`,
		id).Scan(&cleaned); err != nil {
		t.Fatal(err)
	}
	if got := cleaned != 0; got != want {
		t.Fatalf("stage %s cleaned = %t, want %t", id, got, want)
	}
}

func (fixture *artifactStageCleanupWorkerFixture) cleanedCount(t *testing.T) int {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM operation_artifact_stages
		WHERE cleaned_at IS NOT NULL`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path %q remains: %v", path, err)
	}
}

func assertPathPresent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("path %q is missing: %v", path, err)
	}
}

func awaitPathMissing(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("path %q was not removed", path)
}
