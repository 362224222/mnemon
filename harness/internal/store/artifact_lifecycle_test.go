package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestOperationArtifactLifecycleIsAtomicRecoverableAndAcceptanceGated(t *testing.T) {
	test := newOperationArtifactLifecycleTest(t)
	test.begin(t)
	test.prepare(t)
	test.accept(t)
	test.restartAndMarkReady(t)
	test.cleanupReady(t)
}

type operationArtifactLifecycleTest struct {
	fixture    *acceptanceFixture
	operation  model.Operation
	authority  *LocalOperationAuthority
	closure    VerifiedArtifactClosure
	root       VerifiedArtifactRoot
	capture    model.JSON
	begun      OperationArtifactStageResult
	acceptance LocalAcceptanceSpec
}

func newOperationArtifactLifecycleTest(t *testing.T) *operationArtifactLifecycleTest {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "artifact-lifecycle", nil)
	closure, root := peerInboxArtifactEmptyTreeClosure(t, "operation-artifact-lifecycle", 0,
		fixture.now.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	return &operationArtifactLifecycleTest{fixture: fixture, operation: operation,
		authority: authority, closure: closure, root: root, capture: capture}
}

func (test *operationArtifactLifecycleTest) begin(t *testing.T) {
	t.Helper()
	fixture, operation := test.fixture, test.operation
	leaseUntil, _ := operation.LeaseUntil()
	if _, err := fixture.store.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: "wrong-owner", LeaseUntil: leaseUntil,
			At: fixture.now.Add(-20 * time.Second)}); !errors.Is(err, ErrOperationFence) {
		t.Fatalf("wrong Begin fence error = %v", err)
	}
	assertArtifactStageRowCount(t, fixture.store, "operation_artifact_stages", 0)

	var err error
	test.begun, err = fixture.store.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: operation.LeaseOwner(), LeaseUntil: leaseUntil,
			At: fixture.now.Add(-20 * time.Second)})
	if err != nil || test.begun.State() != ArtifactStageStaged ||
		test.begun.Fence().Owner().Generation() != 1 {
		t.Fatalf("BeginOperationArtifactStage() = (%#v,%v)", test.begun, err)
	}
}

func (test *operationArtifactLifecycleTest) prepare(t *testing.T) {
	t.Helper()
	fixture := test.fixture
	mustExec(t, fixture.store, `CREATE TRIGGER test_operation_artifact_publish_abort
		BEFORE UPDATE OF state ON operation_artifact_stages
		WHEN NEW.state='publishing'
		BEGIN SELECT RAISE(ABORT, 'forced operation publish rollback'); END`)
	prepare := PrepareOperationArtifactPublishSpec{Fence: test.begun.Fence(), Capture: test.capture,
		Closure: test.closure, At: fixture.now.Add(-10 * time.Second)}
	if _, err := fixture.store.PrepareOperationArtifactPublish(
		context.Background(), prepare); err == nil {
		t.Fatal("forced operation publishing failure was accepted")
	}
	assertArtifactStageRowCount(t, fixture.store, "artifact_roots", 0)
	assertOperationCaptureNull(t, fixture.store, test.operation.ID())
	mustExec(t, fixture.store, `DROP TRIGGER test_operation_artifact_publish_abort`)

	prepared, err := fixture.store.PrepareOperationArtifactPublish(context.Background(), prepare)
	if err != nil || prepared.State() != ArtifactStagePublishing || prepared.Replayed() {
		t.Fatalf("PrepareOperationArtifactPublish() = (%#v,%v)", prepared, err)
	}
	cleanupProbe, err := fixture.store.ScanArtifactStageCleanupCandidates(context.Background(),
		ScanArtifactStageCleanupSpec{Cutoff: fixture.now.Add(-5 * time.Second),
			At: fixture.now.Add(-4 * time.Second), MaxExamined: 1})
	if err != nil || cleanupProbe.Examined() != 1 ||
		len(cleanupProbe.Candidates()) != 0 {
		t.Fatalf("publishing stage cleanup probe = (%#v,%v)", cleanupProbe, err)
	}
}

func (test *operationArtifactLifecycleTest) accept(t *testing.T) {
	t.Helper()
	fixture := test.fixture
	artifactRef, _ := model.NewArtifactRef(test.root.RootDigest, model.ArtifactProduced)
	test.acceptance = fixture.offer(t, test.authority, "artifact-lifecycle", fixture.reviewers,
		[]model.ArtifactRef{artifactRef}, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), test.acceptance,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatalf("publishing acceptance error = %v", err)
	}
	if _, state, err := readArtifactRoot(context.Background(), fixture.store.db,
		test.root.RootDigest); err != nil || state != "staged" {
		t.Fatalf("accepted root before filesystem publish = (%q,%v), want staged",
			state, err)
	}
}

func (test *operationArtifactLifecycleTest) restartAndMarkReady(t *testing.T) {
	t.Helper()
	fixture := test.fixture
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	checkpoint, found, err := restarted.ReadCommittedOperationArtifactPublish(
		context.Background(), ReadCommittedOperationArtifactPublishSpec{
			OperationID: test.operation.ID(), At: fixture.now.Add(2 * time.Second)})
	if err != nil || !found || checkpoint.State() != ArtifactStagePublishing ||
		checkpoint.Capture().String() != test.capture.String() ||
		!sameVerifiedArtifactClosure(checkpoint.Closure(), test.closure) {
		t.Fatalf("read operation publishing checkpoint = (%#v,%v)", checkpoint, err)
	}
	ready, err := restarted.MarkOperationArtifactReady(context.Background(),
		MarkOperationArtifactReadySpec{Fence: checkpoint.Fence(),
			At: fixture.now.Add(3 * time.Second)})
	if err != nil || ready.State() != ArtifactStageReady {
		t.Fatalf("MarkOperationArtifactReady() = (%#v,%v)", ready, err)
	}
	readyCheckpoint, found, err := restarted.ReadCommittedOperationArtifactPublish(
		context.Background(), ReadCommittedOperationArtifactPublishSpec{
			OperationID: test.operation.ID(), At: fixture.now.Add(4 * time.Second)})
	if err != nil || !found || readyCheckpoint.State() != ArtifactStageReady ||
		!sameVerifiedArtifactClosure(readyCheckpoint.Closure(), test.closure) {
		t.Fatalf("read ready operation checkpoint = (%#v,%v)", readyCheckpoint, err)
	}
}

func (test *operationArtifactLifecycleTest) cleanupReady(t *testing.T) {
	t.Helper()
	fixture, restarted := test.fixture, test.fixture.store
	reclaimAt := fixture.now.Add(5 * time.Second)
	readyCleanup, err := restarted.ScanArtifactStageCleanupCandidates(context.Background(),
		ScanArtifactStageCleanupSpec{Cutoff: fixture.now.Add(4 * time.Second),
			At: reclaimAt, MaxExamined: 1})
	if err != nil || len(readyCleanup.Candidates()) != 1 ||
		readyCleanup.Candidates()[0].State() != ArtifactStageReady {
		t.Fatalf("ready stage cleanup claim = (%#v,%v)", readyCleanup, err)
	}
	if _, err := restarted.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{Candidate: readyCleanup.Candidates()[0],
			At: reclaimAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.GetVerifiedArtifactRoot(context.Background(),
		test.root.RootDigest); err != nil {
		t.Fatalf("ready cleanup removed final Artifact root: %v", err)
	}
	if _, err := restarted.CommitLocalAcceptance(context.Background(), test.acceptance,
		reclaimAt.Add(time.Second)); err != nil {
		t.Fatalf("ready acceptance response-loss replay: %v", err)
	}
}

func TestPeerInboxArtifactExplicitStageRecoversPublishingAfterRestart(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-explicit-publishing", false)
	at := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: at})
	if err != nil || begun.State() != ArtifactStageStaged ||
		begun.Owner().Generation() != 1 {
		t.Fatalf("BeginPeerInboxArtifactStage() = (%#v,%v)", begun, err)
	}
	prepared, err := fixture.store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: begun.Owner(),
			Closure: closure, At: at})
	if err != nil || prepared.Replayed() {
		t.Fatalf("PreparePeerInboxArtifactPublish() = (%#v,%v)", prepared, err)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	recovered, err := restarted.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: at.Add(time.Second)})
	if err != nil || recovered.State() != ArtifactStagePublishing ||
		recovered.Owner() != begun.Owner() {
		t.Fatalf("publishing Inbox restart recovery = (%#v,%v)", recovered, err)
	}
	checkpoint, err := restarted.ReadPeerInboxArtifactPublish(context.Background(),
		ReadPeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: recovered.Owner(),
			At: at.Add(time.Second)})
	if err != nil || checkpoint.State() != ArtifactStagePublishing ||
		!sameVerifiedArtifactClosure(checkpoint.Closure(), closure) {
		t.Fatalf("read Inbox publishing checkpoint = (%#v,%v)", checkpoint, err)
	}
	if _, err := restarted.AcceptPeerInboxArtifactPublish(context.Background(),
		AcceptPeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: recovered.Owner(), At: at.Add(time.Second),
		}); err != nil {
		t.Fatalf("accept recovered Inbox Artifact: %v", err)
	}
	if _, err := restarted.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{
			Fence: claim.Fence(), Owner: recovered.Owner(), At: at.Add(2 * time.Second),
		}); err != nil {
		t.Fatalf("ready recovered Inbox Artifact: %v", err)
	}
	readyCheckpoint, err := restarted.ReadPeerInboxArtifactPublish(context.Background(),
		ReadPeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: recovered.Owner(),
			At: at.Add(2 * time.Second)})
	if err != nil || readyCheckpoint.State() != ArtifactStageReady ||
		!sameVerifiedArtifactClosure(readyCheckpoint.Closure(), closure) {
		t.Fatalf("read ready Inbox checkpoint = (%#v,%v)", readyCheckpoint, err)
	}
}

func TestPeerInboxArtifactPublishRecoveryNormalizesSharedObservationTimes(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-shared-time-recovery", false)
	if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(),
		closure); err != nil {
		t.Fatal(err)
	}
	publishAt := fixture.at.Add(2 * time.Second)
	later := cloneVerifiedArtifactClosureValue(closure)
	for index := range later.Roots {
		later.Roots[index].CreatedAt = publishAt
		later.Roots[index].VerifiedAt = publishAt
	}
	for index := range later.Blocks {
		later.Blocks[index].CreatedAt = publishAt
	}
	begun, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: publishAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: begun.Owner(),
			Closure: later, At: publishAt}); err != nil {
		t.Fatal(err)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	recovered, err := restarted.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: publishAt.Add(time.Second)})
	if err != nil || recovered.State() != ArtifactStagePublishing {
		t.Fatalf("recover shared-time stage = (%#v,%v)", recovered, err)
	}
	checkpoint, err := restarted.ReadPeerInboxArtifactPublish(context.Background(),
		ReadPeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: recovered.Owner(),
			At: publishAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	durable := checkpoint.Closure()
	if durable.Roots[0].CreatedAt.Equal(later.Roots[0].CreatedAt) ||
		!durable.Roots[0].CreatedAt.Equal(closure.Roots[0].CreatedAt) ||
		!durable.Roots[0].VerifiedAt.Equal(later.Roots[0].VerifiedAt) {
		t.Fatalf("durable shared root times = created %s verified %s",
			durable.Roots[0].CreatedAt, durable.Roots[0].VerifiedAt)
	}
	if len(durable.Blocks) != 0 &&
		(!durable.Blocks[0].CreatedAt.Equal(closure.Blocks[0].CreatedAt) ||
			durable.Blocks[0].CreatedAt.Equal(later.Blocks[0].CreatedAt)) {
		t.Fatalf("durable shared block time = %s", durable.Blocks[0].CreatedAt)
	}
	left, err := verifiedClosureDigest(later)
	if err != nil {
		t.Fatal(err)
	}
	right, err := verifiedClosureDigest(durable)
	if err != nil || left != right {
		t.Fatalf("semantic closure digest = (%s,%s,%v)", left, right, err)
	}
}

func TestVerifiedClosureDigestIgnoresObservationTimesAndBindsSemanticClosure(t *testing.T) {
	closure, _, _ := newArtifactSourceClosure(t, "artifact-semantic-digest",
		[]byte("semantic-digest-content"),
		time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC))
	base, err := verifiedClosureDigest(closure)
	if err != nil {
		t.Fatal(err)
	}
	later := cloneVerifiedArtifactClosureValue(closure)
	for index := range later.Roots {
		later.Roots[index].CreatedAt = later.Roots[index].CreatedAt.Add(time.Minute)
		later.Roots[index].VerifiedAt = later.Roots[index].VerifiedAt.Add(time.Minute)
	}
	for index := range later.Blocks {
		later.Blocks[index].CreatedAt = later.Blocks[index].CreatedAt.Add(time.Minute)
	}
	if digest, err := verifiedClosureDigest(later); err != nil || digest != base {
		t.Fatalf("time-only semantic digest = (%s,%v), want %s", digest, err, base)
	}

	manifest, err := model.JSONFrom(map[string]any{"entries": []any{},
		"kind": "different", "total_bytes": 0})
	if err != nil {
		t.Fatal(err)
	}
	differentManifest := cloneVerifiedArtifactClosureValue(closure)
	differentManifest.Roots[0].Manifest = manifest
	differentManifest.Roots[0].ManifestDigest = model.Sum(manifest.Bytes())
	differentSize := cloneVerifiedArtifactClosureValue(closure)
	differentSize.Blocks[0].SizeBytes++
	differentMap := cloneVerifiedArtifactClosureValue(closure)
	differentMap.RootBlocks[0].Mode = 0o644
	for name, candidate := range map[string]VerifiedArtifactClosure{
		"manifest": differentManifest,
		"size":     differentSize,
		"map":      differentMap,
	} {
		digest, err := verifiedClosureDigest(candidate)
		if err != nil || digest == base {
			t.Fatalf("%s semantic digest = (%s,%v), want conflict", name, digest, err)
		}
	}
}

func assertArtifactStageRowCount(t *testing.T, st *Store, table string, want int) {
	t.Helper()
	var got int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
		t.Fatalf("%s row count = (%d,%v), want %d", table, got, err, want)
	}
}

func assertOperationCaptureNull(t *testing.T, st *Store, id model.OperationID) {
	t.Helper()
	var isNull int
	if err := st.db.QueryRow(`SELECT capture_json IS NULL FROM operations
		WHERE operation_id=?`, id.String()).Scan(&isNull); err != nil || isNull != 1 {
		t.Fatalf("operation capture NULL = (%d,%v)", isNull, err)
	}
}

func sameVerifiedArtifactClosure(left, right VerifiedArtifactClosure) bool {
	if len(left.Roots) != len(right.Roots) || len(left.Blocks) != len(right.Blocks) ||
		len(left.RootBlocks) != len(right.RootBlocks) {
		return false
	}
	for index := range left.Roots {
		if left.Roots[index].RootDigest != right.Roots[index].RootDigest ||
			left.Roots[index].Manifest.String() != right.Roots[index].Manifest.String() ||
			left.Roots[index].ManifestDigest != right.Roots[index].ManifestDigest ||
			left.Roots[index].TotalBytes != right.Roots[index].TotalBytes ||
			!left.Roots[index].CreatedAt.Equal(right.Roots[index].CreatedAt) ||
			!left.Roots[index].VerifiedAt.Equal(right.Roots[index].VerifiedAt) {
			return false
		}
	}
	for index := range left.Blocks {
		if left.Blocks[index].Digest != right.Blocks[index].Digest ||
			left.Blocks[index].SizeBytes != right.Blocks[index].SizeBytes ||
			!left.Blocks[index].CreatedAt.Equal(right.Blocks[index].CreatedAt) {
			return false
		}
	}
	return equalArtifactRootBlocks(left.RootBlocks, right.RootBlocks)
}
