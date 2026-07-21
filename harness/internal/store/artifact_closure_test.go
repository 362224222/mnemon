package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestVerifiedArtifactClosureCheckpointSealsMapAndReplaysAfterRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	closure := artifactClosureFixture(t, "one")
	first, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || first.Replayed || len(first.Closure.Roots) != 2 || len(first.Closure.RootBlocks) != 3 {
		t.Fatalf("first checkpoint = (%#v, %v)", first, err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
	if _, err := st.db.Exec(`INSERT INTO artifact_root_blocks(root_digest,ordinal,logical_path,
		offset_bytes,length_bytes,block_digest,mode) VALUES(?,9,'later',0,3,?,384)`,
		closure.Roots[0].RootDigest.String(), closure.Blocks[0].Digest.String()); err == nil {
		t.Fatal("verified root block map accepted an append")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	replayInput := cloneVerifiedClosure(closure)
	for index := range replayInput.Roots {
		replayInput.Roots[index].CreatedAt = replayInput.Roots[index].CreatedAt.Add(time.Hour)
		replayInput.Roots[index].VerifiedAt = replayInput.Roots[index].VerifiedAt.Add(time.Hour)
	}
	for index := range replayInput.Blocks {
		replayInput.Blocks[index].CreatedAt = replayInput.Blocks[index].CreatedAt.Add(time.Hour)
	}
	replayed, err := restarted.CheckpointVerifiedArtifactClosure(context.Background(), replayInput)
	if err != nil || !replayed.Replayed ||
		!replayed.Closure.Roots[0].CreatedAt.Equal(closure.Roots[0].CreatedAt) ||
		!replayed.Closure.Blocks[0].CreatedAt.Equal(closure.Blocks[0].CreatedAt) {
		t.Fatalf("restart replay = (%#v, %v)", replayed, err)
	}
	assertArtifactClosureCounts(t, restarted, 2, 2, 3)
}

func TestVerifiedArtifactClosureSharesBlocksAndPromotesExistingStage(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	closure := artifactClosureFixture(t, "shared")
	root := closure.Roots[0]
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
		total_bytes,state,created_at) VALUES(?,?,?,?,'staged',?)`, root.RootDigest.String(),
		root.Manifest.Bytes(), root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(root.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	result, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || result.Replayed {
		t.Fatalf("staged checkpoint = (%#v, %v)", result, err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedArtifactClosureRaisesNewRootVerificationToSharedBlockTime(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	closure := artifactClosureFixture(t, "shared-block-time")
	blockUses := make(map[model.Digest]int)
	for _, row := range closure.RootBlocks {
		blockUses[row.BlockDigest]++
	}
	var sharedDigest model.Digest
	for digest, uses := range blockUses {
		if uses == 1 {
			sharedDigest = digest
			break
		}
	}
	if sharedDigest.IsZero() {
		t.Fatal("fixture has no block isolated to one new root")
	}
	var shared VerifiedArtifactBlock
	for _, block := range closure.Blocks {
		if block.Digest == sharedDigest {
			shared = block
			break
		}
	}
	if shared.Digest.IsZero() {
		t.Fatal("fixture shared block is missing")
	}
	laterAt := shared.CreatedAt.Add(time.Hour)
	if _, err := st.db.Exec(`INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
		VALUES(?,?,?)`, shared.Digest.String(), shared.SizeBytes, storeTime(laterAt)); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || checkpoint.Replayed {
		t.Fatalf("shared-block time checkpoint = (%#v,%v)", checkpoint, err)
	}
	for _, root := range checkpoint.Closure.Roots {
		usesShared := false
		for _, row := range closure.RootBlocks {
			usesShared = usesShared || row.RootDigest == root.RootDigest && row.BlockDigest == shared.Digest
		}
		want := closure.Roots[0].VerifiedAt
		for _, requested := range closure.Roots {
			if requested.RootDigest == root.RootDigest {
				want = requested.VerifiedAt
				break
			}
		}
		if usesShared {
			want = laterAt
		}
		if !root.VerifiedAt.Equal(want) {
			t.Fatalf("root %s verified_at = %s, want %s", root.RootDigest,
				root.VerifiedAt, want)
		}
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
}

func TestVerifiedArtifactClosureConflictRollsBackEveryNewRow(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := artifactClosureFixture(t, "base")
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	conflict := artifactClosureFixture(t, "conflict")
	changedDigest := conflict.Blocks[0].Digest
	conflict.Blocks[0].SizeBytes++
	for index := range conflict.RootBlocks {
		if conflict.RootBlocks[index].BlockDigest != changedDigest {
			continue
		}
		conflict.RootBlocks[index].LengthBytes++
		for rootIndex := range conflict.Roots {
			if conflict.Roots[rootIndex].RootDigest == conflict.RootBlocks[index].RootDigest {
				conflict.Roots[rootIndex].TotalBytes++
			}
		}
	}
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), conflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting block error = %v", err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)

	mapConflict := base
	mapConflict.RootBlocks[0].Mode = 0o644
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), mapConflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting root map error = %v", err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
}

func TestVerifiedArtifactClosureRejectsMalformedRelationalShape(t *testing.T) {
	t.Parallel()
	base := artifactClosureFixture(t, "malformed")
	tests := []struct {
		name   string
		mutate func(*VerifiedArtifactClosure)
	}{
		{"root order", func(value *VerifiedArtifactClosure) {
			value.Roots[0], value.Roots[1] = value.Roots[1], value.Roots[0]
		}},
		{"block order", func(value *VerifiedArtifactClosure) {
			value.Blocks[0], value.Blocks[1] = value.Blocks[1], value.Blocks[0]
		}},
		{"ordinal gap", func(value *VerifiedArtifactClosure) { value.RootBlocks[1].Ordinal = 2 }},
		{"offset gap", func(value *VerifiedArtifactClosure) { value.RootBlocks[1].OffsetBytes++ }},
		{"unknown root", func(value *VerifiedArtifactClosure) {
			value.RootBlocks[0].RootDigest = model.Sum([]byte("unknown-root"))
		}},
		{"unknown block", func(value *VerifiedArtifactClosure) {
			value.RootBlocks[0].BlockDigest = model.Sum([]byte("unknown-block"))
		}},
		{"unsafe path", func(value *VerifiedArtifactClosure) { value.RootBlocks[0].LogicalPath = "../secret" }},
		{"root total drift", func(value *VerifiedArtifactClosure) { value.Roots[0].TotalBytes++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st := openTestStore(t)
			value := cloneVerifiedClosure(base)
			test.mutate(&value)
			if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), value); err == nil {
				t.Fatal("malformed closure was accepted")
			}
			assertArtifactClosureCounts(t, st, 0, 0, 0)
		})
	}
}

func artifactClosureFixture(t *testing.T, suffix string) VerifiedArtifactClosure {
	t.Helper()
	at := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	contentA := []byte("abc")
	contentB := []byte("defgh")
	blockA, blockB := model.Sum(contentA), model.Sum(contentB)
	manifestA, _ := model.NewJSON([]byte(`{"entries":[{"logical_path":"a.txt"}],"kind":"file"}`))
	manifestB, _ := model.NewJSON([]byte(`{"entries":[{"logical_path":"b.txt"},{"logical_path":"c.txt"}],"kind":"directory"}`))
	rootA := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("root-a-" + suffix)), Manifest: manifestA,
		ManifestDigest: model.Sum(manifestA.Bytes()), TotalBytes: 3, CreatedAt: at, VerifiedAt: at}
	rootB := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("root-b-" + suffix)), Manifest: manifestB,
		ManifestDigest: model.Sum(manifestB.Bytes()), TotalBytes: 8, CreatedAt: at, VerifiedAt: at}
	roots := []VerifiedArtifactRoot{rootA, rootB}
	if roots[0].RootDigest.String() > roots[1].RootDigest.String() {
		roots[0], roots[1] = roots[1], roots[0]
	}
	blocks := []VerifiedArtifactBlock{{Digest: blockA, SizeBytes: 3, CreatedAt: at},
		{Digest: blockB, SizeBytes: 5, CreatedAt: at}}
	if blocks[0].Digest.String() > blocks[1].Digest.String() {
		blocks[0], blocks[1] = blocks[1], blocks[0]
	}
	rowsByRoot := map[model.Digest][]VerifiedArtifactRootBlock{
		rootA.RootDigest: {{RootDigest: rootA.RootDigest, LogicalPath: "a.txt", LengthBytes: 3,
			BlockDigest: blockA, Mode: 0o600}},
		rootB.RootDigest: {{RootDigest: rootB.RootDigest, LogicalPath: "b.txt", LengthBytes: 3,
			BlockDigest: blockA, Mode: 0o600}, {RootDigest: rootB.RootDigest, Ordinal: 1,
			LogicalPath: "c.txt", LengthBytes: 5, BlockDigest: blockB, Mode: 0o600}},
	}
	rows := append([]VerifiedArtifactRootBlock{}, rowsByRoot[roots[0].RootDigest]...)
	rows = append(rows, rowsByRoot[roots[1].RootDigest]...)
	return VerifiedArtifactClosure{Roots: roots, Blocks: blocks, RootBlocks: rows}
}

func cloneVerifiedClosure(value VerifiedArtifactClosure) VerifiedArtifactClosure {
	return VerifiedArtifactClosure{Roots: append([]VerifiedArtifactRoot{}, value.Roots...),
		Blocks:     append([]VerifiedArtifactBlock{}, value.Blocks...),
		RootBlocks: append([]VerifiedArtifactRootBlock{}, value.RootBlocks...)}
}

func assertArtifactClosureCounts(t *testing.T, st *Store, roots, blocks, maps int) {
	t.Helper()
	for _, check := range []struct {
		table string
		want  int
	}{{"artifact_roots", roots}, {"artifact_blocks", blocks}, {"artifact_root_blocks", maps}} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + check.table).Scan(&got); err != nil || got != check.want {
			t.Fatalf("%s count = (%d, %v), want %d", check.table, got, err, check.want)
		}
	}
}

type artifactGCRestartTombstoneFixture struct {
	path    string
	casRoot string
	digest  model.Digest
	at      time.Time
}

func testArtifactGCStartupReconciliationClosesQueuedTombstone(t *testing.T) {
	fixture := prepareArtifactGCRestartTombstone(t)
	restarted, err := OpenExisting(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restartedCAS, err := artifactdomain.NewCAS(fixture.casRoot)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := artifactdomain.NewGCWorker(artifactdomain.GCOptions{
		Store: restarted, CAS: restartedCAS,
		Clock:       func() time.Time { return fixture.at.Add(time.Second) },
		MaxExamined: 2, MaxQueued: 2, MaxBytes: 4 << 20, MaxTemps: 1, MaxQueue: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.ReconcileStartup(context.Background()); err != nil {
		t.Fatalf("startup reconciliation: %v", err)
	}
	if err := worker.ReconcileStartup(context.Background()); err != nil {
		t.Fatalf("startup reconciliation replay: %v", err)
	}
	assertArtifactGCCASAbsent(t, restartedCAS, fixture.digest)
	assertArtifactGCIntegrationClosed(t, restarted, restartedCAS)
	snapshot := worker.Snapshot()
	if snapshot.State != artifactdomain.GCIdle || snapshot.FatalCode != artifactdomain.GCFatalNone ||
		snapshot.StartupReconciliations != 2 || snapshot.QueueItemsCompleted != 1 ||
		snapshot.TombstonesPurged != 1 {
		t.Fatalf("restart recovery snapshot = %#v", snapshot)
	}
	assertArtifactGCSnapshotClosed(t, snapshot, fixture.digest)
}

func prepareArtifactGCRestartTombstone(t *testing.T) artifactGCRestartTombstoneFixture {
	root := t.TempDir()
	path := filepath.Join(root, "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	casRoot := filepath.Join(root, "node", "objects", "sha256")
	cas, err := artifactdomain.NewCAS(casRoot)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("restart-owned tombstone")
	digest := model.Sum(content)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Round(0).UTC().Add(3 * time.Hour)
	cutoff := at.Add(-time.Hour)
	candidates, err := cas.ListObjectsBefore(cutoff, 2)
	if err != nil || len(candidates) != 1 || candidates[0].Digest != digest {
		t.Fatalf("CAS candidate = (%#v, %v)", candidates, err)
	}
	cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: cutoff, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := artifactdomain.GCQueueIdentity{Digest: digest,
		Token: artifactGCStoreUniqueToken("real-restart-tombstone")}
	prepared, err := st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
		Current: cursor, Candidates: []artifactdomain.GCCandidate{{Digest: digest,
			SizeBytes: candidates[0].Size, ModifiedAt: candidates[0].ModifiedAt, Token: identity.Token}},
		PageDone: true, MaxQueued: 1, MaxQueue: 4, At: at,
	})
	if err != nil || prepared.Queued != 1 || !prepared.Next.Done {
		t.Fatalf("prepare crash queue = (%#v, %v)", prepared, err)
	}
	status, err := cas.Tombstone(identity.Digest, identity.Token)
	if err != nil || status.State != artifactdomain.CASTombstoneTrashOnly || !status.Closed {
		t.Fatalf("create crash tombstone = (%#v, %v)", status, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return artifactGCRestartTombstoneFixture{path: path, casRoot: casRoot, digest: digest, at: at}
}

func testArtifactGCWorkerSweepsStagingBeforePhysicalCollection(t *testing.T) {
	fixture := newArtifactGCIntegrationFixture(t, "worker-closure")
	protected, protectedManifest, protectedBlock := artifactGCIntegrationClosure(t, fixture.cas,
		"protected.txt", []byte("durably owned artifact"), fixture.at.Add(time.Second))
	protectedClaim := stageArtifactGCIntegrationInbox(t, fixture, protected, 1, "protected")
	readyAt := protectedClaim.Fence().LeaseUntil().Add(-peerInboxArtifactLease).Add(2 * time.Second)
	ready, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{Fence: protectedClaim.Fence(), At: readyAt})
	if err != nil || ready.Status() != model.InboxReady || !ready.Changed() {
		t.Fatalf("mark protected Inbox ready = (%#v, %v)", ready, err)
	}

	orphan, orphanManifest, orphanBlock := artifactGCIntegrationClosure(t, fixture.cas,
		"orphan.txt", []byte("expired staged artifact"), fixture.at.Add(20*time.Second))
	claim := stageArtifactGCIntegrationInbox(t, fixture, orphan, 2, "orphan")
	staged, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: orphan.Roots[0].RootDigest,
			At: claim.Fence().LeaseUntil().Add(-time.Second)})
	if err != nil || !found || staged.State() != PeerInboxArtifactRootStaged {
		t.Fatalf("read staged root = (%#v, found %t, %v)", staged, found, err)
	}

	gcAt := time.Now().Round(0).UTC().Add(4 * time.Hour)
	worker, err := artifactdomain.NewGCWorker(artifactdomain.GCOptions{
		Store: fixture.store, CAS: fixture.cas, Clock: func() time.Time { return gcAt },
		ObjectTTL: time.Hour, StagingTTL: time.Hour, TempTTL: time.Hour,
		MaxExamined: 2, MaxQueued: 1, MaxBytes: 4 << 20, MaxTemps: 1, MaxQueue: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactGCWorkerSweepsOnlyStaging(t, fixture, worker, gcAt,
		orphan, orphanManifest, orphanBlock)
	collectArtifactGCIntegrationOrphan(t, fixture, worker, orphanManifest, orphanBlock)
	assertArtifactGCCASContent(t, fixture.cas, protectedManifest, protected.ManifestBytes)
	assertArtifactGCCASContent(t, fixture.cas, protectedBlock, []byte("durably owned artifact"))
	if _, err := fixture.store.GetVerifiedArtifactRoot(context.Background(),
		protected.Roots[0].RootDigest); err != nil {
		t.Fatalf("owned root metadata was collected: %v", err)
	}
	assertArtifactGCIntegrationClosed(t, fixture.store, fixture.cas)
	snapshot := worker.Snapshot()
	if snapshot.State != artifactdomain.GCIdle || snapshot.FatalCode != artifactdomain.GCFatalNone ||
		snapshot.StagingSwept != 1 || snapshot.ObjectsQueued != 2 ||
		snapshot.ObjectsTombstoned != 2 || snapshot.TombstonesPurged != 2 ||
		snapshot.QueueItemsCompleted != 2 || snapshot.ObjectsProtected < 2 {
		t.Fatalf("settled GC snapshot = %#v", snapshot)
	}
	assertArtifactGCSnapshotClosed(t, snapshot, orphan.Roots[0].RootDigest,
		orphanManifest, orphanBlock)
}

func assertArtifactGCWorkerSweepsOnlyStaging(t *testing.T, fixture artifactGCIntegrationFixture,
	worker *artifactdomain.GCWorker, gcAt time.Time, orphan artifactGCIntegrationClosureValue,
	orphanManifest, orphanBlock model.Digest,
) {
	t.Helper()
	if err := worker.RunCycle(context.Background()); err != nil {
		t.Fatalf("staging collection cycle: %v", err)
	}
	first := worker.Snapshot()
	if first.StagingExamined != 2 || first.StagingSwept != 1 || first.ObjectsQueued != 0 {
		t.Fatalf("first GC cycle = %#v, want one staged sweep and no physical queue", first)
	}
	stagingCursor, err := fixture.store.OpenArtifactGCStagingScan(context.Background(),
		artifactdomain.GCScanSpec{InitializeCutoff: gcAt.Add(-time.Hour), At: gcAt})
	if err != nil {
		t.Fatalf("open post-sweep staging scan: %v", err)
	}
	stagingClosed, err := fixture.store.SweepArtifactGCStaging(context.Background(),
		artifactdomain.GCStagingSweepSpec{Current: stagingCursor, MaxItems: 4,
			MaxBytes: 4 << 20, At: gcAt})
	if err != nil || stagingClosed.Examined != 1 || stagingClosed.Swept != 0 ||
		!stagingClosed.Next.Done {
		t.Fatalf("post-sweep staging metadata = (%#v, %v), want only protected root",
			stagingClosed, err)
	}
	assertArtifactGCCASContent(t, fixture.cas, orphanManifest, orphan.ManifestBytes)
	assertArtifactGCCASContent(t, fixture.cas, orphanBlock, []byte("expired staged artifact"))
}

func collectArtifactGCIntegrationOrphan(t *testing.T, fixture artifactGCIntegrationFixture,
	worker *artifactdomain.GCWorker, orphanManifest, orphanBlock model.Digest,
) {
	t.Helper()
	observedSingleRemoval := false
	collected := false
	for cycle := 0; cycle < 12; cycle++ {
		before := worker.Snapshot()
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatalf("physical collection cycle %d: %v", cycle, err)
		}
		after := worker.Snapshot()
		assertArtifactGCIntegrationCycleBound(t, before, after, 1)
		queue, err := fixture.store.ListArtifactGCQueue(context.Background(),
			artifactGCMaxQueueList)
		if err != nil || len(queue) > 1 {
			t.Fatalf("physical collection cycle %d durable queue = (%#v, %v), want at most one",
				cycle, queue, err)
		}
		manifestPresent := artifactGCCASPresent(t, fixture.cas, orphanManifest)
		blockPresent := artifactGCCASPresent(t, fixture.cas, orphanBlock)
		if manifestPresent != blockPresent {
			observedSingleRemoval = true
		}
		if !manifestPresent && !blockPresent {
			collected = true
			break
		}
	}
	if !observedSingleRemoval || !collected {
		t.Fatalf("bounded physical collection = (single removal %t, collected %t), want both true",
			observedSingleRemoval, collected)
	}
	assertArtifactGCCASAbsent(t, fixture.cas, orphanManifest)
	assertArtifactGCCASAbsent(t, fixture.cas, orphanBlock)
}
