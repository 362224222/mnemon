package store

import (
	"context"
	"errors"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func preparePeerInboxArtifactPublishForTest(store *Store, fence PeerInboxArtifactFence,
	closure VerifiedArtifactClosure, at time.Time,
) (PeerInboxArtifactStage, artifactdomain.StageOwner, error) {
	begun, err := store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: fence, At: at})
	if err != nil {
		return PeerInboxArtifactStage{}, artifactdomain.StageOwner{}, err
	}
	prepared, err := store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{
			Fence: fence, Owner: begun.Owner(), Closure: closure, At: at,
		})
	return prepared, begun.Owner(), err
}

func mustPreparePeerInboxArtifactPublish(t *testing.T, store *Store,
	fence PeerInboxArtifactFence, closure VerifiedArtifactClosure, at time.Time,
) (PeerInboxArtifactStage, artifactdomain.StageOwner) {
	t.Helper()
	prepared, owner, err := preparePeerInboxArtifactPublishForTest(store, fence, closure, at)
	if err != nil {
		t.Fatalf("prepare Peer Inbox Artifact publish: %v", err)
	}
	return prepared, owner
}

func mustAcceptPeerInboxArtifactPublish(t *testing.T, store *Store,
	fence PeerInboxArtifactFence, owner artifactdomain.StageOwner, at time.Time,
) {
	t.Helper()
	if _, err := store.AcceptPeerInboxArtifactPublish(context.Background(),
		AcceptPeerInboxArtifactPublishSpec{Fence: fence, Owner: owner, At: at}); err != nil {
		t.Fatalf("accept Peer Inbox Artifact publish: %v", err)
	}
}

func testPreparePeerInboxArtifactPublishRestart(t *testing.T) {
	fixture, claim, root, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-stage-restart", false)
	stageAt := fixture.at.Add(2 * time.Second)
	staged, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, stageAt)
	if err != nil || !staged.Changed() || staged.Replayed() {
		t.Fatalf("first stage = (%#v,%v)", staged, err)
	}
	assertPeerInboxArtifactRootState(t, fixture.store, root.RootDigest, "staged")
	assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
		claim.RequiredArtifactRoots(), claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
	if _, err := fixture.store.GetVerifiedArtifactRoot(context.Background(),
		root.RootDigest); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("staged root visible as verified: %v", err)
	}
	replay, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, stageAt)
	if err != nil || replay.Changed() || !replay.Replayed() {
		t.Fatalf("stage response-loss replay = (%#v,%v)", replay, err)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	fixture.store = restarted
	restartReplay, _, err := preparePeerInboxArtifactPublishForTest(
		restarted, claim.Fence(), closure, stageAt)
	if err != nil || restartReplay.Changed() || !restartReplay.Replayed() {
		t.Fatalf("restart stage replay = (%#v,%v)", restartReplay, err)
	}
	checkpoint, found, err := restarted.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{
			Fence: claim.Fence(), RootDigest: root.RootDigest, At: stageAt,
		})
	if err != nil || !found || checkpoint.State() != PeerInboxArtifactRootStaged ||
		checkpoint.RootDigest() != root.RootDigest {
		t.Fatalf("restart staged checkpoint = (%#v,found %t,%v)",
			checkpoint, found, err)
	}
}

func testPreparePeerInboxArtifactPublishOlderObservation(t *testing.T) {
	fixture, claim, root, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-stage-concurrent-time", false)
	olderAt := fixture.at.Add(2 * time.Second)
	laterAt := olderAt.Add(2 * time.Second)
	later := VerifiedArtifactClosure{
		Roots:      append([]VerifiedArtifactRoot(nil), closure.Roots...),
		Blocks:     append([]VerifiedArtifactBlock(nil), closure.Blocks...),
		RootBlocks: append([]VerifiedArtifactRootBlock(nil), closure.RootBlocks...),
	}
	for index := range later.Roots {
		later.Roots[index].CreatedAt = laterAt
		later.Roots[index].VerifiedAt = laterAt
	}
	for index := range later.Blocks {
		later.Blocks[index].CreatedAt = laterAt
	}
	_, owner := mustPreparePeerInboxArtifactPublish(t, fixture.store,
		claim.Fence(), later, laterAt)
	if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{
			Fence: claim.Fence(), RootDigest: root.RootDigest, At: olderAt,
		}); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("older cached-root observation error = %v", err)
	}
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, olderAt); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("older shared stage error = %v", err)
	}
	assertPeerInboxArtifactRootState(t, fixture.store, root.RootDigest, "staged")
	assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1,
		"artifact-stage-concurrent-time-worker", true)
	mustAcceptPeerInboxArtifactPublish(t, fixture.store, claim.Fence(), owner,
		laterAt.Add(time.Second))
	if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{
			Fence: claim.Fence(), Owner: owner, At: laterAt.Add(time.Second),
		}); err != nil {
		t.Fatalf("fresh ready after stale observation: %v", err)
	}
}

func testPreparePeerInboxArtifactPublishFailsClosed(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-stage-fail-closed", false)
	stageAt := fixture.at.Add(2 * time.Second)
	wrong := claim.Fence()
	wrong.attempt++
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, wrong, closure, stageAt); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("wrong stage fence error = %v", err)
	}
	other, _, _ := newArtifactSourceClosure(t, "artifact-stage-other-root",
		[]byte("other"), fixture.at)
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), other, stageAt); !errors.Is(err, ErrPeerInboxArtifactInput) {
		t.Fatalf("wrong exact root error = %v", err)
	}
	mustExec(t, fixture.store, `CREATE TRIGGER test_artifact_stage_pin_abort
		BEFORE INSERT ON artifact_pins WHEN NEW.owner_kind='inbox'
		BEGIN SELECT RAISE(ABORT, 'forced stage rollback'); END`)
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, stageAt); !errors.Is(err, ErrPeerInboxArtifactInvariant) {
		t.Fatalf("forced stage rollback error = %v", err)
	}
	for _, table := range []string{
		"artifact_roots", "artifact_blocks", "artifact_root_blocks", "artifact_pins",
	} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).
			Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s after stage rollback = (%d,%v)", table, count, err)
		}
	}
}

func testPreparePeerInboxArtifactPublishAuthorityLoss(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-stage-authority", false)
	stageAt := fixture.at.Add(2 * time.Second)
	mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined'
		WHERE channel_id=?`, fixture.channel.Channel().ID().String())
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, stageAt); !errors.Is(err, ErrPeerInboxArtifactAuthority) {
		t.Fatalf("stage after authority loss error = %v", err)
	}
	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_roots`).
		Scan(&count); err != nil || count != 0 {
		t.Fatalf("roots after authority loss = (%d,%v)", count, err)
	}
}

func testPreparePeerInboxArtifactPublishSharedOwners(t *testing.T) {
	fixture := newPeerInboxFixture(t, "artifact-stage-shared", 0)
	closureA, rootA, _ := newArtifactSourceClosure(t, "artifact-stage-shared-a",
		[]byte("shared-block"), fixture.at)
	closureB, rootB, _ := newArtifactSourceClosure(t, "artifact-stage-shared-b",
		[]byte("shared-block"), fixture.at)
	closure := combinePeerInboxArtifactClosures(t, closureA, closureB)
	if len(closure.Blocks) != 1 {
		t.Fatalf("combined shared closure blocks = %d, want 1", len(closure.Blocks))
	}
	firstPut := fixture.put(t, peerInboxArtifactPublication(t, fixture, 1, 1,
		"artifact-stage-shared-first", []model.Digest{
			rootA.RootDigest, rootB.RootDigest,
		}), fixture.at)
	secondPut := fixture.put(t, peerInboxArtifactPublication(t, fixture, 2, 2,
		"artifact-stage-shared-second", []model.Digest{
			rootB.RootDigest, rootA.RootDigest,
		}), fixture.at.Add(time.Second))
	firstAt := fixture.at.Add(2 * time.Second)
	first := mustClaimPeerInboxArtifact(t, fixture.store,
		"artifact-stage-shared-first", firstAt)
	_, firstOwner := mustPreparePeerInboxArtifactPublish(t, fixture.store,
		first.Fence(), closure, firstAt)
	secondAt := firstAt.Add(time.Second)
	second := mustClaimPeerInboxArtifact(t, fixture.store,
		"artifact-stage-shared-second", secondAt)
	if second.InboxID() != secondPut.InboxID || first.InboxID() != firstPut.InboxID {
		t.Fatalf("shared Inbox claim order = (%s,%s)",
			first.InboxID(), second.InboxID())
	}
	if _, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{
			Fence: second.Fence(), RootDigest: rootA.RootDigest, At: secondAt,
		}); err != nil || found {
		t.Fatalf("other Inbox unowned stage = (found %t,%v)", found, err)
	}
	secondStage, secondOwner, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, second.Fence(), closure, secondAt)
	if err != nil || !secondStage.Changed() || secondStage.Replayed() {
		t.Fatalf("second shared stage = (%#v,%v)", secondStage, err)
	}
	assertSharedPeerInboxArtifactRows(t, fixture.store)
	mustAcceptPeerInboxArtifactPublish(t, fixture.store, first.Fence(), firstOwner,
		secondAt.Add(time.Second))
	mustMarkPeerInboxArtifactReady(t, fixture.store, first.Fence(), firstOwner,
		secondAt.Add(time.Second))
	assertPeerInboxArtifactStagePins(t, fixture.store, second.InboxID(),
		second.RequiredArtifactRoots(),
		second.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
	mustAcceptPeerInboxArtifactPublish(t, fixture.store, second.Fence(), secondOwner,
		secondAt.Add(2*time.Second))
	mustMarkPeerInboxArtifactReady(t, fixture.store, second.Fence(), secondOwner,
		secondAt.Add(2*time.Second))
	assertPeerInboxArtifactPins(t, fixture.store,
		first.InboxID(), first.RequiredArtifactRoots())
	assertPeerInboxArtifactPins(t, fixture.store,
		second.InboxID(), second.RequiredArtifactRoots())
	assertPeerInboxArtifactRootState(t, fixture.store, rootA.RootDigest, "verified")
	assertPeerInboxArtifactRootState(t, fixture.store, rootB.RootDigest, "verified")
}

func assertSharedPeerInboxArtifactRows(t *testing.T, store *Store) {
	t.Helper()
	for table, want := range map[string]int{
		"artifact_roots": 2, "artifact_blocks": 1,
		"artifact_root_blocks": 2, "artifact_pins": 4,
	} {
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + table).
			Scan(&count); err != nil || count != want {
			t.Fatalf("shared %s count = (%d,%v), want %d",
				table, count, err, want)
		}
	}
}

func mustMarkPeerInboxArtifactReady(t *testing.T, store *Store,
	fence PeerInboxArtifactFence, owner artifactdomain.StageOwner, at time.Time,
) {
	t.Helper()
	if _, err := store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{
			Fence: fence, Owner: owner, At: at,
		}); err != nil {
		t.Fatal(err)
	}
}

func testPeerInboxArtifactAggregateClosureLimit(t *testing.T) {
	fixture := newPeerInboxFixture(t, "artifact-ready-aggregate-limit", 0)
	closureA, rootA := peerInboxArtifactEmptyTreeClosure(t, "artifact-limit-a",
		maxVerifiedClosureEntries/2, fixture.at.Add(-2*time.Second))
	closureB, rootB := peerInboxArtifactEmptyTreeClosure(t, "artifact-limit-b",
		maxVerifiedClosureEntries/2, fixture.at.Add(-2*time.Second))
	for _, closure := range []VerifiedArtifactClosure{closureA, closureB} {
		if _, err := fixture.store.CheckpointVerifiedArtifactClosure(
			context.Background(), closure); err != nil {
			t.Fatal(err)
		}
	}
	publication := peerInboxArtifactPublication(t, fixture, 1, 1,
		"artifact-ready-aggregate-limit",
		[]model.Digest{rootA.RootDigest, rootB.RootDigest})
	put := fixture.put(t, publication, fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store,
		"artifact-limit-worker", claimAt)
	settleAt := claimAt.Add(time.Second)
	closure := combinePeerInboxArtifactClosures(t, closureA, closureB)
	if _, _, err := preparePeerInboxArtifactPublishForTest(
		fixture.store, claim.Fence(), closure, settleAt); !errors.Is(err, ErrPeerInboxArtifactLimit) {
		t.Fatalf("aggregate closure limit error = %v", err)
	}
	assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, nil)
	settled, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(),
		QuarantinePeerInboxArtifactSpec{
			Fence: claim.Fence(), Diagnostic: PeerInboxArtifactLimitExceeded,
			At: settleAt,
		})
	if err != nil || settled.Status() != model.InboxQuarantined ||
		!settled.Changed() {
		t.Fatalf("quarantine aggregate limit = (%#v,%v)", settled, err)
	}
}
