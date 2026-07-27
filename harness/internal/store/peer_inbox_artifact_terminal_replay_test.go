package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerInboxArtifactReadyReadSurvivesSemanticTerminal(t *testing.T) {
	published := newPublishedPeerInboxArtifact(t, "terminal-ready-read")
	terminalizePublishedPeerInboxArtifact(t, published)
	closePeerInboxArtifactChannel(t, published)

	checkpoint, err := published.fixture.store.ReadPeerInboxArtifactPublish(
		context.Background(), ReadPeerInboxArtifactPublishSpec{
			Fence: published.claim.Fence(), Owner: published.owner,
			At: published.readyAt,
		})
	if err != nil || checkpoint.State() != ArtifactStageReady ||
		!sameVerifiedArtifactClosureDigest(checkpoint.Closure(), published.closure) {
		t.Fatalf("terminal ready read = (%#v,%v)", checkpoint, err)
	}
}

func TestPeerInboxArtifactReadyReplaySurvivesSemanticProcessingAndRetry(t *testing.T) {
	published := newPublishedPeerInboxArtifact(t, "semantic-processing-replay")
	claimAt := published.readyAt.Add(time.Second)
	semantic := mustClaimPeerInboxSemantic(t, published.fixture.store,
		"semantic-processing-worker", claimAt)
	assertPublishedPeerInboxArtifactReplay(t, published, published.readyAt)

	retryAt := claimAt.Add(time.Second)
	if _, err := published.fixture.store.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{
			Fence: semantic.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
			RetryAfter: time.Second, At: retryAt,
		}); err != nil {
		t.Fatal(err)
	}
	assertPublishedPeerInboxArtifactReplay(t, published, published.readyAt)
}

func TestEmptyPeerInboxArtifactReadyReplaySurvivesSemanticHandoff(t *testing.T) {
	fixture := newPeerInboxFixture(t, "empty-semantic-replay", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"empty-semantic-replay", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	artifactClaim := mustClaimPeerInboxArtifact(t, fixture.store,
		"empty-artifact-worker", fixture.at.Add(time.Second))
	readyAt := fixture.at.Add(2 * time.Second)
	if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{
			Fence: artifactClaim.Fence(), At: readyAt,
		}); err != nil {
		t.Fatal(err)
	}

	claimAt := readyAt.Add(time.Second)
	semantic := mustClaimPeerInboxSemantic(t, fixture.store,
		"empty-semantic-worker", claimAt)
	assertEmptyPeerInboxArtifactReadyReplay(t, fixture.store, artifactClaim,
		readyAt)

	retryAt := claimAt.Add(time.Second)
	retry, err := fixture.store.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{
			Fence: semantic.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
			RetryAfter: time.Second, At: retryAt,
		})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyPeerInboxArtifactReadyReplay(t, fixture.store, artifactClaim,
		readyAt)

	reclaimed := mustClaimPeerInboxSemantic(t, fixture.store,
		"empty-semantic-terminal-worker", retry.NextAttemptAt())
	decisionAt := retry.NextAttemptAt().Add(time.Second)
	spec := peerInboxSemanticCommitSpec(t, fixture, reclaimed, decisionAt)
	if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
		spec, decisionAt); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, `UPDATE channels
		SET status='closed',topic_state='left' WHERE channel_id=?`,
		fixture.channel.Channel().ID().String())
	assertEmptyPeerInboxArtifactReadyReplay(t, fixture.store, artifactClaim,
		readyAt)
	if put.InboxID != artifactClaim.InboxID() {
		t.Fatalf("empty replay Inbox = %s, want %s", artifactClaim.InboxID(), put.InboxID)
	}
}

func TestPeerInboxArtifactMarkReadyResponseLossSurvivesSemanticTerminalAndCleanup(
	t *testing.T,
) {
	published := newPublishedPeerInboxArtifact(t, "terminal-ready-replay")
	// The first MarkReady committed, but its response is deliberately discarded.
	terminalizePublishedPeerInboxArtifact(t, published)
	closePeerInboxArtifactChannel(t, published)

	replayAt := published.readyAt
	replayed, err := published.fixture.store.MarkPeerInboxArtifactReady(
		context.Background(), MarkPeerInboxArtifactReadySpec{
			Fence: published.claim.Fence(), Owner: published.owner, At: replayAt,
		})
	if err != nil || !replayed.Replayed() || replayed.Changed() {
		t.Fatalf("terminal MarkReady replay = (%#v,%v)", replayed, err)
	}

	cleanupAt := published.readyAt.Add(time.Hour)
	page, err := published.fixture.store.ScanArtifactStageCleanupCandidates(
		context.Background(), ScanArtifactStageCleanupSpec{
			Cutoff: published.readyAt.Add(time.Nanosecond), At: cleanupAt,
			MaxExamined: 1,
		})
	if err != nil || len(page.Candidates()) != 1 ||
		page.Candidates()[0].Owner() != published.owner {
		t.Fatalf("terminal ready cleanup page = (%#v,%v)", page, err)
	}
	if _, err := published.fixture.store.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{
			Candidate: page.Candidates()[0], At: cleanupAt.Add(time.Nanosecond),
		}); err != nil {
		t.Fatal(err)
	}
	sweepAt := cleanupAt.Add(time.Second)
	for pass := 0; pass < 2; pass++ {
		result, err := published.fixture.store.SweepArtifactStaging(context.Background(),
			artifactdomain.StagingSweepSpec{
				Cutoff: cleanupAt, At: sweepAt.Add(time.Duration(pass) * time.Nanosecond),
				MaxRoots: 4,
			})
		if err != nil || result.Roots != 0 {
			t.Fatalf("terminal ready sweep %d = (%#v,%v)", pass, result, err)
		}
	}
	if _, err := published.fixture.store.GetVerifiedArtifactRoot(context.Background(),
		published.closure.Roots[0].RootDigest); err != nil {
		t.Fatalf("terminal cleanup removed verified root: %v", err)
	}
	if err := published.cas.VerifyClosure(context.Background(), published.rebuilt); err != nil {
		t.Fatalf("terminal cleanup removed final CAS closure: %v", err)
	}
	if _, err := published.fixture.store.ReadPeerInboxArtifactPublish(context.Background(),
		ReadPeerInboxArtifactPublishSpec{
			Fence: published.claim.Fence(), Owner: published.owner, At: sweepAt,
		}); err != nil {
		t.Fatalf("terminal read after cleanup: %v", err)
	}
	if replay, err := published.fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{
			Fence: published.claim.Fence(), Owner: published.owner, At: sweepAt,
		}); err != nil || !replay.Replayed() {
		t.Fatalf("terminal MarkReady after cleanup = (%#v,%v)", replay, err)
	}
}

type publishedPeerInboxArtifact struct {
	fixture peerInboxFixture
	claim   PeerInboxArtifactClaim
	owner   artifactdomain.StageOwner
	closure VerifiedArtifactClosure
	rebuilt artifactdomain.Closure
	cas     *artifactdomain.CAS
	readyAt time.Time
}

func newPublishedPeerInboxArtifact(t *testing.T, seed string) publishedPeerInboxArtifact {
	t.Helper()
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t, seed, false)
	ctx := context.Background()
	stageAt := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(ctx,
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(ctx,
		PreparePeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(), Closure: closure, At: stageAt,
		}); err != nil {
		t.Fatal(err)
	}
	cas, err := artifactdomain.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	stage, err := cas.OpenStage(begun.Owner())
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range closure.Roots {
		if _, err := stage.Put(root.ManifestDigest, root.Manifest.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	content := []byte("closure-" + seed)
	if len(closure.Blocks) != 1 || closure.Blocks[0].Digest != model.Sum(content) {
		t.Fatal("unexpected fixture block closure")
	}
	if _, err := stage.Put(closure.Blocks[0].Digest, content); err != nil {
		t.Fatal(err)
	}
	acceptedAt := stageAt.Add(time.Second)
	if _, err := fixture.store.AcceptPeerInboxArtifactPublish(ctx,
		AcceptPeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: acceptedAt,
		}); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := RebuildArtifactClosure(ctx, closure, acceptedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Publish(ctx, rebuilt); err != nil {
		t.Fatal(err)
	}
	readyAt := acceptedAt.Add(time.Second)
	if _, err := fixture.store.MarkPeerInboxArtifactReady(ctx,
		MarkPeerInboxArtifactReadySpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: readyAt,
		}); err != nil {
		t.Fatal(err)
	}
	return publishedPeerInboxArtifact{
		fixture: fixture, claim: claim, owner: begun.Owner(), closure: closure,
		rebuilt: rebuilt, cas: cas, readyAt: readyAt,
	}
}

func terminalizePublishedPeerInboxArtifact(t *testing.T,
	published publishedPeerInboxArtifact,
) {
	t.Helper()
	claim := mustClaimPeerInboxSemantic(t, published.fixture.store,
		"terminal-ready-semantic-worker", published.readyAt.Add(time.Second))
	decisionAt := published.readyAt.Add(2 * time.Second)
	spec := peerInboxSemanticCommitSpec(t, published.fixture, claim, decisionAt)
	if _, err := published.fixture.store.CommitPeerInboxSemantic(context.Background(),
		spec, decisionAt); err != nil {
		t.Fatal(err)
	}
}

func assertPublishedPeerInboxArtifactReplay(t *testing.T,
	published publishedPeerInboxArtifact, at time.Time,
) {
	t.Helper()
	checkpoint, err := published.fixture.store.ReadPeerInboxArtifactPublish(
		context.Background(), ReadPeerInboxArtifactPublishSpec{
			Fence: published.claim.Fence(), Owner: published.owner, At: at,
		})
	if err != nil || checkpoint.State() != ArtifactStageReady {
		t.Fatalf("ready checkpoint replay = (%#v,%v)", checkpoint, err)
	}
	replayed, err := published.fixture.store.MarkPeerInboxArtifactReady(
		context.Background(), MarkPeerInboxArtifactReadySpec{
			Fence: published.claim.Fence(), Owner: published.owner, At: at,
		})
	if err != nil || !replayed.Replayed() || replayed.Changed() {
		t.Fatalf("ready settlement replay = (%#v,%v)", replayed, err)
	}
}

func assertEmptyPeerInboxArtifactReadyReplay(t *testing.T, store *Store,
	claim PeerInboxArtifactClaim, at time.Time,
) {
	t.Helper()
	replayed, err := store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: at})
	if err != nil || !replayed.Replayed() || replayed.Changed() {
		t.Fatalf("empty ready settlement replay = (%#v,%v)", replayed, err)
	}
}

func closePeerInboxArtifactChannel(t *testing.T, published publishedPeerInboxArtifact) {
	t.Helper()
	mustExec(t, published.fixture.store, `UPDATE channels
		SET status='closed',topic_state='left' WHERE channel_id=?`,
		published.fixture.channel.Channel().ID().String())
}
