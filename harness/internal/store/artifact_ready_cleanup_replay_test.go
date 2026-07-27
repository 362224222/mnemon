package store

import (
	"context"
	"testing"
	"time"
)

func TestReadyPeerInboxArtifactReplaySurvivesStageCleanup(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"ready-cleanup-replay", false)
	ctx := context.Background()
	stageAt := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(ctx,
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(ctx,
		PreparePeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(),
			Closure: closure, At: stageAt,
		}); err != nil {
		t.Fatal(err)
	}
	acceptedAt := stageAt.Add(time.Second)
	if _, err := fixture.store.AcceptPeerInboxArtifactPublish(ctx,
		AcceptPeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: acceptedAt,
		}); err != nil {
		t.Fatal(err)
	}
	readyAt := acceptedAt.Add(time.Second)
	if _, err := fixture.store.MarkPeerInboxArtifactReady(ctx,
		MarkPeerInboxArtifactReadySpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: readyAt,
		}); err != nil {
		t.Fatal(err)
	}
	cleanupAt := readyAt.Add(2 * time.Hour)
	page, err := fixture.store.ScanArtifactStageCleanupCandidates(ctx,
		ScanArtifactStageCleanupSpec{
			Cutoff: readyAt.Add(time.Hour), At: cleanupAt, MaxExamined: 1,
		})
	if err != nil || len(page.Candidates()) != 1 {
		t.Fatalf("ready cleanup claim = (%#v,%v)", page, err)
	}
	if _, err := fixture.store.MarkArtifactStageCleaned(ctx,
		MarkArtifactStageCleanedSpec{
			Candidate: page.Candidates()[0], At: cleanupAt,
		}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := fixture.store.ReadPeerInboxArtifactPublish(ctx,
		ReadPeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: cleanupAt,
		})
	if err != nil || checkpoint.State() != ArtifactStageReady {
		t.Fatalf("cleaned ready read = (%#v,%v)", checkpoint, err)
	}
	replay, err := fixture.store.MarkPeerInboxArtifactReady(ctx,
		MarkPeerInboxArtifactReadySpec{
			Fence: claim.Fence(), Owner: begun.Owner(), At: cleanupAt,
		})
	if err != nil || !replay.Replayed() {
		t.Fatalf("cleaned ready settlement replay = (%#v,%v)", replay, err)
	}
}
