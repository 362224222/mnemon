package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func assertPeerInboxSemanticSurvivesReadyCleanup(t *testing.T,
	fixture peerInboxFixture, claim PeerInboxSemanticClaim, inboxID model.InboxID,
	root model.Digest, decisionAt time.Time,
) {
	t.Helper()
	cleanupAt := decisionAt.Add(time.Second)
	page, err := fixture.store.ScanArtifactStageCleanupCandidates(
		context.Background(), ScanArtifactStageCleanupSpec{
			Cutoff: decisionAt.Add(time.Nanosecond), At: cleanupAt,
			MaxExamined: 1,
		})
	if err != nil || len(page.Candidates()) != 1 ||
		page.Candidates()[0].Owner().CanonicalID() != inboxID.String() ||
		page.Candidates()[0].State() != ArtifactStageReady {
		t.Fatalf("post-semantic ready cleanup = (%#v,%v)", page, err)
	}
	if _, err := fixture.store.MarkArtifactStageCleaned(context.Background(),
		MarkArtifactStageCleanedSpec{
			Candidate: page.Candidates()[0], At: cleanupAt,
		}); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
		peerInboxSemanticCommitSpec(t, fixture, claim, decisionAt),
		cleanupAt.Add(time.Hour))
	if err != nil || !replay.Replayed() {
		t.Fatalf("semantic replay after ready cleanup = (%#v,%v)", replay, err)
	}
	if _, err := fixture.store.GetVerifiedArtifactRoot(context.Background(),
		root); err != nil {
		t.Fatalf("post-semantic cleanup removed imported root: %v", err)
	}
}
