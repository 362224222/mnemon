package store

import (
	"context"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

func TestArtifactStagingSweepNeverDeletesFinalArtifactMetadata(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	old := artifactClosureFixture(t, "staging-cleanup-old")
	if _, err := st.CheckpointVerifiedArtifactClosure(ctx, old); err != nil {
		t.Fatal(err)
	}

	cutoff := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	recent := verifiedRoot(t, "staging-cleanup-recent",
		`{"entries":[],"kind":"recent","total_bytes":0}`, 0)
	recent.CreatedAt = cutoff.Add(time.Minute)
	recent.VerifiedAt = recent.CreatedAt.Add(time.Second)
	if _, err := st.CheckpointVerifiedArtifactRoot(ctx, recent); err != nil {
		t.Fatal(err)
	}

	spec := artifactdomain.StagingSweepSpec{
		Cutoff:   cutoff,
		At:       cutoff.Add(time.Hour),
		MaxRoots: 1,
	}
	first, err := st.SweepArtifactStaging(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExpiredPins != 0 {
		t.Fatalf("first compatibility sweep removed final metadata: %#v", first)
	}
	spec.MaxRoots = 8
	second, err := st.SweepArtifactStaging(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ExpiredPins != 0 {
		t.Fatalf("second compatibility sweep removed final metadata: %#v", second)
	}

	var recentRoots int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_roots
		WHERE root_digest=?`, recent.RootDigest.String()).Scan(&recentRoots); err != nil ||
		recentRoots != 1 {
		t.Fatalf("recent root count = (%d, %v), want 1", recentRoots, err)
	}
	var oldRoots, oldMaps, orphanBlocks int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_roots
		WHERE root_digest IN (?,?)`,
		old.Roots[0].RootDigest.String(), old.Roots[1].RootDigest.String()).Scan(&oldRoots); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_root_blocks
		WHERE root_digest IN (?,?)`,
		old.Roots[0].RootDigest.String(), old.Roots[1].RootDigest.String()).Scan(&oldMaps); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_blocks`).Scan(&orphanBlocks); err != nil {
		t.Fatal(err)
	}
	if oldRoots != 2 || oldMaps != 3 || orphanBlocks != 2 {
		t.Fatalf("post-sweep state roots=%d maps=%d blocks=%d",
			oldRoots, oldMaps, orphanBlocks)
	}
}
