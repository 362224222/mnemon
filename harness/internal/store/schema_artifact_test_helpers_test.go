package store

import (
	"context"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func pinSchemaSealedArtifactRoot(t *testing.T, st *Store, root model.Digest,
	created string,
) {
	t.Helper()
	if _, err := st.db.Exec(`INSERT INTO artifact_pins(
		root_digest,owner_kind,owner_id,created_at
	) VALUES(?,'event','schema-sealed-root',?)`, root.String(), created); err != nil {
		t.Fatal(err)
	}
}

func assertSchemaSealedArtifactSurvivesStagingCleanup(t *testing.T, st *Store,
	root model.Digest,
) {
	t.Helper()
	if _, err := st.db.Exec(`DELETE FROM artifact_pins
		WHERE root_digest=? AND owner_kind='event' AND owner_id='schema-sealed-root'`,
		root.String()); err != nil {
		t.Fatal(err)
	}
	cleanupAt := time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC)
	cleanup := artifactdomain.StagingSweepSpec{
		Cutoff: cleanupAt.Add(-time.Hour), At: cleanupAt,
		MaxRoots: 2,
	}
	if result, err := st.SweepArtifactStaging(context.Background(),
		cleanup); err != nil || result.ExpiredPins != 0 {
		t.Fatalf("whole unprovenanced root cleanup = (%#v, %v)", result, err)
	}
	var count int
	if err := st.db.QueryRow(
		`SELECT COUNT(*) FROM artifact_root_blocks WHERE root_digest=?`,
		root.String()).Scan(&count); err != nil || count != 1 {
		t.Fatalf("block map after whole-root cleanup = %d, err=%v", count, err)
	}
}
