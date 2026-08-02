package authority

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestOpenExistingWithArtifactVerifierNeverInitializesAuthority(t *testing.T) {
	ctx := context.Background()
	verifier := ArtifactVerifierFunc(func(context.Context, agency.Digest, int64) error { return nil })

	t.Run("missing", func(t *testing.T) {
		path := testDatabasePath(t)
		opened, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
		if opened != nil || err == nil {
			t.Fatalf("OpenExistingWithArtifactVerifier(missing) = (%v, %v)", opened, err)
		}
		for _, candidate := range []string{path, path + ".writer.lock"} {
			if _, statErr := os.Lstat(candidate); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("strict open created %s: %v", filepath.Base(candidate), statErr)
			}
		}
	})

	t.Run("empty", func(t *testing.T) {
		path := testDatabasePath(t)
		initial, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if err := initial.Close(); err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite", sqliteDSN(path))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA application_id = 0; PRAGMA user_version = 0"); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		opened, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
		if opened != nil || !errors.Is(err, ErrUnsupportedSchema) {
			if opened != nil {
				_ = opened.Close()
			}
			t.Fatalf("OpenExistingWithArtifactVerifier(empty identity) = (%v, %v)", opened, err)
		}
		check, err := sql.Open("sqlite", sqliteDSN(path))
		if err != nil {
			t.Fatal(err)
		}
		defer check.Close()
		var applicationID, version int
		if err := check.QueryRow("PRAGMA application_id").Scan(&applicationID); err != nil {
			t.Fatal(err)
		}
		if err := check.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
			t.Fatal(err)
		}
		if applicationID != 0 || version != 0 {
			t.Fatalf("strict open repaired schema identity = (%d,%d)", applicationID, version)
		}
	})

	t.Run("missing writer guard", func(t *testing.T) {
		path := testDatabasePath(t)
		created, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if err := created.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path + ".writer.lock"); err != nil {
			t.Fatal(err)
		}
		opened, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
		if opened != nil || err == nil {
			t.Fatalf("OpenExistingWithArtifactVerifier(missing guard) = (%v, %v)", opened, err)
		}
		if _, statErr := os.Lstat(path + ".writer.lock"); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("strict open recreated writer guard: %v", statErr)
		}
	})
}

func TestOpenExistingWithArtifactVerifierOwnsExactStoreAndPrincipal(t *testing.T) {
	ctx := context.Background()
	path := testDatabasePath(t)
	principal, err := agency.NewAgentPrincipalID("principal:existing")
	if err != nil {
		t.Fatal(err)
	}
	created, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.EnrollPrincipal(ctx, principal); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	verifier := ArtifactVerifierFunc(func(context.Context, agency.Digest, int64) error { return nil })
	opened, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.RequirePrincipal(ctx, principal); err != nil {
		t.Fatalf("RequirePrincipal(enrolled) = %v", err)
	}
	missing, _ := agency.NewAgentPrincipalID("principal:missing")
	if err := opened.RequirePrincipal(ctx, missing); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("RequirePrincipal(missing) = %v", err)
	}
	second, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
	if second != nil || !errors.Is(err, ErrWriterActive) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second strict open = (%v, %v)", second, err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExistingWithArtifactVerifier(ctx, path, verifier)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
