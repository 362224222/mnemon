package authority

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenInitializesAndReopensPrivateWALStore(t *testing.T) {
	path := testDatabasePath(t)
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if store.Path() != path {
		t.Fatalf("Path() = %q, want %q", store.Path(), path)
	}
	var applicationID, version, foreignKeys, synchronous int
	var journalMode string
	queries := []struct {
		query string
		dest  any
	}{
		{"PRAGMA application_id", &applicationID}, {"PRAGMA user_version", &version},
		{"PRAGMA foreign_keys", &foreignKeys}, {"PRAGMA synchronous", &synchronous},
		{"PRAGMA journal_mode", &journalMode},
	}
	for _, query := range queries {
		if err := store.db.QueryRow(query.query).Scan(query.dest); err != nil {
			t.Fatalf("%s: %v", query.query, err)
		}
	}
	if applicationID != schemaApplicationID || version != SchemaVersion || foreignKeys != 1 ||
		synchronous != 2 || journalMode != "wal" {
		t.Fatalf("SQLite configuration = app:%d version:%d fk:%d sync:%d journal:%q",
			applicationID, version, foreignKeys, synchronous, journalMode)
	}
	assertMode(t, filepath.Dir(path), privateDirectoryMode)
	for _, candidate := range authorityFilePaths(path) {
		if _, err := os.Lstat(candidate); err == nil {
			assertMode(t, candidate, privateFileMode)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen exact store: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCreatesOnlyItsImmediatePrivateDirectory(t *testing.T) {
	base := t.TempDir()
	directory := filepath.Join(base, "authority")
	path := filepath.Join(directory, "node.db")
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	assertMode(t, directory, privateDirectoryMode)

	missingIntermediate := filepath.Join(base, "missing", "authority", "node.db")
	if opened, err := Open(context.Background(), missingIntermediate); err == nil || opened != nil {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("Open(path with missing parent chain) = (%v, %v)", opened, err)
	}
}

func TestOpenRejectsNoncanonicalPathAndDoesNotRepairExistingDirectory(t *testing.T) {
	if store, err := Open(context.Background(), "relative/node.db"); err == nil || store != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("Open(relative path) = (%v, %v)", store, err)
	}

	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(context.Background(), filepath.Join(directory, "node.db")); err == nil || store != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("Open(shared parent) = (%v, %v)", store, err)
	}
	assertMode(t, directory, 0o755)
}

func TestOpenRejectsSecondWriter(t *testing.T) {
	path := testDatabasePath(t)
	first, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(context.Background(), path)
	if second != nil || !errors.Is(err, ErrWriterActive) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second Open() = (%v, %v), want ErrWriterActive", second, err)
	}
}

func TestOpenRejectsUnsafePrivateFilesWithoutChangingTargets(t *testing.T) {
	for _, suffix := range []string{"", ".writer.lock", "-wal", "-shm"} {
		t.Run("symlink"+suffix, func(t *testing.T) {
			path := testDatabasePath(t)
			target := filepath.Join(filepath.Dir(path), "target")
			contents := []byte("must-not-change")
			if err := os.WriteFile(target, contents, privateFileMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path+suffix); err != nil {
				t.Fatal(err)
			}
			if store, err := Open(context.Background(), path); err == nil || store != nil {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("Open(symlink %q) = (%v, %v)", suffix, store, err)
			}
			got, err := os.ReadFile(target)
			if err != nil || string(got) != string(contents) {
				t.Fatalf("target changed: bytes=%q error=%v", got, err)
			}
		})
	}

	for _, suffix := range []string{"", ".writer.lock", "-wal", "-shm"} {
		t.Run("hardlink"+suffix, func(t *testing.T) {
			path := testDatabasePath(t)
			candidate := path + suffix
			if err := os.WriteFile(candidate, nil, privateFileMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(candidate, candidate+".alias"); err != nil {
				t.Skipf("hard links unavailable: %v", err)
			}
			if store, err := Open(context.Background(), path); err == nil || store != nil {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("Open(hard-linked %q) = (%v, %v)", suffix, store, err)
			}
		})

		t.Run("wrong-mode"+suffix, func(t *testing.T) {
			path := testDatabasePath(t)
			candidate := path + suffix
			if err := os.WriteFile(candidate, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(candidate, 0o644); err != nil {
				t.Fatal(err)
			}
			if store, err := Open(context.Background(), path); err == nil || store != nil {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("Open(wrong-mode %q) = (%v, %v)", suffix, store, err)
			}
			assertMode(t, candidate, 0o644)
		})
	}
}

func TestOpenRejectsWrongOrIncompleteSchemaIdentity(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{name: "version without application", statements: []string{"PRAGMA user_version = 1"}},
		{name: "claimed identity without tables", statements: []string{
			"PRAGMA application_id = 1296978487", "PRAGMA user_version = 1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := testDatabasePath(t)
			db := createPrivateSQLite(t, path)
			for _, statement := range test.statements {
				if _, err := db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := Open(context.Background(), path)
			if store != nil || !errors.Is(err, ErrUnsupportedSchema) {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("Open() = (%v, %v), want ErrUnsupportedSchema", store, err)
			}
		})
	}
}

func TestOpenRejectsSchemaDriftAndMissingClock(t *testing.T) {
	for _, mutation := range []string{"CREATE TABLE unexpected(value TEXT) STRICT",
		"ALTER TABLE attachments RENAME COLUMN mode TO wrong_mode", "DELETE FROM authority_clock",
		"DROP INDEX handlings_attachment_slot",
		`DROP INDEX handlings_attachment_slot;
		 CREATE UNIQUE INDEX handlings_attachment_slot ON handlings(claim_attachment_id) WHERE 0`} {
		t.Run(mutation, func(t *testing.T) {
			path := testDatabasePath(t)
			store, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", sqliteDSN(path))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(mutation); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			opened, err := Open(context.Background(), path)
			if opened != nil || !errors.Is(err, ErrUnsupportedSchema) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("Open(drifted schema) = (%v, %v), want ErrUnsupportedSchema", opened, err)
			}
		})
	}
}

func TestOpenRejectsSameIdentityWithWeakenedSchemaConstraint(t *testing.T) {
	path := testDatabasePath(t)
	db := createPrivateSQLite(t, path)
	needle := "state TEXT NOT NULL CHECK (state IN ('open', 'terminal'))"
	weakened := strings.Replace(currentSchema, needle, "state TEXT NOT NULL", 1)
	if weakened == currentSchema {
		t.Fatal("test did not weaken the embedded schema")
	}
	if _, err := db.Exec(weakened); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(context.Background(), path)
	if opened != nil || !errors.Is(err, ErrUnsupportedSchema) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("Open(weakened schema) = (%v, %v), want ErrUnsupportedSchema", opened, err)
	}
}

func TestCloseIsIdempotentAndClosesOperations(t *testing.T) {
	store, err := Open(context.Background(), testDatabasePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	artifact, err := VerifyArtifact([]byte("closed"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CatalogArtifact(context.Background(), artifact); !errors.Is(err, ErrClosed) {
		t.Fatalf("CatalogArtifact(after Close) = %v, want ErrClosed", err)
	}
}

func testDatabasePath(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "authority")
	if err := os.Mkdir(directory, privateDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, privateDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "authority.db")
}

func createPrivateSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, privateFileMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}
