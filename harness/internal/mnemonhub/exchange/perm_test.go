package exchange

import (
	"os"
	"path/filepath"
	"testing"
)

// T1 floor on the sync-first path: `sync pull --once` before setup/local run creates the private
// store dir — it must be owner-only like every other creation site (operator probe found 0755).
func TestSyncFirstStoreDirIsOwnerOnly(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, ".mnemon", "harness", "local", "governed.db")
	if err := SetSyncPullCursor(path, "peer-1", "1"); err != nil {
		t.Fatalf("sync-first cursor write: %v", err)
	}
	st, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o700 {
		t.Fatalf("sync-created store dir mode %o, want 0700", st.Mode().Perm())
	}
}
