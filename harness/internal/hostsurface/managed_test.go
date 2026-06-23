package hostsurface

import (
	"os"
	"path/filepath"
	"testing"
)

// classifyManaged is the no-clobber decision for a managed definition file: write when the file is
// absent or still matches what we last wrote (ours); preserve (conflict) when the on-disk content
// differs and we have no record that we wrote it, or it diverges from what we last wrote — on install
// and reinstall alike. We never overwrite a file we did not write.
func TestClassifyManaged(t *testing.T) {
	dir := t.TempDir()
	desired := []byte("desired content\n")

	t.Run("absent file writes", func(t *testing.T) {
		if got := classifyManaged(filepath.Join(dir, "absent"), desired, ""); got != classWrite {
			t.Fatalf("absent file must write; got %v", got)
		}
	})

	t.Run("matches desired writes (idempotent re-install)", func(t *testing.T) {
		dst := filepath.Join(dir, "same")
		mustWrite(t, dst, desired)
		if got := classifyManaged(dst, desired, ""); got != classWrite {
			t.Fatalf("a file already equal to desired must write (idempotent); got %v", got)
		}
	})

	t.Run("prior-match writes", func(t *testing.T) {
		dst := filepath.Join(dir, "ours")
		mustWrite(t, dst, desired)
		if got := classifyManaged(dst, []byte("an update"), hashBytes(desired)); got != classWrite {
			t.Fatalf("a file unmodified since we wrote it must write; got %v", got)
		}
	})

	t.Run("user-modified conflicts", func(t *testing.T) {
		dst := filepath.Join(dir, "edited")
		mustWrite(t, dst, []byte("the user changed this"))
		if got := classifyManaged(dst, desired, hashBytes([]byte("what we last wrote"))); got != classConflict {
			t.Fatalf("a user-edited managed file must be preserved; got %v", got)
		}
	})

	t.Run("pre-existing unknown differing file is preserved on install", func(t *testing.T) {
		dst := filepath.Join(dir, "preexisting")
		mustWrite(t, dst, []byte("pre-existing unmanaged content"))
		if got := classifyManaged(dst, desired, ""); got != classConflict {
			t.Fatalf("install must NOT clobber a pre-existing unknown differing file; got %v", got)
		}
	})
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
