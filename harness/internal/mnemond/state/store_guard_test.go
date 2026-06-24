package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// S11: a WAL database must be single-writer and live on local disk. OpenStore rejects a second writer of
// the same file (an exclusive PID lockfile), refuses a networked filesystem (WAL silently corrupts on NFS),
// and pins synchronous(FULL). :memory: and distinct temp files are unaffected.

func TestOpenStoreRejectsSecondWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	s1, err := OpenStore(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer s1.Close()
	if s2, err := OpenStore(path); err == nil {
		s2.Close()
		t.Fatal("second writer on the same file must be rejected (single-writer lock)")
	}
}

func TestMemoryAndTempUnaffected(t *testing.T) {
	// :memory: skips the guard entirely — repeated opens are independent in-memory DBs.
	for i := 0; i < 3; i++ {
		s, err := OpenStore(":memory:")
		if err != nil {
			t.Fatalf("memory open %d: %v", i, err)
		}
		s.Close()
	}
	// distinct temp files are independent writers.
	d := t.TempDir()
	a, err := OpenStore(filepath.Join(d, "a.db"))
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	defer a.Close()
	b, err := OpenStore(filepath.Join(d, "b.db"))
	if err != nil {
		t.Fatalf("b (distinct file) must open alongside a: %v", err)
	}
	defer b.Close()
	// close-then-reopen the SAME file must succeed (Close releases the lock).
	a.Close()
	a2, err := OpenStore(filepath.Join(d, "a.db"))
	if err != nil {
		t.Fatalf("reopen after close must succeed: %v", err)
	}
	a2.Close()
}

func TestDSNHasSynchronousFull(t *testing.T) {
	dsn := dsnFor("/tmp/x.db")
	if !strings.Contains(dsn, "synchronous(FULL)") {
		t.Fatalf("file DSN must request synchronous(FULL); got %q", dsn)
	}
	if got := dsnFor(":memory:"); got != ":memory:" {
		t.Fatalf(":memory: DSN must stay bare, got %q", got)
	}
}

func TestStatfsGuardRejectsFakeNFS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	fakeNFS := func(string) (fsKind, error) { return fsKind{name: "nfs", networked: true}, nil }
	if _, err := openGuard(path, fakeNFS); err == nil {
		t.Fatal("guard must reject a networked filesystem (WAL on NFS is FATAL)")
	}
	// a local filesystem passes and yields a working release.
	release, err := openGuard(path, func(string) (fsKind, error) { return fsKind{name: "apfs"}, nil })
	if err != nil {
		t.Fatalf("local fs must pass the guard: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// A leftover lockfile from a crashed owner (no live flock holder) must not block a new writer —
// and acquiring over it must need no read-check-remove reaping (the TOCTOU the flock swap removes).
func TestOpenStoreAcquiresOverStaleLockfile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "k.db")
	if err := os.WriteFile(dbPath+".writer.lock", []byte("999999"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("stale lockfile must not block a new writer: %v", err)
	}
	defer s.Close()
}
