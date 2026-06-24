package state

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// fsKind classifies the filesystem hosting a database path for the anti-NFS guard. networked is true for
// any filesystem on which SQLite's WAL is unsafe (NFS/SMB/CIFS/FUSE/webdav). name/magic are diagnostics
// (the GOOS-specific defaultStatFS fills whichever its platform exposes).
type fsKind struct {
	name      string
	magic     int64
	networked bool
}

// statFSFunc classifies the filesystem under a path. It is injected into openGuard so a unit test can
// simulate a network mount without one (review blocker #9); the GOOS-tagged defaultStatFS is the real impl.
type statFSFunc func(path string) (fsKind, error)

// openGuard enforces S11 for a file-backed store: (1) the path must not live on a networked filesystem
// (a WAL DB on NFS silently corrupts — the one FATAL), and (2) only one writer may hold the file at a time
// (an exclusive flock on a lockfile next to it; self-releasing on process death). Both checks are
// skipped for :memory: and return a no-op release. The returned release MUST be called on Close.
func openGuard(path string, statFS statFSFunc) (func() error, error) {
	if path == ":memory:" {
		return func() error { return nil }, nil
	}
	kind, err := statFS(path)
	if err != nil {
		return nil, err
	}
	if kind.networked {
		return nil, fmt.Errorf("refusing to open %q on networked filesystem %q: WAL requires local disk (S11)", path, kind.name)
	}
	return acquireWriterLock(path + ".writer.lock")
}

// acquireWriterLock takes an exclusive flock on the lockfile. flock is per open-file-description and
// self-releasing on process death, so there is no stale-lock reaping and no read-check-remove race
// (the PID-reap predecessor could admit two writers when two openers raced over a crashed owner's
// lockfile). The holder PID is written into the file as a DIAGNOSTIC only; nothing parses it.
func acquireWriterLock(lock string) (func() error, error) {
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("database %q is locked by another live writer (%s)", strings.TrimSuffix(lock, ".writer.lock"), lock)
	}
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d", os.Getpid())
	// Release by closing the fd. Do NOT os.Remove the file: unlinking on release would let a
	// concurrent acquirer flock the orphaned inode while a third opener creates a fresh file.
	return func() error { return f.Close() }, nil
}
