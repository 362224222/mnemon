package agencyclient

import (
	"errors"
	"os"
)

// errWindowsUnsupported marks R7 durable-journal filesystem primitives that
// rely on Unix-only semantics (POSIX owner uid, hard-link counts, openat
// directory flags, flock, peercred). The agency command tree is excluded from
// Windows builds (see cmd/agency/terminal_unix.go, setup.go), so these helpers
// are never exercised there; they fail closed rather than silently trusting a
// file when compiled for Windows.
var errWindowsUnsupported = errors.New("R7 agency durable journal operations require a Unix host")

// fsStat is the platform-independent subset of POSIX file metadata the R7
// durable-journal layer uses for owner-only and hard-link safety checks. On
// Unix it is filled from syscall.Stat_t; on Windows the helpers return
// (nil, false) so the existing !ok fail-closed branches keep the durable
// journal unavailable instead of trusting a file.
type fsStat struct {
	Dev   uint64
	Ino   uint64
	Uid   uint32
	Nlink uint64
	Mode  uint32
}

// validateUnixOwner mirrors the original Unix owner/mode/type check against the
// platform-independent stat. The file-type test is delegated to fileTypeMatches
// because the S_IFMT constants are Unix-only.
func validateUnixOwner(stat *fsStat, mode os.FileMode, directory bool, ownerUID uint32) error {
	if stat == nil || stat.Uid != ownerUID || os.FileMode(stat.Mode).Perm() != mode {
		return errors.New("R7 client journal entry has unsafe ownership or mode")
	}
	if !fileTypeMatches(stat.Mode, directory) {
		return errors.New("R7 client journal entry has the wrong type")
	}
	return nil
}

// sameUnixIdentity reports whether the opened file still matches the stat taken
// earlier from its directory entry, preventing symlink/relink substitution.
func sameUnixIdentity(info os.FileInfo, stat *fsStat) bool {
	opened, ok := fsStatOf(info)
	return ok && stat != nil && opened.Dev == stat.Dev && opened.Ino == stat.Ino
}
