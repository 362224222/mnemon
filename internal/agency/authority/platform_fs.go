package authority

import "errors"

// errWindowsUnsupported marks authority-store filesystem primitives that rely
// on Unix-only semantics (POSIX owner uid, hard-link counts, open flags,
// flock). The agency command tree is excluded from Windows builds, so these
// helpers are never exercised at runtime there; they fail closed rather than
// silently trusting a file when compiled for Windows.
var errWindowsUnsupported = errors.New("R7 authority store operations require a Unix host")

// fsStat is the platform-independent subset of POSIX file metadata used for
// the owner-only authority store checks. On Unix it is filled from
// syscall.Stat_t; on Windows the helper returns (nil, false) so the existing
// !ok branches fail closed.
type fsStat struct {
	Uid   uint32
	Nlink uint64
}
