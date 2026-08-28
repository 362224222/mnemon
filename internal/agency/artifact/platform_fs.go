package artifact

// fsStat is the platform-independent subset of POSIX file metadata used for
// the owner-only object checks. On Unix it is filled from syscall.Stat_t; on
// Windows the helper returns (nil, false) so the existing !ok branches fail
// closed instead of trusting a file. The agency command tree is excluded from
// Windows builds, so these helpers are never exercised at runtime there.
type fsStat struct {
	Uid   uint32
	Nlink uint64
}
