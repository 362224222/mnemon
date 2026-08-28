package attach

// fsStat is the platform-independent subset of POSIX file metadata used for
// the owner-only projection checks. On Unix it is filled from syscall.Stat_t;
// on Windows the helper returns (nil, false) so the existing !ok branches fail
// closed instead of trusting a file.
type fsStat struct {
	Uid   uint32
	Nlink uint64
}
