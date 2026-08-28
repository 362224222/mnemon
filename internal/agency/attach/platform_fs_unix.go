//go:build !windows

package attach

import (
	"os"
	"syscall"
)

func fsStatOf(info os.FileInfo) (*fsStat, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, false
	}
	return &fsStat{Uid: uint32(s.Uid), Nlink: uint64(s.Nlink)}, true
}
