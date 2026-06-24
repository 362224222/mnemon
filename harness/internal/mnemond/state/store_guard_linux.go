//go:build linux

package state

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Linux network/userspace filesystem magics (linux/magic.h) on which SQLite WAL is unsafe.
const (
	magicNFS  = 0x6969
	magicSMB  = 0x517b
	magicCIFS = 0xff534d42
	magicFUSE = 0x65735546
)

// defaultStatFS classifies the filesystem hosting path via statfs(2) f_type. It stats the parent directory
// so it works before the DB file itself exists (a fresh create).
func defaultStatFS(path string) (fsKind, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(path), &st); err != nil {
		return fsKind{}, err
	}
	magic := int64(st.Type)
	networked := magic == magicNFS || magic == magicSMB || magic == magicCIFS || magic == magicFUSE
	return fsKind{magic: magic, networked: networked}, nil
}
