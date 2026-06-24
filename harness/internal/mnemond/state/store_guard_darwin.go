//go:build darwin

package state

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// defaultStatFS classifies the filesystem hosting path via statfs(2) f_fstypename. It stats the parent
// directory so it works before the DB file itself exists (a fresh create). A type name containing nfs,
// smbfs, or webdav is a networked mount on which SQLite WAL is unsafe.
func defaultStatFS(path string) (fsKind, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(path), &st); err != nil {
		return fsKind{}, err
	}
	name := fstypename(st.Fstypename[:])
	low := strings.ToLower(name)
	networked := strings.Contains(low, "nfs") || strings.Contains(low, "smbfs") || strings.Contains(low, "webdav")
	return fsKind{name: name, networked: networked}, nil
}

// fstypename converts the fixed-size, NUL-terminated f_fstypename buffer to a string. The element type is
// int8 on darwin; byte(c) handles either int8 or byte without an array-type assertion.
func fstypename[T ~int8 | ~byte](buf []T) string {
	b := make([]byte, 0, len(buf))
	for _, c := range buf {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}
