//go:build linux

package artifact

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameStageNoReplace(oldPath, newPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath,
		unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EEXIST) {
		return os.ErrExist
	}
	return err
}
