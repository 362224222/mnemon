//go:build darwin

package artifact

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameStageNoReplace(oldPath, newPath string) error {
	err := unix.RenamexNp(oldPath, newPath, unix.RENAME_EXCL)
	if errors.Is(err, unix.EEXIST) {
		return os.ErrExist
	}
	return err
}
