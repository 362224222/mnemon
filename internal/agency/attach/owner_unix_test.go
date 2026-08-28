//go:build !windows

package attach

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestOwnerDriftFailsClosed verifies that a file or directory whose recorded
// POSIX owner differs from the current effective user fails validation. It
// relies on syscall.Stat_t (a Unix-only type), so it is excluded from Windows
// builds where owner-uid semantics do not exist.
func TestOwnerDriftFailsClosed(t *testing.T) {
	path := filepath.Join(testWorkspace(t), "owned")
	if err := os.WriteFile(path, []byte("owned"), projectedMode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	foreign := foreignOwnerInfo{FileInfo: info, uid: uint32(os.Geteuid() + 1)}
	if err := validateSafeFile(foreign, projectedMode, 64); err == nil {
		t.Fatal("foreign-owner file passed validation")
	}
	directory, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	foreignDirectory := foreignOwnerInfo{FileInfo: directory, uid: uint32(os.Geteuid() + 1)}
	if err := validateSafeDirectory(foreignDirectory, false); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("foreign-owner directory validation = %v", err)
	}
}

type foreignOwnerInfo struct {
	os.FileInfo
	uid uint32
}

func (info foreignOwnerInfo) Sys() any {
	stat := *info.FileInfo.Sys().(*syscall.Stat_t)
	stat.Uid = info.uid
	return &stat
}
