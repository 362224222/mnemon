package localapi

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveStaleOwnerUnixRecoversOnlyUnreachableCanonicalSocket(t *testing.T) {
	t.Parallel()
	directory := staleSocketTestDirectory(t)
	path := filepath.Join(directory, "control.sock")
	removed, err := RemoveStaleOwnerUnix(context.Background(), path)
	if err != nil || removed {
		t.Fatalf("missing RemoveStaleOwnerUnix() = (%t, %v)", removed, err)
	}
	listener := staleSocketTestListener(t, path)
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(path, ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	removed, err = RemoveStaleOwnerUnix(context.Background(), path)
	if err != nil || !removed {
		t.Fatalf("stale RemoveStaleOwnerUnix() = (%t, %v)", removed, err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale socket remains: %v", err)
	}
}

func TestRemoveStaleOwnerUnixPreservesActiveAndUnsafePaths(t *testing.T) {
	t.Parallel()
	t.Run("active", func(t *testing.T) {
		directory := staleSocketTestDirectory(t)
		path := filepath.Join(directory, "control.sock")
		listener := staleSocketTestListener(t, path)
		defer listener.Close()
		if err := os.Chmod(path, ownerSocketMode); err != nil {
			t.Fatal(err)
		}
		if removed, err := RemoveStaleOwnerUnix(context.Background(), path); removed || !errors.Is(err, ErrOwnerUnixActive) {
			t.Fatalf("active RemoveStaleOwnerUnix() = (%t, %v)", removed, err)
		}
		if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("active socket changed: %v, %v", info, err)
		}
	})

	for _, kind := range []string{"regular", "symlink"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			directory := staleSocketTestDirectory(t)
			path := filepath.Join(directory, "control.sock")
			if kind == "regular" {
				if err := os.WriteFile(path, nil, ownerSocketMode); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(filepath.Join(directory, "outside"), path); err != nil {
				t.Fatal(err)
			}
			if removed, err := RemoveStaleOwnerUnix(context.Background(), path); err == nil || removed {
				t.Fatalf("unsafe RemoveStaleOwnerUnix() = (%t, %v)", removed, err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("unsafe path was removed: %v", err)
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RemoveStaleOwnerUnix(canceled, filepath.Join(t.TempDir(), "control.sock")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery error = %v", err)
	}
}

func staleSocketTestListener(t *testing.T, path string) *net.UnixListener {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func staleSocketTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "mn-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return directory
}
