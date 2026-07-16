package localapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const staleSocketProbeTimeout = 100 * time.Millisecond

var ErrOwnerUnixActive = errors.New("owner Unix socket is active")

// RemoveStaleOwnerUnix removes only an unreachable socket with the exact
// owner-only control shape. The caller must already own the daemon writer
// authority; this function independently refuses active, replaced or unsafe
// paths so crash recovery cannot become a generic filesystem deletion API.
func RemoveStaleOwnerUnix(ctx context.Context, socketPath string) (bool, error) {
	if ctx == nil || socketPath == "" || !filepath.IsAbs(socketPath) ||
		filepath.Clean(socketPath) != socketPath {
		return false, errors.New("local API: stale socket path is invalid")
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("local API: stale socket recovery: %w", err)
	}
	parent := filepath.Dir(socketPath)
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 ||
		parentInfo.Mode().Perm() != ownerDirectoryMode {
		return false, errors.New("local API: stale socket directory is not owner-only")
	}
	ownerUID, err := fileOwnerUID(parentInfo)
	if err != nil || ownerUID != uint32(os.Geteuid()) {
		return false, errors.New("local API: stale socket directory has the wrong owner")
	}
	before, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("local API: inspect stale socket: %w", err)
	}
	if err := validateStaleOwnerSocket(before, ownerUID); err != nil {
		return false, err
	}

	probeCtx, cancel := context.WithTimeout(ctx, staleSocketProbeTimeout)
	connection, dialErr := (&net.Dialer{}).DialContext(probeCtx, "unix", socketPath)
	cancel()
	if dialErr == nil {
		_ = connection.Close()
		return false, ErrOwnerUnixActive
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("local API: stale socket recovery: %w", err)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		if errors.Is(dialErr, os.ErrNotExist) {
			if _, statErr := os.Lstat(socketPath); errors.Is(statErr, os.ErrNotExist) {
				return false, nil
			}
		}
		return false, fmt.Errorf("local API: stale socket reachability is uncertain: %w", dialErr)
	}
	after, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(before, after) {
		return false, errors.New("local API: stale socket identity changed")
	}
	if err := validateStaleOwnerSocket(after, ownerUID); err != nil {
		return false, err
	}
	if err := os.Remove(socketPath); err != nil {
		return false, fmt.Errorf("local API: remove stale socket: %w", err)
	}
	directory, err := os.Open(parent)
	if err != nil {
		return false, fmt.Errorf("local API: persist stale socket removal: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return false, fmt.Errorf("local API: persist stale socket removal: %w",
			errors.Join(syncErr, closeErr))
	}
	return true, nil
}

func validateStaleOwnerSocket(info os.FileInfo, ownerUID uint32) error {
	if info == nil || info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != ownerSocketMode {
		return errors.New("local API: stale socket is not an owner-only socket")
	}
	owner, err := fileOwnerUID(info)
	if err != nil || owner != ownerUID {
		return errors.New("local API: stale socket has the wrong owner")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("local API: stale socket has an unsafe link count")
	}
	return nil
}
