//go:build linux

package node

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// observeLifecycleSocket pins the socket inode with Linux O_PATH so immediate
// inode recycling cannot make a replacement pass os.SameFile.
func observeLifecycleSocket(state *identityNodeState) (
	*controlSocketObservation, bool, error,
) {
	fd, err := unix.Openat(int(state.dir.Fd()), controlSocketName,
		unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	for errors.Is(err, unix.EINTR) {
		fd, err = unix.Openat(int(state.dir.Fd()), controlSocketName,
			unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	}
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	pin := os.NewFile(uintptr(fd), controlSocketName)
	if pin == nil {
		_ = unix.Close(fd)
		return nil, false, errors.New("open control socket returned no file")
	}
	info, err := pin.Stat()
	if err != nil {
		_ = pin.Close()
		return nil, false, err
	}
	if err := validateLifecycleSocket(info, state.ownerUID); err != nil {
		_ = pin.Close()
		return nil, false, err
	}
	return &controlSocketObservation{identity: info, pin: pin}, true, nil
}
