package daemon

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func recoverLinkedTransportIdentity(directory, pending, final string) error {
	if !sameIdentityLink(pending, final) {
		return nil
	}
	if err := os.Remove(pending); err != nil {
		return fmt.Errorf("provision transport identity: recover linked identity: %w", err)
	}
	return syncOwnerDirectory(directory)
}

func replayTransportIdentity(final, pending string) (TransportIdentity, bool, error) {
	identity, raw, found, err := readTransportIdentityFile(final)
	if err != nil || !found {
		return TransportIdentity{}, false, err
	}
	if err := settleMatchingPendingIdentity(pending, raw); err != nil {
		return TransportIdentity{}, false, err
	}
	return identity.projection, true, nil
}

func recoverPendingTransportIdentity(directory, pending, final string) (
	TransportIdentity, bool, error,
) {
	identity, raw, found, err := readTransportIdentityFile(pending)
	if err != nil {
		if discardErr := discardInterruptedPendingIdentity(directory, pending); discardErr != nil {
			return TransportIdentity{}, false, errors.Join(err, discardErr)
		}
		return TransportIdentity{}, false, nil
	}
	if !found {
		return TransportIdentity{}, false, nil
	}
	if err := syncPendingTransportIdentity(pending); err != nil {
		return TransportIdentity{}, false, err
	}
	if err := publishTransportIdentity(directory, pending, final, raw); err != nil {
		return TransportIdentity{}, false, err
	}
	return identity.projection, true, nil
}

func discardInterruptedPendingIdentity(directory, pending string) error {
	info, err := os.Lstat(pending)
	if err != nil {
		return fmt.Errorf("provision transport identity: inspect interrupted pending: %w", err)
	}
	if err := requireOwnerRegularFile(info); err != nil {
		return err
	}
	if err := os.Remove(pending); err != nil {
		return fmt.Errorf("provision transport identity: discard interrupted pending: %w", err)
	}
	return syncOwnerDirectory(directory)
}

func syncPendingTransportIdentity(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("provision transport identity: reopen pending: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return errors.New("provision transport identity: pending is unavailable")
	}
	info, statErr := file.Stat()
	current, pathErr := os.Lstat(path)
	if statErr != nil || pathErr != nil || !os.SameFile(info, current) {
		_ = file.Close()
		return errors.New("provision transport identity: pending identity changed before sync")
	}
	if err := requireOwnerRegularFile(info); err != nil {
		_ = file.Close()
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
