//go:build linux

package localapi

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func peerUID(connection *net.UnixConn) (uint32, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("local API: peer credential connection: %w", err)
	}
	var credential *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("local API: peer credential control: %w", err)
	}
	if controlErr != nil || credential == nil {
		return 0, fmt.Errorf("local API: read peer credential: %w", controlErr)
	}
	return credential.Uid, nil
}
