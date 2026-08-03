//go:build linux

package agencycli

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func controlPeerUID(connection *net.UnixConn) (uint32, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("R7 Agency peer credential connection: %w", err)
	}
	var credential *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("R7 Agency peer credential control: %w", err)
	}
	if controlErr != nil || credential == nil {
		return 0, fmt.Errorf("R7 Agency read peer credential: %w", controlErr)
	}
	return credential.Uid, nil
}
