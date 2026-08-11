//go:build darwin

package daemon

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func peerUID(connection *net.UnixConn) (uint32, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("daemon control: peer credential connection: %w", err)
	}
	var credential *unix.Xucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("daemon control: peer credential control: %w", err)
	}
	if controlErr != nil || credential == nil {
		return 0, fmt.Errorf("daemon control: read peer credential: %w", controlErr)
	}
	return credential.Uid, nil
}
