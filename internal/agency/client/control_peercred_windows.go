//go:build windows

package agencyclient

import (
	"net"
)

// controlPeerUID returns the peer UID of a Unix-domain control socket. Windows
// has no Unix-domain socket peer credentials, so the local-authority peer-owner
// check fails closed. The agency command tree is excluded from Windows builds,
// so this path is never reached at runtime.
func controlPeerUID(_ *net.UnixConn) (uint32, error) {
	return 0, errWindowsUnsupported
}
