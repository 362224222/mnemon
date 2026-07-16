package localapi

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

const (
	ownerDirectoryMode os.FileMode = 0o700
	ownerSocketMode    os.FileMode = 0o600
)

type ownerUnixListener struct {
	listener *net.UnixListener
	path     string
	identity os.FileInfo
	ownerUID uint32
}

func ListenOwnerUnix(socketPath string) (net.Listener, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, errors.New("local API: socket path must be absolute and canonical")
	}
	parent := filepath.Dir(socketPath)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("local API: inspect socket directory: %w", err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 || parentInfo.Mode().Perm() != ownerDirectoryMode {
		return nil, errors.New("local API: socket directory must be an owner-only real directory")
	}
	ownerUID, err := fileOwnerUID(parentInfo)
	if err != nil || ownerUID != uint32(os.Geteuid()) {
		return nil, errors.New("local API: socket directory is not owned by the daemon user")
	}
	if _, err := os.Lstat(socketPath); err == nil {
		return nil, errors.New("local API: socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("local API: inspect socket path: %w", err)
	}

	address := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("local API: listen: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	fail := func(cause error) (net.Listener, error) {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, cause
	}
	if err := os.Chmod(socketPath, ownerSocketMode); err != nil {
		return fail(fmt.Errorf("local API: protect socket: %w", err))
	}
	identity, err := os.Lstat(socketPath)
	if err != nil || identity.Mode()&os.ModeSocket == 0 || identity.Mode().Perm() != ownerSocketMode {
		return fail(errors.New("local API: socket did not become owner-only"))
	}
	socketOwner, err := fileOwnerUID(identity)
	if err != nil || socketOwner != ownerUID {
		return fail(errors.New("local API: socket owner does not match its directory"))
	}
	return &ownerUnixListener{listener: listener, path: socketPath, identity: identity, ownerUID: ownerUID}, nil
}

func (l *ownerUnixListener) Accept() (net.Conn, error) {
	for {
		connection, err := l.listener.AcceptUnix()
		if err != nil {
			return nil, err
		}
		uid, err := peerUID(connection)
		if err == nil && uid == l.ownerUID {
			return connection, nil
		}
		_ = connection.Close()
	}
}

func (l *ownerUnixListener) Addr() net.Addr { return l.listener.Addr() }

func (l *ownerUnixListener) Close() error {
	if l == nil || l.listener == nil {
		return nil
	}
	err := l.listener.Close()
	current, statErr := os.Lstat(l.path)
	if statErr == nil && os.SameFile(current, l.identity) {
		if removeErr := os.Remove(l.path); removeErr != nil && err == nil {
			err = removeErr
		}
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) && err == nil {
		err = statErr
	}
	return err
}

func fileOwnerUID(info os.FileInfo) (uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("local API: filesystem owner metadata is unavailable")
	}
	return stat.Uid, nil
}
