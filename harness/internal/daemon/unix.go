package daemon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	ownerDirectoryMode    os.FileMode = 0o700
	ownerSocketMode       os.FileMode = 0o600
	maxControlConnections             = 8
)

type ownerUnixListener struct {
	listener *net.UnixListener
	path     string
	identity os.FileInfo
	ownerUID uint32
	budget   chan struct{}
	close    sync.Once
	closeErr error
}

type budgetedUnixConnection struct {
	net.Conn
	release chan struct{}
	once    sync.Once
}

// removeStaleOwnerSocket is called only after Runtime owns the authority
// writer. That writer exclusion proves no live R7 daemon can own an existing
// control socket. Only an exact owner socket may be removed; every other path
// fails closed.
func removeStaleOwnerSocket(socketPath string) error {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return errors.New("daemon control: socket path must be absolute and canonical")
	}
	parent := filepath.Dir(socketPath)
	if err := requireOwnerDirectory(parent); err != nil {
		return fmt.Errorf("daemon control: unsafe socket directory: %w", err)
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("daemon control: inspect socket directory: %w", err)
	}
	ownerUID, err := fileOwnerUID(parentInfo)
	if err != nil {
		return fmt.Errorf("daemon control: inspect socket owner: %w", err)
	}
	identity, present, err := ownerSocketInfo(socketPath, ownerUID)
	if err != nil || !present {
		return err
	}
	connection, dialErr := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("daemon control: existing owner socket is active")
	}
	if errors.Is(dialErr, os.ErrNotExist) || errors.Is(dialErr, syscall.ENOENT) {
		return nil
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("daemon control: probe existing socket: %w", dialErr)
	}
	current, present, err := ownerSocketInfo(socketPath, ownerUID)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if !os.SameFile(identity, current) {
		return errors.New("daemon control: existing socket changed during recovery")
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("daemon control: remove stale socket: %w", err)
	}
	return syncOwnerDirectory(parent)
}

func ownerSocketInfo(path string, ownerUID uint32) (os.FileInfo, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("daemon control: inspect socket path: %w", err)
	}
	if info.Mode()&os.ModeType != os.ModeSocket || info.Mode().Perm() != ownerSocketMode {
		return nil, true, errors.New("daemon control: existing path is not an owner-only socket")
	}
	socketOwner, err := fileOwnerUID(info)
	stat, ok := info.Sys().(*syscall.Stat_t)
	if err != nil || socketOwner != ownerUID || !ok || stat.Nlink != 1 {
		return nil, true, errors.New("daemon control: existing socket has the wrong owner")
	}
	return info, true, nil
}

func listenOwnerUnix(socketPath string) (net.Listener, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, errors.New("daemon control: socket path must be absolute and canonical")
	}
	parent := filepath.Dir(socketPath)
	if err := requireOwnerDirectory(parent); err != nil {
		return nil, fmt.Errorf("daemon control: unsafe socket directory: %w", err)
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("daemon control: inspect socket directory: %w", err)
	}
	ownerUID, err := fileOwnerUID(parentInfo)
	if err != nil {
		return nil, fmt.Errorf("daemon control: inspect socket owner: %w", err)
	}
	if _, err := os.Lstat(socketPath); err == nil {
		return nil, errors.New("daemon control: socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("daemon control: inspect socket path: %w", err)
	}

	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("daemon control: listen: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	fail := func(cause error) (net.Listener, error) {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, cause
	}
	if err := os.Chmod(socketPath, ownerSocketMode); err != nil {
		return fail(fmt.Errorf("daemon control: protect socket: %w", err))
	}
	identity, err := os.Lstat(socketPath)
	if err != nil || identity.Mode()&os.ModeType != os.ModeSocket ||
		identity.Mode().Perm() != ownerSocketMode {
		return fail(errors.New("daemon control: socket did not become owner-only"))
	}
	socketOwner, err := fileOwnerUID(identity)
	if err != nil || socketOwner != ownerUID {
		return fail(errors.New("daemon control: socket owner does not match its directory"))
	}
	return &ownerUnixListener{listener: listener, path: socketPath,
		identity: identity, ownerUID: ownerUID,
		budget: make(chan struct{}, maxControlConnections)}, nil
}

func (listener *ownerUnixListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.listener.AcceptUnix()
		if err != nil {
			return nil, err
		}
		uid, err := peerUID(connection)
		if err == nil && uid == listener.ownerUID {
			select {
			case listener.budget <- struct{}{}:
				return &budgetedUnixConnection{Conn: connection,
					release: listener.budget}, nil
			default:
			}
		}
		_ = connection.Close()
	}
}

func (connection *budgetedUnixConnection) Close() error {
	if connection == nil || connection.Conn == nil {
		return nil
	}
	err := connection.Conn.Close()
	connection.once.Do(func() { <-connection.release })
	return err
}

func (listener *ownerUnixListener) Addr() net.Addr { return listener.listener.Addr() }

func (listener *ownerUnixListener) Close() error {
	if listener == nil || listener.listener == nil {
		return nil
	}
	listener.close.Do(func() {
		listener.closeErr = listener.listener.Close()
		current, err := os.Lstat(listener.path)
		switch {
		case err == nil && os.SameFile(current, listener.identity):
			if removeErr := os.Remove(listener.path); removeErr != nil {
				listener.closeErr = errors.Join(listener.closeErr, removeErr)
			}
		case err != nil && !errors.Is(err, os.ErrNotExist):
			listener.closeErr = errors.Join(listener.closeErr, err)
		}
	})
	return listener.closeErr
}

func fileOwnerUID(info os.FileInfo) (uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("daemon control: filesystem owner metadata is unavailable")
	}
	return stat.Uid, nil
}
