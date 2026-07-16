//go:build linux

package localapi

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestPeerUIDLinux(t *testing.T) {
	t.Parallel()
	path := filepath.Join(shortTempDir(t), "peer.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	result := make(chan uint32, 1)
	errors := make(chan error, 1)
	go func() {
		connection, err := listener.AcceptUnix()
		if err != nil {
			errors <- err
			return
		}
		defer connection.Close()
		uid, err := peerUID(connection)
		if err != nil {
			errors <- err
			return
		}
		result <- uid
	}()
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case err := <-errors:
		t.Fatal(err)
	case uid := <-result:
		if uid != uint32(os.Geteuid()) {
			t.Fatalf("peer UID = %d, want %d", uid, os.Geteuid())
		}
	}
}
