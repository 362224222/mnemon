package daemon

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOwnerUnixListenerAuthenticatesPeerAndPreservesReplacement(t *testing.T) {
	directory := canonicalTempDir(t)
	if err := os.Chmod(directory, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "control.sock")
	listener, err := listenOwnerUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeType != os.ModeSocket ||
		info.Mode().Perm() != ownerSocketMode {
		t.Fatalf("owner socket = (%v, %v)", info, err)
	}

	accepted := make(chan net.Conn, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			acceptErrors <- err
			return
		}
		accepted <- connection
	}()
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case connection := <-accepted:
		_ = connection.Close()
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("owner peer was not accepted")
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("must survive listener close")
	if err := os.WriteFile(path, replacement, ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(replacement) {
		t.Fatalf("replacement changed: %q, %v", got, err)
	}
}

func TestOwnerUnixListenerRejectsUnsafeDirectoryAndExistingPath(t *testing.T) {
	directory := canonicalTempDir(t)
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "control.sock")
	if listener, err := listenOwnerUnix(path); err == nil || listener != nil {
		if listener != nil {
			_ = listener.Close()
		}
		t.Fatalf("unsafe directory listener = (%v, %v)", listener, err)
	}
	if err := os.Chmod(directory, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("occupied"), ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if listener, err := listenOwnerUnix(path); err == nil || listener != nil {
		if listener != nil {
			_ = listener.Close()
		}
		t.Fatalf("occupied path listener = (%v, %v)", listener, err)
	}
}

func TestStaleRecoveryPreservesActiveOwnerSocket(t *testing.T) {
	directory := canonicalTempDir(t)
	socket := filepath.Join(directory, "active.sock")
	listener, err := listenOwnerUnix(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	before, err := os.Lstat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := removeStaleOwnerSocket(socket); err == nil {
		t.Fatal("stale recovery accepted an active owner socket")
	}
	after, err := os.Lstat(socket)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("active owner socket changed: %v", err)
	}
}

func TestStaleRecoveryPreservesUnsafeExistingPath(t *testing.T) {
	directory := canonicalTempDir(t)
	path := filepath.Join(directory, "unsafe.sock")
	if err := os.WriteFile(path, []byte("not a socket"), ownerFileMode); err != nil {
		t.Fatal(err)
	}
	if err := removeStaleOwnerSocket(path); err == nil {
		t.Fatal("stale recovery accepted a regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "not a socket" {
		t.Fatalf("unsafe path changed: content=%q error=%v", raw, err)
	}
}

func TestOwnerUnixListenerBoundsConnectionsAndReleasesBudget(t *testing.T) {
	directory := canonicalTempDir(t)
	if err := os.Chmod(directory, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "bounded.sock")
	listener, err := listenOwnerUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clients := make([]net.Conn, 0, maxControlConnections+1)
	servers := make([]net.Conn, 0, maxControlConnections+1)
	defer func() {
		for _, connection := range clients {
			_ = connection.Close()
		}
		for _, connection := range servers {
			_ = connection.Close()
		}
	}()
	clients, servers = fillConnectionBudget(t, listener, path, clients, servers)

	nextAccepted := acceptOne(listener)
	overflow, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = overflow.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	if _, err := overflow.Read(one[:]); err == nil {
		t.Fatal("connection beyond the fixed budget remained open")
	}
	_ = overflow.Close()

	if err := servers[0].Close(); err != nil {
		t.Fatal(err)
	}
	servers = servers[1:]
	replacement, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, replacement)
	select {
	case result := <-nextAccepted:
		if result.err != nil {
			t.Fatal(result.err)
		}
		servers = append(servers, result.connection)
	case <-time.After(5 * time.Second):
		t.Fatal("released connection budget was not reusable")
	}
}

func fillConnectionBudget(t *testing.T, listener net.Listener, path string,
	clients, servers []net.Conn,
) ([]net.Conn, []net.Conn) {
	t.Helper()
	for range maxControlConnections {
		accepted := acceptOne(listener)
		client, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
		select {
		case result := <-accepted:
			if result.err != nil {
				t.Fatal(result.err)
			}
			servers = append(servers, result.connection)
		case <-time.After(5 * time.Second):
			t.Fatal("bounded connection was not accepted")
		}
	}
	return clients, servers
}

type acceptResult struct {
	connection net.Conn
	err        error
}

func acceptOne(listener net.Listener) <-chan acceptResult {
	result := make(chan acceptResult, 1)
	go func() {
		connection, err := listener.Accept()
		result <- acceptResult{connection: connection, err: err}
	}()
	return result
}
