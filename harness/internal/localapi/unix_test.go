package localapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestListenOwnerUnixServesOnlyThroughProtectedSocket(t *testing.T) {
	t.Parallel()
	state := filepath.Join(shortTempDir(t), "node")
	if err := os.Mkdir(state, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "control.sock")
	listener, err := ListenOwnerUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != ownerSocketMode {
		t.Fatalf("socket info = %#v, %v", info, err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	}), ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Get("http://mnemon/health")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(body) != "ok" {
		t.Fatalf("Unix HTTP body = %q, %v", body, err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket remains after close: %v", err)
	}
}

func TestListenOwnerUnixRejectsUnsafePaths(t *testing.T) {
	t.Parallel()
	root := shortTempDir(t)
	unsafe := filepath.Join(root, "unsafe")
	if err := os.Mkdir(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	if listener, err := ListenOwnerUnix(filepath.Join(unsafe, "control.sock")); err == nil || listener != nil {
		t.Fatalf("unsafe parent listener = %v, %v", listener, err)
	}
	safe := filepath.Join(root, "safe")
	if err := os.Mkdir(safe, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(safe, "control.sock")
	if err := os.WriteFile(path, []byte("not a socket"), ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if listener, err := ListenOwnerUnix(path); err == nil || listener != nil {
		t.Fatalf("existing path listener = %v, %v", listener, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(safe, "target")
	if err := os.WriteFile(target, nil, ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if listener, err := ListenOwnerUnix(path); err == nil || listener != nil {
		t.Fatalf("symlink listener = %v, %v", listener, err)
	}
	if listener, err := ListenOwnerUnix("relative.sock"); err == nil || listener != nil ||
		!strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative listener = %v, %v", listener, err)
	}
}

func TestOwnerUnixClosePreservesReplacementPath(t *testing.T) {
	t.Parallel()
	state := filepath.Join(shortTempDir(t), "node")
	if err := os.Mkdir(state, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "control.sock")
	listener, err := ListenOwnerUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	moved := path + ".old"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), ownerSocketMode); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "replacement" {
		t.Fatalf("replacement = %q, %v", contents, err)
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "mnemon-localapi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}
