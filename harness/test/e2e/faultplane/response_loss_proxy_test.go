package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunProxyDropsResponseAfterFirstByte(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	original := filepath.Join(root, "daemon.sock")
	upstream := filepath.Join(root, "upstream.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: original, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Rename(original, upstream); err != nil {
		t.Fatalf("relocate listening socket: %v", err)
	}

	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.AcceptUnix()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		request := make([]byte, len("opaque-request"))
		if _, readErr := io.ReadFull(connection, request); readErr != nil {
			serverDone <- readErr
			return
		}
		if string(request) != "opaque-request" {
			serverDone <- errors.New("upstream request bytes changed")
			return
		}
		_, writeErr := connection.Write([]byte("opaque-response-must-not-reach-client"))
		serverDone <- writeErr
	}()

	config := testProxyConfig(root, original, upstream, 3*time.Second)
	proxyDone := make(chan error, 1)
	go func() { proxyDone <- runProxy(context.Background(), config) }()
	waitForFile(t, config.readyPath)

	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: original, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("opaque-request")); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	count, readErr := client.Read(buffer)
	_ = client.Close()
	if count != 0 {
		t.Fatalf("client received %d response bytes: %q", count, buffer[:count])
	}
	if readErr == nil {
		t.Fatal("client read unexpectedly succeeded")
	}

	if err := <-proxyDone; err != nil {
		t.Fatalf("proxy failed: %v", err)
	}
	if err := <-serverDone; err != nil && !isClosedConnectionError(err) {
		t.Fatalf("upstream failed: %v", err)
	}
	if _, err := os.Lstat(original); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("proxy listener survived one-shot completion: %v", err)
	}

	var receipt lossReceipt
	readJSON(t, config.receiptPath, &receipt)
	if receipt.Outcome != "response_dropped_after_first_byte" ||
		!receipt.FirstResponseByteObserved || receipt.ResponseBytesForwarded != 0 {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if receipt.RequestBytesForwarded != int64(len("opaque-request")) {
		t.Fatalf("request bytes=%d", receipt.RequestBytesForwarded)
	}
	if strings.Contains(mustRead(t, config.receiptPath), "opaque-response") {
		t.Fatal("receipt retained domain response bytes")
	}
}

func TestRunProxyFailsWhenUpstreamNeverResponds(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: upstream, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		connection, acceptErr := listener.AcceptUnix()
		if acceptErr == nil {
			defer connection.Close()
			buffer := make([]byte, 16)
			_, _ = connection.Read(buffer)
			<-serverDone
		}
	}()

	config := testProxyConfig(root, filepath.Join(root, "proxy.sock"), upstream, 250*time.Millisecond)
	proxyDone := make(chan error, 1)
	go func() { proxyDone <- runProxy(context.Background(), config) }()
	waitForFile(t, config.readyPath)
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: config.listenPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = client.Write([]byte("request"))
	err = <-proxyDone
	close(serverDone)
	_ = client.Close()
	if err == nil || !strings.Contains(err.Error(), "timed out before") {
		t.Fatalf("error=%v, want bounded response timeout", err)
	}
	if _, err := os.Lstat(config.receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed injection wrote a success receipt: %v", err)
	}
}

func TestRunProxyRejectsResponseWithoutForwardedRequest(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: upstream, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.AcceptUnix()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		_, writeErr := connection.Write([]byte("unsolicited-response"))
		serverDone <- writeErr
	}()

	config := testProxyConfig(root, filepath.Join(root, "proxy.sock"), upstream, time.Second)
	proxyDone := make(chan error, 1)
	go func() { proxyDone <- runProxy(context.Background(), config) }()
	waitForFile(t, config.readyPath)
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: config.listenPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	err = <-proxyDone
	_ = client.Close()
	if err == nil || !strings.Contains(err.Error(), "before any client request byte") {
		t.Fatalf("error=%v, want zero-request rejection", err)
	}
	if err := <-serverDone; err != nil && !isClosedConnectionError(err) {
		t.Fatalf("upstream failed: %v", err)
	}
	if _, err := os.Lstat(config.receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("zero-request exchange wrote a success receipt: %v", err)
	}
}

func TestRunProxyCancellationOwnsListener(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: upstream, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	config := testProxyConfig(root, filepath.Join(root, "proxy.sock"), upstream, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	proxyDone := make(chan error, 1)
	go func() { proxyDone <- runProxy(ctx, config) }()
	waitForFile(t, config.readyPath)
	cancel()
	select {
	case err := <-proxyDone:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("error=%v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not honor cancellation")
	}
	if _, err := os.Lstat(config.listenPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled proxy listener remains: %v", err)
	}
}

func TestValidateProxyConfigRejectsSymlinkAndExistingOutputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	realSocket := filepath.Join(root, "real.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: realSocket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	symlink := filepath.Join(root, "upstream.sock")
	if err := os.Symlink(realSocket, symlink); err != nil {
		t.Fatal(err)
	}
	config := testProxyConfig(root, filepath.Join(root, "proxy.sock"), symlink, time.Second)
	if err := validateProxyConfig(config); err == nil || !strings.Contains(err.Error(), "not a Unix socket") {
		t.Fatalf("symlink validation error=%v", err)
	}

	config.upstreamPath = realSocket
	if err := os.WriteFile(config.receiptPath, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateProxyConfig(config); err == nil || !strings.Contains(err.Error(), "must not already exist") {
		t.Fatalf("existing output validation error=%v", err)
	}
}

func testProxyConfig(root, listen, upstream string, timeout time.Duration) proxyConfig {
	return proxyConfig{
		listenPath:   listen,
		upstreamPath: upstream,
		receiptPath:  filepath.Join(root, "receipt.json"),
		readyPath:    filepath.Join(root, "ready.json"),
		startedPath:  filepath.Join(root, "started.json"),
		token:        "fault-token-1",
		timeout:      timeout,
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func readJSON(t *testing.T, path string, destination any) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(content, destination); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func isClosedConnectionError(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "broken pipe")
}
