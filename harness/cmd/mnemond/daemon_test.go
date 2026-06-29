package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

// status/down/logs operate on the pidfile + logfile under .mnemon/harness/local without spawning a
// process, so they are unit-testable; the full up→serve→down lifecycle is proven by the e2e leg.

func TestDaemonStatusStoppedWhenNoPidfile(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if err := daemonStatus([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out.String(), "stopped") {
		t.Fatalf("no pidfile must read stopped, got %q", out.String())
	}
}

func TestDaemonStatusRunningForLivePid(t *testing.T) {
	root := t.TempDir()
	dir, pidPath, _ := daemonPaths(root)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// our own pid is guaranteed alive.
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := daemonStatus([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out.String(), "running") {
		t.Fatalf("a live pid must read running, got %q", out.String())
	}
}

func TestDaemonDownStalePidfileIsIdempotent(t *testing.T) {
	root := t.TempDir()
	dir, pidPath, _ := daemonPaths(root)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// pid 2^30 is not a live process: down must clean the stale pidfile, not error.
	if err := os.WriteFile(pidPath, []byte("1073741824\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := daemonDown([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("down on stale pidfile must not error: %v", err)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("down must remove the stale pidfile (err=%v)", err)
	}
}

func TestDaemonDownNotRunning(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if err := daemonDown([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("down with no pidfile must be a no-op, got %v", err)
	}
	if !strings.Contains(out.String(), "not running") {
		t.Fatalf("down with no pidfile must report not running, got %q", out.String())
	}
}

func TestDaemonLogsPrintsFile(t *testing.T) {
	root := t.TempDir()
	dir, _, logPath := daemonPaths(root)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("Local Mnemon: ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := daemonLogs([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if !strings.Contains(out.String(), "Local Mnemon: ready") {
		t.Fatalf("logs must print the captured output, got %q", out.String())
	}
}

func TestDaemonLogsNoFileYet(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if err := daemonLogs([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("logs with no file must not error: %v", err)
	}
	if !strings.Contains(out.String(), "no log yet") {
		t.Fatalf("logs with no file must say so, got %q", out.String())
	}
}

func TestDaemonUpRefusesOccupiedListenAddress(t *testing.T) {
	root := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	if _, err := app.New(root).Setup(context.Background(), io.Discard, io.Discard, app.SetupOptions{
		Host:       "codex",
		Principal:  "planner@team",
		ControlURL: "http://" + addr,
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var out bytes.Buffer
	err = daemonUp([]string{"--root", root, "--addr", addr}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "listen address") || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("daemon up should refuse occupied address, got %v output=%q", err, out.String())
	}
	_, pidPath, _ := daemonPaths(root)
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("daemon up must not write pidfile when address is occupied, stat err=%v", err)
	}
}

func TestDaemonPathsUnderLocalStateDir(t *testing.T) {
	_, pidPath, logPath := daemonPaths("/proj")
	if pidPath != filepath.FromSlash("/proj/.mnemon/harness/local/mnemond.pid") {
		t.Fatalf("pidfile path: %s", pidPath)
	}
	if logPath != filepath.FromSlash("/proj/.mnemon/harness/local/mnemond.log") {
		t.Fatalf("logfile path: %s", logPath)
	}
}
