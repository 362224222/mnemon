package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

func TestDoctorReportsMissingLocalConfigWithoutMutating(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if err := daemonDoctor([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("doctor: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"mnemond doctor",
		"- Local event node config: missing",
		"- Setup remediation: mnemon-harness setup --host codex",
		"- Boot chain: not ready (setup required)",
		"- Background daemon: stopped",
		"- Remote Workspace: not connected",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
	if _, err := os.Stat(filepath.Join(root, ".mnemon")); !os.IsNotExist(err) {
		t.Fatalf("doctor must not create local state, stat err=%v", err)
	}
}

func TestDoctorReportsConfiguredLocalEventNode(t *testing.T) {
	root := t.TempDir()
	if _, err := app.New(root).Setup(context.Background(), io.Discard, io.Discard, app.SetupOptions{
		Host:       "codex",
		Principal:  "codex@project",
		ControlURL: "http://127.0.0.1:9007",
		UseToken:   true,
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var out bytes.Buffer
	if err := daemonDoctor([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("doctor: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"- Local event node config: configured",
		"- Endpoint: http://127.0.0.1:9007",
		"- Principal: codex@project",
		"- Boot chain: ready (bindings=1)",
		"- Store: " + filepath.Join(root, ".mnemon", "harness", "local", "governed.db"),
		"- Background daemon: stopped",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}

func TestDoctorReportsBackgroundDaemonState(t *testing.T) {
	root := t.TempDir()
	dir, pidPath, _ := daemonPaths(root)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := daemonDoctor([]string{"--root", root}, &out, &out); err != nil {
		t.Fatalf("doctor: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "- Background daemon: running (pid "+strconv.Itoa(os.Getpid())+")") {
		t.Fatalf("doctor did not report live daemon pid:\n%s", got)
	}
}
