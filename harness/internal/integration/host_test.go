package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestDetectHostUsesCompletePreflightAndStableAutoPriority(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	codex := hostTestExecutable(t, directory, "codex")
	lookup := func(name string) (string, error) {
		if name != "codex" {
			return "", os.ErrNotExist
		}
		return codex, nil
	}
	run := func(_ context.Context, path string, args ...string) ([]byte, error) {
		key := filepath.Base(path) + " " + strings.Join(args, " ")
		return map[string][]byte{
			"codex --version":         []byte("codex-cli 0.144.4\n"),
			"codex app-server --help": []byte("Usage: codex app-server [OPTIONS]\n"),
		}[key], nil
	}

	automatic, err := detectHost(context.Background(), "auto", lookup, run)
	if err != nil || automatic.Host != assets.HostCodex || automatic.Executable != codex ||
		automatic.Version != "codex-cli 0.144.4" {
		t.Fatalf("auto DetectHost() = (%#v, %v)", automatic, err)
	}
	fallbackRun := func(ctx context.Context, path string, args ...string) ([]byte, error) {
		if filepath.Base(path) == "codex" && len(args) == 2 {
			return []byte("unrelated help\n"), nil
		}
		return run(ctx, path, args...)
	}
	fallback, err := detectHost(context.Background(), "auto", lookup, fallbackRun)
	if fallback != (HostObservation{}) || !errors.Is(err, ErrHostUnavailable) {
		t.Fatalf("broken auto DetectHost() = (%#v, %v)", fallback, err)
	}
	if _, err := detectHost(context.Background(), "codex", lookup, fallbackRun); !errors.Is(err, ErrHostUnavailable) {
		t.Fatalf("explicit broken Codex error = %v", err)
	}
}

func TestDetectHostRejectsUnsafeMissingAndMalformedAdapters(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	safe := hostTestExecutable(t, directory, "codex-safe")
	unsafe := hostTestExecutable(t, directory, "codex-unsafe")
	if err := os.Chmod(unsafe, 0o777); err != nil {
		t.Fatal(err)
	}
	validRun := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 1 {
			return []byte("codex-cli 1.0\n"), nil
		}
		return []byte("Usage: codex app-server\n"), nil
	}
	for _, test := range []struct {
		name      string
		selection string
		lookup    hostLookup
		run       hostRun
	}{
		{name: "unknown selection", selection: "other", lookup: func(string) (string, error) { return safe, nil }, run: validRun},
		{name: "removed Claude selection", selection: "claude-code", lookup: func(string) (string, error) { return safe, nil }, run: validRun},
		{name: "missing", selection: "codex", lookup: func(string) (string, error) { return "", os.ErrNotExist }, run: validRun},
		{name: "unsafe executable", selection: "codex", lookup: func(string) (string, error) { return unsafe, nil }, run: validRun},
		{name: "multiline version", selection: "codex", lookup: func(string) (string, error) { return safe, nil },
			run: func(context.Context, string, ...string) ([]byte, error) { return []byte("one\ntwo\n"), nil }},
		{name: "failed probe", selection: "codex", lookup: func(string) (string, error) { return safe, nil },
			run: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("failed") }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := detectHost(context.Background(), test.selection, test.lookup, test.run); !errors.Is(err, ErrHostUnavailable) {
				t.Fatalf("detectHost() error = %v", err)
			}
		})
	}
}

func TestRunHostProbeBoundsOutputAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	overflow := filepath.Join(directory, "overflow")
	if err := os.WriteFile(overflow, []byte("#!/bin/sh\nhead -c 9000 /dev/zero\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if output, err := runHostProbe(context.Background(), overflow); err == nil || output != nil {
		t.Fatalf("overflow probe = (%d bytes, %v)", len(output), err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runHostProbe(canceled, overflow); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe error = %v", err)
	}
	buffer := &boundedProbeBuffer{}
	if count, err := buffer.Write([]byte("ok")); err != nil || count != 2 || string(buffer.Bytes()) != "ok" {
		t.Fatalf("bounded buffer = (%d, %v, %q)", count, err, buffer.Bytes())
	}
	if count, err := buffer.Write(make([]byte, hostProbeOutputMax)); err == nil || count != 0 || buffer.Bytes() != nil {
		t.Fatalf("overflow buffer = (%d, %v, %v)", count, err, buffer.Bytes())
	}
}

func hostTestExecutable(t *testing.T, directory, name string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
