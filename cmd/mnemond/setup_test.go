package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/internal/attach"
	"github.com/mnemon-dev/mnemon/internal/daemon"
)

func TestSetupProvisionsOnlyAfterTheFirstReadinessAttempt(t *testing.T) {
	var calls []string
	deps := setupDependencies{
		workingDirectory: func() (string, error) { return "/workspace", nil },
		resolveState: func(requested string) (string, string, error) {
			calls = append(calls, "resolve:"+requested)
			return "/workspace", "/workspace/.mnemon/agency", nil
		},
		ensure: func(_ context.Context, state string) error {
			calls = append(calls, "ensure:"+state)
			if len(calls) == 2 {
				return errors.New("not ready")
			}
			return nil
		},
		provision: func(_ context.Context, root string) (string, error) {
			calls = append(calls, "provision:"+root)
			return "/workspace/.mnemon/agency", nil
		},
		install: func(root string) error {
			calls = append(calls, "install:"+root)
			return nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runSetup(context.Background(), nil, &stdout, &stderr, deps)
	wantCalls := []string{"resolve:/workspace", "ensure:/workspace/.mnemon/agency",
		"provision:/workspace", "ensure:/workspace/.mnemon/agency", "install:/workspace"}
	if exit != 0 || stdout.String() !=
		`{"schema":"mnemon.setup","status":"ready","version":1}`+"\n" ||
		stderr.Len() != 0 || !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("fresh setup = exit %d stdout %q stderr %q calls %#v",
			exit, stdout.String(), stderr.String(), calls)
	}
}

func TestSetupReadyReplaySkipsProvisionAndStillVerifiesProjection(t *testing.T) {
	provisions := 0
	installs := 0
	deps := setupDependencies{
		workingDirectory: func() (string, error) { return "/unused", nil },
		resolveState: func(requested string) (string, string, error) {
			if requested != "/project" {
				t.Fatalf("requested project = %q", requested)
			}
			return requested, requested + "/.mnemon/agency", nil
		},
		ensure: func(context.Context, string) error { return nil },
		provision: func(context.Context, string) (string, error) {
			provisions++
			return "", nil
		},
		install: func(string) error { installs++; return nil },
	}
	if exit := runSetup(context.Background(), []string{"--runtime", "pi", "--project-root", "/project"},
		io.Discard, io.Discard, deps); exit != 0 || provisions != 0 || installs != 1 {
		t.Fatalf("ready setup = exit %d provisions %d installs %d", exit, provisions, installs)
	}
}

func TestSetupNeverInstallsAfterProvisionOrReadinessFailure(t *testing.T) {
	provisionFailure := errors.New("corrupt authority")
	installs := 0
	deps := setupDependencies{
		workingDirectory: func() (string, error) { return "/workspace", nil },
		resolveState: func(string) (string, string, error) {
			return "/workspace", "/workspace/state", nil
		},
		ensure:    func(context.Context, string) error { return errors.New("not ready") },
		provision: func(context.Context, string) (string, error) { return "", provisionFailure },
		install:   func(string) error { installs++; return nil },
	}
	var stderr bytes.Buffer
	exit := runSetup(context.Background(), nil, io.Discard, &stderr, deps)
	if exit != 1 || installs != 0 || !bytes.Contains(stderr.Bytes(), []byte("corrupt authority")) {
		t.Fatalf("failed setup = exit %d installs %d stderr %q", exit, installs, stderr.String())
	}
}

func TestSetupRejectsUnknownRuntimeAndMalformedOptionsBeforeEffects(t *testing.T) {
	effects := 0
	deps := setupDependencies{
		workingDirectory: func() (string, error) { effects++; return "", nil },
		resolveState:     func(string) (string, string, error) { effects++; return "", "", nil },
		ensure:           func(context.Context, string) error { effects++; return nil },
		provision: func(context.Context, string) (string, error) {
			effects++
			return "", nil
		},
		install: func(string) error { effects++; return nil },
	}
	for _, args := range [][]string{{"--runtime", "codex"}, {"--project-root"},
		{"--runtime", "pi", "extra"}, {"--runtime", "pi", "--runtime", "pi"}} {
		if exit := runSetup(context.Background(), args, io.Discard, io.Discard, deps); exit != 2 {
			t.Fatalf("malformed setup %v exit = %d", args, exit)
		}
	}
	if effects != 0 {
		t.Fatalf("malformed setup caused %d effects", effects)
	}
}

func TestSetupComposesRealProvisionAndPiProjection(t *testing.T) {
	base, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp(base, "mnemon-setup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	if err := os.Chmod(workspace, 0o700); err != nil {
		t.Fatal(err)
	}

	deps := productionSetupDependencies()
	ensureCalls := 0
	deps.ensure = func(ctx context.Context, state string) error {
		ensureCalls++
		if ensureCalls == 1 {
			if _, err := os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("first Ensure state = %v", err)
			}
			return errors.New("node is not provisioned")
		}
		runtime, err := daemon.OpenProvisioned(ctx, state)
		if err != nil {
			return err
		}
		return runtime.Close(context.Background())
	}
	var stdout, stderr bytes.Buffer
	exit := runSetup(context.Background(), []string{"--project-root", workspace},
		&stdout, &stderr, deps)
	if exit != 0 || ensureCalls != 2 || stderr.Len() != 0 {
		t.Fatalf("real setup = exit %d ensures %d stdout %q stderr %q",
			exit, ensureCalls, stdout.String(), stderr.String())
	}
	if err := attach.VerifyPi(workspace); err != nil {
		t.Fatalf("real setup Pi projection: %v", err)
	}
	_, state, err := daemon.ResolveProjectState(workspace)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := daemon.OpenProvisioned(context.Background(), state)
	if err != nil {
		t.Fatalf("real setup authority: %v", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
