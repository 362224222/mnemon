package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestRun(t *testing.T) {
	t.Parallel()

	runCommand := func(ctx context.Context, args ...string) (string, string, error) {
		t.Helper()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		err := run(ctx, args, &stdout, &stderr)
		return stdout.String(), stderr.String(), err
	}

	wantHelp, wantStderr, err := runCommand(context.Background())
	if err != nil {
		t.Fatalf("empty invocation: %v", err)
	}
	if wantStderr != "" {
		t.Fatalf("empty invocation stderr = %q", wantStderr)
	}
	if wantHelp != helpText {
		t.Fatalf("empty invocation help mismatch:\n%s", wantHelp)
	}

	for _, arg := range []string{"-h", "--help", "help"} {
		arg := arg
		t.Run("help_"+strings.TrimLeft(arg, "-"), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stdout, stderr, err := runCommand(ctx, arg)
			if err != nil || stderr != "" || stdout != wantHelp {
				t.Fatalf("run(%q) = stdout %q, stderr %q, err %v", arg, stdout, stderr, err)
			}
		})
	}

	for _, arg := range []string{"--version", "version"} {
		stdout, stderr, err := runCommand(context.Background(), arg)
		if err != nil || stderr != "" || stdout != "mnemond version dev\n" {
			t.Fatalf("run(%q) = stdout %q, stderr %q, err %v", arg, stdout, stderr, err)
		}
	}

	for _, command := range []string{
		"sync", "connect", "hub", "tower", "session", "config", "loop",
		"control", "local", "daemon", "multica", "acceptance", "agent",
	} {
		stdout, stderr, err := runCommand(context.Background(), command)
		want := fmt.Sprintf("unsupported command %q", command)
		if stdout != "" || stderr != "" || err == nil || err.Error() != want {
			t.Errorf("run(%q) = stdout %q, stderr %q, err %v; want %q", command, stdout, stderr, err, want)
		}
	}

	lowerHelp := strings.ToLower(wantHelp)
	for _, forbidden := range []string{
		"hub", "remote workspace", "multica", "generic capability", "mcp",
		"memory", "evolution", "tower", "session",
	} {
		if strings.Contains(lowerHelp, forbidden) {
			t.Errorf("help contains retired vocabulary %q", forbidden)
		}
	}
	if strings.Contains(lowerHelp, "activate") || strings.Contains(lowerHelp, "deactivate") {
		t.Error("help exposes a managed activation command")
	}
}

type fakeDaemonRuntime struct {
	served bool
	closed bool
	err    error
}

func (daemon *fakeDaemonRuntime) Serve(ctx context.Context) error {
	daemon.served = ctx != nil
	return daemon.err
}

func (daemon *fakeDaemonRuntime) Close() error {
	daemon.closed = true
	return nil
}

func TestRunServeResolvesOneCanonicalProjectRoot(t *testing.T) {
	project := t.TempDir()
	resolvedProject, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "project-link")
	if err := os.Symlink(project, link); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"serve", "--project-root", link}, {"serve", "--project-root=" + link}} {
		daemon := &fakeDaemonRuntime{}
		var opened node.DaemonOptions
		open := func(_ context.Context, options node.DaemonOptions) (daemonRuntime, error) {
			opened = options
			return daemon, nil
		}
		var stdout, stderr bytes.Buffer
		if err := runWithDaemon(context.Background(), args, &stdout, &stderr, open); err != nil {
			t.Fatalf("runWithDaemon(%v) error = %v", args, err)
		}
		if opened.Workspace != resolvedProject || opened.Clock != nil || !daemon.served || !daemon.closed ||
			stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("serve state = options %#v daemon %#v stdout=%q stderr=%q",
				opened, daemon, stdout.String(), stderr.String())
		}
	}
}

func TestRunServeRejectsMalformedArgumentsBeforeOpening(t *testing.T) {
	opened := 0
	open := func(context.Context, node.DaemonOptions) (daemonRuntime, error) {
		opened++
		return &fakeDaemonRuntime{}, nil
	}
	for _, args := range [][]string{{"serve", "--unknown"}, {"serve", "--project-root"},
		{"serve", "--project-root="}, {"serve", "one", "two"}} {
		if err := runWithDaemon(context.Background(), args, io.Discard, io.Discard, open); err == nil {
			t.Fatalf("runWithDaemon(%v) succeeded", args)
		}
	}
	if opened != 0 {
		t.Fatalf("malformed serve opened %d daemons", opened)
	}
}

func TestRunInitializeCallsTheNodeWriterAndEmitsClosedReceipt(t *testing.T) {
	project := t.TempDir()
	resolved, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("initialize-assets")).String()
	var received node.ProvisionOptions
	provision := func(_ context.Context, options node.ProvisionOptions) (node.ProvisionResult, error) {
		received = options
		return node.ProvisionResult{Created: true}, nil
	}
	open := func(context.Context, node.DaemonOptions) (daemonRuntime, error) {
		t.Fatal("initialize opened the daemon")
		return nil, nil
	}
	args := []string{"initialize", "--asset-revision", revision, "--project-root", project,
		"--host", "codex"}
	var stdout, stderr bytes.Buffer
	if err := runWithNode(context.Background(), args, &stdout, &stderr, open, provision, nil, nil); err != nil {
		t.Fatal(err)
	}
	want := `{"asset_revision":"` + revision +
		`","created":true,"host":"codex","schema_version":1,"status":"initialized"}` + "\n"
	if stdout.String() != want || stderr.Len() != 0 || received.Workspace != resolved ||
		received.Host != model.HostCodex || received.AssetRevision != revision || received.Clock != nil {
		t.Fatalf("initialize = stdout %q stderr %q options %#v", stdout.String(), stderr.String(), received)
	}
}

func TestRunInitializeRejectsMalformedAuthorityBeforeProvision(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("initialize-assets")).String()
	called := 0
	provision := func(context.Context, node.ProvisionOptions) (node.ProvisionResult, error) {
		called++
		return node.ProvisionResult{}, nil
	}
	for _, args := range [][]string{
		{"initialize"},
		{"initialize", "--project-root", project, "--host", "unknown", "--asset-revision", revision},
		{"initialize", "--project-root", project, "--host", "codex", "--asset-revision", "asset-r5"},
		{"initialize", "--project-root", project, "--project-root", project, "--host", "codex", "--asset-revision", revision},
	} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard, nil, provision, nil, nil); err == nil {
			t.Fatalf("runWithNode(%v) succeeded", args)
		}
	}
	if called != 0 {
		t.Fatalf("malformed initialize provisioned %d Nodes", called)
	}
}

func TestRunActivateCallsTheNodeWriterAndEmitsClosedReceipt(t *testing.T) {
	project := t.TempDir()
	resolved, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("active-assets")).String()
	for _, changed := range []bool{true, false} {
		changed := changed
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			var received node.ActivateOptions
			activate := func(_ context.Context, options node.ActivateOptions) (node.ActivateResult, error) {
				received = options
				return node.ActivateResult{Changed: changed}, nil
			}
			open := func(context.Context, node.DaemonOptions) (daemonRuntime, error) {
				t.Fatal("activate opened the daemon")
				return nil, nil
			}
			provision := func(context.Context, node.ProvisionOptions) (node.ProvisionResult, error) {
				t.Fatal("activate provisioned the Node")
				return node.ProvisionResult{}, nil
			}
			args := []string{"activate", "--host", "claude-code", "--asset-revision", revision,
				"--project-root", project}
			var stdout, stderr bytes.Buffer
			if err := runWithNode(context.Background(), args, &stdout, &stderr, open, provision, activate, nil); err != nil {
				t.Fatal(err)
			}
			want := `{"asset_revision":"` + revision + `","changed":` + fmt.Sprint(changed) +
				`,"host":"claude-code","schema_version":1,"status":"active"}` + "\n"
			if stdout.String() != want || stderr.Len() != 0 || received.Workspace != resolved ||
				received.Host != model.HostClaudeCode || received.AssetRevision != revision ||
				received.Clock != nil || received.Install != nil {
				t.Fatalf("activate = stdout %q stderr %q options %#v", stdout.String(), stderr.String(), received)
			}
		})
	}
}

func TestRunActivateRejectsMalformedAuthorityBeforeActivation(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("active-assets")).String()
	called := 0
	activate := func(context.Context, node.ActivateOptions) (node.ActivateResult, error) {
		called++
		return node.ActivateResult{}, nil
	}
	for _, args := range [][]string{
		{"activate"},
		{"activate", "--project-root", project, "--host", "unknown", "--asset-revision", revision},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", "asset-r5"},
		{"activate", "--project-root", project, "--project-root", project, "--host", "codex", "--asset-revision", revision},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "trailing"},
	} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard, nil, nil, activate, nil); err == nil {
			t.Fatalf("runWithNode(%v) succeeded", args)
		}
	}
	if called != 0 {
		t.Fatalf("malformed activate activated %d Nodes", called)
	}
}

func TestRunActivateHonorsCancellationBeforeActivation(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("active-assets")).String()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := 0
	activate := func(context.Context, node.ActivateOptions) (node.ActivateResult, error) {
		called++
		return node.ActivateResult{}, nil
	}
	args := []string{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision}
	if err := runWithNode(ctx, args, io.Discard, io.Discard, nil, nil, activate, nil); err != context.Canceled {
		t.Fatalf("canceled activate error = %v", err)
	}
	if called != 0 {
		t.Fatalf("canceled activate called writer %d times", called)
	}
}

func TestRunDeactivateCallsTheNodeWriterAndEmitsClosedReceipt(t *testing.T) {
	project := t.TempDir()
	resolved, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("inactive-assets")).String()
	for _, changed := range []bool{true, false} {
		changed := changed
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			var received node.DeactivateOptions
			deactivate := func(_ context.Context, options node.DeactivateOptions) (node.DeactivateResult, error) {
				received = options
				return node.DeactivateResult{Changed: changed}, nil
			}
			args := []string{"deactivate", "--asset-revision", revision, "--project-root", project,
				"--host", "codex"}
			var stdout, stderr bytes.Buffer
			if err := runWithNode(context.Background(), args, &stdout, &stderr, nil, nil, nil, deactivate); err != nil {
				t.Fatal(err)
			}
			want := `{"asset_revision":"` + revision + `","changed":` + fmt.Sprint(changed) +
				`,"host":"codex","schema_version":1,"status":"inactive"}` + "\n"
			if stdout.String() != want || stderr.Len() != 0 || received.Workspace != resolved ||
				received.Host != model.HostCodex || received.AssetRevision != revision || received.Clock != nil {
				t.Fatalf("deactivate = stdout %q stderr %q options %#v", stdout.String(), stderr.String(), received)
			}
		})
	}
}

func TestRunDeactivateRejectsMalformedAuthorityBeforeDeactivation(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("inactive-assets")).String()
	called := 0
	deactivate := func(context.Context, node.DeactivateOptions) (node.DeactivateResult, error) {
		called++
		return node.DeactivateResult{}, nil
	}
	for _, args := range [][]string{
		{"deactivate"},
		{"deactivate", "--project-root", project, "--host", "unknown", "--asset-revision", revision},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", "asset-r5"},
		{"deactivate", "--project-root", project, "--host", "codex", "--host", "codex", "--asset-revision", revision},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "trailing"},
	} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard, nil, nil, nil, deactivate); err == nil {
			t.Fatalf("runWithNode(%v) succeeded", args)
		}
	}
	if called != 0 {
		t.Fatalf("malformed deactivate called writer %d times", called)
	}
}
