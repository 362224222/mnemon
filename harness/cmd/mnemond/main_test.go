package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
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
	if strings.Contains(lowerHelp, "inspect") {
		t.Error("help exposes the offline authority inspector")
	}
}

type fakeDaemonRuntime struct {
	served   bool
	closed   bool
	err      error
	closeErr error
}

func (daemon *fakeDaemonRuntime) Serve(ctx context.Context) error {
	daemon.served = ctx != nil
	return daemon.err
}

func (daemon *fakeDaemonRuntime) Close() error {
	daemon.closed = true
	return daemon.closeErr
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
		if opened.Workspace != resolvedProject || opened.Clock != nil ||
			opened.GracefulShutdownBudget != gracefulShutdownBudget ||
			!daemon.served || !daemon.closed ||
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

func TestRunServeReturnsGracefulShutdownDiagnosticToExecutable(t *testing.T) {
	project := t.TempDir()
	diagnostic := fmt.Errorf("%w: test component", node.ErrGracefulShutdownDeadline)
	daemon := &fakeDaemonRuntime{closeErr: diagnostic}
	open := func(context.Context, node.DaemonOptions) (daemonRuntime, error) {
		return daemon, nil
	}
	err := runWithDaemon(context.Background(),
		[]string{"serve", "--project-root", project}, io.Discard, io.Discard, open)
	if !errors.Is(err, node.ErrGracefulShutdownDeadline) || !daemon.served || !daemon.closed {
		t.Fatalf("serve result = (%v, %#v), want shutdown diagnostic after close", err, daemon)
	}
}

func TestProductionRunServeRequiresManagedLaunchPermit(t *testing.T) {
	project := t.TempDir()
	t.Setenv("MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD", "")
	if err := run(context.Background(), []string{"serve", "--project-root", project},
		io.Discard, io.Discard); !errors.Is(err, node.ErrDaemonLaunchPermit) {
		t.Fatalf("production serve without launch permit error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(project, ".mnemon", "harness", "node", "node.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("permit-free serve touched Store authority: %v", err)
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
		return node.ProvisionResult{Created: true,
			Profile: commandTestProfile(t, resolved, model.HostCodex, revision, false)}, nil
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
		{"initialize", "--project-root", project, "--host", "claude-code", "--asset-revision", revision},
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

func TestRunInitializeReceiptReportsDurableReplayAuthority(t *testing.T) {
	project := t.TempDir()
	resolved, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	requested := model.Sum([]byte("requested-initialize-assets")).String()
	durable := model.Sum([]byte("durable-initialize-assets")).String()
	provision := func(context.Context, node.ProvisionOptions) (node.ProvisionResult, error) {
		return node.ProvisionResult{Profile: commandTestProfile(t, resolved,
			model.HostCodex, durable, false)}, nil
	}
	args := []string{"initialize", "--project-root", project, "--host", "codex",
		"--asset-revision", requested}
	var stdout bytes.Buffer
	if err := runWithNode(context.Background(), args, &stdout, io.Discard, nil, provision, nil, nil); err != nil {
		t.Fatal(err)
	}
	want := `{"asset_revision":"` + durable +
		`","created":false,"host":"codex","schema_version":1,"status":"initialized"}` + "\n"
	if stdout.String() != want {
		t.Fatalf("replay receipt = %q, want %q", stdout.String(), want)
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
				return node.ActivateResult{Changed: changed,
					Profile: commandTestProfile(t, resolved, model.HostCodex, revision, true)}, nil
			}
			open := func(context.Context, node.DaemonOptions) (daemonRuntime, error) {
				t.Fatal("activate opened the daemon")
				return nil, nil
			}
			provision := func(context.Context, node.ProvisionOptions) (node.ProvisionResult, error) {
				t.Fatal("activate provisioned the Node")
				return node.ProvisionResult{}, nil
			}
			expected := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
			if changed {
				expected = expected.Add(-time.Second)
			}
			args := []string{"activate", "--host", "codex", "--asset-revision", revision,
				"--project-root", project, "--expected-updated-at", expected.Format(time.RFC3339Nano)}
			var stdout, stderr bytes.Buffer
			if err := runWithNode(context.Background(), args, &stdout, &stderr, open, provision, activate, nil); err != nil {
				t.Fatal(err)
			}
			want := `{"asset_revision":"` + revision + `","changed":` + fmt.Sprint(changed) +
				`,"host":"codex","schema_version":1,"status":"active",` +
				`"updated_at":"2026-07-17T12:00:00Z"}` + "\n"
			if stdout.String() != want || stderr.Len() != 0 || received.Workspace != resolved ||
				received.Host != model.HostCodex || received.AssetRevision != revision ||
				!received.ExpectedUpdatedAt.Equal(expected) || received.Clock != nil || received.Install != nil {
				t.Fatalf("activate = stdout %q stderr %q options %#v", stdout.String(), stderr.String(), received)
			}
		})
	}
}

func TestRunActivateRejectsMalformedAuthorityBeforeActivation(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("active-assets")).String()
	generation := "2026-07-17T11:59:59Z"
	called := 0
	activate := func(context.Context, node.ActivateOptions) (node.ActivateResult, error) {
		called++
		return node.ActivateResult{}, nil
	}
	for _, args := range [][]string{
		{"activate"},
		{"activate", "--expected-updated-at"},
		{"activate", "--project-root", project, "--host", "claude-code", "--asset-revision", revision, "--expected-updated-at", generation},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", "asset-r5", "--expected-updated-at", generation},
		{"activate", "--project-root", project, "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", generation},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", generation, "trailing"},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", "2026-07-16T20:00:00-04:00"},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", "9999-12-31T23:59:59Z"},
		{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", generation, "--expected-updated-at", generation},
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
	args := []string{"activate", "--project-root", project, "--host", "codex", "--asset-revision", revision,
		"--expected-updated-at", "2026-07-17T11:59:59Z"}
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
				return node.DeactivateResult{Changed: changed,
					Profile: commandTestProfile(t, resolved, model.HostCodex, revision, false)}, nil
			}
			expected := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
			if changed {
				expected = expected.Add(-time.Second)
			}
			args := []string{"deactivate", "--asset-revision", revision, "--project-root", project,
				"--host", "codex", "--expected-updated-at", expected.Format(time.RFC3339Nano)}
			var stdout, stderr bytes.Buffer
			if err := runWithNode(context.Background(), args, &stdout, &stderr, nil, nil, nil, deactivate); err != nil {
				t.Fatal(err)
			}
			want := `{"asset_revision":"` + revision + `","changed":` + fmt.Sprint(changed) +
				`,"host":"codex","schema_version":1,"status":"inactive",` +
				`"updated_at":"2026-07-17T12:00:00Z"}` + "\n"
			if stdout.String() != want || stderr.Len() != 0 || received.Workspace != resolved ||
				received.Host != model.HostCodex || received.AssetRevision != revision ||
				!received.ExpectedUpdatedAt.Equal(expected) || received.Clock != nil {
				t.Fatalf("deactivate = stdout %q stderr %q options %#v", stdout.String(), stderr.String(), received)
			}
		})
	}
}

func TestRunDeactivateRejectsMalformedAuthorityBeforeDeactivation(t *testing.T) {
	project := t.TempDir()
	revision := model.Sum([]byte("inactive-assets")).String()
	generation := "2026-07-17T11:59:59Z"
	called := 0
	deactivate := func(context.Context, node.DeactivateOptions) (node.DeactivateResult, error) {
		called++
		return node.DeactivateResult{}, nil
	}
	for _, args := range [][]string{
		{"deactivate"},
		{"deactivate", "--expected-updated-at"},
		{"deactivate", "--project-root", project, "--host", "claude-code", "--asset-revision", revision, "--expected-updated-at", generation},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", "asset-r5", "--expected-updated-at", generation},
		{"deactivate", "--project-root", project, "--host", "codex", "--host", "codex", "--asset-revision", revision, "--expected-updated-at", generation},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", generation, "trailing"},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", "not-a-time"},
		{"deactivate", "--project-root", project, "--host", "codex", "--asset-revision", revision, "--expected-updated-at", "9999-12-31T23:59:59Z"},
	} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard, nil, nil, nil, deactivate); err == nil {
			t.Fatalf("runWithNode(%v) succeeded", args)
		}
	}
	if called != 0 {
		t.Fatalf("malformed deactivate called writer %d times", called)
	}
}

func TestRunInspectCallsExistingOnlyReaderAndEmitsCanonicalReceipt(t *testing.T) {
	project := t.TempDir()
	resolved, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("inspect-assets")).String()
	peerID, _ := model.ParsePeerID("peer-command-inspect")
	at := time.Date(2026, 7, 17, 6, 7, 8, 9, time.UTC)
	called := 0
	inspect := func(ctx context.Context, workspace string) (localapi.AuthoritySnapshot, error) {
		called++
		if ctx == nil || workspace != resolved {
			t.Fatalf("inspect input = (%v, %q)", ctx, workspace)
		}
		return localapi.AuthoritySnapshot{Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
			Enabled: false, AssetRevision: revision, UpdatedAt: at, PeerID: peerID,
			ActiveAssetRevision: revision}, nil
	}
	var stdout, stderr bytes.Buffer
	if err := runWithNode(context.Background(), []string{"inspect", "--project-root", project},
		&stdout, &stderr, nil, nil, nil, nil, inspect); err != nil {
		t.Fatal(err)
	}
	want := `{"active_asset_revision":"` + revision + `","asset_revision":"` + revision +
		`","enabled":false,"host":"codex","peer_id":"peer-command-inspect",` +
		`"runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T06:07:08.000000009Z"}` + "\n"
	if stdout.String() != want || stderr.Len() != 0 || called != 1 {
		t.Fatalf("inspect = stdout %q stderr %q called=%d", stdout.String(), stderr.String(), called)
	}
}

func TestRunInspectRejectsMalformedOrCancelledInvocationBeforeReading(t *testing.T) {
	project := t.TempDir()
	called := 0
	inspect := func(context.Context, string) (localapi.AuthoritySnapshot, error) {
		called++
		return localapi.AuthoritySnapshot{}, nil
	}
	for _, args := range [][]string{{"inspect"}, {"inspect", "--project-root"},
		{"inspect", "--project-root=" + project}, {"inspect", "--project-root", project, "trailing"},
		{"inspect", "--project-root", ""}} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard,
			nil, nil, nil, nil, inspect); err == nil {
			t.Fatalf("runWithNode(%v) succeeded", args)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runWithNode(ctx, []string{"inspect", "--project-root", project}, io.Discard, io.Discard,
		nil, nil, nil, nil, inspect); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled inspect error = %v", err)
	}
	if called != 0 {
		t.Fatalf("rejected inspect called reader %d times", called)
	}
}

func TestRunConfirmOfflineEmitsExactAuthorityAndClassifiesActiveWriter(t *testing.T) {
	project, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("confirm-offline-command-assets")).String()
	if _, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: project, Host: model.HostCodex, AssetRevision: revision,
		Credentials: localapi.NodeRuntime{},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := node.InspectAuthorityWithCredentials(context.Background(), project,
		localapi.NodeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := localapi.NewAuthorityResponse(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := localapi.AuthorityDigest(expected)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"confirm-offline", "--expected-authority-digest", digest.String(),
		"--project-root", project}
	var stdout bytes.Buffer
	if err := runWithNode(context.Background(), args, &stdout, io.Discard,
		nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	want, err := model.CanonicalMarshal(expected)
	if err != nil || stdout.String() != string(append(want, '\n')) {
		t.Fatalf("confirm-offline stdout = %q, marshal=%v", stdout.String(), err)
	}

	st, err := store.OpenExisting(context.Background(),
		filepath.Join(project, ".mnemon", "harness", "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stdout.Reset()
	if err := runWithNode(context.Background(), args, &stdout, io.Discard,
		nil, nil, nil, nil); !errors.Is(err, node.ErrOfflineAuthorityActive) ||
		stdout.Len() != 0 {
		t.Fatalf("writer-active confirm-offline = stdout %q error %v", stdout.String(), err)
	}
}

func TestRunConfirmOfflineRejectsMalformedAndMismatchedAuthority(t *testing.T) {
	project, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("confirm-offline-mismatch-assets")).String()
	if _, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: project, Host: model.HostCodex, AssetRevision: revision,
		Credentials: localapi.NodeRuntime{},
	}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"confirm-offline"},
		{"confirm-offline", "--project-root", project},
		{"confirm-offline", "--expected-authority-digest", revision},
		{"confirm-offline", "--project-root", project, "--expected-authority-digest", "bad"},
		{"confirm-offline", "--project-root", project, "--project-root", project,
			"--expected-authority-digest", revision},
	} {
		if err := runWithNode(context.Background(), args, io.Discard, io.Discard,
			nil, nil, nil, nil); err == nil {
			t.Fatalf("malformed confirm-offline %v succeeded", args)
		}
	}
	other := model.Sum([]byte("different-authority")).String()
	if err := runWithNode(context.Background(), []string{"confirm-offline", "--project-root", project,
		"--expected-authority-digest", other}, io.Discard, io.Discard,
		nil, nil, nil, nil); !errors.Is(err, node.ErrOfflineAuthority) {
		t.Fatalf("mismatched confirm-offline error = %v", err)
	}
}

func commandTestProfile(t *testing.T, workspace string, host model.HostKind,
	revision string, enabled bool,
) model.Profile {
	t.Helper()
	runtimeKind, ok := model.RuntimeForHost(host)
	if !ok {
		t.Fatalf("invalid command test Host %q", host)
	}
	at := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-command", WorkspaceRoot: workspace, Host: host, Runtime: runtimeKind,
		CredentialHash: model.Sum([]byte("command-profile-credential")), ActiveAssetRevision: revision,
		HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: enabled, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}
