package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestCompanionRunnerDiscoversExactPairAndFreezesLifecycleCommands(t *testing.T) {
	fixture := newCompanionFixture(t)
	dependencies := fixture.dependencies()
	var commands []*exec.Cmd
	dependencies.commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		command := exec.CommandContext(ctx, name, args...)
		commands = append(commands, command)
		return command
	}
	runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace, "r5-test",
		dependencies)
	if err != nil {
		t.Fatal(err)
	}
	generation := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	initialized, err := runner.Initialize(context.Background(), model.HostCodex, fixture.revision)
	if err != nil || !initialized.Created || initialized.Status != "initialized" {
		t.Fatalf("Initialize() = (%#v, %v)", initialized, err)
	}
	authority, err := runner.Inspect(context.Background())
	if err != nil || !authority.Enabled || authority.Host != string(model.HostCodex) ||
		authority.AssetRevision != fixture.revision {
		t.Fatalf("Inspect() = (%#v, %v)", authority, err)
	}
	confirmed, err := runner.ConfirmOffline(context.Background(), authority)
	if err != nil || confirmed != authority {
		t.Fatalf("ConfirmOffline() = (%#v, %v)", confirmed, err)
	}
	activated, err := runner.Activate(context.Background(), model.HostCodex, fixture.revision, generation)
	if err != nil || !activated.Changed || activated.Status != "active" ||
		activated.UpdatedAt != generation.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("Activate() = (%#v, %v)", activated, err)
	}
	deactivated, err := runner.Deactivate(context.Background(), model.HostCodex, fixture.revision, generation)
	if err != nil || !deactivated.Changed || deactivated.Status != "inactive" ||
		deactivated.UpdatedAt != generation.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("Deactivate() = (%#v, %v)", deactivated, err)
	}
	rawLog, err := os.ReadFile(fixture.log)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		fixture.workspace + "|--version",
		fixture.workspace + "|initialize --project-root " + fixture.workspace +
			" --host codex --asset-revision " + fixture.revision,
		fixture.workspace + "|inspect --project-root " + fixture.workspace,
		fixture.workspace + "|confirm-offline --project-root " + fixture.workspace +
			" --expected-authority-digest " + mustCompanionAuthorityDigest(t, authority),
		fixture.workspace + "|activate --project-root " + fixture.workspace +
			" --host codex --asset-revision " + fixture.revision +
			" --expected-updated-at 2026-07-17T00:00:00Z",
		fixture.workspace + "|deactivate --project-root " + fixture.workspace +
			" --host codex --asset-revision " + fixture.revision +
			" --expected-updated-at 2026-07-17T00:00:00Z",
	}
	if got := strings.Split(strings.TrimSuffix(string(rawLog), "\n"), "\n"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("command log = %#v, want %#v", got, want)
	}
	if len(commands) != len(want) {
		t.Fatalf("commands = %d, want %d", len(commands), len(want))
	}
	for _, command := range commands {
		stdin, ok := command.Stdin.(*os.File)
		if !ok || stdin.Name() != os.DevNull || command.Dir != fixture.workspace ||
			command.WaitDelay != companionWaitDelay {
			t.Fatalf("subprocess confinement = cwd %q stdin %#v wait %s", command.Dir,
				command.Stdin, command.WaitDelay)
		}
	}
}

func TestCompanionRunnerStartsFixedExecutionBudgetAfterProcessStart(t *testing.T) {
	fixture := newCompanionFixture(t)
	dependencies := fixture.dependencies()
	type observedContext struct {
		deadline time.Time
		has      bool
	}
	var observed []observedContext
	dependencies.commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		deadline, has := ctx.Deadline()
		observed = append(observed, observedContext{deadline: deadline, has: has})
		return exec.CommandContext(ctx, name, args...)
	}
	callerCtx, cancelCaller := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCaller()
	callerDeadline, _ := callerCtx.Deadline()
	runner, err := newCompanionRunnerWith(callerCtx, fixture.workspace, "r5-test", dependencies)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed) != 1 || !observed[0].has || !observed[0].deadline.Equal(callerDeadline) {
		t.Fatalf("version construction context = %#v, want only caller deadline %s",
			observed, callerDeadline)
	}

	// A caller without a deadline gives command construction no hidden fixed
	// deadline, while the post-Start execution is still terminated by the
	// supplied operation budget.
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "inspect-sleep")
	if _, err := runner.execute(context.Background(), "inspect", 40*time.Millisecond,
		companionResponseBytes, "inspect", "--project-root", fixture.workspace); err == nil ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-Start execution timeout error = %v", err)
	}
	if len(observed) != 2 || observed[1].has {
		t.Fatalf("background construction context = %#v, want no pre-Start deadline", observed)
	}
}

func TestWaitCompanionCommandRejectsExpiredAbsoluteDeadlineAndDrainsChild(t *testing.T) {
	commandCtx, cancelCommand := context.WithCancel(context.Background())
	defer cancelCommand()
	command := exec.CommandContext(commandCtx, "/bin/sh", "-c", "exec sleep 5")
	command.WaitDelay = companionWaitDelay
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	runErr, contextErr := waitCompanionCommand(context.Background(), cancelCommand,
		command, time.Now().Add(-time.Nanosecond))
	if runErr == nil || !errors.Is(contextErr, context.DeadlineExceeded) ||
		command.ProcessState == nil || command.ProcessState.Success() {
		t.Fatalf("expired deadline result = run=%v context=%v state=%#v",
			runErr, contextErr, command.ProcessState)
	}
}

func TestCompanionRunnerClassifiesOnlyClosedOfflineWriterContention(t *testing.T) {
	fixture := newCompanionFixture(t)
	runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
		"r5-test", fixture.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	expected, err := runner.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "confirm-writer-active")
	if response, err := runner.ConfirmOffline(context.Background(), expected); !errors.Is(err,
		node.ErrOfflineAuthorityActive) || response != (localapi.AuthorityResponse{}) {
		t.Fatalf("writer-active ConfirmOffline() = (%#v, %v)", response, err)
	}
	for _, mode := range []string{"confirm-wrong", "confirm-secret", "confirm-exit"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MNEMON_COMPANION_TEST_MODE", mode)
			response, err := runner.ConfirmOffline(context.Background(), expected)
			if err == nil || errors.Is(err, node.ErrOfflineAuthorityActive) ||
				!errors.Is(err, errManagedCompanion) ||
				response != (localapi.AuthorityResponse{}) || strings.Contains(err.Error(), "raw-secret") {
				t.Fatalf("permanent ConfirmOffline() = (%#v, %v)", response, err)
			}
		})
	}
	if response, err := runner.ConfirmOffline(context.Background(), localapi.AuthorityResponse{}); err == nil ||
		!errors.Is(err, errManagedCompanion) || response != (localapi.AuthorityResponse{}) {
		t.Fatalf("invalid expected ConfirmOffline() = (%#v, %v)", response, err)
	}
}

func TestCompanionRunnerMapsRealMnemondWriterActiveExitAndOfflineReceipt(t *testing.T) {
	directory := physicalTempDir(t)
	workspace := physicalTempDir(t)
	harnessExecutable := filepath.Join(directory, "mnemon-harness")
	mnemondExecutable := filepath.Join(directory, "mnemond")
	writeExecutable(t, harnessExecutable, "#!/bin/sh\nexit 0\n", 0o700)
	buildContext, cancelBuild := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelBuild()
	buildOutput := newBoundedCompanionBuffer(64 << 10)
	defer buildOutput.clear()
	build := exec.CommandContext(buildContext, "go", "build", "-trimpath", "-ldflags",
		"-X main.version=r5-test", "-o", mnemondExecutable, "./harness/cmd/mnemond")
	build.Dir = companionTestRepositoryRoot(t)
	build.Stdin = nil
	build.Stdout = buildOutput
	build.Stderr = buildOutput
	build.WaitDelay = companionWaitDelay
	if err := build.Run(); err != nil || buildOutput.overflowed() {
		t.Fatalf("build real mnemond = %v, output bytes=%d overflow=%t", err,
			buildOutput.len(), buildOutput.overflowed())
	}
	if err := os.Chmod(mnemondExecutable, 0o700); err != nil {
		t.Fatal(err)
	}

	revision := model.Sum([]byte("real-companion-offline-assets")).String()
	if _, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: revision,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := node.InspectAuthority(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := localapi.NewAuthorityResponse(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := newCompanionRunnerWith(context.Background(), workspace, "r5-test",
		companionRunnerDependencies{
			currentExecutable: func() (string, error) { return harnessExecutable, nil },
			lookPath:          func(string) (string, error) { return harnessExecutable, nil },
			commandContext:    exec.CommandContext,
		})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.OpenExisting(context.Background(),
		filepath.Join(workspace, ".mnemon", "harness", "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := runner.ConfirmOffline(context.Background(), expected); !errors.Is(err,
		node.ErrOfflineAuthorityActive) || response != (localapi.AuthorityResponse{}) {
		_ = st.Close()
		t.Fatalf("real writer-active ConfirmOffline() = (%#v, %v)", response, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := runner.ConfirmOffline(context.Background(), expected)
	if err != nil || response != expected {
		t.Fatalf("real offline ConfirmOffline() = (%#v, %v)", response, err)
	}
}

func TestCompanionRunnerRejectsPATHVersionAndExecutableDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *companionFixture, *companionRunnerDependencies)
	}{
		{name: "PATH mismatch", mutate: func(t *testing.T, fixture *companionFixture,
			dependencies *companionRunnerDependencies,
		) {
			other := filepath.Join(physicalTempDir(t), "mnemon-harness")
			writeExecutable(t, other, "#!/bin/sh\nexit 0\n", 0o700)
			dependencies.lookPath = func(string) (string, error) { return other, nil }
		}},
		{name: "relative PATH", mutate: func(_ *testing.T, _ *companionFixture,
			dependencies *companionRunnerDependencies,
		) {
			dependencies.lookPath = func(string) (string, error) { return "mnemon-harness", nil }
		}},
		{name: "version mismatch", mutate: func(t *testing.T, _ *companionFixture,
			_ *companionRunnerDependencies,
		) {
			t.Setenv("MNEMON_COMPANION_TEST_MODE", "bad-version")
		}},
		{name: "symlink companion", mutate: func(t *testing.T, fixture *companionFixture,
			_ *companionRunnerDependencies,
		) {
			target := filepath.Join(physicalTempDir(t), "mnemond")
			if err := os.Rename(fixture.companion, target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, fixture.companion); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "writable companion", mutate: func(t *testing.T, fixture *companionFixture,
			_ *companionRunnerDependencies,
		) {
			if err := os.Chmod(fixture.companion, 0o722); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "nonexecutable companion", mutate: func(t *testing.T, fixture *companionFixture,
			_ *companionRunnerDependencies,
		) {
			if err := os.Chmod(fixture.companion, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "writable PATH harness", mutate: func(t *testing.T, fixture *companionFixture,
			_ *companionRunnerDependencies,
		) {
			if err := os.Chmod(fixture.harness, 0o722); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "writable workspace", mutate: func(t *testing.T, fixture *companionFixture,
			_ *companionRunnerDependencies,
		) {
			if err := os.Chmod(fixture.workspace, 0o722); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompanionFixture(t)
			dependencies := fixture.dependencies()
			test.mutate(t, fixture, &dependencies)
			if runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
				"r5-test", dependencies); err == nil || runner != nil ||
				!errors.Is(err, errManagedCompanion) {
				t.Fatalf("newCompanionRunnerWith() = (%#v, %v)", runner, err)
			}
		})
	}

	t.Run("PATH symlink resolves to running physical binary", func(t *testing.T) {
		fixture := newCompanionFixture(t)
		link := filepath.Join(physicalTempDir(t), "mnemon-harness")
		if err := os.Symlink(fixture.harness, link); err != nil {
			t.Fatal(err)
		}
		dependencies := fixture.dependencies()
		dependencies.lookPath = func(string) (string, error) { return link, nil }
		if runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
			"r5-test", dependencies); err != nil || runner == nil {
			t.Fatalf("newCompanionRunnerWith() = (%#v, %v)", runner, err)
		}
	})

	t.Run("replacement after discovery", func(t *testing.T) {
		fixture := newCompanionFixture(t)
		runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
			"r5-test", fixture.dependencies())
		if err != nil {
			t.Fatal(err)
		}
		replacement := filepath.Join(filepath.Dir(fixture.companion), "replacement")
		writeExecutable(t, replacement, companionScript(fixture.revision), 0o700)
		if err := os.Rename(replacement, fixture.companion); err != nil {
			t.Fatal(err)
		}
		if _, err := runner.Inspect(context.Background()); err == nil ||
			!errors.Is(err, errManagedCompanion) {
			t.Fatalf("Inspect() replacement error = %v", err)
		}
	})
}

func TestCompanionRunnerBoundsAndSanitizesSubprocessFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    string
		version bool
		cancel  bool
	}{
		{name: "version overflow", mode: "version-overflow", version: true},
		{name: "version cancellation", mode: "version-sleep", version: true, cancel: true},
		{name: "response overflow", mode: "inspect-overflow"},
		{name: "response cancellation", mode: "inspect-sleep", cancel: true},
		{name: "secret stderr", mode: "inspect-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompanionFixture(t)
			if test.version {
				t.Setenv("MNEMON_COMPANION_TEST_MODE", test.mode)
				ctx := context.Background()
				var cancel context.CancelFunc
				if test.cancel {
					ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
					defer cancel()
				}
				runner, err := newCompanionRunnerWith(ctx, fixture.workspace, "r5-test",
					fixture.dependencies())
				if err == nil || runner != nil || strings.Contains(err.Error(), "raw-secret") {
					t.Fatalf("version validation = (%#v, %v)", runner, err)
				}
				if test.cancel && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("version cancellation error = %v", err)
				}
				return
			}
			runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
				"r5-test", fixture.dependencies())
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("MNEMON_COMPANION_TEST_MODE", test.mode)
			ctx := context.Background()
			var cancel context.CancelFunc
			if test.cancel {
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
			}
			_, err = runner.Inspect(ctx)
			if err == nil || !errors.Is(err, errManagedCompanion) ||
				strings.Contains(err.Error(), "raw-secret") {
				t.Fatalf("Inspect() error = %v", err)
			}
			if test.cancel && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Inspect() cancellation error = %v", err)
			}
		})
	}
}

func TestCompanionRunnerRejectsOpenNoncanonicalAndUnknownReceipts(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    string
		inspect bool
	}{
		{name: "unknown lifecycle field", mode: "initialize-unknown"},
		{name: "noncanonical lifecycle", mode: "initialize-noncanonical"},
		{name: "multiline lifecycle", mode: "initialize-multiline"},
		{name: "unknown authority field", mode: "inspect-unknown", inspect: true},
		{name: "noncanonical authority", mode: "inspect-noncanonical", inspect: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompanionFixture(t)
			runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
				"r5-test", fixture.dependencies())
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("MNEMON_COMPANION_TEST_MODE", test.mode)
			if test.inspect {
				_, err = runner.Inspect(context.Background())
			} else {
				_, err = runner.Initialize(context.Background(), model.HostCodex, fixture.revision)
			}
			if err == nil || !errors.Is(err, errManagedCompanion) ||
				strings.Contains(err.Error(), "raw-secret") {
				t.Fatalf("closed response error = %v", err)
			}
		})
	}
}

func TestCompanionRunnerRejectsInvalidLifecycleGenerations(t *testing.T) {
	fixture := newCompanionFixture(t)
	runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
		"r5-test", fixture.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]time.Time{
		"zero request":      {},
		"pre-epoch request": time.Unix(-1, 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := runner.Activate(context.Background(), model.HostCodex,
				fixture.revision, expected); err == nil || !errors.Is(err, errManagedCompanion) {
				t.Fatalf("Activate() error = %v", err)
			}
		})
	}
	for _, mode := range []string{"activate-missing-time", "activate-noncanonical-time",
		"activate-out-of-range-time", "activate-equal-change", "activate-replay-drift"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MNEMON_COMPANION_TEST_MODE", mode)
			if _, err := runner.Activate(context.Background(), model.HostCodex, fixture.revision,
				time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)); err == nil ||
				!errors.Is(err, errManagedCompanion) {
				t.Fatalf("Activate() error = %v", err)
			}
		})
	}
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "activate-replay")
	replayed, err := runner.Activate(context.Background(), model.HostCodex, fixture.revision,
		time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC))
	if err != nil || replayed.Changed || replayed.UpdatedAt != "2026-07-17T00:00:00Z" {
		t.Fatalf("replayed Activate() = (%#v, %v)", replayed, err)
	}
}

func TestCompanionRunnerReportsDurableInitializeReplayForSetupToDecide(t *testing.T) {
	fixture := newCompanionFixture(t)
	runner, err := newCompanionRunnerWith(context.Background(), fixture.workspace,
		"r5-test", fixture.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "initialize-replay")
	receipt, err := runner.Initialize(context.Background(), model.HostCodex, fixture.revision)
	otherRevision := model.Sum([]byte("durable-replay-assets")).String()
	if err != nil || receipt.Created || receipt.Host != string(model.HostClaudeCode) ||
		receipt.AssetRevision != otherRevision {
		t.Fatalf("Initialize() replay = (%#v, %v)", receipt, err)
	}
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "initialize-created-drift")
	if _, err := runner.Initialize(context.Background(), model.HostCodex, fixture.revision); err == nil {
		t.Fatal("Initialize() accepted a newly-created receipt for different authority")
	}
}

type companionFixture struct {
	workspace string
	directory string
	harness   string
	companion string
	log       string
	revision  string
}

func newCompanionFixture(t *testing.T) *companionFixture {
	t.Helper()
	directory := physicalTempDir(t)
	workspace := physicalTempDir(t)
	revision := model.Sum([]byte("companion-test-assets")).String()
	fixture := &companionFixture{workspace: workspace, directory: directory,
		harness:   filepath.Join(directory, "mnemon-harness"),
		companion: filepath.Join(directory, "mnemond"),
		log:       filepath.Join(directory, "argv.log"), revision: revision}
	writeExecutable(t, fixture.harness, "#!/bin/sh\nexit 0\n", 0o700)
	writeExecutable(t, fixture.companion, companionScript(revision), 0o700)
	t.Setenv("MNEMON_COMPANION_TEST_LOG", fixture.log)
	t.Setenv("MNEMON_COMPANION_TEST_MODE", "")
	return fixture
}

func (fixture *companionFixture) dependencies() companionRunnerDependencies {
	return companionRunnerDependencies{
		currentExecutable: func() (string, error) { return fixture.harness, nil },
		lookPath:          func(string) (string, error) { return fixture.harness, nil },
		commandContext:    productionCompanionRunnerDependencies().commandContext,
	}
}

func physicalTempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func writeExecutable(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func companionScript(revision string) string {
	template := `#!/bin/sh
printf '%s|%s\n' "$PWD" "$*" >> "$MNEMON_COMPANION_TEST_LOG"
mode=${MNEMON_COMPANION_TEST_MODE-}
case "$1" in
  --version)
    case "$mode" in
      bad-version) printf 'mnemond version wrong\n';;
      version-overflow) head -c 4096 /dev/zero | tr '\000' x;;
      version-sleep) sleep 5; printf 'mnemond version r5-test\n';;
      *) printf 'mnemond version r5-test\n';;
    esac
    ;;
  initialize)
    case "$mode" in
      initialize-unknown) printf '{"asset_revision":"@REVISION@","created":true,"host":"codex","raw_secret":"raw-secret","schema_version":1,"status":"initialized"}\n';;
      initialize-noncanonical) printf '{ "asset_revision":"@REVISION@","created":true,"host":"codex","schema_version":1,"status":"initialized"}\n';;
      initialize-multiline) printf '{"asset_revision":"@REVISION@","created":true,"host":"codex","schema_version":1,"status":"initialized"}\n{}\n';;
      initialize-replay) printf '{"asset_revision":"@REPLAY_REVISION@","created":false,"host":"claude-code","schema_version":1,"status":"initialized"}\n';;
      initialize-created-drift) printf '{"asset_revision":"@REPLAY_REVISION@","created":true,"host":"claude-code","schema_version":1,"status":"initialized"}\n';;
      *) printf '{"asset_revision":"@REVISION@","created":true,"host":"codex","schema_version":1,"status":"initialized"}\n';;
    esac
    ;;
  inspect)
    case "$mode" in
      inspect-overflow) head -c 4096 /dev/zero | tr '\000' x;;
      inspect-sleep) sleep 5; printf '{}\n';;
      inspect-secret) printf 'raw-secret\n' >&2; exit 9;;
      inspect-unknown) printf '{"active_asset_revision":"@REVISION@","asset_revision":"@REVISION@","enabled":true,"host":"codex","peer_id":"peer-companion-test","raw_secret":"raw-secret","runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T00:00:00Z"}\n';;
      inspect-noncanonical) printf '{ "active_asset_revision":"@REVISION@","asset_revision":"@REVISION@","enabled":true,"host":"codex","peer_id":"peer-companion-test","runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T00:00:00Z"}\n';;
      *) printf '{"active_asset_revision":"@REVISION@","asset_revision":"@REVISION@","enabled":true,"host":"codex","peer_id":"peer-companion-test","runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T00:00:00Z"}\n';;
    esac
    ;;
  confirm-offline)
    case "$mode" in
      confirm-writer-active) exit 75;;
      confirm-wrong) printf '{"active_asset_revision":"@REVISION@","asset_revision":"@REVISION@","enabled":false,"host":"codex","peer_id":"peer-companion-test","runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T00:00:00Z"}\n';;
      confirm-secret) printf 'raw-secret\n' >&2; exit 9;;
      confirm-exit) exit 9;;
      *) printf '{"active_asset_revision":"@REVISION@","asset_revision":"@REVISION@","enabled":true,"host":"codex","peer_id":"peer-companion-test","runtime":"codex-app-server","schema_version":1,"updated_at":"2026-07-17T00:00:00Z"}\n';;
    esac
    ;;
  activate)
    case "$mode" in
      activate-missing-time) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active"}\n';;
      activate-noncanonical-time) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active","updated_at":"2026-07-16T20:00:00-04:00"}\n';;
      activate-out-of-range-time) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active","updated_at":"9999-12-31T23:59:59Z"}\n';;
      activate-equal-change) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active","updated_at":"2026-07-17T00:00:00Z"}\n';;
      activate-replay) printf '{"asset_revision":"@REVISION@","changed":false,"host":"codex","schema_version":1,"status":"active","updated_at":"2026-07-17T00:00:00Z"}\n';;
      activate-replay-drift) printf '{"asset_revision":"@REVISION@","changed":false,"host":"codex","schema_version":1,"status":"active","updated_at":"2026-07-17T00:00:01Z"}\n';;
      *) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active","updated_at":"2026-07-17T00:00:01Z"}\n';;
    esac
    ;;
  deactivate) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"inactive","updated_at":"2026-07-17T00:00:01Z"}\n';;
  *) exit 8;;
esac
`
	template = strings.ReplaceAll(template, "@REVISION@", revision)
	return strings.ReplaceAll(template, "@REPLAY_REVISION@",
		model.Sum([]byte("durable-replay-assets")).String())
}

func mustCompanionAuthorityDigest(t *testing.T, response localapi.AuthorityResponse) string {
	t.Helper()
	digest, err := localapi.AuthorityDigest(response)
	if err != nil {
		t.Fatal(err)
	}
	return digest.String()
}

func companionTestRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve companion test source")
	}
	for directory := filepath.Dir(source); ; directory = filepath.Dir(directory) {
		raw, err := os.ReadFile(filepath.Join(directory, "go.mod"))
		if err == nil && bytes.Contains(raw,
			[]byte("module github.com/mnemon-dev/mnemon\n")) {
			physical, err := filepath.EvalSymlinks(directory)
			if err != nil {
				t.Fatal(err)
			}
			return physical
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root is unavailable")
		}
	}
}
