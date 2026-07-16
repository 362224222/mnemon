package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
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
	initialized, err := runner.Initialize(context.Background(), model.HostCodex, fixture.revision)
	if err != nil || !initialized.Created || initialized.Status != "initialized" {
		t.Fatalf("Initialize() = (%#v, %v)", initialized, err)
	}
	authority, err := runner.Inspect(context.Background())
	if err != nil || !authority.Enabled || authority.Host != string(model.HostCodex) ||
		authority.AssetRevision != fixture.revision {
		t.Fatalf("Inspect() = (%#v, %v)", authority, err)
	}
	activated, err := runner.Activate(context.Background(), model.HostCodex, fixture.revision)
	if err != nil || !activated.Changed || activated.Status != "active" {
		t.Fatalf("Activate() = (%#v, %v)", activated, err)
	}
	deactivated, err := runner.Deactivate(context.Background(), model.HostCodex, fixture.revision)
	if err != nil || !deactivated.Changed || deactivated.Status != "inactive" {
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
		fixture.workspace + "|activate --project-root " + fixture.workspace +
			" --host codex --asset-revision " + fixture.revision,
		fixture.workspace + "|deactivate --project-root " + fixture.workspace +
			" --host codex --asset-revision " + fixture.revision,
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
  activate) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"active"}\n';;
  deactivate) printf '{"asset_revision":"@REVISION@","changed":true,"host":"codex","schema_version":1,"status":"inactive"}\n';;
  *) exit 8;;
esac
`
	template = strings.ReplaceAll(template, "@REVISION@", revision)
	return strings.ReplaceAll(template, "@REPLAY_REVISION@",
		model.Sum([]byte("durable-replay-assets")).String())
}
