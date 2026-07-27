package corecontract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestGateRunnerWritesOneBoundMergeReport(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	reportPath, err := fixture.runner().run(
		context.Background(), fixture.root, RunModeMerge,
	)
	if err != nil {
		t.Fatal(err)
	}
	report := readGeneratedGateReport(t, fixture.root, reportPath)
	contract, err := Load(fixture.root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateGateReport(contract, report); err != nil {
		t.Fatal(err)
	}
	if len(report.Steps) != len(gateStepRules)-2 ||
		len(report.Bundles) != 1 || report.Bundles[0].Runtime != "scripted" {
		t.Fatalf("merge report = %d steps, bundles %+v",
			len(report.Steps), report.Bundles)
	}
	for _, step := range report.Steps {
		if step.Gate == "G-LIVE" {
			t.Fatalf("merge report contains Live step %s", step.ID)
		}
	}
	assertGateReportPermissions(t, fixture.root, reportPath)
	if status := runTestGit(t, fixture.root, "status", "--porcelain=v1",
		"--untracked-files=all"); status != "" {
		t.Fatalf("ignored gate report changed source status: %s", status)
	}
}

func TestGateRunnerReleaseUsesExactHermeticRunAndImage(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	reportPath, err := fixture.runner().run(
		context.Background(), fixture.root, RunModeRelease,
	)
	if err != nil {
		t.Fatal(err)
	}
	report := readGeneratedGateReport(t, fixture.root, reportPath)
	if len(report.Steps) != len(gateStepRules) ||
		len(report.Bundles) != 2 ||
		report.Bundles[0].Runtime != "scripted" ||
		report.Bundles[1].Runtime != "codex" {
		t.Fatalf("release report = %d steps, bundles %+v",
			len(report.Steps), report.Bundles)
	}
	live := fixture.command.callByExecutable(t,
		"harness/test/e2e/runner/run_live_codex.sh")
	if got := environmentValue(live.environment, "HERMETIC_RUN"); got != report.Bundles[0].RunID {
		t.Fatalf("Live HERMETIC_RUN = %q, want %q", got, report.Bundles[0].RunID)
	}
	if got := environmentValue(live.environment, "IMAGE"); got != gateRunnerFixtureImageReference {
		t.Fatalf("Live IMAGE = %q", got)
	}
	if !slices.Equal(live.argv, []string{
		"harness/test/e2e/runner/run_live_codex.sh",
		"--run", report.Bundles[1].RunID,
	}) {
		t.Fatalf("Live argv = %v", live.argv)
	}
}

func TestGateRunnerStopsAtFirstFailureWithoutPublishingReport(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	fixture.command.failCall = 2
	reportPath, err := fixture.runner().run(
		context.Background(), fixture.root, RunModeMerge,
	)
	if err == nil || reportPath != "" || len(fixture.command.calls) != 2 {
		t.Fatalf("failed run = path %q calls %d error %v",
			reportPath, len(fixture.command.calls), err)
	}
	assertNoGateReport(t, fixture.root)
}

func TestGateRunnerRejectsDifferentLiveImageWithoutPublishingReport(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	fixture.command.liveDigest = "sha256:" + strings.Repeat("2", 64)
	reportPath, err := fixture.runner().run(
		context.Background(), fixture.root, RunModeRelease,
	)
	if err == nil || !strings.Contains(err.Error(), "different candidate image") ||
		reportPath != "" {
		t.Fatalf("mismatched Live image = path %q error %v", reportPath, err)
	}
	assertNoGateReport(t, fixture.root)
}

func TestGateRunnerRejectsSourceMutationAtFinish(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	fixture.command.mutateSource = true
	reportPath, err := fixture.runner().run(
		context.Background(), fixture.root, RunModeMerge,
	)
	if err == nil || !strings.Contains(err.Error(), "clean source worktree") ||
		reportPath != "" {
		t.Fatalf("mutating run = path %q error %v", reportPath, err)
	}
	assertNoGateReport(t, fixture.root)
}

func TestGateRunnerRejectsDirtyStartAndSymlinkRuntimeRoot(t *testing.T) {
	t.Run("dirty", func(t *testing.T) {
		fixture := newGateRunnerFixture(t)
		writeTestFile(t, fixture.root, "dirty.txt", "not committed\n")
		if path, err := fixture.runner().run(
			context.Background(), fixture.root, RunModeMerge,
		); err == nil || path != "" {
			t.Fatalf("dirty run = path %q error %v", path, err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		fixture := newGateRunnerFixture(t)
		target := t.TempDir()
		if err := os.Symlink(target, filepath.Join(fixture.root, ".testdata")); err != nil {
			t.Fatal(err)
		}
		if path, err := fixture.runner().run(
			context.Background(), fixture.root, RunModeMerge,
		); err == nil || path != "" {
			t.Fatalf("symlink run = path %q error %v", path, err)
		}
	})
}

type gateRunnerFixture struct {
	root    string
	clock   *gateRunnerClock
	command *gateRunnerCommand
}

func newGateRunnerFixture(t *testing.T) *gateRunnerFixture {
	t.Helper()
	root := t.TempDir()
	runTestGit(t, root, "init", "--quiet")
	runTestGit(t, root, "config", "user.email", "gate@example.invalid")
	runTestGit(t, root, "config", "user.name", "Gate Test")
	writeTestFile(t, root, ".gitignore", ".testdata/\n")
	copyGateFixtureFile(t, root, DocumentPath)
	copyGateFixtureFile(t, root, RegistryPath)
	writeTestFile(t, root, "tracked.txt", "stable\n")
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "--quiet", "-m", "fixture")
	clock := &gateRunnerClock{
		next: time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC),
	}
	command := &gateRunnerCommand{t: t, root: root}
	return &gateRunnerFixture{root: root, clock: clock, command: command}
}

func copyGateFixtureFile(t *testing.T, targetRoot, relative string) {
	t.Helper()
	source := filepath.Join("../../..", filepath.FromSlash(relative))
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, targetRoot, relative, string(data))
}

func (fixture *gateRunnerFixture) runner() gateRunner {
	return gateRunner{
		now:     fixture.clock.now,
		random:  bytes.NewReader(bytes.Repeat([]byte{0x2a}, 24)),
		command: fixture.command.run, progress: io.Discard,
	}
}

type gateRunnerClock struct{ next time.Time }

func (clock *gateRunnerClock) now() time.Time {
	value := clock.next
	clock.next = clock.next.Add(time.Second)
	return value
}

type gateCommandCall struct {
	argv, environment []string
}

type gateRunnerCommand struct {
	t            *testing.T
	root         string
	calls        []gateCommandCall
	failCall     int
	mutateSource bool
	liveDigest   string
}

func (command *gateRunnerCommand) run(_ context.Context, root string, argv, environment []string,
	stdout, _ io.Writer,
) (int, error) {
	command.calls = append(command.calls, gateCommandCall{
		argv:        append([]string(nil), argv...),
		environment: append([]string(nil), environment...),
	})
	if command.mutateSource && len(command.calls) == 1 {
		writeTestFile(command.t, root, "tracked.txt", "changed\n")
	}
	if command.failCall == len(command.calls) {
		return 17, nil
	}
	if len(argv) != 0 && (strings.HasSuffix(argv[0], "/run_docker.sh") ||
		strings.HasSuffix(argv[0], "/run_live_codex.sh")) {
		command.writeBundle(argv, environment)
	}
	_, err := io.WriteString(stdout, "gate command passed\n")
	return 0, err
}

const gateRunnerFixtureImageReference = "mnemon-r5-e2e:fixture"

func (command *gateRunnerCommand) writeBundle(argv, environment []string) {
	command.t.Helper()
	runID := argumentValue(command.t, argv, "--run")
	runtimeName := "scripted"
	var paired *string
	if strings.HasSuffix(argv[0], "/run_live_codex.sh") {
		runtimeName = "codex"
		value := environmentValue(environment, "HERMETIC_RUN")
		paired = &value
	}
	commit := runTestGit(command.t, command.root, "rev-parse", "HEAD")
	tree := runTestGit(command.t, command.root, "rev-parse", "HEAD^{tree}")
	suite := runtimeSuiteReport{
		SchemaVersion: 1, RunID: runID, BundleKind: "single-case",
		Runtime: runtimeName, Status: "passed", GitSHA: commit,
		PairedHermeticRun: paired,
	}
	suite.Image.Reference = gateRunnerFixtureImageReference
	suite.Image.Digest = "sha256:" + strings.Repeat("1", 64)
	if runtimeName == "codex" && command.liveDigest != "" {
		suite.Image.Digest = command.liveDigest
	}
	suite.Image.Revision = commit
	suite.Image.SourceTree = tree
	data, err := json.Marshal(suite)
	if err != nil {
		command.t.Fatal(err)
	}
	base := filepath.Join(command.root, ".testdata", "r5", "runs", runID)
	if err := os.MkdirAll(base, 0o700); err != nil {
		command.t.Fatal(err)
	}
	if err := os.Chmod(base, 0o700); err != nil {
		command.t.Fatal(err)
	}
	writePrivateFixtureFile(command.t, filepath.Join(base, "report.json"), data)
	writePrivateFixtureFile(command.t, filepath.Join(base, "manifest.json"), []byte("{}\n"))
}

func writePrivateFixtureFile(t *testing.T, filename string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filename, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filename, 0o600); err != nil {
		t.Fatal(err)
	}
}

func argumentValue(t *testing.T, arguments []string, name string) string {
	t.Helper()
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] == name {
			return arguments[index+1]
		}
	}
	t.Fatalf("%s is absent from %v", name, arguments)
	return ""
}

func environmentValue(environment []string, name string) string {
	prefix := name + "="
	for _, value := range environment {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimPrefix(value, prefix)
		}
	}
	return ""
}

func (command *gateRunnerCommand) callByExecutable(t *testing.T,
	executable string,
) gateCommandCall {
	t.Helper()
	for _, call := range command.calls {
		if len(call.argv) != 0 && call.argv[0] == executable {
			return call
		}
	}
	t.Fatalf("command %s was not run", executable)
	return gateCommandCall{}
}

func readGeneratedGateReport(t *testing.T, root, relative string) GateReport {
	t.Helper()
	report, err := LoadGateReport(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func assertGateReportPermissions(t *testing.T, root, relative string) {
	t.Helper()
	filename := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Lstat(filename)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("gate report mode = %v error %v", info, err)
	}
	for directory := filepath.Dir(filename); directory != root; directory = filepath.Dir(directory) {
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("gate directory %s mode = %v error %v", directory, info, err)
		}
	}
}

func assertNoGateReport(t *testing.T, root string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(
		root, ".testdata", "r5", "core-gates", "*", "gate-report.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("failed run published reports: %v", matches)
	}
}

func TestGateRunnerRejectsInvalidModeAndRandomFailure(t *testing.T) {
	fixture := newGateRunnerFixture(t)
	runner := fixture.runner()
	if path, err := runner.run(context.Background(), fixture.root, "other"); err == nil || path != "" {
		t.Fatalf("invalid mode = path %q error %v", path, err)
	}
	runner.random = errorReader{}
	if path, err := runner.run(context.Background(), fixture.root, RunModeMerge); err == nil || path != "" {
		t.Fatalf("random failure = path %q error %v", path, err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("unavailable") }
