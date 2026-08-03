package corecontract

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReportBindsTreeInputsExactStepOutputAndActualTestPass(t *testing.T) {
	fixture := newReportFixture(t)
	if err := ValidateReport(fixture.root, fixture.contract, fixture.registry, fixture.report); err != nil {
		t.Fatal(err)
	}
	tampered := fixture.report
	tampered.Steps = append([]StepResult(nil), fixture.report.Steps...)
	tampered.Steps[0].Argv = []string{"go", "test", "./p"}
	if err := ValidateReport(fixture.root, fixture.contract, fixture.registry, tampered); err == nil {
		t.Fatal("tampered argv passed report validation")
	}
	tampered = fixture.report
	tampered.Inputs.RegistrySHA256 = bytesSHA256([]byte("other"))
	if err := ValidateReport(fixture.root, fixture.contract, fixture.registry, tampered); err == nil {
		t.Fatal("tampered input digest passed report validation")
	}
}

type reportFixture struct {
	root     string
	contract Contract
	registry Registry
	report   GateReport
}

func newReportFixture(t *testing.T) reportFixture {
	t.Helper()
	root := initializeFixtureRepository(t)
	testRef := "./p::TestProof"
	step := GateStep{
		ID: "proof", Kind: "go-test", Argv: []string{"go", "test", "-json", "./p"},
		Oracles: []string{"test:" + testRef},
	}
	registry := fixtureRegistry(testRef, step)
	contract := Contract{Lifecycle: LifecycleActive,
		Invariants: append([]string(nil), expectedInvariants...), Gates: append([]string(nil), expectedGates...)}
	report := fixtureReport(t, root, testRef, step, registry)
	return reportFixture{root: root, contract: contract, registry: registry, report: report}
}

func initializeFixtureRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFixture(t, root, ".gitignore", ".testdata/\n")
	writeFixture(t, root, DocumentPath, "contract\n")
	writeFixture(t, root, RegistryPath, "registry\n")
	gitFixture(t, root, "init", "--quiet")
	gitFixture(t, root, "config", "user.email", "r7@example.invalid")
	gitFixture(t, root, "config", "user.name", "R7 Test")
	gitFixture(t, root, "add", ".")
	gitFixture(t, root, "commit", "--quiet", "-m", "fixture")
	return root
}

func fixtureRegistry(testRef string, step GateStep) Registry {
	registry := Registry{SchemaVersion: 1, Invariants: []InvariantBinding{}, Gates: []GateBinding{}}
	for _, id := range expectedInvariants {
		registry.Invariants = append(registry.Invariants, InvariantBinding{ID: id,
			Oracles: []InvariantOracle{{ID: "proof", Test: testRef}}})
	}
	for _, id := range expectedGates {
		registry.Gates = append(registry.Gates, GateBinding{ID: id, Steps: []GateStep{step}})
	}
	return registry
}

func fixtureReport(t *testing.T, root, testRef string, step GateStep, registry Registry) GateReport {
	t.Helper()
	runID := "fixture"
	base := filepath.ToSlash(filepath.Join(".testdata", "r7", "core-gates", runID))
	if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(base)), 0o700); err != nil {
		t.Fatal(err)
	}
	stdout := []byte(`{"Action":"pass","Package":"example/harness/p","Test":"TestProof"}` + "\n")
	stdoutPath := filepath.ToSlash(filepath.Join(base, "proof.stdout"))
	stderrPath := filepath.ToSlash(filepath.Join(base, "proof.stderr"))
	writeFixture(t, root, stdoutPath, string(stdout))
	writeFixture(t, root, stderrPath, "")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	report := GateReport{
		SchemaVersion: 1, RunID: runID, StartedAt: now, FinishedAt: now,
		Source: ReportSource{
			Commit:       gitFixture(t, root, "rev-parse", "HEAD"),
			Tree:         gitFixture(t, root, "rev-parse", "HEAD^{tree}"),
			CleanAtStart: true, CleanAtFinish: true,
		},
		Steps: []StepResult{{
			ID: step.ID, Kind: step.Kind, Argv: step.Argv, Oracles: step.Oracles,
			StartedAt: now, FinishedAt: now, ExitCode: 0,
			Output: ReportOutput{StdoutPath: stdoutPath, StdoutSHA256: bytesSHA256(stdout),
				StderrPath: stderrPath, StderrSHA256: bytesSHA256(nil)},
			PassedTests: []string{testRef},
		}},
		Gates: []GateResult{},
	}
	var err error
	report.Inputs.ContractSHA256, err = fileSHA256(filepath.Join(root, filepath.FromSlash(DocumentPath)))
	if err != nil {
		t.Fatal(err)
	}
	report.Inputs.RegistrySHA256, err = fileSHA256(filepath.Join(root, filepath.FromSlash(RegistryPath)))
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range registry.Gates {
		report.Gates = append(report.Gates, GateResult{ID: gate.ID, StepIDs: []string{"proof"}, Passed: true})
	}
	return report
}

func writeFixture(t *testing.T, root, relative, contents string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func gitFixture(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return strings.TrimSpace(string(output))
}
