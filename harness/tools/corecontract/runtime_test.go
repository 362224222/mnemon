package corecontract

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestMappingAloneCannotCloseRequirement(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.mapTest("proof_test.go::TestProof")
	fixture.seal(t)

	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if got := UnresolvedMust(closure); !slices.Equal(got, []string{"SC-01"}) {
		t.Fatalf("mapping-only unresolved MUST = %v", got)
	}
	if got := UnresolvedGates(closure); !slices.Equal(got, []string{"G-PROCESS"}) {
		t.Fatalf("mapping-only unresolved gates = %v", got)
	}
}

func TestEmptyTestAndPackagePassCannotCloseRequirement(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.mapTest("proof_test.go::TestProof")
	fixture.seal(t)
	fixture.addStep(t, "process",
		`{"Action":"pass","Package":"example.invalid/proof"}`+"\n")

	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	result := closure.Requirements[0]
	if result.Status != RequirementPending ||
		!strings.Contains(result.Reason, "exact passing runtime event") {
		t.Fatalf("package-only result = %+v", result)
	}
}

func TestEmptyTestAndExactPassCannotCloseRequirement(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	writeTestFile(t, fixture.root, "proof_test.go",
		"package proof\nfunc TestProof() {}\n")
	fixture.mapTest("proof_test.go::TestProof")
	fixture.seal(t)
	fixture.addStep(t, "process",
		`{"Action":"pass","Package":"example.invalid/proof","Test":"TestProof"}`+"\n")

	if _, err := fixture.evaluate(); err == nil ||
		!strings.Contains(err.Error(), "empty body") {
		t.Fatalf("empty test with exact pass error = %v", err)
	}
}

func TestSkippedTestEventCannotCloseRequirement(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.mapTest("proof_test.go::TestProof")
	fixture.seal(t)
	fixture.addStep(t, "process",
		`{"Action":"skip","Package":"example.invalid/proof","Test":"TestProof"}`+"\n"+
			`{"Action":"pass","Package":"example.invalid/proof"}`+"\n")

	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if closure.Requirements[0].Status != RequirementPending {
		t.Fatalf("skipped test closed requirement: %+v", closure.Requirements[0])
	}
}

func TestExactPassingTestEventClosesRequirement(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.mapTest("proof_test.go::TestProof")
	fixture.seal(t)
	fixture.addStep(t, "process",
		`{"Action":"pass","Package":"example.invalid/proof","Test":"TestProof"}`+"\n")

	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if closure.Requirements[0].Status != RequirementVerified {
		t.Fatalf("exact passing event result = %+v", closure.Requirements[0])
	}
}

func TestClosureRejectsStaleSourceAndInputDigests(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.seal(t)
	cases := map[string]func(*GateReport){
		"commit": func(report *GateReport) { report.Source.Commit = strings.Repeat("0", 40) },
		"tree":   func(report *GateReport) { report.Source.Tree = strings.Repeat("0", 40) },
		"contract": func(report *GateReport) {
			report.Inputs.ContractSHA256 = "sha256:" + strings.Repeat("0", 64)
		},
		"registry": func(report *GateReport) {
			report.Inputs.RequirementsSHA256 = "sha256:" + strings.Repeat("0", 64)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			report := fixture.report
			mutate(&report)
			if _, err := EvaluateClosure(fixture.root, fixture.contract,
				fixture.registry, report); err == nil {
				t.Fatal("stale authority unexpectedly accepted")
			}
		})
	}
}

func TestGateReportRejectsNonCanonicalCommand(t *testing.T) {
	fixture := newClosureFixture(t, "G-PROCESS")
	fixture.seal(t)
	fixture.addStep(t, "process", "{}\n")
	fixture.report.Steps[0].Argv = []string{"true"}
	if err := ValidateGateReport(fixture.contract, fixture.report); err == nil ||
		!strings.Contains(err.Error(), "argv is not canonical") {
		t.Fatalf("fake command error = %v", err)
	}
}

func TestHarnessGateCommandsUseIndependentModule(t *testing.T) {
	tests := map[string][]string{
		"contract": {"go", "-C", "harness", "test", "-json",
			"./tools/corecontract", "./test/contracts", "-count=1"},
		"harness-build": {"go", "-C", "harness", "build", "./cmd/..."},
		"harness-unit": {"go", "-C", "harness", "test", "-json",
			"./cmd/...", "./internal/...", "./tools/...", "./test/contracts", "-count=1"},
		"harness-race": {"go", "-C", "harness", "test", "-json", "-race",
			"./cmd/...", "./internal/...", "./tools/...", "./test/contracts", "-count=1"},
		"fuzz-model": {"go", "-C", "harness", "test", "-json", "./internal/model",
			"-run", "^$", "-fuzz", "^FuzzParseSignedPublication$", "-fuzztime=100x"},
		"fuzz-peer": {"go", "-C", "harness", "test", "-json", "./internal/peer",
			"-run", "^$", "-fuzz", "^FuzzReadChannelFrame$", "-fuzztime=100x"},
		"fuzz-artifact": {"go", "-C", "harness", "test", "-json", "./internal/artifact",
			"-run", "^$", "-fuzz", "^FuzzParseManifest$", "-fuzztime=100x"},
		"process": {"go", "-C", "harness", "test", "-json",
			"./test/process", "-count=1"},
	}
	for id, want := range tests {
		rule, ok := gateStepRule(id)
		if !ok || !slices.Equal(rule.argv, want) {
			t.Errorf("%s argv = %v, want %v", id, rule.argv, want)
		}
	}
}

func TestGateReportRejectsSelfReportedFuzzCountAndFakeCommand(t *testing.T) {
	if _, err := DecodeGateReport([]byte(
		`{"schema_version":1,"steps":[{"fuzz_executions":100}]}`)); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("self-reported fuzz count error = %v", err)
	}
	fixture := newClosureFixture(t, "G-UNIT")
	fixture.seal(t)
	fixture.addStep(t, "fuzz-model", "{}\n")
	report := fixture.report
	report.Steps = append([]GateStep(nil), report.Steps...)
	report.Steps[0].Argv = append([]string(nil), report.Steps[0].Argv...)
	report.Steps[0].Argv[len(report.Steps[0].Argv)-1] = "-fuzztime=1x"
	if err := ValidateGateReport(fixture.contract, report); err == nil ||
		!strings.Contains(err.Error(), "argv is not canonical") {
		t.Fatalf("fake fuzz command error = %v", err)
	}
}

func TestFuzzClosureRequiresExactPassingRuntimeEvent(t *testing.T) {
	fixture := newClosureFixture(t, "G-UNIT")
	fixture.mapTest(
		"harness/internal/model/model_test.go::FuzzParseSignedPublication")
	writeTestFile(t, fixture.root, "harness/internal/model/model_test.go",
		`package model
import "testing"
func FuzzParseSignedPublication(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, input []byte) { _ = input })
}
`)
	fixture.seal(t)
	for _, id := range []string{
		"harness-build", "harness-unit", "harness-race",
		"fuzz-model", "fuzz-peer", "fuzz-artifact",
	} {
		fixture.addStep(t, id, "{}\n")
	}
	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if closure.Requirements[0].Status != RequirementPending {
		t.Fatal("fuzz count without exact pass event closed requirement")
	}

	fixture.replaceStepOutput(t, "fuzz-model",
		`{"Action":"pass","Package":"example.invalid/proof/harness/internal/model",`+
			`"Test":"FuzzParseSignedPublication"}`+"\n")
	closure, err = fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if closure.Requirements[0].Status != RequirementVerified {
		t.Fatalf("canonical fuzz pass result = %+v", closure.Requirements[0])
	}
}

type closureFixture struct {
	root     string
	contract Contract
	registry Registry
	report   GateReport
}

func newClosureFixture(t *testing.T, gate string) *closureFixture {
	t.Helper()
	root := t.TempDir()
	runTestGit(t, root, "init", "--quiet")
	runTestGit(t, root, "config", "user.email", "runtime@example.invalid")
	runTestGit(t, root, "config", "user.name", "Runtime Test")
	writeTestFile(t, root, ".gitignore", ".testdata/\n")
	writeTestFile(t, root, "go.mod", "module example.invalid/proof\n\ngo 1.22\n")
	writeTestFile(t, root, "proof_test.go", `package proof
import "testing"
func TestProof(t *testing.T) {
	if got := 2 + 2; got != 4 { t.Fatal(got) }
}
`)
	return &closureFixture{
		root: root, contract: oneRequirementContract(gate),
		registry: oneRequirementRegistry(),
	}
}

func (fixture *closureFixture) mapTest(reference string) {
	fixture.registry.Requirements[0].TestSymbols = []string{reference}
}

func (fixture *closureFixture) seal(t *testing.T) {
	t.Helper()
	writeTestFile(t, fixture.root, DocumentPath, "fixture contract\n")
	writeJSONTestFile(t, fixture.root, RegistryPath, fixture.registry)
	runTestGit(t, fixture.root, "add", ".")
	runTestGit(t, fixture.root, "commit", "--quiet", "-m", "fixture")
	fixture.report = GateReport{
		SchemaVersion: GateReportSchemaVersion,
		RunID:         "fixture",
		StartedAt:     "2026-07-27T10:00:00Z",
		FinishedAt:    "2026-07-27T10:10:00Z",
		Source: GateSource{
			Commit:       runTestGit(t, fixture.root, "rev-parse", "HEAD"),
			Tree:         runTestGit(t, fixture.root, "rev-parse", "HEAD^{tree}"),
			CleanAtStart: true, CleanAtFinish: true,
		},
		Steps:   []GateStep{},
		Bundles: []GateBundleRef{},
	}
	var err error
	fixture.report.Inputs.ContractSHA256, err = fileDigest(
		fixture.root + "/" + DocumentPath)
	if err != nil {
		t.Fatal(err)
	}
	fixture.report.Inputs.RequirementsSHA256, err = fileDigest(
		fixture.root + "/" + RegistryPath)
	if err != nil {
		t.Fatal(err)
	}
}

func (fixture *closureFixture) addStep(t *testing.T, id, output string) {
	t.Helper()
	rule, ok := gateStepRule(id)
	if !ok {
		t.Fatalf("unknown fixture step %s", id)
	}
	path := ".testdata/r5/gates/" + id + ".log"
	writeTestFile(t, fixture.root, path, output)
	argv, err := expectedStepArgv(rule, fixture.report)
	if err != nil {
		t.Fatal(err)
	}
	step := GateStep{
		ID: id, Gate: rule.gate, Kind: rule.kind, Argv: append([]string(nil), argv...),
		StartedAt: "2026-07-27T10:01:00Z", FinishedAt: "2026-07-27T10:02:00Z",
		ExitCode: 0, Output: GateOutput{Path: path, SHA256: bytesDigest([]byte(output))},
	}
	fixture.report.Steps = append(fixture.report.Steps, step)
}

func (fixture *closureFixture) replaceStepOutput(t *testing.T, id, output string) {
	t.Helper()
	for index := range fixture.report.Steps {
		if fixture.report.Steps[index].ID != id {
			continue
		}
		writeTestFile(t, fixture.root, fixture.report.Steps[index].Output.Path, output)
		fixture.report.Steps[index].Output.SHA256 = bytesDigest([]byte(output))
		return
	}
	t.Fatalf("fixture step %s is absent", id)
}

func (fixture *closureFixture) evaluate() (Closure, error) {
	return EvaluateClosure(fixture.root, fixture.contract, fixture.registry, fixture.report)
}

func writeJSONTestFile(t *testing.T, root, relative string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, relative, string(data)+"\n")
}
