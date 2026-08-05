package corecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

var reportRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func LoadReport(path string) (GateReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return GateReport{}, fmt.Errorf("read R7 gate report: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var report GateReport
	if err := decoder.Decode(&report); err != nil {
		return GateReport{}, fmt.Errorf("decode R7 gate report: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return GateReport{}, fmt.Errorf("R7 gate report contains trailing JSON")
	}
	canonical, err := canonicalJSON(report)
	if err != nil {
		return GateReport{}, err
	}
	if !bytes.Equal(data, canonical) {
		return GateReport{}, fmt.Errorf("R7 gate report is not canonical JSON")
	}
	return report, nil
}

func ValidateReport(root string, contract Contract, registry Registry, report GateReport) error {
	validators := []func() error{
		func() error { return validateReportIdentity(root, report) },
		func() error { return validateReportInputs(root, report.Inputs) },
		func() error { return validateReportSteps(root, registry, report) },
		func() error { return validateReportGates(registry, report.Gates) },
		func() error { return validateReportContract(contract) },
	}
	for _, validate := range validators {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateReportIdentity(root string, report GateReport) error {
	if report.SchemaVersion != ReportSchemaVersion || !reportRunIDPattern.MatchString(report.RunID) ||
		!report.Source.CleanAtStart || !report.Source.CleanAtFinish {
		return fmt.Errorf("report identity or clean-worktree binding is invalid")
	}
	started, err := time.Parse(time.RFC3339Nano, report.StartedAt)
	if err != nil {
		return fmt.Errorf("invalid report started_at: %w", err)
	}
	finished, err := time.Parse(time.RFC3339Nano, report.FinishedAt)
	if err != nil {
		return fmt.Errorf("invalid report finished_at: %w", err)
	}
	if finished.Before(started) {
		return fmt.Errorf("report finished_at precedes started_at")
	}
	if err := requireClean(root, "report evaluated against a dirty worktree"); err != nil {
		return err
	}
	commit, err := gitValue(root, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	tree, err := gitValue(root, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return err
	}
	if report.Source.Commit != commit || report.Source.Tree != tree {
		return fmt.Errorf("report source does not bind current HEAD commit and tree")
	}
	return nil
}

func validateReportInputs(root string, inputs ReportInputs) error {
	contractDigest, err := fileSHA256(filepath.Join(root, filepath.FromSlash(DocumentPath)))
	if err != nil {
		return err
	}
	registryDigest, err := fileSHA256(filepath.Join(root, filepath.FromSlash(RegistryPath)))
	if err != nil {
		return err
	}
	if inputs.ContractSHA256 != contractDigest || inputs.RegistrySHA256 != registryDigest {
		return fmt.Errorf("report input digests do not bind the tracked contract and registry")
	}
	return nil
}

func validateReportSteps(root string, registry Registry, report GateReport) error {
	expectedSteps := uniqueSteps(registry)
	if len(report.Steps) != len(expectedSteps) {
		return fmt.Errorf("report has %d steps, want %d", len(report.Steps), len(expectedSteps))
	}
	passedTests := make(map[string]struct{})
	for index, expected := range expectedSteps {
		observed, err := validateReportStep(root, report.RunID, expected, report.Steps[index])
		if err != nil {
			return err
		}
		for _, test := range observed {
			if _, duplicate := passedTests[test]; duplicate {
				return fmt.Errorf("bound test %s was duplicated across steps", test)
			}
			passedTests[test] = struct{}{}
		}
	}
	return requireEveryBoundTest(registry, passedTests)
}

func validateReportStep(root, runID string, expected GateStep, actual StepResult) ([]string, error) {
	if actual.ID != expected.ID || actual.Kind != expected.Kind ||
		!slices.Equal(actual.Argv, expected.Argv) || !slices.Equal(actual.Oracles, expected.Oracles) ||
		actual.ExitCode != 0 {
		return nil, fmt.Errorf("report step does not exactly bind registry step %s", expected.ID)
	}
	stdout, err := validateOutput(root, actual.Output.StdoutPath, actual.Output.StdoutSHA256,
		runID, actual.ID+".stdout")
	if err != nil {
		return nil, err
	}
	if _, err := validateOutput(root, actual.Output.StderrPath, actual.Output.StderrSHA256,
		runID, actual.ID+".stderr"); err != nil {
		return nil, err
	}
	observed, err := verifyStepOracles(expected, stdout)
	if err != nil {
		return nil, fmt.Errorf("report step %s: %w", actual.ID, err)
	}
	if !slices.Equal(actual.PassedTests, observed) {
		return nil, fmt.Errorf("report step %s passed_tests do not match output", actual.ID)
	}
	return observed, nil
}

func requireEveryBoundTest(registry Registry, passed map[string]struct{}) error {
	for _, test := range invariantTests(registry) {
		if _, found := passed[test]; !found {
			return fmt.Errorf("bound invariant test %s did not pass", test)
		}
	}
	return nil
}

func validateReportGates(registry Registry, results []GateResult) error {
	if len(results) != len(registry.Gates) {
		return fmt.Errorf("report has %d gates, want %d", len(results), len(registry.Gates))
	}
	for index, binding := range registry.Gates {
		result := results[index]
		stepIDs := make([]string, len(binding.Steps))
		for stepIndex, step := range binding.Steps {
			stepIDs[stepIndex] = step.ID
		}
		if result.ID != binding.ID || !result.Passed || !slices.Equal(result.StepIDs, stepIDs) {
			return fmt.Errorf("report gate %d does not exactly close %s", index, binding.ID)
		}
	}
	return nil
}

func validateReportContract(contract Contract) error {
	if !slices.Equal(contract.Invariants, expectedInvariants) || !slices.Equal(contract.Gates, expectedGates) {
		return fmt.Errorf("report evaluated against a non-canonical contract")
	}
	return nil
}

func validateOutput(root, relative, digest, runID, base string) ([]byte, error) {
	wantPrefix := filepath.ToSlash(filepath.Join(".testdata", "r7", "core-gates", runID)) + "/"
	if filepath.ToSlash(relative) != wantPrefix+base || strings.Contains(relative, "..") {
		return nil, fmt.Errorf("report output path %q is not exact", relative)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return nil, fmt.Errorf("read report output %s: %w", relative, err)
	}
	if bytesSHA256(data) != digest {
		return nil, fmt.Errorf("report output %s digest mismatch", relative)
	}
	return data, nil
}
