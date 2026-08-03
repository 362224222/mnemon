package corecontract

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type preparedRun struct {
	root     string
	base     string
	contract Contract
	registry Registry
	report   GateReport
}

func RunGates(ctx context.Context, root string) (string, error) {
	run, err := prepareRun(root)
	if err != nil {
		return "", err
	}
	if err := run.execute(ctx); err != nil {
		return "", err
	}
	return run.finish()
}

func prepareRun(root string) (preparedRun, error) {
	root, err := canonicalRepositoryRoot(root)
	if err != nil {
		return preparedRun{}, err
	}
	if err := requireClean(root, "R7 gate requires a clean worktree"); err != nil {
		return preparedRun{}, err
	}
	contract, registry, err := loadGateInputs(root)
	if err != nil {
		return preparedRun{}, err
	}
	report, base, err := newReport(root)
	if err != nil {
		return preparedRun{}, err
	}
	return preparedRun{root: root, base: base, contract: contract,
		registry: registry, report: report}, nil
}

func loadGateInputs(root string) (Contract, Registry, error) {
	contract, err := Load(root)
	if err != nil {
		return Contract{}, Registry{}, err
	}
	registry, err := LoadRegistry(root)
	if err != nil {
		return Contract{}, Registry{}, err
	}
	if err := ValidateBindings(root, contract, registry); err != nil {
		return Contract{}, Registry{}, err
	}
	if err := ValidateAuthorityCutover(root); err != nil {
		return Contract{}, Registry{}, err
	}
	return contract, registry, nil
}

func newReport(root string) (GateReport, string, error) {
	commit, err := gitValue(root, "rev-parse", "HEAD")
	if err != nil {
		return GateReport{}, "", err
	}
	tree, err := gitValue(root, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return GateReport{}, "", err
	}
	started := time.Now().UTC()
	runID := started.Format("20060102T150405.000000000Z") + "-" + tree[:12]
	base := filepath.ToSlash(filepath.Join(".testdata", "r7", "core-gates", runID))
	if err := prepareReportDirectory(root, base); err != nil {
		return GateReport{}, "", err
	}
	report := GateReport{SchemaVersion: ReportSchemaVersion, RunID: runID,
		StartedAt: started.Format(time.RFC3339Nano),
		Source:    ReportSource{Commit: commit, Tree: tree, CleanAtStart: true},
		Steps:     []StepResult{}, Gates: []GateResult{}}
	if report.Inputs.ContractSHA256, err = fileSHA256(
		filepath.Join(root, filepath.FromSlash(DocumentPath))); err != nil {
		return GateReport{}, "", err
	}
	if report.Inputs.RegistrySHA256, err = fileSHA256(
		filepath.Join(root, filepath.FromSlash(RegistryPath))); err != nil {
		return GateReport{}, "", err
	}
	return report, base, nil
}

func prepareReportDirectory(root, base string) error {
	if err := ensureIgnored(root, base); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(base)), 0o700); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	return nil
}

func (run *preparedRun) execute(ctx context.Context) error {
	for _, step := range uniqueSteps(run.registry) {
		result, err := runStep(ctx, run.root, run.base, step)
		if err != nil {
			return err
		}
		run.report.Steps = append(run.report.Steps, result)
	}
	if err := requireInvariantPasses(run.registry, run.report.Steps); err != nil {
		return err
	}
	run.report.Gates = closedGateResults(run.registry)
	return nil
}

func requireInvariantPasses(registry Registry, steps []StepResult) error {
	passed := make(map[string]struct{})
	for _, step := range steps {
		for _, test := range step.PassedTests {
			passed[test] = struct{}{}
		}
	}
	for _, test := range invariantTests(registry) {
		if _, found := passed[test]; !found {
			return fmt.Errorf("bound invariant test %s was not observed passing", test)
		}
	}
	return nil
}

func closedGateResults(registry Registry) []GateResult {
	results := make([]GateResult, 0, len(registry.Gates))
	for _, gate := range registry.Gates {
		ids := make([]string, len(gate.Steps))
		for index, step := range gate.Steps {
			ids[index] = step.ID
		}
		results = append(results, GateResult{ID: gate.ID, StepIDs: ids, Passed: true})
	}
	return results
}

func (run *preparedRun) finish() (string, error) {
	if err := requireClean(run.root, "worktree changed while R7 gates ran"); err != nil {
		return "", err
	}
	run.report.Source.CleanAtFinish = true
	run.report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := ValidateReport(run.root, run.contract, run.registry, run.report); err != nil {
		return "", fmt.Errorf("validate generated report: %w", err)
	}
	relative := filepath.ToSlash(filepath.Join(run.base, "gate-report.json"))
	data, err := canonicalJSON(run.report)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(run.root, filepath.FromSlash(relative)), data, 0o600); err != nil {
		return "", fmt.Errorf("write gate report: %w", err)
	}
	return relative, nil
}

func requireClean(root, message string) error {
	clean, err := worktreeClean(root)
	if err != nil {
		return err
	}
	if !clean {
		return fmt.Errorf("%s", message)
	}
	return nil
}
