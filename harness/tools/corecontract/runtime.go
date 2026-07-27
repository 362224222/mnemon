package corecontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func ValidateGateReport(contract Contract, report GateReport) error {
	if report.SchemaVersion != GateReportSchemaVersion {
		return fmt.Errorf("Core gate report schema_version = %d, want %d",
			report.SchemaVersion, GateReportSchemaVersion)
	}
	if !runIDPattern.MatchString(report.RunID) {
		return fmt.Errorf("Core gate report run_id %q is malformed", report.RunID)
	}
	started, finished, err := validateInterval(report.StartedAt, report.FinishedAt)
	if err != nil {
		return fmt.Errorf("Core gate report interval: %w", err)
	}
	if !gitObjectPattern.MatchString(report.Source.Commit) ||
		!gitObjectPattern.MatchString(report.Source.Tree) {
		return fmt.Errorf("Core gate report source commit/tree is malformed")
	}
	if !report.Source.CleanAtStart || !report.Source.CleanAtFinish {
		return fmt.Errorf("Core gate report source is not clean at both boundaries")
	}
	if !digestPattern.MatchString(report.Inputs.ContractSHA256) ||
		!digestPattern.MatchString(report.Inputs.RequirementsSHA256) {
		return fmt.Errorf("Core gate report input digest is malformed")
	}
	if report.Steps == nil || report.Bundles == nil {
		return fmt.Errorf("Core gate report steps and bundles must be arrays")
	}
	if err := validateGateSteps(contract, report, started, finished); err != nil {
		return err
	}
	return validateGateBundles(report)
}

func validateGateSteps(contract Contract, report GateReport, started, finished time.Time) error {
	gates := make(map[string]struct{}, len(contract.Gates))
	for _, gate := range contract.Gates {
		gates[gate.ID] = struct{}{}
	}
	seenSteps := make(map[string]struct{}, len(report.Steps))
	for _, step := range report.Steps {
		rule, ok := gateStepRule(step.ID)
		if !ok || rule.gate != step.Gate || rule.kind != step.Kind {
			return fmt.Errorf("Core gate step %q has unknown or mismatched gate/kind", step.ID)
		}
		if _, ok := gates[step.Gate]; !ok {
			return fmt.Errorf("Core gate step %s references unavailable gate %s", step.ID, step.Gate)
		}
		if _, duplicate := seenSteps[step.ID]; duplicate {
			return fmt.Errorf("Core gate report repeats step %s", step.ID)
		}
		seenSteps[step.ID] = struct{}{}
		stepStart, stepFinish, err := validateInterval(step.StartedAt, step.FinishedAt)
		if err != nil || stepStart.Before(started) || stepFinish.After(finished) {
			return fmt.Errorf("Core gate step %s has an invalid interval", step.ID)
		}
		expectedArgv, err := expectedStepArgv(rule, report)
		if err != nil || !slices.Equal(step.Argv, expectedArgv) {
			return fmt.Errorf("Core gate step %s argv is not canonical", step.ID)
		}
		if step.ExitCode < 0 || step.ExitCode > 255 {
			return fmt.Errorf("Core gate step %s has an invalid exit code", step.ID)
		}
		if err := validateRuntimeFile(step.Output.Path, step.Output.SHA256); err != nil {
			return fmt.Errorf("Core gate step %s output: %w", step.ID, err)
		}
	}
	return nil
}

func validateGateBundles(report GateReport) error {
	seenRuntimes := make(map[string]struct{}, len(report.Bundles))
	for _, bundle := range report.Bundles {
		if bundle.Runtime != "scripted" && bundle.Runtime != "codex" {
			return fmt.Errorf("Core gate bundle runtime %q is invalid", bundle.Runtime)
		}
		if _, duplicate := seenRuntimes[bundle.Runtime]; duplicate {
			return fmt.Errorf("Core gate report repeats %s bundle", bundle.Runtime)
		}
		seenRuntimes[bundle.Runtime] = struct{}{}
		if !runIDPattern.MatchString(bundle.RunID) {
			return fmt.Errorf("Core gate bundle run_id %q is malformed", bundle.RunID)
		}
		if err := validateRuntimeFile(bundle.ReportPath, bundle.ReportSHA256); err != nil {
			return fmt.Errorf("Core gate %s bundle report: %w", bundle.Runtime, err)
		}
		if err := validateRuntimeFile(bundle.ManifestPath, bundle.ManifestSHA256); err != nil {
			return fmt.Errorf("Core gate %s bundle manifest: %w", bundle.Runtime, err)
		}
	}
	return nil
}

func EvaluateClosure(root string, contract Contract, registry Registry,
	report GateReport,
) (Closure, error) {
	if err := ValidateBindings(root, contract, registry); err != nil {
		return Closure{}, err
	}
	if err := ValidateGateReport(contract, report); err != nil {
		return Closure{}, err
	}
	logs, err := validateRuntimeAuthority(root, report)
	if err != nil {
		return Closure{}, err
	}
	if err := validateRuntimeBundles(root, report); err != nil {
		return Closure{}, err
	}
	closure := Closure{Gates: evaluateGates(contract, report)}
	gateStatus := make(map[string]GateResult, len(closure.Gates))
	for _, gate := range closure.Gates {
		gateStatus[gate.ID] = gate
	}
	records := make(map[string]EvidenceRecord, len(registry.Requirements))
	for _, record := range registry.Requirements {
		records[record.ID] = record
	}
	for _, requirement := range contract.Requirements {
		record := records[requirement.ID]
		result, err := evaluateRequirement(
			root, report, logs, requirement, gateStatus[requirement.PrimaryGate], record)
		if err != nil {
			return Closure{}, err
		}
		closure.Requirements = append(closure.Requirements, result)
	}
	return closure, nil
}

func evaluateRequirement(root string, report GateReport, logs []stepLog,
	requirement Requirement, gate GateResult, record EvidenceRecord,
) (RequirementResult, error) {
	result := RequirementResult{ID: requirement.ID, Gate: requirement.PrimaryGate,
		Status: RequirementPending, Proofs: []string{}}
	if gate.Status != RequirementVerified {
		result.Reason = gate.Reason
		return result, nil
	}
	if len(record.TestSymbols)+len(record.ScenarioKeys)+len(record.LiveScenarioKeys) == 0 {
		result.Reason = "no tracked runtime evidence mapping"
		return result, nil
	}
	ok, proof, reason, err := evaluateRecord(root, report, logs, record)
	if err != nil {
		return result, fmt.Errorf("requirement %s runtime evidence: %w", record.ID, err)
	}
	if !ok {
		result.Reason = reason
		return result, nil
	}
	result.Status, result.Proofs = RequirementVerified, proof
	return result, nil
}

func UnresolvedMust(closure Closure) []string {
	var unresolved []string
	for _, result := range closure.Requirements {
		if result.Status != RequirementVerified {
			unresolved = append(unresolved, result.ID)
		}
	}
	slices.Sort(unresolved)
	return unresolved
}

func UnresolvedGates(closure Closure) []string {
	open := make(map[string]struct{})
	for _, gate := range closure.Gates {
		if gate.Status != RequirementVerified {
			open[gate.ID] = struct{}{}
		}
	}
	for _, requirement := range closure.Requirements {
		if requirement.Status != RequirementVerified {
			open[requirement.Gate] = struct{}{}
		}
	}
	unresolved := make([]string, 0, len(open))
	for gate := range open {
		unresolved = append(unresolved, gate)
	}
	slices.Sort(unresolved)
	return unresolved
}

type stepLog struct {
	step GateStep
	data []byte
}

func validateRuntimeAuthority(root string, report GateReport) ([]stepLog, error) {
	commit, err := runGit(root, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(commit)) != report.Source.Commit {
		return nil, fmt.Errorf("Core gate report commit differs from current HEAD")
	}
	tree, err := runGit(root, "rev-parse", "HEAD^{tree}")
	if err != nil || strings.TrimSpace(string(tree)) != report.Source.Tree {
		return nil, fmt.Errorf("Core gate report tree differs from current HEAD")
	}
	status, err := runGit(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil || len(bytes.TrimSpace(status)) != 0 {
		return nil, fmt.Errorf("Core gate report requires a clean current worktree")
	}
	for relative, want := range map[string]string{
		DocumentPath: report.Inputs.ContractSHA256,
		RegistryPath: report.Inputs.RequirementsSHA256,
	} {
		got, err := fileDigest(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil || got != want {
			return nil, fmt.Errorf("Core gate report input %s digest differs from current tree",
				relative)
		}
	}
	logs := make([]stepLog, 0, len(report.Steps))
	for _, step := range report.Steps {
		data, err := readRuntimeFile(root, step.Output.Path, step.Output.SHA256)
		if err != nil {
			return nil, fmt.Errorf("Core gate step %s: %w", step.ID, err)
		}
		logs = append(logs, stepLog{step: step, data: data})
	}
	for _, bundle := range report.Bundles {
		if _, err := readRuntimeFile(root, bundle.ReportPath, bundle.ReportSHA256); err != nil {
			return nil, fmt.Errorf("Core gate %s bundle report: %w", bundle.Runtime, err)
		}
		if _, err := readRuntimeFile(root, bundle.ManifestPath, bundle.ManifestSHA256); err != nil {
			return nil, fmt.Errorf("Core gate %s bundle manifest: %w", bundle.Runtime, err)
		}
	}
	return logs, nil
}

func evaluateGates(contract Contract, report GateReport) []GateResult {
	steps := make(map[string]GateStep, len(report.Steps))
	for _, step := range report.Steps {
		steps[step.ID] = step
	}
	results := make([]GateResult, 0, len(contract.Gates))
	for _, gate := range contract.Gates {
		result := GateResult{ID: gate.ID, Status: RequirementVerified}
		for _, rule := range gateStepRules {
			if rule.gate != gate.ID {
				continue
			}
			step, ok := steps[rule.id]
			if !ok {
				result.Status, result.Reason = RequirementPending,
					"required gate step "+rule.id+" was not run"
				break
			}
			if step.ExitCode != 0 {
				result.Status, result.Reason = RequirementPending,
					fmt.Sprintf("required gate step %s exited %d", rule.id, step.ExitCode)
				break
			}
		}
		results = append(results, result)
	}
	return results
}

func evaluateRecord(root string, report GateReport, logs []stepLog,
	record EvidenceRecord,
) (bool, []string, string, error) {
	proofs := make([]string, 0, len(record.TestSymbols)+len(record.ScenarioKeys)+
		len(record.LiveScenarioKeys))
	for _, reference := range record.TestSymbols {
		ok, err := exactTestPassed(root, logs, reference)
		if err != nil {
			return false, nil, "", err
		}
		if !ok {
			return false, nil, "mapped test did not emit an exact passing runtime event: " +
				reference, nil
		}
		proofs = append(proofs, reference)
	}
	for _, value := range record.ScenarioKeys {
		key, _ := ParseScenarioKey(value)
		ok, reason, err := validateScenarioRuntimeEvidence(root, report, key, "scripted")
		if err != nil {
			return false, nil, "", err
		}
		if !ok {
			return false, nil, reason, nil
		}
		proofs = append(proofs, value)
	}
	for _, value := range record.LiveScenarioKeys {
		key, _ := ParseScenarioKey(value)
		ok, reason, err := validateScenarioRuntimeEvidence(root, report, key, "codex")
		if err != nil {
			return false, nil, "", err
		}
		if !ok {
			return false, nil, reason, nil
		}
		proofs = append(proofs, value)
	}
	return true, proofs, "", nil
}

func exactTestPassed(root string, logs []stepLog, reference string) (bool, error) {
	relative, symbol, _ := ParseTestSymbol(reference)
	command := exec.Command("go", "list", "-f", "{{.ImportPath}}", ".")
	command.Dir = filepath.Dir(filepath.Join(root, filepath.FromSlash(relative)))
	output, err := command.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("resolve test package for %s: %w: %s", reference, err,
			strings.TrimSpace(string(output)))
	}
	importPath := strings.TrimSpace(string(output))
	for _, log := range logs {
		wantKind := "go-test"
		if strings.HasPrefix(symbol, "Fuzz") {
			wantKind = "go-fuzz"
		}
		if log.step.Kind != wantKind || log.step.ExitCode != 0 {
			continue
		}
		passed, err := testEventPassed(log.data, importPath, symbol)
		if err != nil {
			return false, fmt.Errorf("parse step %s test output: %w", log.step.ID, err)
		}
		if passed {
			return true, nil
		}
	}
	return false, nil
}

func validateInterval(start, finish string) (time.Time, time.Time, error) {
	started, err := time.Parse(time.RFC3339Nano, start)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	finished, err := time.Parse(time.RFC3339Nano, finish)
	if err != nil || finished.Before(started) {
		return time.Time{}, time.Time{}, fmt.Errorf("finish precedes start or is malformed")
	}
	return started, finished, nil
}

func validateRuntimeFile(relative, digest string) error {
	if err := validateEvidencePath(relative); err != nil ||
		!strings.HasPrefix(relative, ".testdata/r5/") {
		return fmt.Errorf("%q is not an ignored R5 runtime path", relative)
	}
	if !digestPattern.MatchString(digest) {
		return fmt.Errorf("digest %q is malformed", digest)
	}
	return nil
}

func readRuntimeFile(root, relative, want string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return nil, err
	}
	if got := bytesDigest(data); got != want {
		return nil, fmt.Errorf("%s digest = %s, want %s", relative, got, want)
	}
	return data, nil
}

func bytesDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func runGit(root string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", arguments...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err,
			strings.TrimSpace(string(output)))
	}
	return output, nil
}
