package corecontract

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type GateReport struct {
	SchemaVersion int          `json:"schema_version"`
	RunID         string       `json:"run_id"`
	StartedAt     string       `json:"started_at"`
	FinishedAt    string       `json:"finished_at"`
	Source        ReportSource `json:"source"`
	Inputs        ReportInputs `json:"inputs"`
	Steps         []StepResult `json:"steps"`
	Gates         []GateResult `json:"gates"`
}

type ReportSource struct {
	Commit        string `json:"commit"`
	Tree          string `json:"tree"`
	CleanAtStart  bool   `json:"clean_at_start"`
	CleanAtFinish bool   `json:"clean_at_finish"`
}

type ReportInputs struct {
	ContractSHA256 string `json:"contract_sha256"`
	RegistrySHA256 string `json:"registry_sha256"`
}

type StepResult struct {
	ID          string       `json:"id"`
	Kind        string       `json:"kind"`
	Argv        []string     `json:"argv"`
	Oracles     []string     `json:"oracles"`
	StartedAt   string       `json:"started_at"`
	FinishedAt  string       `json:"finished_at"`
	ExitCode    int          `json:"exit_code"`
	Output      ReportOutput `json:"output"`
	PassedTests []string     `json:"passed_tests"`
}

type ReportOutput struct {
	StdoutPath   string `json:"stdout_path"`
	StdoutSHA256 string `json:"stdout_sha256"`
	StderrPath   string `json:"stderr_path"`
	StderrSHA256 string `json:"stderr_sha256"`
}

type GateResult struct {
	ID      string   `json:"id"`
	StepIDs []string `json:"step_ids"`
	Passed  bool     `json:"passed"`
}

type commandResult struct {
	exitCode int
	stdout   []byte
	stderr   []byte
}

var runCommand = executeCommand

func runStep(ctx context.Context, root, base string, step GateStep) (StepResult, error) {
	started := time.Now().UTC()
	result := runCommand(ctx, root, step.Argv)
	stdoutPath := filepath.ToSlash(filepath.Join(base, step.ID+".stdout"))
	stderrPath := filepath.ToSlash(filepath.Join(base, step.ID+".stderr"))
	if err := writeOutput(root, stdoutPath, result.stdout); err != nil {
		return StepResult{}, err
	}
	if err := writeOutput(root, stderrPath, result.stderr); err != nil {
		return StepResult{}, err
	}
	if result.exitCode != 0 {
		return StepResult{}, fmt.Errorf("step %s exited %d; see %s and %s",
			step.ID, result.exitCode, stdoutPath, stderrPath)
	}
	passedTests, err := verifyStepOracles(step, result.stdout)
	if err != nil {
		return StepResult{}, fmt.Errorf("step %s: %w", step.ID, err)
	}
	return StepResult{
		ID: step.ID, Kind: step.Kind, Argv: slices.Clone(step.Argv),
		Oracles:    slices.Clone(step.Oracles),
		StartedAt:  started.Format(time.RFC3339Nano),
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		ExitCode:   0,
		Output: ReportOutput{
			StdoutPath: stdoutPath, StdoutSHA256: bytesSHA256(result.stdout),
			StderrPath: stderrPath, StderrSHA256: bytesSHA256(result.stderr),
		},
		PassedTests: passedTests,
	}, nil
}

func verifyStepOracles(step GateStep, stdout []byte) ([]string, error) {
	if step.Kind == "shell" {
		lines := strings.Split(strings.ReplaceAll(string(stdout), "\r\n", "\n"), "\n")
		for _, oracle := range step.Oracles {
			want := strings.TrimPrefix(oracle, "stdout:")
			count := 0
			for _, line := range lines {
				if line == want {
					count++
				}
			}
			if count != 1 {
				return nil, fmt.Errorf("stdout oracle %q occurred %d times, want exactly one", want, count)
			}
		}
		return []string{}, nil
	}
	required := make(map[string]struct{}, len(step.Oracles))
	for _, oracle := range step.Oracles {
		required[strings.TrimPrefix(oracle, "test:")] = struct{}{}
	}
	passed, err := parseGoTestJSON(stdout, required)
	if err != nil {
		return nil, err
	}
	return passed, nil
}

type goTestEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
}

func parseGoTestJSON(output []byte, required map[string]struct{}) ([]string, error) {
	counts := make(map[string]int, len(required))
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var event goTestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("go-test emitted non-JSON output: %w", err)
		}
		if event.Test == "" || (event.Action != "pass" && event.Action != "fail" && event.Action != "skip") {
			continue
		}
		for reference := range required {
			packagePath, symbol, _ := strings.Cut(reference, "::")
			if event.Test != symbol || !packageMatches(event.Package, packagePath) {
				continue
			}
			if event.Action != "pass" {
				return nil, fmt.Errorf("required test %s reported %s", reference, event.Action)
			}
			counts[reference]++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan go-test JSON: %w", err)
	}
	passed := make([]string, 0, len(required))
	for reference := range required {
		if counts[reference] != 1 {
			return nil, fmt.Errorf("required test %s passed %d times, want exactly one", reference, counts[reference])
		}
		passed = append(passed, reference)
	}
	slices.Sort(passed)
	return passed, nil
}

func packageMatches(importPath, relative string) bool {
	suffix := strings.TrimPrefix(relative, ".")
	return strings.HasSuffix(importPath, suffix)
}

func executeCommand(ctx context.Context, root string, argv []string) commandResult {
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Dir = root
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		exitCode = -1
		if exit, ok := err.(*exec.ExitError); ok {
			exitCode = exit.ExitCode()
		}
	}
	return commandResult{exitCode: exitCode, stdout: stdout.Bytes(), stderr: stderr.Bytes()}
}

func canonicalRepositoryRoot(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}
	resolved, err := gitValue(absolute, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve Git root: %w", err)
	}
	if filepath.Clean(absolute) != filepath.Clean(resolved) {
		return "", fmt.Errorf("--root %q is not the repository root %q", absolute, resolved)
	}
	return resolved, nil
}

func worktreeClean(root string) (bool, error) {
	value, err := gitValue(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return false, err
	}
	return value == "", nil
}

func gitValue(root string, arguments ...string) (string, error) {
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err,
			strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func ensureIgnored(root, relative string) error {
	command := exec.Command("git", "-C", root, "check-ignore", "-q", "--", relative)
	if err := command.Run(); err != nil {
		return fmt.Errorf("report path %s is not ignored", relative)
	}
	return nil
}

func writeOutput(root, relative string, data []byte) error {
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(relative)), data, 0o600); err != nil {
		return fmt.Errorf("write step output %s: %w", relative, err)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read digest input %s: %w", path, err)
	}
	return bytesSHA256(data), nil
}

func bytesSHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
