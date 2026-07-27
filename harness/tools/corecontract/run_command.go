package corecontract

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func replaceEnvironment(current []string, replacements map[string]string) []string {
	keys := make([]string, 0, len(replacements))
	for key := range replacements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(current)+len(keys))
	for _, value := range current {
		key, _, found := strings.Cut(value, "=")
		if _, replace := replacements[key]; found && replace {
			continue
		}
		result = append(result, value)
	}
	for _, key := range keys {
		result = append(result, key+"="+replacements[key])
	}
	return result
}

func (runner gateRunner) runStep(ctx context.Context, state gateRun, index int,
	rule stepRule, argv, environment []string,
) (GateStep, string, error) {
	base := fmt.Sprintf("%02d-%s", index+1, rule.id)
	stdoutRelative := filepath.ToSlash(filepath.Join(
		".testdata", "r5", "core-gates", state.report.RunID, "steps", base+".stdout",
	))
	stderrRelative := filepath.ToSlash(filepath.Join(
		".testdata", "r5", "core-gates", state.report.RunID, "steps", base+".stderr",
	))
	stdout, err := openExclusivePrivate(filepath.Join(state.root,
		filepath.FromSlash(stdoutRelative)))
	if err != nil {
		return GateStep{}, stderrRelative, err
	}
	stderr, err := openExclusivePrivate(filepath.Join(state.root,
		filepath.FromSlash(stderrRelative)))
	if err != nil {
		_ = stdout.Close()
		return GateStep{}, stderrRelative, err
	}
	started := runner.now().UTC()
	fmt.Fprintf(runner.progress, "core-gate: start %s\n", rule.id)
	exitCode, runErr := runner.command(
		ctx, state.root, argv, environment, stdout, stderr,
	)
	closeErr := errors.Join(stdout.Close(), stderr.Close())
	finished := runner.now().UTC()
	if runErr == nil && closeErr != nil {
		runErr = closeErr
	}
	if exitCode < 0 || exitCode > 255 {
		exitCode = 255
	}
	digest, digestErr := fileDigest(filepath.Join(state.root,
		filepath.FromSlash(stdoutRelative)))
	if runErr == nil && digestErr != nil {
		runErr = digestErr
	}
	step := GateStep{
		ID: rule.id, Gate: rule.gate, Kind: rule.kind, Argv: argv,
		StartedAt:  started.Format(time.RFC3339Nano),
		FinishedAt: finished.Format(time.RFC3339Nano), ExitCode: exitCode,
		Output: GateOutput{Path: stdoutRelative, SHA256: digest},
	}
	if runErr == nil && exitCode == 0 {
		fmt.Fprintf(runner.progress, "core-gate: pass %s\n", rule.id)
	}
	return step, stderrRelative, runErr
}

func executeGateCommand(ctx context.Context, root string, argv, environment []string,
	stdout, stderr io.Writer,
) (int, error) {
	if len(argv) == 0 {
		return 255, errors.New("Core gate command is empty")
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Dir, command.Env = root, environment
	command.Stdin, command.Stdout, command.Stderr = nil, stdout, stderr
	err := command.Run()
	if err == nil {
		return 0, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() >= 0 {
		return exitError.ExitCode(), nil
	}
	return 255, err
}
