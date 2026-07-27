package corecontract

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type RunMode string

const (
	RunModeMerge   RunMode = "merge"
	RunModeRelease RunMode = "release"
)

type gateCommandFunc func(context.Context, string, []string, []string,
	io.Writer, io.Writer) (int, error)

type gateRunner struct {
	now      func() time.Time
	random   io.Reader
	command  gateCommandFunc
	progress io.Writer
}

type gateAuthority struct {
	commit, tree                   string
	contractDigest, registryDigest string
}

type gateRun struct {
	root           string
	directory      string
	scriptedRun    string
	codexRun       string
	imageReference string
	imageDigest    string
	report         GateReport
}

// RunGates executes the canonical R5 Core gate commands and writes one ignored
// report bound to the unchanged source tree. A partial or failed run never
// returns a report path.
func RunGates(ctx context.Context, root string, mode RunMode) (string, error) {
	runner := gateRunner{
		now: time.Now, random: rand.Reader, command: executeGateCommand,
		progress: os.Stderr,
	}
	return runner.run(ctx, root, mode)
}

func (runner gateRunner) run(ctx context.Context, root string, mode RunMode) (string, error) {
	if ctx == nil {
		return "", errors.New("Core gate context is required")
	}
	if mode != RunModeMerge && mode != RunModeRelease {
		return "", fmt.Errorf("Core gate mode %q is invalid", mode)
	}
	canonicalRoot, err := canonicalRepositoryRoot(root)
	if err != nil {
		return "", err
	}
	contract, err := Load(canonicalRoot)
	if err != nil {
		return "", err
	}
	startAuthority, err := readGateAuthority(canonicalRoot)
	if err != nil {
		return "", err
	}
	runID, err := runner.newRunID(mode)
	if err != nil {
		return "", err
	}
	state, err := runner.startRun(canonicalRoot, runID, startAuthority)
	if err != nil {
		return "", err
	}
	for index, rule := range gateStepRules {
		if mode == RunModeMerge && rule.gate == "G-LIVE" {
			continue
		}
		if err := runner.executeRule(ctx, &state, index, rule); err != nil {
			return "", err
		}
	}
	if err := runner.finishRun(&state, contract, startAuthority); err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join(
		".testdata", "r5", "core-gates", runID, "gate-report.json",
	)), nil
}

func (runner gateRunner) newRunID(mode RunMode) (string, error) {
	random := make([]byte, 12)
	if _, err := io.ReadFull(runner.random, random); err != nil {
		return "", fmt.Errorf("create Core gate run identity: %w", err)
	}
	stamp := runner.now().UTC().Format("20060102T150405.000000000Z")
	return "core-" + string(mode) + "-" + stamp + "-" + hex.EncodeToString(random), nil
}

func (runner gateRunner) startRun(root, runID string, authority gateAuthority,
) (gateRun, error) {
	relative := filepath.Join(".testdata", "r5", "core-gates", runID)
	directory, err := makePrivateDirectory(root, relative)
	if err != nil {
		return gateRun{}, err
	}
	if _, err := makePrivateDirectory(root, filepath.Join(relative, "steps")); err != nil {
		return gateRun{}, err
	}
	started := runner.now().UTC().Format(time.RFC3339Nano)
	return gateRun{
		root: root, directory: directory,
		scriptedRun: runID + "-scripted", codexRun: runID + "-codex",
		report: GateReport{
			SchemaVersion: GateReportSchemaVersion, RunID: runID, StartedAt: started,
			Source: GateSource{
				Commit: authority.commit, Tree: authority.tree, CleanAtStart: true,
			},
			Inputs: GateInputs{
				ContractSHA256:     authority.contractDigest,
				RequirementsSHA256: authority.registryDigest,
			},
			Steps: []GateStep{}, Bundles: []GateBundleRef{},
		},
	}, nil
}

func (runner gateRunner) executeRule(ctx context.Context, state *gateRun,
	index int, rule stepRule,
) error {
	runID := ""
	switch rule.id {
	case "docker", "evidence-hermetic":
		runID = state.scriptedRun
	case "live", "evidence-live":
		runID = state.codexRun
	}
	argv := append([]string(nil), rule.argv...)
	if runID != "" {
		argv = gateStepArgv(rule, runID)
	}
	environment := os.Environ()
	if rule.id == "live" {
		environment = replaceEnvironment(environment, map[string]string{
			"HERMETIC_RUN": state.scriptedRun,
			"IMAGE":        state.imageReference,
		})
	}
	step, stderrPath, err := runner.runStep(ctx, *state, index, rule, argv, environment)
	state.report.Steps = append(state.report.Steps, step)
	if err != nil {
		return fmt.Errorf("Core gate step %s failed; stderr: %s: %w",
			rule.id, stderrPath, err)
	}
	if step.ExitCode != 0 {
		return fmt.Errorf("Core gate step %s exited %d; stderr: %s",
			rule.id, step.ExitCode, stderrPath)
	}
	switch rule.id {
	case "docker":
		ref, identity, err := completedGateBundle(
			state.root, "scripted", state.scriptedRun, state.report.Source, "",
		)
		if err != nil {
			return err
		}
		state.report.Bundles = append(state.report.Bundles, ref)
		state.imageReference, state.imageDigest = identity.reference, identity.digest
	case "live":
		ref, identity, err := completedGateBundle(
			state.root, "codex", state.codexRun, state.report.Source,
			state.scriptedRun,
		)
		if err != nil {
			return err
		}
		if identity.digest != state.imageDigest {
			return errors.New("Live Core gate used a different candidate image")
		}
		state.report.Bundles = append(state.report.Bundles, ref)
	}
	return nil
}

func (runner gateRunner) finishRun(state *gateRun, contract Contract,
	start gateAuthority,
) error {
	finish, err := readGateAuthority(state.root)
	if err != nil {
		return err
	}
	if finish != start {
		return errors.New("Core gate source authority changed during the run")
	}
	state.report.Source.CleanAtFinish = true
	state.report.FinishedAt = runner.now().UTC().Format(time.RFC3339Nano)
	if err := ValidateGateReport(contract, state.report); err != nil {
		return fmt.Errorf("validate generated Core gate report: %w", err)
	}
	data, err := json.MarshalIndent(state.report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Core gate report: %w", err)
	}
	data = append(data, '\n')
	reportPath := filepath.Join(state.directory, "gate-report.json")
	if err := writeExclusivePrivate(reportPath, data); err != nil {
		return err
	}
	final, err := readGateAuthority(state.root)
	if err != nil || final != start {
		_ = os.Remove(reportPath)
		return errors.New("Core gate source authority changed while publishing the report")
	}
	return nil
}
