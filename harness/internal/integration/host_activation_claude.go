package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	claudeActivationCommand = "/hooks"
	claudeActivationSkill   = "mnemon-harness"
)

type claudeActivationInit struct {
	Type              string   `json:"type"`
	Subtype           string   `json:"subtype"`
	CWD               string   `json:"cwd"`
	SessionID         string   `json:"session_id"`
	Tools             []string `json:"tools"`
	MCPServers        []any    `json:"mcp_servers"`
	ClaudeCodeVersion string   `json:"claude_code_version"`
	Skills            []string `json:"skills"`
}

type claudeActivationResult struct {
	Type          string `json:"type"`
	Subtype       string `json:"subtype"`
	IsError       bool   `json:"is_error"`
	DurationAPIMS uint64 `json:"duration_api_ms"`
	NumTurns      uint64 `json:"num_turns"`
	Result        string `json:"result"`
	SessionID     string `json:"session_id"`
	TotalCostUSD  uint64 `json:"total_cost_usd"`
	Usage         struct {
		InputTokens         uint64 `json:"input_tokens"`
		CacheCreationTokens uint64 `json:"cache_creation_input_tokens"`
		CacheReadTokens     uint64 `json:"cache_read_input_tokens"`
		OutputTokens        uint64 `json:"output_tokens"`
	} `json:"usage"`
}

func verifyAlternateHostActivation(ctx context.Context, workspace, nodeState string,
	observation HostObservation, bundle assets.Bundle,
) error {
	if observation.Host != assets.HostClaudeCode {
		return hostActivationUnavailable("Host has no bounded activation observation surface", nil)
	}
	return verifyClaudeHostActivation(ctx, workspace, nodeState, observation, bundle)
}

// verifyClaudeHostActivation uses only local Claude Code commands. `doctor`
// makes malformed project settings observable; the synthetic /hooks print
// command then emits system/init without a provider turn. That init is the
// Host-owned proof that the exact projected Skill was discovered. The exact
// Hook registration remains independently byte- and ownership-verified by
// VerifyHostProjection immediately before and after these observations.
func verifyClaudeHostActivation(ctx context.Context, workspace, nodeState string,
	observation HostObservation, bundle assets.Bundle,
) error {
	executable, err := verifyClaudeActivationExecutable(observation)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, hostActivationTimeout)
	defer cancel()
	doctor, stream, err := observeClaudeActivation(probeCtx, executable, workspace)
	if err != nil {
		return hostActivationUnavailable("run Claude activation observations", err)
	}
	settingsPath := filepath.Join(workspace, ".claude", "settings.json")
	if claudeDoctorRejectsProjectSettings(doctor.stdout, settingsPath) {
		return fmt.Errorf("%w: Claude rejected the projected project settings",
			ErrHostActivationRequired)
	}
	if len(stream.stderr) != 0 {
		return fmt.Errorf("%w: Claude activation emitted diagnostics", ErrHostActivationRequired)
	}
	if err := validateClaudeActivationStream(stream.stdout, workspace, observation.Version); err != nil {
		return err
	}
	return VerifyHostProjection(workspace, nodeState, observation.Host, bundle)
}

func verifyClaudeActivationExecutable(observation HostObservation) (string, error) {
	if observation.Host != assets.HostClaudeCode {
		return "", hostActivationUnavailable("Claude Host authority differs", nil)
	}
	executable, err := verifyHostExecutable(observation.Executable)
	if err != nil || executable != observation.Executable {
		return "", hostActivationUnavailable("revalidate Claude executable", err)
	}
	version, err := canonicalHostVersion([]byte(observation.Version))
	if err != nil || version != observation.Version {
		return "", hostActivationUnavailable("revalidate Claude version", err)
	}
	return executable, nil
}

type claudeActivationCommandResult struct {
	stdout []byte
	stderr []byte
}

type claudeActivationObservation struct {
	kind   string
	result claudeActivationCommandResult
	err    error
}

func observeClaudeActivation(ctx context.Context, executable, workspace string,
) (claudeActivationCommandResult, claudeActivationCommandResult, error) {
	ownedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	specs := []struct {
		kind      string
		arguments []string
	}{
		{kind: "doctor", arguments: []string{"doctor"}},
		{kind: "stream", arguments: []string{"-p", claudeActivationCommand,
			"--output-format", "stream-json", "--verbose", "--tools", "",
			"--no-session-persistence", "--setting-sources", "project"}},
	}
	results := make(chan claudeActivationObservation, len(specs))
	var wait sync.WaitGroup
	for _, spec := range specs {
		spec := spec
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := runClaudeActivationCommand(ownedCtx, executable, workspace, spec.arguments...)
			if err != nil {
				cancel()
			}
			results <- claudeActivationObservation{kind: spec.kind, result: result, err: err}
		}()
	}
	wait.Wait()
	close(results)
	var doctor, stream claudeActivationCommandResult
	var combined error
	for observed := range results {
		if observed.err != nil {
			combined = errors.Join(combined, fmt.Errorf("%s: %w", observed.kind, observed.err))
		}
		if observed.kind == "doctor" {
			doctor = observed.result
		} else {
			stream = observed.result
		}
	}
	return doctor, stream, combined
}

func runClaudeActivationCommand(ctx context.Context, executable, workspace string,
	arguments ...string,
) (claudeActivationCommandResult, error) {
	var stdout, stderr claudeActivationBuffer
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Dir = workspace
	command.Env = hostActivationEnvironment(os.Environ())
	command.Stdin = nil
	command.Stdout, command.Stderr = &stdout, &stderr
	command.WaitDelay = 250 * time.Millisecond
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	command.Cancel = func() error {
		if command.Process != nil {
			terminateHostActivationProcessGroup(command.Process.Pid)
		}
		return nil
	}
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return claudeActivationCommandResult{}, ctx.Err()
		}
		return claudeActivationCommandResult{}, err
	}
	if stdout.full || stderr.full {
		return claudeActivationCommandResult{}, errors.New("Claude activation output exceeded its bound")
	}
	return claudeActivationCommandResult{stdout: stdout.Bytes(), stderr: stderr.Bytes()}, nil
}

type claudeActivationBuffer struct {
	buffer bytes.Buffer
	full   bool
}

func (buffer *claudeActivationBuffer) Write(value []byte) (int, error) {
	if buffer == nil || buffer.full || buffer.buffer.Len()+len(value) > hostActivationOutputMax {
		if buffer != nil {
			buffer.full = true
		}
		return 0, errors.New("Claude activation output exceeds its closed bound")
	}
	return buffer.buffer.Write(value)
}

func (buffer *claudeActivationBuffer) Bytes() []byte {
	if buffer == nil || buffer.full {
		return nil
	}
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

func claudeDoctorRejectsProjectSettings(output []byte, settingsPath string) bool {
	if len(output) == 0 || len(output) > hostActivationOutputMax || !utf8.Valid(output) ||
		bytes.IndexByte(output, 0) >= 0 {
		return true
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, settingsPath) {
			return true
		}
	}
	return false
}

func validateClaudeActivationStream(raw []byte, workspace, observedVersion string) error {
	proof, err := scanClaudeActivationStream(raw)
	if err != nil {
		return err
	}
	if proof.init == nil || proof.result == nil || proof.messages < 2 {
		return claudeActivationRequired("Claude activation proof is incomplete")
	}
	return validateClaudeActivationProof(*proof.init, *proof.result, workspace, observedVersion)
}

type claudeActivationStreamProof struct {
	init     *claudeActivationInit
	result   *claudeActivationResult
	messages int
	total    int
}

func scanClaudeActivationStream(raw []byte) (claudeActivationStreamProof, error) {
	reader := bufio.NewScanner(bytes.NewReader(raw))
	reader.Buffer(make([]byte, 4096), hostActivationOutputMax+1)
	proof := claudeActivationStreamProof{}
	for reader.Scan() {
		line := append([]byte(nil), reader.Bytes()...)
		proof.messages++
		proof.total += len(line) + 1
		if len(line) == 0 || proof.messages > hostActivationMessages ||
			proof.total > hostActivationOutputMax ||
			!utf8.Valid(line) {
			return claudeActivationStreamProof{},
				claudeActivationRequired("Claude activation stream exceeded its bound")
		}
		if err := proof.observe(line); err != nil {
			return claudeActivationStreamProof{}, err
		}
	}
	if err := reader.Err(); err != nil {
		return claudeActivationStreamProof{},
			hostActivationUnavailable("read Claude activation stream", err)
	}
	return proof, nil
}

func (proof *claudeActivationStreamProof) observe(line []byte) error {
	kind, subtype, err := claudeActivationMessageKind(line)
	if err != nil {
		return claudeActivationRequired("Claude activation stream is malformed")
	}
	if kind == "system" && subtype == "init" {
		if proof.init != nil {
			return claudeActivationRequired("Claude emitted duplicate initialization")
		}
		value := new(claudeActivationInit)
		if err := decodeClaudeActivationJSON(line, value); err != nil {
			return claudeActivationRequired("Claude initialization is malformed")
		}
		proof.init = value
	}
	if kind == "result" {
		if proof.result != nil {
			return claudeActivationRequired("Claude emitted duplicate completion")
		}
		value := new(claudeActivationResult)
		if err := decodeClaudeActivationJSON(line, value); err != nil {
			return claudeActivationRequired("Claude completion is malformed")
		}
		proof.result = value
	}
	return nil
}

func claudeActivationMessageKind(raw []byte) (string, string, error) {
	var header struct {
		Type    string `json:"type"`
		Subtype string `json:"subtype"`
	}
	if err := decodeClaudeActivationJSON(raw, &header); err != nil || header.Type == "" {
		return "", "", errors.New("Claude activation message has no type")
	}
	return header.Type, header.Subtype, nil
}

func decodeClaudeActivationJSON(raw []byte, target any) error {
	if _, err := model.CanonicalizeJSON(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("Claude activation JSON has trailing data")
	}
	return nil
}

func validateClaudeActivationProof(init claudeActivationInit, result claudeActivationResult,
	workspace, observedVersion string,
) error {
	version := strings.TrimSuffix(observedVersion, " (Claude Code)")
	if !validClaudeActivationInit(init, workspace, version) {
		return claudeActivationRequired("Claude did not load one exact project Skill in the closed Runtime")
	}
	if !validClaudeActivationResult(result, init.SessionID) {
		return claudeActivationRequired("Claude activation unexpectedly reached a provider turn")
	}
	return nil
}

func validClaudeActivationInit(init claudeActivationInit, workspace, version string) bool {
	return init.Type == "system" && init.Subtype == "init" && init.CWD == workspace &&
		init.SessionID != "" && len(init.SessionID) <= 128 && init.ClaudeCodeVersion == version &&
		len(init.Tools) == 0 && len(init.MCPServers) == 0 &&
		countClaudeActivationSkill(init.Skills) == 1
}

func validClaudeActivationResult(result claudeActivationResult, sessionID string) bool {
	return result.Type == "result" && result.Subtype == "success" && !result.IsError &&
		result.SessionID == sessionID && result.DurationAPIMS == 0 && result.NumTurns == 0 &&
		result.TotalCostUSD == 0 && result.Usage.InputTokens == 0 &&
		result.Usage.CacheCreationTokens == 0 && result.Usage.CacheReadTokens == 0 &&
		result.Usage.OutputTokens == 0
}

func countClaudeActivationSkill(skills []string) int {
	count := 0
	for _, skill := range skills {
		if skill == claudeActivationSkill {
			count++
		}
	}
	return count
}

func claudeActivationRequired(message string) error {
	return fmt.Errorf("%w: %s", ErrHostActivationRequired, message)
}

var _ io.Writer = (*claudeActivationBuffer)(nil)
