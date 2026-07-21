package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestVerifyClaudeHostActivationObservesProjectSkillWithoutProviderTurn(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostClaudeCode, bundle); err != nil {
		t.Fatal(err)
	}
	executable, capture := writeClaudeActivationHost(t, workspace, "")
	observation := HostObservation{Host: assets.HostClaudeCode, Executable: executable,
		Version: "2.1.215 (Claude Code)"}
	if err := VerifyHostActivation(context.Background(), workspace, nodeState,
		observation, bundle); err != nil {
		t.Fatalf("VerifyHostActivation() error = %v", err)
	}
	raw, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	stream := "-p|/hooks|--output-format|stream-json|--verbose|--tools||" +
		"--no-session-persistence|--setting-sources|project"
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 2 || !containsClaudeActivationLine(lines, "doctor") ||
		!containsClaudeActivationLine(lines, stream) {
		t.Fatalf("Claude activation commands = %q", raw)
	}
}

func containsClaudeActivationLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

func TestVerifyClaudeHostActivationFailsClosedForSettingsSkillAndProviderDrift(t *testing.T) {
	tests := []struct {
		mode string
		want error
	}{
		{mode: "invalid-settings", want: ErrHostActivationRequired},
		{mode: "missing-skill", want: ErrHostActivationRequired},
		{mode: "duplicate-skill", want: ErrHostActivationRequired},
		{mode: "provider-turn", want: ErrHostActivationRequired},
		{mode: "stderr", want: ErrHostActivationRequired},
		{mode: "malformed", want: ErrHostActivationRequired},
		{mode: "duplicate-field", want: ErrHostActivationRequired},
		{mode: "drift", want: ErrProjectionConflict},
	}
	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState,
				assets.HostClaudeCode, bundle); err != nil {
				t.Fatal(err)
			}
			executable, _ := writeClaudeActivationHost(t, workspace, test.mode)
			err := VerifyHostActivation(context.Background(), workspace, nodeState,
				HostObservation{Host: assets.HostClaudeCode, Executable: executable,
					Version: "2.1.215 (Claude Code)"}, bundle)
			if !errors.Is(err, test.want) {
				t.Fatalf("VerifyHostActivation() error = %v, want category %v", err, test.want)
			}
		})
	}
}

func TestVerifyClaudeHostActivationCancelsBoundedProbe(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostClaudeCode, bundle); err != nil {
		t.Fatal(err)
	}
	executable, _ := writeClaudeActivationHost(t, workspace, "timeout")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := VerifyHostActivation(ctx, workspace, nodeState,
		HostObservation{Host: assets.HostClaudeCode, Executable: executable,
			Version: "2.1.215 (Claude Code)"}, bundle)
	if !errors.Is(err, ErrHostUnavailable) || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("VerifyHostActivation(timeout) error = %v", err)
	}
}

func TestClaudeActivationClosedDecodersRejectIncompleteAuthority(t *testing.T) {
	validInit := claudeActivationInitJSON(t, "/workspace", []string{claudeActivationSkill})
	validResult := claudeActivationResultJSON(t, "session-test", false)
	for _, raw := range [][]byte{
		nil,
		[]byte("{}\n"),
		append(append([]byte(nil), validInit...), validInit...),
		append(append([]byte(nil), validResult...), validResult...),
		[]byte("{\"type\":\"system\",\"type\":\"system\",\"subtype\":\"init\"}\n"),
	} {
		if err := validateClaudeActivationStream(raw, "/workspace",
			"2.1.215 (Claude Code)"); !errors.Is(err, ErrHostActivationRequired) {
			t.Fatalf("validateClaudeActivationStream(%q) error = %v", raw, err)
		}
	}
}

func writeClaudeActivationHost(t *testing.T, workspace, mode string) (string, string) {
	t.Helper()
	root := t.TempDir()
	executable := filepath.Join(root, "claude")
	capture := filepath.Join(root, "commands")
	init := strings.TrimSuffix(string(claudeActivationInitJSON(t, workspace,
		claudeActivationSkills(mode))), "\n")
	result := strings.TrimSuffix(string(claudeActivationResultJSON(t, "session-test",
		mode == "provider-turn")), "\n")
	if mode == "malformed" {
		init = "{not-json}"
	}
	if mode == "duplicate-field" {
		init = `{"type":"system","type":"system","subtype":"init"}`
	}
	doctor := "Claude Code doctor\\nNo installation issues found."
	if mode == "invalid-settings" {
		doctor = "Invalid settings\\n- " + filepath.Join(workspace, ".claude", "settings.json") +
			" › hooks: invalid"
	}
	script := "#!/bin/sh\nset -eu\n" +
		"if [ \"${1:-}\" = doctor ]; then\n" +
		"  printf '%s\\n' doctor >> " + shellActivationQuote(capture) + "\n" +
		"  printf '%b\\n' " + shellActivationQuote(doctor) + "\n" +
		"  exit 0\n" +
		"fi\n" +
		"printf '%s\\n' \"$(printf '%s|' \"$@\" | sed 's/|$//')\" >> " +
		shellActivationQuote(capture) + "\n"
	if mode == "timeout" {
		script += "while :; do :; done\n"
	} else {
		script += "printf '%s\\n' " + shellActivationQuote(init) + "\n" +
			"printf '%s\\n' " + shellActivationQuote(result) + "\n"
	}
	if mode == "stderr" {
		script += "printf '%s\\n' diagnostic >&2\n"
	}
	if mode == "drift" {
		hook := filepath.Join(workspace, ".claude", "hooks", "mnemon-harness", "hook.sh")
		script += "printf '%s\\n' drift > " + shellActivationQuote(hook) + "\n"
	}
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	return physical, capture
}

func claudeActivationSkills(mode string) []string {
	skills := []string{claudeActivationSkill, "adjacent-user-skill"}
	switch mode {
	case "missing-skill":
		return skills[1:]
	case "duplicate-skill":
		return append(skills, claudeActivationSkill)
	default:
		return skills
	}
}

func claudeActivationInitJSON(t *testing.T, workspace string, skills []string) []byte {
	t.Helper()
	value := map[string]any{
		"type": "system", "subtype": "init", "cwd": workspace,
		"session_id": "session-test", "tools": []string{}, "mcp_servers": []any{},
		"claude_code_version": "2.1.215", "skills": skills,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func claudeActivationResultJSON(t *testing.T, session string, providerTurn bool) []byte {
	t.Helper()
	duration, turns, tokens := 0, 0, 0
	if providerTurn {
		duration, turns, tokens = 1, 1, 1
	}
	value := map[string]any{
		"type": "result", "subtype": "success", "is_error": false,
		"duration_api_ms": duration, "num_turns": turns, "result": "synthetic",
		"session_id": session, "total_cost_usd": 0,
		"usage": map[string]any{"input_tokens": tokens, "cache_creation_input_tokens": 0,
			"cache_read_input_tokens": 0, "output_tokens": 0},
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}
