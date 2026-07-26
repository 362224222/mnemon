package integration

import (
	"bytes"
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

func TestVerifyHostActivationProvesExactTrustedCodexHookReadOnly(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	response := validCodexHooksResponse(t, workspace, bundle)
	executable, capture := writeActivationHost(t, workspace, response, "")
	observation := HostObservation{Host: assets.HostCodex, Executable: executable,
		Version: "codex activation-test"}
	if err := VerifyHostActivation(context.Background(), workspace, nodeState,
		observation, bundle); err != nil {
		t.Fatalf("VerifyHostActivation() error = %v", err)
	}
	raw, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 4 || !strings.Contains(lines[0], `"method":"initialize"`) ||
		!strings.Contains(lines[0], `"experimentalApi":true`) ||
		lines[1] != `{"method":"initialized"}` ||
		!strings.Contains(lines[2], `"method":"hooks/list"`) ||
		!strings.Contains(lines[2], workspace) ||
		!strings.Contains(lines[3], `"method":"skills/list"`) ||
		!strings.Contains(lines[3], `"forceReload":true`) {
		t.Fatalf("Host protocol requests = %q", raw)
	}
}

func TestVerifyHostActivationRequiresExactDiscoveredCodexSkill(t *testing.T) {
	for _, mode := range []string{"skill-absent", "skill-error", "skill-disabled",
		"skill-wrong-name", "skill-wrong-path", "skill-wrong-scope", "skill-empty-description",
		"skill-duplicate"} {
		t.Run(mode, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			executable, _ := writeActivationHost(t, workspace,
				validCodexHooksResponse(t, workspace, bundle), mode)
			err := VerifyHostActivation(context.Background(), workspace, nodeState,
				HostObservation{Host: assets.HostCodex, Executable: executable,
					Version: "codex activation-test"}, bundle)
			if !errors.Is(err, ErrHostActivationRequired) {
				t.Fatalf("VerifyHostActivation() error = %v", err)
			}
		})
	}
}

func TestVerifyHostActivationRequiresOneEnabledTrustedExactHook(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*codexHooksListResponse)
	}{
		{name: "no cwd entry", mutate: func(value *codexHooksListResponse) { value.Data = nil }},
		{name: "wrong cwd", mutate: func(value *codexHooksListResponse) { value.Data[0].CWD += "-other" }},
		{name: "config error", mutate: func(value *codexHooksListResponse) {
			value.Data[0].Errors = []codexHookError{{Message: "invalid", Path: value.Data[0].Hooks[0].SourcePath}}
		}},
		{name: "absent", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks = nil }},
		{name: "duplicate", mutate: func(value *codexHooksListResponse) {
			value.Data[0].Hooks = append(value.Data[0].Hooks, value.Data[0].Hooks[0])
		}},
		{name: "disabled", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].Enabled = false }},
		{name: "untrusted", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].TrustStatus = "untrusted" }},
		{name: "modified", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].TrustStatus = "modified" }},
		{name: "wrong command", mutate: func(value *codexHooksListResponse) {
			changed := *value.Data[0].Hooks[0].Command + "-changed"
			value.Data[0].Hooks[0].Command = &changed
		}},
		{name: "wrong status", mutate: func(value *codexHooksListResponse) {
			changed := *value.Data[0].Hooks[0].StatusMessage + " changed"
			value.Data[0].Hooks[0].StatusMessage = &changed
		}},
		{name: "wrong source", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].Source = "user" }},
		{name: "wrong event", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].EventName = "stop" }},
		{name: "wrong handler", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].HandlerType = "prompt" }},
		{name: "wrong timeout", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].TimeoutSec++ }},
		{name: "managed contradiction", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].IsManaged = true }},
		{name: "empty hash", mutate: func(value *codexHooksListResponse) { value.Data[0].Hooks[0].CurrentHash = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			response := validCodexHooksResponse(t, workspace, bundle)
			test.mutate(&response)
			executable, _ := writeActivationHost(t, workspace, response, "")
			err := VerifyHostActivation(context.Background(), workspace, nodeState,
				HostObservation{Host: assets.HostCodex, Executable: executable,
					Version: "codex activation-test"}, bundle)
			if !errors.Is(err, ErrHostActivationRequired) {
				t.Fatalf("VerifyHostActivation() error = %v", err)
			}
		})
	}
}

func TestVerifyHostActivationFailsClosedForProtocolAndUnsupportedHost(t *testing.T) {
	t.Run("unsupported Host", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		err := VerifyHostActivation(context.Background(), workspace, nodeState,
			HostObservation{Host: assets.Host("unsupported")}, bundle)
		if !errors.Is(err, ErrHostUnavailable) || errors.Is(err, ErrHostActivationRequired) {
			t.Fatalf("unsupported VerifyHostActivation() error = %v", err)
		}
	})

	tests := []struct {
		name       string
		mode       string
		wantCancel bool
	}{
		{name: "malformed", mode: "malformed"},
		{name: "wrong response id", mode: "wrong-id"},
		{name: "rpc error", mode: "rpc-error"},
		{name: "oversized", mode: "oversized"},
		{name: "nonzero exit", mode: "nonzero"},
		{name: "timeout", mode: "timeout", wantCancel: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			executable, _ := writeActivationHost(t, workspace,
				validCodexHooksResponse(t, workspace, bundle), test.mode)
			ctx := context.Background()
			if test.wantCancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
			}
			err := VerifyHostActivation(ctx, workspace, nodeState,
				HostObservation{Host: assets.HostCodex, Executable: executable,
					Version: "codex activation-test"}, bundle)
			if !errors.Is(err, ErrHostUnavailable) || errors.Is(err, ErrHostActivationRequired) {
				t.Fatalf("VerifyHostActivation() error = %v", err)
			}
		})
	}
}

func TestVerifyHostActivationReverifiesProjectionAfterObservation(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
	response := validCodexHooksResponse(t, workspace, bundle)
	executable, _ := writeActivationHost(t, workspace, response, "drift:"+hook)
	err := VerifyHostActivation(context.Background(), workspace, nodeState,
		HostObservation{Host: assets.HostCodex, Executable: executable,
			Version: "codex activation-test"}, bundle)
	if !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("VerifyHostActivation() error = %v", err)
	}
}

func validCodexHooksResponse(t *testing.T, workspace string,
	bundle assets.Bundle,
) codexHooksListResponse {
	t.Helper()
	registration, ok := bundle.Registration(assets.HostCodex)
	if !ok {
		t.Fatal("Codex registration is absent")
	}
	command := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
	status := registration.Value.Hook.StatusMessage
	return codexHooksListResponse{Data: []codexHooksListEntry{{CWD: workspace,
		Errors: []codexHookError{}, Warnings: []string{"unrelated bounded warning"},
		Hooks: []codexHookMetadata{{Command: &command, CurrentHash: "sha256:activation-test",
			DisplayOrder: 1, Enabled: true, EventName: "userPromptSubmit",
			HandlerType: "command", IsManaged: false, Key: "project:userPromptSubmit:1",
			Source: "project", SourcePath: filepath.Join(workspace, ".codex", registration.Target),
			StatusMessage: &status, TimeoutSec: uint64(registration.Value.Hook.Timeout),
			TrustStatus: "trusted"}},
	}}}
}

func writeActivationHost(t *testing.T, workspace string, response codexHooksListResponse,
	mode string,
) (string, string) {
	t.Helper()
	root := t.TempDir()
	executable := filepath.Join(root, "codex")
	capture := filepath.Join(root, "requests.jsonl")
	result, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	initialize := `{"id":1,"result":{"codexHome":"/bounded","platformFamily":"unix","platformOs":"test","userAgent":"activation-test"}}`
	list := `{"id":2,"result":` + string(result) + `}`
	skills := validCodexSkillsResponse(workspace)
	switch mode {
	case "skill-absent":
		skills.Data[0].Skills = nil
	case "skill-error":
		skills.Data[0].Errors = []codexSkillError{{Message: "invalid Skill", Path: skills.Data[0].Skills[0].Path}}
	case "skill-disabled":
		skills.Data[0].Skills[0].Enabled = false
	case "skill-wrong-name":
		skills.Data[0].Skills[0].Name = "other"
	case "skill-wrong-path":
		skills.Data[0].Skills[0].Path += "-other"
	case "skill-wrong-scope":
		skills.Data[0].Skills[0].Scope = "user"
	case "skill-empty-description":
		skills.Data[0].Skills[0].Description = " "
	case "skill-duplicate":
		skills.Data[0].Skills = append(skills.Data[0].Skills, skills.Data[0].Skills[0])
	}
	skillsRaw, err := json.Marshal(skills)
	if err != nil {
		t.Fatal(err)
	}
	skillsList := `{"id":3,"result":` + string(skillsRaw) + `}`
	switch mode {
	case "malformed":
		list = `{not-json}`
	case "wrong-id":
		list = `{"id":7,"result":` + string(result) + `}`
	case "rpc-error":
		list = `{"error":{"code":-1,"message":"failed"},"id":2}`
	case "oversized":
		list = `{"id":2,"result":{"padding":"` + strings.Repeat("x", hostActivationOutputMax) + `"}}`
	}
	script := "#!/bin/sh\nset -eu\n" +
		"test \"$#\" -eq 2\ntest \"$1\" = app-server\ntest \"$2\" = --stdio\n" +
		"IFS= read -r first\nprintf '%s\\n' \"$first\" >> " + shellActivationQuote(capture) + "\n" +
		"printf '%s\\n' " + shellActivationQuote(initialize) + "\n" +
		"IFS= read -r second\nprintf '%s\\n' \"$second\" >> " + shellActivationQuote(capture) + "\n" +
		"IFS= read -r third\nprintf '%s\\n' \"$third\" >> " + shellActivationQuote(capture) + "\n" +
		"printf '%s\\n' '{\"method\":\"activation/test\",\"params\":{}}'\n"
	switch {
	case mode == "timeout":
		script += "while :; do :; done\n"
	case mode == "grandchild-timeout":
		script += "(trap '' TERM INT; while :; do :; done) &\n"
		script += "child=$!\nprintf '%s\\n' \"$child\" > " + shellActivationQuote(capture+".child") + "\n"
		script += "while :; do :; done\n"
	case mode == "nonzero":
		script += "exit 9\n"
	default:
		script += "printf '%s\\n' " + shellActivationQuote(list) + "\n"
		script += "IFS= read -r fourth\nprintf '%s\\n' \"$fourth\" >> " + shellActivationQuote(capture) + "\n"
		if strings.HasPrefix(mode, "drift:") {
			script += "printf '%s\\n' drift > " + shellActivationQuote(strings.TrimPrefix(mode, "drift:")) + "\n"
		}
		script += "printf '%s\\n' " + shellActivationQuote(skillsList) + "\n"
	}
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executable, 0o700); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	_ = workspace
	return physical, capture
}

func validCodexSkillsResponse(workspace string) codexSkillsListResponse {
	return codexSkillsListResponse{Data: []codexSkillsListEntry{{CWD: workspace,
		Errors: []codexSkillError{}, Skills: []codexSkillMetadata{{
			Dependencies: json.RawMessage("null"),
			Description:  "Process one mnemond-managed Teamwork event.",
			Enabled:      true,
			Interface:    json.RawMessage("null"),
			Name:         "mnemon-harness",
			Path: filepath.Join(workspace, ".agents", "skills", "mnemon-harness",
				"SKILL.md"),
			Scope: "repo",
		}},
	}}}
}

func shellActivationQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestHostActivationEnvironmentIsClosed(t *testing.T) {
	environment := hostActivationEnvironment([]string{
		"HOME=/home/test", "CODEX_HOME=/home/test/.codex", "PATH=/bin", "LANG=C",
		"LC_ALL=C", "OPENAI_API_KEY=secret", "MNEMON_HARNESS_RUN_ATTACHMENT=secret",
	})
	joined := strings.Join(environment, "\n")
	if !strings.Contains(joined, "CODEX_HOME=") || !strings.Contains(joined, "LC_ALL=") ||
		strings.Contains(joined, "OPENAI_API_KEY") || strings.Contains(joined, "ATTACHMENT") {
		t.Fatalf("closed Host activation environment = %q", joined)
	}
	for _, entry := range environment {
		if bytes.Contains([]byte(entry), []byte("secret")) {
			t.Fatalf("secret-bearing environment survived: %q", entry)
		}
	}
}
