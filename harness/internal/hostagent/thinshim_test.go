package hostagent

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestRenderThinHookIsGenericLifecycleShim(t *testing.T) {
	body, err := RenderThinHook(assets.FS, ThinHookOptions{
		Host:   "codex",
		Timing: "remind",
	})
	if err != nil {
		t.Fatalf("render thin hook: %v", err)
	}
	for _, want := range []string{
		`LOCAL_ENV="${PROJECT_ROOT}/.mnemon/harness/local/env.sh"`,
		`GUIDE_PATH="${PROJECT_ROOT}/.mnemon/harness/local/guide.md"`,
		"Evaluate whether governed context should be read before responding.",
		`grep -q '\[mnemon:wake\]'`,
		`--intent teamwork.events --lifecycle remind --surface hook`,
		`"systemMessage": "${SYSTEM_MESSAGE}"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("thin hook missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{
		"MEMORY.md",
		"control pull",
		"control observe",
		"TOKEN_ARGS",
		"assignment",
		"progress_digest",
		"agent_profile",
		"project_intent",
		"teamwork_signal",
		"expected_work",
		"Assignment ",
	} {
		if strings.Contains(body, blocked) {
			t.Fatalf("thin hook must not contain dynamic/per-loop content %q:\n%s", blocked, body)
		}
	}
}

func TestRenderThinHookWakeRendersWithoutTokenFile(t *testing.T) {
	root := t.TempDir()
	hookDir := filepath.Join(root, ".codex", "hooks", "mnemon-r1")
	if err := os.MkdirAll(hookDir, 0o755); err != nil {
		t.Fatalf("mkdir hook dir: %v", err)
	}
	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin dir: %v", err)
	}

	hook, err := RenderStandardThinHook("codex", "remind")
	if err != nil {
		t.Fatalf("render codex remind hook: %v", err)
	}
	hookPath := filepath.Join(hookDir, "remind.sh")
	if err := os.WriteFile(hookPath, []byte(hook), 0o755); err != nil {
		t.Fatalf("write hook: %v", err)
	}

	harnessPath := filepath.Join(binDir, "mnemon-harness")
	stub := `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"--token-file"* ]]; then
  exit 7
fi
if [[ "$*" != *"control render"* || "$*" != *"--intent teamwork.events"* ]]; then
  exit 8
fi
printf '[mnemon:work]\nassignment addressed to this principal\n'
`
	if err := os.WriteFile(harnessPath, []byte(stub), 0o755); err != nil {
		t.Fatalf("write harness stub: %v", err)
	}

	cmd := exec.Command("bash", hookPath)
	cmd.Stdin = strings.NewReader("[mnemon:wake]\n")
	cmd.Env = append(os.Environ(),
		"MNEMON_HARNESS_BIN="+harnessPath,
		"MNEMON_CONTROL_ADDR=http://127.0.0.1:8791",
		"MNEMON_CONTROL_PRINCIPAL=planner@team",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run hook without token file: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "[mnemon:work]") {
		t.Fatalf("wake hook must include rendered governed context, got:\n%s", out)
	}
}

func TestRenderThinHookPrimeLoadsManagedGuide(t *testing.T) {
	body, err := RenderStandardThinHook("codex", "prime")
	if err != nil {
		t.Fatalf("codex prime thin hook: %v", err)
	}
	for _, want := range []string{
		`GUIDE_PATH="${PROJECT_ROOT}/.mnemon/harness/local/guide.md"`,
		"Follow the loaded GUIDE",
		`cat "${GUIDE_PATH}"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("prime hook missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range businessHookTerms() {
		if strings.Contains(body, blocked) {
			t.Fatalf("prime hook source must not contain business term %q:\n%s", blocked, body)
		}
	}
}

func TestRenderThinHookHostDialect(t *testing.T) {
	codex, err := RenderStandardThinHook("codex", "nudge")
	if err != nil {
		t.Fatalf("codex thin hook: %v", err)
	}
	if !strings.Contains(codex, `"systemMessage": "${SYSTEM_MESSAGE}"`) || !strings.Contains(codex, "json_escape") {
		t.Fatalf("codex thin hook must adapt to JSON system-message dialect:\n%s", codex)
	}
	claude, err := RenderStandardThinHook("claude-code", "nudge")
	if err != nil {
		t.Fatalf("claude thin hook: %v", err)
	}
	if !strings.Contains(claude, `printf '%s\n' "${HOOK_BODY}"`) || strings.Contains(claude, `"systemMessage"`) {
		t.Fatalf("claude thin hook must use plain output:\n%s", claude)
	}
}

func TestRenderThinHookRejectsUnknownInputs(t *testing.T) {
	for _, tc := range []ThinHookOptions{
		{Host: "../codex", Timing: "remind"},
		{Host: "codex", Timing: "boot"},
	} {
		if _, err := RenderThinHook(assets.FS, tc); err == nil {
			t.Fatalf("RenderThinHook(%+v) must fail closed", tc)
		}
	}
}

func businessHookTerms() []string {
	return []string{"teamwork", "assignment", "progress_digest", "agent_profile", "project_intent", "teamwork_signal"}
}
