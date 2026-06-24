package hostagent

import (
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
		`"systemMessage": "${SYSTEM_MESSAGE}"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("thin hook missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{
		"MEMORY.md",
		"control render",
		"control pull",
		"control observe",
		"teamwork",
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
