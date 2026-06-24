package hostagent

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

func TestRenderThinHookIsStaticRenderShim(t *testing.T) {
	body, err := RenderThinHook(assets.FS, ThinHookOptions{
		Host:         "codex",
		Timing:       "remind",
		RenderIntent: presentation.IntentTeamworkEvents,
	})
	if err != nil {
		t.Fatalf("render thin hook: %v", err)
	}
	for _, want := range []string{
		"control render",
		`--intent "teamwork.events"`,
		`--lifecycle "remind"`,
		`LOCAL_ENV="${PROJECT_ROOT}/.mnemon/harness/local/env.sh"`,
		`TOKEN_ARGS=(--token-file "${TOKEN_PATH}")`,
		"continue only with local context",
		`"systemMessage": "${SYSTEM_MESSAGE}"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("thin hook missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{
		"GUIDE.md",
		"MEMORY.md",
		"control pull",
		"control observe",
		"expected_work",
		"Assignment ",
	} {
		if strings.Contains(body, blocked) {
			t.Fatalf("thin hook must not contain dynamic/per-loop content %q:\n%s", blocked, body)
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
	if !strings.Contains(claude, `printf '%s\n' "${RENDER_BODY}"`) || strings.Contains(claude, `"systemMessage"`) {
		t.Fatalf("claude thin hook must use plain output:\n%s", claude)
	}
}

func TestRenderThinHookRejectsUnknownInputs(t *testing.T) {
	for _, tc := range []ThinHookOptions{
		{Host: "../codex", Timing: "remind", RenderIntent: presentation.IntentTeamworkEvents},
		{Host: "codex", Timing: "boot", RenderIntent: presentation.IntentTeamworkEvents},
		{Host: "codex", Timing: "remind", RenderIntent: "memory.events"},
	} {
		if _, err := RenderThinHook(assets.FS, tc); err == nil {
			t.Fatalf("RenderThinHook(%+v) must fail closed", tc)
		}
	}
}
