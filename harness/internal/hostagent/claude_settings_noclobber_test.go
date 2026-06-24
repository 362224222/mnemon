package hostagent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// patch/unpatch of Claude settings.json must touch ONLY the hooks we projected (identified by their
// command path under hooks/<marker>/), never a user's own hook entry whose command/matcher merely
// mentions the loop name — and must never delete the user's settings.json.
func TestClaudeSettingsPreservesUserHook(t *testing.T) {
	dir := t.TempDir()
	settings := filepath.Join(dir, "settings.json")
	// A user-authored Stop hook whose command names a companion script after the loop, plus a model key.
	userHook := "~/scripts/mnemon-memory-sync.sh"
	initial := `{"model":"opus","hooks":{"Stop":[{"hooks":[{"type":"command","command":"` + userHook + `"}]}]}}`
	if err := os.WriteFile(settings, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := patchClaudeSettings(settings, dir, "mnemon-memory", claudeHookOptions{Nudge: true}); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if raw, _ := os.ReadFile(settings); !strings.Contains(string(raw), userHook) {
		t.Fatalf("install dropped the user's own hook: %s", raw)
	}

	if err := unpatchClaudeSettings(settings, "mnemon-memory"); err != nil {
		t.Fatalf("unpatch: %v", err)
	}
	raw, err := os.ReadFile(settings)
	if err != nil {
		t.Fatalf("uninstall deleted the user's settings.json: %v", err)
	}
	if !strings.Contains(string(raw), userHook) {
		t.Fatalf("uninstall dropped the user's own hook: %s", raw)
	}
	if !strings.Contains(string(raw), "opus") {
		t.Fatalf("uninstall dropped a user setting: %s", raw)
	}
}
