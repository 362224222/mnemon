package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKimiWriteSkill(t *testing.T) {
	dir := t.TempDir()

	skillPath, err := KimiWriteSkill(dir)
	if err != nil {
		t.Fatalf("write skill: %v", err)
	}
	if skillPath != filepath.Join(dir, "skills", "mnemon", "SKILL.md") {
		t.Fatalf("skill path = %q", skillPath)
	}
	data, err := os.ReadFile(skillPath)
	if err != nil {
		t.Fatalf("read skill: %v", err)
	}
	if !strings.Contains(string(data), "Kimi Code") {
		t.Fatalf("kimi skill should mention Kimi Code: %s", string(data))
	}
}

func TestKimiWriteHook(t *testing.T) {
	dir := t.TempDir()

	hookPath, err := KimiWriteHook(dir, "prime.sh", []byte("#!/bin/bash\n"))
	if err != nil {
		t.Fatalf("write hook: %v", err)
	}
	if hookPath != filepath.Join(dir, "hooks", "mnemon", "prime.sh") {
		t.Fatalf("hook path = %q", hookPath)
	}
	info, err := os.Stat(hookPath)
	if err != nil {
		t.Fatalf("stat hook: %v", err)
	}
	if info.Mode().Perm() != 0755 {
		t.Fatalf("hook permissions = %v, want 0755", info.Mode().Perm())
	}
}

func TestKimiRegisterHooksPreservesUnrelatedConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(configPath, []byte(`model = "kimi"

[[hooks]]
event = "SessionStart"
command = "/old/mnemon/prime.sh"
timeout = 1

[[hooks]]
event = "Notification"
matcher = "task\\.completed"
command = "/keep/custom.sh"
timeout = 3
`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := KimiRegisterHooks(dir); err != nil {
		t.Fatalf("register hooks: %v", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `model = "kimi"`) {
		t.Fatalf("unrelated setting should be preserved: %s", text)
	}
	if !strings.Contains(text, `command = "/keep/custom.sh"`) {
		t.Fatalf("custom hook should be preserved: %s", text)
	}
	if strings.Contains(text, "/old/mnemon/prime.sh") {
		t.Fatalf("old mnemon hook should be removed: %s", text)
	}
	for _, event := range []string{`event = "SessionStart"`, `event = "UserPromptSubmit"`, `event = "Stop"`} {
		if !strings.Contains(text, event) {
			t.Fatalf("missing %s hook: %s", event, text)
		}
	}
	if strings.Contains(text, "loop_limit") {
		t.Fatalf("kimi hook schema should not include loop_limit: %s", text)
	}
}

func TestKimiEjectRemovesOnlyMnemonFilesAndHooks(t *testing.T) {
	dir := t.TempDir()
	if _, err := KimiWriteSkill(dir); err != nil {
		t.Fatalf("write skill: %v", err)
	}
	if _, err := KimiWriteHook(dir, "prime.sh", []byte("#!/bin/bash\n")); err != nil {
		t.Fatalf("write hook: %v", err)
	}
	if _, err := KimiRegisterHooks(dir); err != nil {
		t.Fatalf("register hooks: %v", err)
	}
	customSkillDir := filepath.Join(dir, "skills", "custom")
	if err := os.MkdirAll(customSkillDir, 0755); err != nil {
		t.Fatalf("create custom skill: %v", err)
	}
	configPath := filepath.Join(dir, "config.toml")
	existing, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	existing = append(existing, []byte(`
[[hooks]]
event = "Notification"
command = "/keep/custom.sh"
timeout = 3
`)...)
	if err := os.WriteFile(configPath, existing, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	errs := KimiEject(dir)
	if len(errs) > 0 {
		t.Fatalf("eject errors: %v", errs)
	}
	if _, err := os.Stat(filepath.Join(dir, "skills", "mnemon")); !os.IsNotExist(err) {
		t.Fatalf("mnemon skill should be removed, err=%v", err)
	}
	if _, err := os.Stat(customSkillDir); err != nil {
		t.Fatalf("custom skill should be preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "hooks", "mnemon")); !os.IsNotExist(err) {
		t.Fatalf("mnemon hooks should be removed, err=%v", err)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config after eject: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "mnemon") {
		t.Fatalf("mnemon hooks should be removed: %s", text)
	}
	if !strings.Contains(text, `command = "/keep/custom.sh"`) {
		t.Fatalf("custom hook should be preserved: %s", text)
	}
}
