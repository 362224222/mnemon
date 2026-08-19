package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestZCodeRegisterHooksPreservesUnrelatedConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "cli", "config.json")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(configPath, []byte(`{
  "theme": "dark",
  "hooks": {
    "enabled": false,
    "timeoutMs": 15000,
    "events": {
      "SessionStart": [
        {"hooks": [{"type": "process", "command": "bash", "args": ["/old/mnemon/prime.sh"]}]},
        {"hooks": [{"type": "process", "command": "node", "args": ["/keep/custom.mjs"]}]}
      ],
      "PreToolUse": [
        {"matcher": "Write", "hooks": [{"type": "process", "command": "node", "args": ["/keep/check.mjs"]}]}
      ]
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := ZCodeRegisterHooks(dir); err != nil {
		t.Fatalf("register hooks: %v", err)
	}

	data, err := ReadJSONFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if data["theme"] != "dark" {
		t.Fatalf("unrelated root config should be preserved: %#v", data)
	}
	hooks := data["hooks"].(map[string]interface{})
	if hooks["enabled"] != true || hooks["timeoutMs"].(float64) != 15000 {
		t.Fatalf("hooks settings not preserved/enabled: %#v", hooks)
	}
	events := hooks["events"].(map[string]interface{})
	if _, ok := events["PreToolUse"]; !ok {
		t.Fatalf("custom event should be preserved: %#v", events)
	}
	sessionStart := events["SessionStart"].([]interface{})
	if len(sessionStart) != 2 {
		t.Fatalf("expected custom hook plus new prime hook: %#v", sessionStart)
	}
	newHook := sessionStart[1].(map[string]interface{})["hooks"].([]interface{})[0].(map[string]interface{})
	args := newHook["args"].([]interface{})
	if newHook["type"] != "process" || newHook["command"] != "bash" || len(args) != 1 || !strings.Contains(args[0].(string), "hooks/mnemon/prime.sh") {
		t.Fatalf("unexpected ZCode process hook: %#v", newHook)
	}
	for _, event := range []string{"UserPromptSubmit", "Stop"} {
		if _, ok := events[event]; !ok {
			t.Fatalf("missing %s hook: %#v", event, events)
		}
	}
}

func TestZCodeEjectRemovesOnlyMnemonFilesAndHooks(t *testing.T) {
	dir := t.TempDir()
	if _, err := ZCodeWriteSkill(dir); err != nil {
		t.Fatalf("write skill: %v", err)
	}
	if _, err := ZCodeWriteHook(dir, "prime.sh", []byte("#!/bin/bash\n")); err != nil {
		t.Fatalf("write hook: %v", err)
	}
	if _, err := ZCodeRegisterHooks(dir); err != nil {
		t.Fatalf("register hooks: %v", err)
	}

	customSkillDir := filepath.Join(dir, "skills", "custom")
	if err := os.MkdirAll(customSkillDir, 0o755); err != nil {
		t.Fatalf("create custom skill: %v", err)
	}
	configPath := filepath.Join(dir, "cli", "config.json")
	data, err := ReadJSONFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	hooks := data["hooks"].(map[string]interface{})
	events := hooks["events"].(map[string]interface{})
	events["SessionStart"] = append(events["SessionStart"].([]interface{}), map[string]interface{}{
		"hooks": []interface{}{map[string]interface{}{
			"type": "process", "command": "node", "args": []interface{}{`/keep/custom.mjs`},
		}},
	})
	if err := WriteJSONFile(configPath, data); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if errs := ZCodeEject(dir); len(errs) > 0 {
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

	data, err = ReadJSONFile(configPath)
	if err != nil {
		t.Fatalf("read config after eject: %v", err)
	}
	hooks = data["hooks"].(map[string]interface{})
	events = hooks["events"].(map[string]interface{})
	sessionStart := events["SessionStart"].([]interface{})
	if len(sessionStart) != 1 || containsMnemon(sessionStart[0]) {
		t.Fatalf("custom hook should be preserved and mnemon removed: %#v", sessionStart)
	}
	if _, ok := events["UserPromptSubmit"]; ok {
		t.Fatalf("user prompt hook should be removed: %#v", events)
	}
	if _, ok := events["Stop"]; ok {
		t.Fatalf("stop hook should be removed: %#v", events)
	}
}
