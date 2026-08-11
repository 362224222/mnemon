package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type nestedSettingsHost struct {
	name       string
	writeSkill func(string) (string, error)
	writeHook  func(string, string, []byte) (string, error)
	register   func(string) (string, error)
	eject      func(string) []error
}

var nestedSettingsHosts = []nestedSettingsHost{
	{
		name:       "CodeBuddy",
		writeSkill: CodeBuddyWriteSkill,
		writeHook:  CodeBuddyWriteHook,
		register:   CodeBuddyRegisterHooks,
		eject:      CodeBuddyEject,
	},
	{
		name:       "WorkBuddy",
		writeSkill: WorkBuddyWriteSkill,
		writeHook:  WorkBuddyWriteHook,
		register:   WorkBuddyRegisterHooks,
		eject:      WorkBuddyEject,
	},
}

func TestNestedSettingsHostsRegisterHooks(t *testing.T) {
	for _, host := range nestedSettingsHosts {
		t.Run(host.name, func(t *testing.T) {
			dir := t.TempDir()
			settingsPath := filepath.Join(dir, "settings.json")
			if err := os.WriteFile(settingsPath, []byte(`{
  "hooks": {
    "SessionStart": [
      {"hooks": [{"type": "command", "command": "/old/mnemon/prime.sh"}]},
      {"hooks": [{"type": "command", "command": "/keep/custom.sh"}]}
    ],
    "Stop": [
      {"hooks": [{"type": "command", "command": "/old/mnemon/stop.sh"}]}
    ]
  },
  "other": true
}`), 0o644); err != nil {
				t.Fatalf("write settings: %v", err)
			}

			if _, err := host.register(dir); err != nil {
				t.Fatalf("register hooks: %v", err)
			}

			data, err := ReadJSONFile(settingsPath)
			if err != nil {
				t.Fatalf("read settings: %v", err)
			}
			if data["other"] != true {
				t.Fatalf("unrelated setting should be preserved: %#v", data)
			}
			hooks := data["hooks"].(map[string]any)
			sessionStart := hooks["SessionStart"].([]any)
			if len(sessionStart) != 2 {
				t.Fatalf("expected custom hook plus new prime hook: %#v", sessionStart)
			}
			command := sessionStart[1].(map[string]any)["hooks"].([]any)[0].(map[string]any)["command"].(string)
			if !strings.Contains(command, "hooks/mnemon/prime.sh") {
				t.Fatalf("expected new prime hook, got %#v", sessionStart[1])
			}
			if _, ok := hooks["UserPromptSubmit"]; !ok {
				t.Fatalf("user prompt hook should be registered: %#v", hooks)
			}
			stop := hooks["Stop"].([]any)
			if len(stop) != 1 {
				t.Fatalf("expected one stop hook: %#v", stop)
			}
			if _, ok := stop[0].(map[string]any)["loop_limit"]; ok {
				t.Fatalf("nested settings hook schema should not include loop_limit: %#v", stop[0])
			}
		})
	}
}

func TestNestedSettingsHostsEject(t *testing.T) {
	for _, host := range nestedSettingsHosts {
		t.Run(host.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := host.writeSkill(dir); err != nil {
				t.Fatalf("write skill: %v", err)
			}
			if _, err := host.writeHook(dir, "prime.sh", []byte("#!/bin/bash\n")); err != nil {
				t.Fatalf("write hook: %v", err)
			}
			if _, err := host.register(dir); err != nil {
				t.Fatalf("register hooks: %v", err)
			}
			customSkillDir := filepath.Join(dir, "skills", "custom")
			if err := os.MkdirAll(customSkillDir, 0o755); err != nil {
				t.Fatalf("create custom skill: %v", err)
			}
			settingsPath := filepath.Join(dir, "settings.json")
			data, err := ReadJSONFile(settingsPath)
			if err != nil {
				t.Fatalf("read settings: %v", err)
			}
			hooks := data["hooks"].(map[string]any)
			hooks["SessionStart"] = append(hooks["SessionStart"].([]any), map[string]any{
				"hooks": []any{map[string]any{"type": "command", "command": "/keep/custom.sh"}},
			})
			if err := WriteJSONFile(settingsPath, data); err != nil {
				t.Fatalf("write settings: %v", err)
			}

			if errs := host.eject(dir); len(errs) > 0 {
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
			data, err = ReadJSONFile(settingsPath)
			if err != nil {
				t.Fatalf("read settings after eject: %v", err)
			}
			hooks = data["hooks"].(map[string]any)
			sessionStart := hooks["SessionStart"].([]any)
			if len(sessionStart) != 1 || containsMnemon(sessionStart[0]) {
				t.Fatalf("custom hook should be preserved and mnemon removed: %#v", sessionStart)
			}
			if _, ok := hooks["UserPromptSubmit"]; ok {
				t.Fatalf("user prompt hooks should be removed: %#v", hooks)
			}
			if _, ok := hooks["Stop"]; ok {
				t.Fatalf("stop hooks should be removed: %#v", hooks)
			}
		})
	}
}
