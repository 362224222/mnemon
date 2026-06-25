package hostagent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type codexHookOptions struct {
	Remind  bool
	Nudge   bool
	Compact bool
}

func patchCodexHooks(hooksPath, configDir, marker string, opts codexHookOptions) error {
	data, err := loadCodexHooks(hooksPath)
	if err != nil {
		return err
	}
	removeCodexHooks(data, marker)
	hooksDir := pathJoin(configDir, "hooks", marker)
	addCodexHook(data, "SessionStart", pathJoin(hooksDir, "prime.sh"))
	if opts.Remind {
		addCodexHook(data, "UserPromptSubmit", pathJoin(hooksDir, "remind.sh"))
	}
	if opts.Nudge {
		addCodexHook(data, "Stop", pathJoin(hooksDir, "nudge.sh"))
	}
	if opts.Compact {
		addCodexHook(data, "PreCompact", pathJoin(hooksDir, "compact.sh"))
	}
	return writeCodexHooks(hooksPath, data)
}

func unpatchCodexHooks(hooksPath, marker string) error {
	if _, err := os.Stat(hooksPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat Codex hooks %s: %w", hooksPath, err)
	}
	data, err := loadCodexHooks(hooksPath)
	if err != nil {
		return err
	}
	removeCodexHooks(data, marker)
	return writeCodexHooks(hooksPath, data)
}

func loadCodexHooks(hooksPath string) (map[string]any, error) {
	content, err := os.ReadFile(hooksPath)
	if os.IsNotExist(err) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Codex hooks %s: %w", hooksPath, err)
	}
	if len(strings.TrimSpace(string(content))) == 0 {
		return map[string]any{}, nil
	}
	var data map[string]any
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("parse Codex hooks %s: %w", hooksPath, err)
	}
	if data == nil {
		data = map[string]any{}
	}
	return data, nil
}

func writeCodexHooks(hooksPath string, data map[string]any) error {
	if _, ok := data["hooks"]; !ok {
		data["hooks"] = map[string]any{}
	}
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Codex hooks: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(hooksPath), 0o755); err != nil {
		return fmt.Errorf("mkdir Codex hooks dir: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(hooksPath, content, 0o644); err != nil {
		return fmt.Errorf("write Codex hooks %s: %w", hooksPath, err)
	}
	return nil
}

func removeCodexHooks(data map[string]any, marker string) {
	hooks, ok := data["hooks"].(map[string]any)
	if !ok {
		return
	}
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "Stop", "PreCompact"} {
		rawEntries, ok := hooks[event].([]any)
		if !ok {
			continue
		}
		kept := rawEntries[:0]
		for _, entry := range rawEntries {
			if !entryUsesHookPath(entry, marker) {
				kept = append(kept, entry)
			}
		}
		if len(kept) == 0 {
			delete(hooks, event)
		} else {
			hooks[event] = kept
		}
	}
	if len(hooks) == 0 {
		data["hooks"] = map[string]any{}
	}
}

// entryUsesHookPath reports whether a settings hook entry is one WE projected — matched by its command
// path under hooks/<marker>/, NOT by a loose substring on the marker (which would drop a user's own
// hook that merely names the loop). Shared by both hosts: the entry shape ({hooks:[{command}]}) is the
// same for codex hooks.json and claude settings.json.
func entryUsesHookPath(value any, marker string) bool {
	entry, ok := value.(map[string]any)
	if !ok {
		return false
	}
	rawHandlers, ok := entry["hooks"].([]any)
	if !ok {
		return false
	}
	for _, rawHandler := range rawHandlers {
		handler, ok := rawHandler.(map[string]any)
		if !ok {
			continue
		}
		command, ok := handler["command"].(string)
		if !ok {
			continue
		}
		if commandUsesHookPath(command, marker) {
			return true
		}
	}
	return false
}

func commandUsesHookPath(command, marker string) bool {
	unixNeedle := "/hooks/" + marker + "/"
	windowsNeedle := `\hooks\` + marker + `\`
	return strings.Contains(command, unixNeedle) ||
		strings.Contains(command, windowsNeedle) ||
		strings.HasPrefix(command, "hooks/"+marker+"/")
}

func addCodexHook(data map[string]any, event, command string) {
	hooks, ok := data["hooks"].(map[string]any)
	if !ok {
		hooks = map[string]any{}
		data["hooks"] = hooks
	}
	entries, ok := hooks[event].([]any)
	if !ok {
		entries = []any{}
	}
	entries = append(entries, map[string]any{
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
			},
		},
	})
	hooks[event] = entries
}
