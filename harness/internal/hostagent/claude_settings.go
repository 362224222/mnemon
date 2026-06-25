package hostagent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type claudeHookOptions struct {
	Remind  bool
	Nudge   bool
	Compact bool
}

func patchClaudeSettings(settingsPath, configDir, marker string, opts claudeHookOptions) error {
	data, err := loadClaudeSettings(settingsPath)
	if err != nil {
		return err
	}
	removeClaudeHooks(data, marker)
	hooksDir := pathJoin(configDir, "hooks", marker)
	addClaudeHook(data, "SessionStart", pathJoin(hooksDir, "prime.sh"))
	if opts.Remind {
		addClaudeHook(data, "UserPromptSubmit", pathJoin(hooksDir, "remind.sh"))
	}
	if opts.Nudge {
		addClaudeHook(data, "Stop", pathJoin(hooksDir, "nudge.sh"))
	}
	if opts.Compact {
		addClaudeHook(data, "PreCompact", pathJoin(hooksDir, "compact.sh"))
	}
	return writeClaudeSettings(settingsPath, data)
}

func unpatchClaudeSettings(settingsPath, marker string) error {
	data, err := loadClaudeSettings(settingsPath)
	if err != nil {
		return err
	}
	removeClaudeHooks(data, marker)
	if len(data) == 0 {
		if err := os.Remove(settingsPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove Claude settings %s: %w", settingsPath, err)
		}
		return nil
	}
	return writeClaudeSettings(settingsPath, data)
}

func loadClaudeSettings(settingsPath string) (map[string]any, error) {
	content, err := os.ReadFile(settingsPath)
	if os.IsNotExist(err) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Claude settings %s: %w", settingsPath, err)
	}
	if len(strings.TrimSpace(string(content))) == 0 {
		return map[string]any{}, nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(stripJSON5(string(content))), &data); err != nil {
		return nil, fmt.Errorf("parse Claude settings %s: %w", settingsPath, err)
	}
	if data == nil {
		data = map[string]any{}
	}
	return data, nil
}

func writeClaudeSettings(settingsPath string, data map[string]any) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Claude settings: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(settingsPath), 0o755); err != nil {
		return fmt.Errorf("mkdir Claude settings dir: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(settingsPath, content, 0o644); err != nil {
		return fmt.Errorf("write Claude settings %s: %w", settingsPath, err)
	}
	return nil
}

func removeClaudeHooks(data map[string]any, marker string) {
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
		delete(data, "hooks")
	}
}

func addClaudeHook(data map[string]any, event, command string) {
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

func stripJSON5(text string) string {
	var out strings.Builder
	inString := false
	escaped := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if escaped {
			out.WriteByte(ch)
			escaped = false
			continue
		}
		if inString {
			if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inString = false
			}
			out.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inString = true
			out.WriteByte(ch)
			continue
		}
		if ch == '/' && i+1 < len(text) && text[i+1] == '/' {
			for i < len(text) && text[i] != '\n' {
				i++
			}
			continue
		}
		if ch == ',' {
			j := i + 1
			for j < len(text) && (text[j] == ' ' || text[j] == '\t' || text[j] == '\r' || text[j] == '\n') {
				j++
			}
			if j < len(text) && (text[j] == ']' || text[j] == '}') {
				continue
			}
		}
		out.WriteByte(ch)
	}
	return out.String()
}
