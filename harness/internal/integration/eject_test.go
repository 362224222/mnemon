package integration

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestEjectHostProjectionRemovesOnlyManagedAssetsAndReplays(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	configPath := hostConfigPath(workspace, assets.HostCodex)
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatal(err)
	}
	userEntry := map[string]any{"hooks": []any{map[string]any{"command": "user-hook",
		"timeout": float64(9), "type": "command"}}}
	original := map[string]any{"theme": "dark", "hooks": map[string]any{
		"Stop": []any{userEntry}, "UserPromptSubmit": []any{userEntry}}}
	writeTestJSON(t, configPath, original)
	installed, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := EjectHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil || receipt.Replayed || receipt.RemovedFiles != 3 ||
		!receipt.RegistrationRemoved || receipt.Revision != bundle.Manifest().AssetRevision {
		t.Fatalf("EjectHostProjection() = (%#v, %v)", receipt, err)
	}
	for _, path := range []string{
		filepath.Join(workspace, ".codex", "skills", "mnemon-harness", "SKILL.md"),
		filepath.Join(workspace, ".codex", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md"),
		filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"),
		installed.OwnershipPath,
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("managed path %s remains: %v", path, err)
		}
	}
	document := readTestJSON(t, configPath)
	if document["theme"] != "dark" {
		t.Fatalf("adjacent config changed: %#v", document)
	}
	hooks := document["hooks"].(map[string]any)
	if !reflect.DeepEqual(hooks["Stop"], original["hooks"].(map[string]any)["Stop"]) ||
		!reflect.DeepEqual(hooks["UserPromptSubmit"], []any{userEntry}) {
		t.Fatalf("adjacent hooks changed: %#v", hooks)
	}
	if err := VerifyNodeBundle(nodeState, bundle); err != nil {
		t.Fatalf("eject removed immutable Node bundle: %v", err)
	}
	replay, err := EjectHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil || !replay.Replayed || replay.RemovedFiles != 0 || replay.RegistrationRemoved {
		t.Fatalf("replayed EjectHostProjection() = (%#v, %v)", replay, err)
	}
}

func TestEjectHostProjectionConvergesMissingManagedEntries(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	installed, err := InstallHostProjection(workspace, nodeState, assets.HostClaudeCode, bundle)
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(workspace, ".claude", "skills", "mnemon-harness", "SKILL.md")
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	receipt, err := EjectHostProjection(workspace, nodeState, assets.HostClaudeCode, bundle)
	if err != nil || receipt.RemovedFiles != 2 || !receipt.RegistrationRemoved {
		t.Fatalf("partial EjectHostProjection() = (%#v, %v)", receipt, err)
	}
	if _, err := os.Lstat(installed.OwnershipPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ownership remains after converged eject: %v", err)
	}
}

func TestEjectHostProjectionPreservesDriftWithoutPartialRemoval(t *testing.T) {
	for _, test := range []struct {
		name   string
		tamper func(*testing.T, string, assets.Bundle)
	}{
		{name: "file", tamper: func(t *testing.T, workspace string, _ assets.Bundle) {
			path := filepath.Join(workspace, ".codex", "skills", "mnemon-harness", "SKILL.md")
			if err := os.WriteFile(path, []byte("user-modified\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "registration", tamper: func(t *testing.T, workspace string, _ assets.Bundle) {
			path := hostConfigPath(workspace, assets.HostCodex)
			document := readTestJSON(t, path)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["timeout"] = float64(77)
			writeTestJSON(t, path, document)
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			installed, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
			if err != nil {
				t.Fatal(err)
			}
			beforeHook, err := os.ReadFile(filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"))
			if err != nil {
				t.Fatal(err)
			}
			test.tamper(t, workspace, bundle)
			if _, err := EjectHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("EjectHostProjection() error = %v", err)
			}
			afterHook, err := os.ReadFile(filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"))
			if err != nil || !bytes.Equal(beforeHook, afterHook) {
				t.Fatalf("failed eject removed exact Hook: %v", err)
			}
			if _, err := os.Lstat(installed.OwnershipPath); err != nil {
				t.Fatalf("failed eject removed ownership: %v", err)
			}
		})
	}
}
