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

func TestVerifyHostProjectionAbsentAcceptsUnrelatedConfigurationReadOnly(t *testing.T) {
	for _, host := range []assets.Host{assets.HostCodex} {
		host := host
		t.Run(string(host), func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			plan, err := prepareProjection(workspace, nodeState, host, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(plan.configPath), 0o755); err != nil {
				t.Fatal(err)
			}
			unrelated := map[string]any{
				"theme": "dark",
				"hooks": map[string]any{
					"Stop": []any{map[string]any{"hooks": []any{map[string]any{
						"command": "user-stop", "statusMessage": "user status", "type": "command",
					}}}},
					plan.registration.Value.Event: []any{map[string]any{"hooks": []any{map[string]any{
						"command": "user-prompt-hook", "statusMessage": "unrelated prompt hook",
						"timeout": float64(7), "type": "command",
					}}}},
				},
			}
			writeTestJSON(t, plan.configPath, unrelated)
			before, err := os.ReadFile(plan.configPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeInfo, err := os.Stat(plan.configPath)
			if err != nil {
				t.Fatal(err)
			}

			for attempt := 0; attempt < 2; attempt++ {
				if err := VerifyHostProjectionAbsent(workspace, nodeState, host, bundle); err != nil {
					t.Fatalf("VerifyHostProjectionAbsent() error = %v", err)
				}
			}
			after, err := os.ReadFile(plan.configPath)
			if err != nil {
				t.Fatal(err)
			}
			afterInfo, err := os.Stat(plan.configPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) || !os.SameFile(beforeInfo, afterInfo) ||
				!beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
				t.Fatal("absence verification rewrote unrelated Host configuration")
			}
			if _, err := os.Lstat(filepath.Join(workspace, ".mnemon", "harness", "integrations")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("absence verification created ownership directories: %v", err)
			}
			if err := VerifyNodeBundle(nodeState, bundle); err != nil {
				t.Fatalf("absence verification changed canonical Node bundle: %v", err)
			}
		})
	}
}

func TestVerifyHostProjectionAbsentRejectsManagedResidueAndUnsafePaths(t *testing.T) {
	tests := []struct {
		name  string
		want  error
		setup func(*testing.T, string, string, assets.Bundle, projectionPlan)
	}{
		{name: "ownership journal", want: ErrProjectionConflict,
			setup: func(t *testing.T, workspace, nodeState string, bundle assets.Bundle, _ projectionPlan) {
				if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "partial eject", want: ErrProjectionConflict,
			setup: func(t *testing.T, workspace, nodeState string, bundle assets.Bundle, plan projectionPlan) {
				receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(receipt.OwnershipPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(plan.files[0].path); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "user drift at managed path", want: ErrProjectionConflict,
			setup: func(t *testing.T, _, _ string, _ assets.Bundle, plan projectionPlan) {
				if err := os.MkdirAll(filepath.Dir(plan.files[0].path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(plan.files[0].path, []byte("user-owned drift\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "exact registration", want: ErrProjectionConflict,
			setup: func(t *testing.T, _, _ string, _ assets.Bundle, plan projectionPlan) {
				writeResidualRegistration(t, plan, plan.entry)
			}},
		{name: "managed command registration", want: ErrProjectionConflict,
			setup: func(t *testing.T, _, _ string, _ assets.Bundle, plan projectionPlan) {
				writeResidualRegistration(t, plan, map[string]any{"hooks": []any{map[string]any{
					"command": plan.entry.Hooks[0].Command, "statusMessage": "changed status",
					"timeout": float64(77), "type": "command",
				}}})
			}},
		{name: "managed status registration", want: ErrProjectionConflict,
			setup: func(t *testing.T, _, _ string, _ assets.Bundle, plan projectionPlan) {
				writeResidualRegistration(t, plan, map[string]any{"hooks": []any{map[string]any{
					"command": "user-command", "statusMessage": plan.entry.Hooks[0].StatusMessage,
					"timeout": float64(77), "type": "command",
				}}})
			}},
		{name: "Host directory symlink", want: ErrUnsafeProjection,
			setup: func(t *testing.T, workspace, _ string, _ assets.Bundle, _ projectionPlan) {
				if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".codex")); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "Skill directory symlink", want: ErrUnsafeProjection,
			setup: func(t *testing.T, workspace, _ string, _ assets.Bundle, _ projectionPlan) {
				if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".agents")); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "ownership directory symlink", want: ErrUnsafeProjection,
			setup: func(t *testing.T, workspace, _ string, _ assets.Bundle, _ projectionPlan) {
				path := filepath.Join(workspace, ".mnemon", "harness", "integrations")
				if err := os.Symlink(t.TempDir(), path); err != nil {
					t.Fatal(err)
				}
			}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			plan, err := prepareProjection(workspace, nodeState, assets.HostCodex, bundle)
			if err != nil {
				t.Fatal(err)
			}
			test.setup(t, workspace, nodeState, bundle, plan)
			if err := VerifyHostProjectionAbsent(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, test.want) {
				t.Fatalf("VerifyHostProjectionAbsent() error = %v, want %v", err, test.want)
			}
		})
	}
}

func writeResidualRegistration(t *testing.T, plan projectionPlan, entry any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(plan.configPath), 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestJSON(t, plan.configPath, map[string]any{"hooks": map[string]any{
		plan.registration.Value.Event: []any{entry},
	}})
}

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
		filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md"),
		filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md"),
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
	installed, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	receipt, err := EjectHostProjection(workspace, nodeState, assets.HostCodex, bundle)
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
			path := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
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
		{name: "registration managed status only", tamper: func(t *testing.T, workspace string,
			_ assets.Bundle,
		) {
			path := hostConfigPath(workspace, assets.HostCodex)
			document := readTestJSON(t, path)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["command"] = "user-changed-command"
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
