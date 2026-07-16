package integration

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestPreflightHostProjectionUpgradeIsStrictAndReadOnly(t *testing.T) {
	t.Run("success freezes old and new facts without mutation", func(t *testing.T) {
		workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
		beforeConfig := readTestJSON(t, previous.plan.configPath)

		var receipt HostProjectionUpgradePreflight
		assertUpgradePreflightReadOnly(t, workspace, func() error {
			var err error
			receipt, err = PreflightHostProjectionUpgrade(workspace, nodeState,
				assets.HostCodex, previous.manifest.AssetRevision, bundle)
			return err
		})
		want := HostProjectionUpgradePreflight{
			ConfigPath:       previous.plan.configPath,
			Host:             assets.HostCodex,
			OwnershipPath:    previous.plan.ownershipPath,
			PreviousRevision: previous.manifest.AssetRevision,
			Revision:         bundle.Manifest().AssetRevision,
		}
		if receipt != want {
			t.Fatalf("PreflightHostProjectionUpgrade() = %#v, want %#v", receipt, want)
		}
		if afterConfig := readTestJSON(t, previous.plan.configPath); !reflect.DeepEqual(afterConfig, beforeConfig) {
			t.Fatalf("preflight changed adjacent Host configuration: %#v", afterConfig)
		}
	})

	t.Run("active revision mismatch", func(t *testing.T) {
		workspace, nodeState, bundle, _ := newUpgradePreflightFixture(t, assets.HostCodex)
		assertUpgradePreflightConflict(t, workspace, func() error {
			_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.HostCodex,
				digest([]byte("different active revision")), bundle)
			return err
		})
	})

	for _, test := range []struct {
		name   string
		tamper func(*testing.T, syntheticPreviousProjection)
	}{
		{name: "previous file content drift", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			file := previous.plan.files[0]
			if err := os.WriteFile(file.path, []byte("drift\n"), file.mode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "previous file mode drift", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			if err := os.Chmod(previous.plan.files[0].path, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "previous registration drift", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			document := readTestJSON(t, previous.plan.configPath)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["timeout"] = float64(91)
			writeTestJSON(t, previous.plan.configPath, document)
		}},
		{name: "previous file missing", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			if err := os.Remove(previous.plan.files[0].path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "previous registration missing", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			document := readTestJSON(t, previous.plan.configPath)
			delete(document["hooks"].(map[string]any), "UserPromptSubmit")
			writeTestJSON(t, previous.plan.configPath, document)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
			test.tamper(t, previous)
			assertUpgradePreflightConflict(t, workspace, func() error {
				_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.HostCodex,
					previous.manifest.AssetRevision, bundle)
				return err
			})
		})
	}

	for _, test := range []struct {
		name   string
		tamper func(*testing.T, string, syntheticPreviousProjection, assets.Bundle)
	}{
		{name: "ownership missing", tamper: func(t *testing.T, _ string, previous syntheticPreviousProjection, _ assets.Bundle) {
			if err := os.Remove(previous.plan.ownershipPath); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ownership partial", tamper: func(t *testing.T, _ string, previous syntheticPreviousProjection, _ assets.Bundle) {
			if err := os.WriteFile(previous.plan.ownershipPath, []byte("{\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ownership upgrading", tamper: func(t *testing.T, _ string, previous syntheticPreviousProjection, bundle assets.Bundle) {
			interrupted := errors.New("stop after upgrade journal")
			_, err := installHostProjection(previous.plan.workspace,
				filepath.Join(previous.plan.workspace, ".mnemon", "harness", "node"),
				previous.plan.applied.Host, bundle, func(stage string) error {
					if stage == "after_upgrade_journal" {
						return interrupted
					}
					return nil
				})
			if !errors.Is(err, interrupted) {
				t.Fatalf("interrupt upgrade journal: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
			test.tamper(t, workspace, previous, bundle)
			assertUpgradePreflightConflict(t, workspace, func() error {
				_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.HostCodex,
					previous.manifest.AssetRevision, bundle)
				return err
			})
		})
	}

	t.Run("ownership Host mismatch", func(t *testing.T) {
		workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
		manifest := previous.manifest
		manifest.Host = assets.HostClaudeCode
		writeOwnershipManifest(t, previous.plan.ownershipPath, manifest)
		assertUpgradePreflightConflict(t, workspace, func() error {
			_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.HostCodex,
				previous.manifest.AssetRevision, bundle)
			return err
		})
	})

	t.Run("desired canonical Node bundle drift", func(t *testing.T) {
		workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
		desiredSkill := filepath.Join(nodeState, "assets", bundle.Manifest().AssetRevision, "SKILL.md")
		if err := os.WriteFile(desiredSkill, []byte("desired drift\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		assertUpgradePreflightConflict(t, workspace, func() error {
			_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.HostCodex,
				previous.manifest.AssetRevision, bundle)
			return err
		})
	})

	for _, test := range []struct {
		name      string
		workspace func(string) string
		nodeState func(string, string) string
	}{
		{name: "unclean workspace", workspace: func(workspace string) string {
			return workspace + string(filepath.Separator) + "missing" + string(filepath.Separator) + ".."
		}},
		{name: "unfrozen Node state", nodeState: func(workspace, _ string) string {
			return filepath.Join(workspace, "other-node")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
			callWorkspace, callNodeState := workspace, nodeState
			if test.workspace != nil {
				callWorkspace = test.workspace(workspace)
			}
			if test.nodeState != nil {
				callNodeState = test.nodeState(workspace, nodeState)
			}
			assertUpgradePreflightReadOnly(t, workspace, func() error {
				_, err := PreflightHostProjectionUpgrade(callWorkspace, callNodeState,
					assets.HostCodex, previous.manifest.AssetRevision, bundle)
				if !errors.Is(err, ErrUnsafeProjection) {
					t.Fatalf("PreflightHostProjectionUpgrade() error = %v", err)
				}
				return nil
			})
		})
	}

	t.Run("invalid Host", func(t *testing.T) {
		workspace, nodeState, bundle, previous := newUpgradePreflightFixture(t, assets.HostCodex)
		assertUpgradePreflightReadOnly(t, workspace, func() error {
			_, err := PreflightHostProjectionUpgrade(workspace, nodeState, assets.Host("other"),
				previous.manifest.AssetRevision, bundle)
			if !errors.Is(err, ErrUnsafeProjection) {
				t.Fatalf("PreflightHostProjectionUpgrade() error = %v", err)
			}
			return nil
		})
	})
}

func newUpgradePreflightFixture(t *testing.T, host assets.Host) (string, string,
	assets.Bundle, syntheticPreviousProjection,
) {
	t.Helper()
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, host, bundle); err != nil {
		t.Fatal(err)
	}
	previous := installSyntheticPreviousProjection(t, workspace, nodeState, host, bundle)
	return workspace, nodeState, bundle, previous
}

func assertUpgradePreflightConflict(t *testing.T, workspace string, call func() error) {
	t.Helper()
	assertUpgradePreflightReadOnly(t, workspace, func() error {
		err := call()
		if !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("PreflightHostProjectionUpgrade() error = %v", err)
		}
		return nil
	})
}

type upgradePreflightSnapshot struct {
	info os.FileInfo
	raw  []byte
}

func assertUpgradePreflightReadOnly(t *testing.T, workspace string, call func() error) {
	t.Helper()
	before := snapshotUpgradePreflightTree(t, workspace)
	if err := call(); err != nil {
		t.Fatal(err)
	}
	after := snapshotUpgradePreflightTree(t, workspace)
	if len(after) != len(before) {
		t.Fatalf("preflight changed entry count: %d -> %d", len(before), len(after))
	}
	for path, want := range before {
		got, ok := after[path]
		if !ok || !os.SameFile(want.info, got.info) || want.info.Mode() != got.info.Mode() ||
			want.info.Size() != got.info.Size() || !want.info.ModTime().Equal(got.info.ModTime()) ||
			!bytes.Equal(want.raw, got.raw) {
			t.Fatalf("preflight mutated %s: present=%t", path, ok)
		}
	}
}

func snapshotUpgradePreflightTree(t *testing.T, root string) map[string]upgradePreflightSnapshot {
	t.Helper()
	result := make(map[string]upgradePreflightSnapshot)
	var paths []string
	if err := filepath.Walk(root, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		var raw []byte
		if info.Mode().IsRegular() {
			raw, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		result[relative] = upgradePreflightSnapshot{info: info, raw: raw}
	}
	return result
}
