package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestInstallHostProjectionUsesFrozenPathsAndPreservesAdjacentConfiguration(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	for _, host := range []assets.Host{assets.HostCodex} {
		host := host
		t.Run(string(host), func(t *testing.T) {
			configPath := hostConfigPath(workspace, host)
			if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
				t.Fatal(err)
			}
			userEntry := map[string]any{"matcher": "user", "hooks": []any{map[string]any{
				"command": "/usr/local/bin/user-hook", "timeout": float64(11), "type": "command",
			}}}
			original := map[string]any{
				"features": map[string]any{"preview": true},
				"hooks": map[string]any{
					"Stop":             []any{map[string]any{"hooks": []any{map[string]any{"command": "user-stop"}}}},
					"UserPromptSubmit": []any{userEntry},
				},
				"theme": "dark",
			}
			writeTestJSON(t, configPath, original)

			receipt, err := InstallHostProjection(workspace, nodeState, host, bundle)
			if err != nil || receipt.Replayed || receipt.ConfigPath != configPath ||
				receipt.Revision != bundle.Manifest().AssetRevision {
				t.Fatalf("InstallHostProjection() = (%#v, %v)", receipt, err)
			}
			if err := VerifyHostProjection(workspace, nodeState, host, bundle); err != nil {
				t.Fatalf("VerifyHostProjection() error = %v", err)
			}
			assertProjectedFiles(t, workspace, host, bundle)
			if host == assets.HostCodex {
				if _, err := os.Lstat(filepath.Join(workspace, ".codex", "skills")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Codex legacy Skill surface exists: %v", err)
				}
				manifest, _ := readOwnershipManifest(t, receipt.OwnershipPath)
				wantPaths := []string{
					".agents/skills/mnemon-harness/SKILL.md",
					".agents/skills/mnemon-harness/guides/teamwork/GUIDE.md",
					".codex/hooks/mnemon-harness/hook.sh",
				}
				for index, want := range wantPaths {
					if manifest.Files[index].Path != want {
						t.Fatalf("ownership file %d path = %q, want %q", index, manifest.Files[index].Path, want)
					}
				}
				if manifest.Registrations[0].Path != ".codex/hooks.json" {
					t.Fatalf("ownership registration path = %q", manifest.Registrations[0].Path)
				}
			}

			installed := readTestJSON(t, configPath)
			if !reflect.DeepEqual(installed["features"], original["features"]) || installed["theme"] != "dark" {
				t.Fatalf("adjacent settings changed: %#v", installed)
			}
			hooks := installed["hooks"].(map[string]any)
			if !reflect.DeepEqual(hooks["Stop"], original["hooks"].(map[string]any)["Stop"]) {
				t.Fatalf("adjacent Hook changed: %#v", hooks["Stop"])
			}
			entries := hooks["UserPromptSubmit"].([]any)
			if len(entries) != 2 || !reflect.DeepEqual(entries[0], userEntry) {
				t.Fatalf("UserPromptSubmit entries = %#v", entries)
			}
			managed := entries[1].(map[string]any)
			managedHooks := managed["hooks"].([]any)
			managedHook := managedHooks[0].(map[string]any)
			wantHook := filepath.Join(workspace, hostDirectory(host), "hooks", "mnemon-harness", "hook.sh")
			if managedHook["command"] != wantHook || !filepath.IsAbs(managedHook["command"].(string)) {
				t.Fatalf("managed Hook command = %#v, want %s", managedHook["command"], wantHook)
			}
			configRaw, err := os.ReadFile(configPath)
			if err != nil || bytes.Contains(configRaw, []byte("{{HOOK_PATH}}")) || bytes.Contains(configRaw, []byte("managed_key")) {
				t.Fatalf("shared Host config contains template/private fields: %v\n%s", err, configRaw)
			}
		})
	}
}

func TestInstallHostProjectionAcceptsEstablishedEmptyAndJSON5SharedConfig(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "comments and trailing commas", raw: "{\n  // preserved semantically by the root setup model\n  \"user\": {\"enabled\": true,},\n}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			configPath := hostConfigPath(workspace, assets.HostCodex)
			if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(configPath, []byte(test.raw), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			document := readTestJSON(t, configPath)
			if test.name != "empty" && !reflect.DeepEqual(document["user"], map[string]any{"enabled": true}) {
				t.Fatalf("adjacent JSON5 value changed: %#v", document)
			}
		})
	}
}

func TestInstallHostProjectionExactReplayDoesNotRewriteFiles(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	first, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{first.ConfigPath, first.OwnershipPath,
		filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md"),
		filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")}
	before := make(map[string]time.Time, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = info.ModTime()
	}
	time.Sleep(20 * time.Millisecond)
	second, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil || !second.Replayed || second != (HostProjectionReceipt{
		ConfigPath: first.ConfigPath, Host: first.Host, OwnershipPath: first.OwnershipPath,
		Replayed: true, Revision: first.Revision,
	}) {
		t.Fatalf("replay InstallHostProjection() = (%#v, %v)", second, err)
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil || !info.ModTime().Equal(before[path]) {
			t.Fatalf("replay rewrote %s: %v, %v -> %v", path, err, before[path], info.ModTime())
		}
	}
	document := readTestJSON(t, first.ConfigPath)
	entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
	if len(entries) != 1 {
		t.Fatalf("replay duplicated managed registration: %#v", entries)
	}
}

func TestInstallHostProjectionResumesInstallingJournalAtEveryDurableBoundary(t *testing.T) {
	stages := []string{
		"after_journal",
		"after_file:.agents/skills/mnemon-harness/SKILL.md",
		"after_file:.agents/skills/mnemon-harness/guides/teamwork/GUIDE.md",
		"after_file:.codex/hooks/mnemon-harness/hook.sh",
		"after_config",
		"before_applied",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			interrupted := errors.New("simulated process interruption")
			fired := false
			boundary := func(current string) error {
				if current == stage && !fired {
					fired = true
					return interrupted
				}
				return nil
			}
			if _, err := installHostProjection(workspace, nodeState, assets.HostCodex, bundle, boundary); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted InstallHostProjection() error = %v", err)
			}
			if !fired {
				t.Fatalf("boundary %q did not fire", stage)
			}
			ownershipPath := filepath.Join(workspace, ".mnemon", "harness", "integrations", "codex", "ownership.json")
			manifest, info := readOwnershipManifest(t, ownershipPath)
			if manifest.State != projectionInstalling || info.Mode().Perm() != 0o600 {
				t.Fatalf("interrupted ownership = (%#v, %04o)", manifest, info.Mode().Perm())
			}
			if stage == "after_journal" {
				for _, root := range []string{".agents", ".codex"} {
					if _, err := os.Lstat(filepath.Join(workspace, root)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Host surface %s changed before desired journal: %v", root, err)
					}
				}
			}
			if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("installing VerifyHostProjection() error = %v", err)
			}
			receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
			if err != nil || receipt.Replayed {
				t.Fatalf("resumed InstallHostProjection() = (%#v, %v)", receipt, err)
			}
			if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			manifest, info = readOwnershipManifest(t, ownershipPath)
			if manifest.State != projectionApplied || info.Mode().Perm() != 0o600 {
				t.Fatalf("resumed ownership = (%#v, %04o)", manifest, info.Mode().Perm())
			}
		})
	}
}

func TestInstallHostProjectionRepairsOnlyMissingAppliedObjects(t *testing.T) {
	t.Run("projected file", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("missing VerifyHostProjection() error = %v", err)
		}
		repaired, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil || repaired.Replayed {
			t.Fatalf("repair InstallHostProjection() = (%#v, %v)", repaired, err)
		}
		if repaired.OwnershipPath != receipt.OwnershipPath {
			t.Fatal("repair changed ownership identity")
		}
		if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("registration subentry", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		configPath := hostConfigPath(workspace, assets.HostCodex)
		if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
			t.Fatal(err)
		}
		writeTestJSON(t, configPath, map[string]any{"user": map[string]any{"keep": true}})
		if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
			t.Fatal(err)
		}
		document := readTestJSON(t, configPath)
		hooks := document["hooks"].(map[string]any)
		delete(hooks, "UserPromptSubmit")
		writeTestJSON(t, configPath, document)
		repaired, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil || repaired.Replayed {
			t.Fatalf("registration repair = (%#v, %v)", repaired, err)
		}
		document = readTestJSON(t, configPath)
		if !reflect.DeepEqual(document["user"], map[string]any{"keep": true}) {
			t.Fatalf("registration repair changed user JSON: %#v", document)
		}
		entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
		if len(entries) != 1 {
			t.Fatalf("registration repair entries = %#v", entries)
		}
		if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("shared config file", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(receipt.ConfigPath); err != nil {
			t.Fatal(err)
		}
		repaired, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil || repaired.Replayed {
			t.Fatalf("config repair = (%#v, %v)", repaired, err)
		}
		if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
			t.Fatal(err)
		}
	})
}

func TestInstallingJournalNeverAdoptsDrift(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	interrupted := errors.New("interrupt after first file")
	boundary := func(stage string) error {
		if stage == "after_file:.codex/hooks/mnemon-harness/hook.sh" {
			return interrupted
		}
		return nil
	}
	if _, err := installHostProjection(workspace, nodeState, assets.HostCodex, bundle, boundary); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted InstallHostProjection() error = %v", err)
	}
	hookPath := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
	if err := os.WriteFile(hookPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("InstallHostProjection() adopted drift: %v", err)
	}
	got, _ := os.ReadFile(hookPath)
	if string(got) != "#!/bin/sh\nexit 0\n" {
		t.Fatal("drifted desired file was overwritten")
	}
}

func TestAppliedOwnershipRemovalDoesNotAuthorizeOrphanAdoption(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(receipt.OwnershipPath); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("InstallHostProjection() adopted orphaned projection: %v", err)
	}
}

func TestInstallHostProjectionSafelyUpgradesPreviousAppliedRevision(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	previous := installSyntheticPreviousProjection(t, workspace, nodeState, assets.HostCodex, bundle)
	receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
	if err != nil || receipt.Replayed {
		t.Fatalf("upgrade InstallHostProjection() = (%#v, %v)", receipt, err)
	}
	if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	manifest, _ := readOwnershipManifest(t, receipt.OwnershipPath)
	if manifest.State != projectionApplied || manifest.AssetRevision != bundle.Manifest().AssetRevision || manifest.Previous != nil {
		t.Fatalf("final ownership = %#v", manifest)
	}
	for _, file := range previous.plan.files {
		got, err := os.ReadFile(file.path)
		if err != nil || !bytes.Equal(got, file.content) {
			t.Fatalf("upgraded file %s differs: %v", file.record.Path, err)
		}
	}
	document := readTestJSON(t, receipt.ConfigPath)
	if !reflect.DeepEqual(document["upgrade_user"], map[string]any{"keep": true}) {
		t.Fatalf("upgrade changed adjacent JSON: %#v", document)
	}
}

func TestHostProjectionUpgradeResumesAtEveryDurableBoundary(t *testing.T) {
	stages := []string{
		"after_upgrade_journal",
		"after_file:.agents/skills/mnemon-harness/SKILL.md",
		"after_file:.agents/skills/mnemon-harness/guides/teamwork/GUIDE.md",
		"after_file:.codex/hooks/mnemon-harness/hook.sh",
		"after_config",
		"before_applied",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			previous := installSyntheticPreviousProjection(t, workspace, nodeState, assets.HostCodex, bundle)
			interrupted := errors.New("simulated upgrade interruption")
			fired := false
			boundary := func(current string) error {
				if current == stage && !fired {
					fired = true
					return interrupted
				}
				return nil
			}
			if _, err := installHostProjection(workspace, nodeState, assets.HostCodex, bundle, boundary); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted upgrade error = %v", err)
			}
			if !fired {
				t.Fatalf("upgrade boundary %q did not fire", stage)
			}
			manifest, info := readOwnershipManifest(t, previous.plan.ownershipPath)
			if manifest.State != projectionUpgrading || manifest.Previous == nil ||
				manifest.Previous.AssetRevision != previous.manifest.AssetRevision || info.Mode().Perm() != 0o600 {
				t.Fatalf("interrupted upgrade journal = (%#v, %04o)", manifest, info.Mode().Perm())
			}
			if stage == "after_upgrade_journal" {
				for path, oldContent := range previous.fileContent {
					got, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(got, oldContent) {
						t.Fatalf("Host changed before upgrade journal boundary at %s: %v", path, err)
					}
				}
			}
			if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("upgrading VerifyHostProjection() error = %v", err)
			}
			receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
			if err != nil || receipt.Replayed {
				t.Fatalf("resumed upgrade = (%#v, %v)", receipt, err)
			}
			if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHostProjectionUpgradeFillsMissingPreviousObjects(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	previous := installSyntheticPreviousProjection(t, workspace, nodeState, assets.HostCodex, bundle)
	missing := previous.plan.files[1].path
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	document := readTestJSON(t, previous.plan.configPath)
	hooks := document["hooks"].(map[string]any)
	delete(hooks, "UserPromptSubmit")
	writeTestJSON(t, previous.plan.configPath, document)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(missing); err != nil || !bytes.Equal(got, previous.plan.files[1].content) {
		t.Fatalf("missing upgrade file not restored: %v", err)
	}
}

func TestHostProjectionUpgradeRejectsPreviousDriftBeforeJournaling(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(t *testing.T, previous syntheticPreviousProjection)
	}{
		{name: "file content", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			file := previous.plan.files[0]
			if err := os.WriteFile(file.path, []byte("unknown drift\n"), file.mode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "file mode", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			if err := os.Chmod(previous.plan.files[0].path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "registration content", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			document := readTestJSON(t, previous.plan.configPath)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["timeout"] = float64(77)
			writeTestJSON(t, previous.plan.configPath, document)
		}},
		{name: "duplicate logical registration", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			document := readTestJSON(t, previous.plan.configPath)
			hooks := document["hooks"].(map[string]any)
			entries := hooks["UserPromptSubmit"].([]any)
			hooks["UserPromptSubmit"] = append(entries, entries[0])
			writeTestJSON(t, previous.plan.configPath, document)
		}},
		{name: "managed key", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			manifest := previous.manifest
			manifest.Registrations = append([]ownershipRegistration(nil), manifest.Registrations...)
			manifest.Registrations[0].ManagedKey = "not-mnemon"
			writeOwnershipManifest(t, previous.plan.ownershipPath, manifest)
		}},
		{name: "frozen path", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			manifest := previous.manifest
			manifest.Files = append([]ownershipFile(nil), manifest.Files...)
			manifest.Files[0].Path = "outside/SKILL.md"
			writeOwnershipManifest(t, previous.plan.ownershipPath, manifest)
		}},
		{name: "Host identity", tamper: func(t *testing.T, previous syntheticPreviousProjection) {
			manifest := previous.manifest
			manifest.Host = assets.Host("unsupported")
			writeOwnershipManifest(t, previous.plan.ownershipPath, manifest)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
				t.Fatal(err)
			}
			previous := installSyntheticPreviousProjection(t, workspace, nodeState, assets.HostCodex, bundle)
			test.tamper(t, previous)
			before, err := os.ReadFile(previous.plan.ownershipPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("upgrade accepted previous drift: %v", err)
			}
			after, err := os.ReadFile(previous.plan.ownershipPath)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatalf("failed preflight changed ownership journal: %v", err)
			}
		})
	}
}

func TestHostProjectionUpgradeJournalAcceptsOnlyPreviousOrDesired(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	previous := installSyntheticPreviousProjection(t, workspace, nodeState, assets.HostCodex, bundle)
	interrupted := errors.New("stop after first upgrade file")
	boundary := func(stage string) error {
		if stage == "after_file:.codex/hooks/mnemon-harness/hook.sh" {
			return interrupted
		}
		return nil
	}
	if _, err := installHostProjection(workspace, nodeState, assets.HostCodex, bundle, boundary); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted upgrade error = %v", err)
	}
	first := previous.plan.files[0]
	if err := os.WriteFile(first.path, []byte("neither previous nor desired\n"), first.mode); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("upgrade journal adopted unknown bytes: %v", err)
	}
	manifest, _ := readOwnershipManifest(t, previous.plan.ownershipPath)
	if manifest.State != projectionUpgrading || manifest.Previous == nil {
		t.Fatalf("failed upgrade lost recovery journal: %#v", manifest)
	}
}

func TestInstallHostProjectionNeverAdoptsUnownedFilesOrRegistrations(t *testing.T) {
	t.Run("exact canonical file", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		path := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		content, _ := bundle.Read("SKILL.md")
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("InstallHostProjection() error = %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Fatal("unowned file was overwritten")
		}
	})

	t.Run("exact canonical registration", func(t *testing.T) {
		workspace, nodeState, bundle := newProjectionWorkspace(t)
		plan, err := prepareProjection(workspace, nodeState, assets.HostCodex, bundle)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(plan.configPath), 0o755); err != nil {
			t.Fatal(err)
		}
		writeTestJSON(t, plan.configPath, map[string]any{"hooks": map[string]any{
			"UserPromptSubmit": []any{plan.entry},
		}})
		before, _ := os.ReadFile(plan.configPath)
		if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("InstallHostProjection() error = %v", err)
		}
		after, _ := os.ReadFile(plan.configPath)
		if !bytes.Equal(after, before) {
			t.Fatal("unowned registration was rewritten")
		}
	})
}

func TestHostProjectionDriftFailsClosedWithoutRepair(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(t *testing.T, workspace string, receipt HostProjectionReceipt)
	}{
		{name: "managed file content", tamper: func(t *testing.T, workspace string, _ HostProjectionReceipt) {
			path := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
			if err := os.WriteFile(path, []byte("user changed this\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "managed file mode", tamper: func(t *testing.T, workspace string, _ HostProjectionReceipt) {
			path := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
			if err := os.Chmod(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "managed registration", tamper: func(t *testing.T, _ string, receipt HostProjectionReceipt) {
			document := readTestJSON(t, receipt.ConfigPath)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["timeout"] = float64(99)
			document["user_after_install"] = "preserve me"
			writeTestJSON(t, receipt.ConfigPath, document)
		}},
		{name: "managed registration command", tamper: func(t *testing.T, _ string, receipt HostProjectionReceipt) {
			document := readTestJSON(t, receipt.ConfigPath)
			entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
			hook := entries[0].(map[string]any)["hooks"].([]any)[0].(map[string]any)
			hook["command"] = "/tmp/user-replaced-managed-hook"
			writeTestJSON(t, receipt.ConfigPath, document)
		}},
		{name: "duplicate logical registration", tamper: func(t *testing.T, _ string, receipt HostProjectionReceipt) {
			document := readTestJSON(t, receipt.ConfigPath)
			hooks := document["hooks"].(map[string]any)
			entries := hooks["UserPromptSubmit"].([]any)
			duplicate := entries[0].(map[string]any)
			duplicateHooks := duplicate["hooks"].([]any)
			duplicateHook := duplicateHooks[0].(map[string]any)
			duplicateHook["timeout"] = float64(4)
			hooks["UserPromptSubmit"] = append(entries, duplicate)
			writeTestJSON(t, receipt.ConfigPath, document)
		}},
		{name: "ownership manifest", tamper: func(t *testing.T, _ string, receipt HostProjectionReceipt) {
			raw, err := os.ReadFile(receipt.OwnershipPath)
			if err != nil {
				t.Fatal(err)
			}
			raw = bytes.Replace(raw, []byte(`"schema":1`), []byte(`"schema":1,"unknown":true`), 1)
			if err := os.WriteFile(receipt.OwnershipPath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			receipt, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle)
			if err != nil {
				t.Fatal(err)
			}
			test.tamper(t, workspace, receipt)
			configBefore, _ := os.ReadFile(receipt.ConfigPath)
			if err := VerifyHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("VerifyHostProjection() error = %v", err)
			}
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("InstallHostProjection() repaired drift: %v", err)
			}
			configAfter, _ := os.ReadFile(receipt.ConfigPath)
			if !bytes.Equal(configAfter, configBefore) {
				t.Fatal("drift check rewrote shared Host config")
			}
		})
	}
}

func TestHostProjectionRequiresExactCanonicalNodeBundleAndFrozenStatePath(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, filepath.Join(workspace, "other"), assets.HostCodex, bundle); !errors.Is(err, ErrUnsafeProjection) {
		t.Fatalf("non-frozen Node path error = %v", err)
	}
	skill := filepath.Join(nodeState, "assets", bundle.Manifest().AssetRevision, "SKILL.md")
	if err := os.WriteFile(skill, []byte("drift\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("drifted Node bundle error = %v", err)
	}
	for _, root := range []string{".agents", ".codex"} {
		if _, err := os.Lstat(filepath.Join(workspace, root)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Host surface %s changed before Node verification: %v", root, err)
		}
	}
}

func TestHostProjectionRejectsUnsafeHostSurfaces(t *testing.T) {
	for _, root := range []string{".agents", ".codex"} {
		root := root
		t.Run(root+" symlink", func(t *testing.T) {
			workspace, nodeState, bundle := newProjectionWorkspace(t)
			outside := t.TempDir()
			if err := os.Symlink(outside, filepath.Join(workspace, root)); err != nil {
				t.Fatal(err)
			}
			if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); !errors.Is(err, ErrUnsafeProjection) {
				t.Fatalf("InstallHostProjection() error = %v", err)
			}
		})
	}
}

func newProjectionWorkspace(t *testing.T) (string, string, assets.Bundle) {
	t.Helper()
	workspace := t.TempDir()
	if err := os.Chmod(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(workspace, ".mnemon"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(workspace, ".mnemon", "harness"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	return workspace, nodeState, bundle
}

func hostDirectory(host assets.Host) string {
	return ".codex"
}

func hostConfigPath(workspace string, host assets.Host) string {
	return filepath.Join(workspace, hostDirectory(host), "hooks.json")
}

func assertProjectedFiles(t *testing.T, workspace string, host assets.Host, bundle assets.Bundle) {
	t.Helper()
	wants := map[string]string{
		filepath.Join(skillDirectory(host), "SKILL.md"):                          "SKILL.md",
		filepath.Join(skillDirectory(host), "guides", "teamwork", "GUIDE.md"):    "guides/teamwork/GUIDE.md",
		filepath.Join(hostDirectory(host), "hooks", "mnemon-harness", "hook.sh"): "hosts/" + string(host) + "/hook.sh",
	}
	for relative, source := range wants {
		path := filepath.Join(workspace, relative)
		got, err := os.ReadFile(path)
		want, sourceErr := bundle.Read(source)
		info, statErr := os.Stat(path)
		wantMode := os.FileMode(0o644)
		if strings.HasSuffix(path, "hook.sh") {
			wantMode = 0o755
		}
		if err != nil || sourceErr != nil || statErr != nil || !bytes.Equal(got, want) || info.Mode().Perm() != wantMode {
			t.Fatalf("projected %s differs: %v, %v, %v, mode %04o", relative, err, sourceErr, statErr, info.Mode().Perm())
		}
	}
}

func skillDirectory(host assets.Host) string {
	return filepath.Join(".agents", "skills", "mnemon-harness")
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readTestJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func readOwnershipManifest(t *testing.T, path string) (ownershipManifest, os.FileInfo) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest ownershipManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest, info
}

type syntheticPreviousProjection struct {
	fileContent map[string][]byte
	manifest    ownershipManifest
	plan        projectionPlan
}

func installSyntheticPreviousProjection(t *testing.T, workspace, nodeState string, host assets.Host, bundle assets.Bundle) syntheticPreviousProjection {
	t.Helper()
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		t.Fatal(err)
	}
	previous := plan.applied
	previous.AssetRevision = digest([]byte("synthetic previous revision for " + string(host)))
	previous.Files = append([]ownershipFile(nil), plan.applied.Files...)
	previous.Registrations = append([]ownershipRegistration(nil), plan.applied.Registrations...)
	previous.Previous = nil
	fileContent := make(map[string][]byte, len(plan.files))
	for index, file := range plan.files {
		oldContent := append([]byte(nil), file.content...)
		oldContent = append(oldContent, []byte("\nsynthetic previous asset: "+file.record.Path+"\n")...)
		if err := os.WriteFile(file.path, oldContent, file.mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(file.path, file.mode); err != nil {
			t.Fatal(err)
		}
		previous.Files[index].InstalledDigest = digest(oldContent)
		fileContent[file.path] = oldContent
	}
	document := readTestJSON(t, plan.configPath)
	document["upgrade_user"] = map[string]any{"keep": true}
	entries := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
	managed := entries[0].(map[string]any)
	hook := managed["hooks"].([]any)[0].(map[string]any)
	hook["statusMessage"] = "Previous Mnemon Teamwork"
	hook["timeout"] = float64(2)
	canonical, err := json.Marshal(managed)
	if err != nil {
		t.Fatal(err)
	}
	previous.Registrations[0].InstalledDigest = digest(canonical)
	writeTestJSON(t, plan.configPath, document)
	writeOwnershipManifest(t, plan.ownershipPath, previous)
	return syntheticPreviousProjection{fileContent: fileContent, manifest: previous, plan: plan}
}

func writeOwnershipManifest(t *testing.T, path string, manifest ownershipManifest) {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}
