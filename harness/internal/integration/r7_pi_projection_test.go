package integration

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestInstallR7PiProjectionUsesDistinctExactFilesAndPreservesAdjacentAssets(t *testing.T) {
	workspace, projection := newR7PiProjectionWorkspace(t)
	legacySkill := filepath.Join(workspace, ".pi", "skills", "mnemon", "SKILL.md")
	customExtension := filepath.Join(workspace, ".pi", "extensions", "custom.ts")
	writeR7PiAdjacentFile(t, legacySkill, "legacy memory\n")
	writeR7PiAdjacentFile(t, customExtension, "export default function () {}\n")

	receipt, err := InstallR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	wantGuide := filepath.Join(workspace, ".pi", "skills", "mnemond", "SKILL.md")
	wantExtension := filepath.Join(workspace, ".pi", "extensions", "mnemond.ts")
	wantOwnership := filepath.Join(workspace, ".mnemon", "harness", "integrations",
		"pi-mnemond", "ownership.json")
	if receipt.GuidePath != wantGuide || receipt.ExtensionPath != wantExtension ||
		receipt.OwnershipPath != wantOwnership || receipt.Revision == "" || receipt.Replayed {
		t.Fatalf("InstallR7PiProjection() receipt = %#v", receipt)
	}
	assertR7PiFile(t, wantGuide, projection.Guide(), 0o644)
	assertR7PiFile(t, wantExtension, projection.PiExtension(), 0o644)
	assertR7PiFile(t, legacySkill, []byte("legacy memory\n"), 0o644)
	assertR7PiFile(t, customExtension, []byte("export default function () {}\n"), 0o644)
	assertR7PiMode(t, wantOwnership, 0o600)
	assertR7PiMode(t, filepath.Dir(wantOwnership), 0o700)
	if err := VerifyR7PiProjection(workspace, projection); err != nil {
		t.Fatalf("VerifyR7PiProjection() error = %v", err)
	}
	for _, legacyTarget := range []string{
		filepath.Join(workspace, ".pi", "extensions", "mnemon.ts"),
		filepath.Join(workspace, ".mnemon", "prompt", "guide.md"),
	} {
		if _, err := os.Lstat(legacyTarget); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("R7 projection touched legacy target %s: %v", legacyTarget, err)
		}
	}

	plan, err := prepareR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	ownership, _, err := readR7PiOwnership(plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(ownership.Files) != 2 || ownership.Files[0].Path != ".pi/extensions/mnemond.ts" ||
		ownership.Files[0].Source != "r7/pi/mnemond.ts" ||
		ownership.Files[1].Path != ".pi/skills/mnemond/SKILL.md" ||
		ownership.Files[1].Source != "r7/mnemond.md" {
		t.Fatalf("R7 Pi ownership files = %#v", ownership.Files)
	}
}

func TestInstallR7PiProjectionReplayDoesNotRewriteFiles(t *testing.T) {
	workspace, projection := newR7PiProjectionWorkspace(t)
	first, err := InstallR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{first.GuidePath, first.ExtensionPath, first.OwnershipPath}
	identities := make([]os.FileInfo, len(paths))
	for index, path := range paths {
		identities[index], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	second, err := InstallR7PiProjection(workspace, projection)
	if err != nil || !second.Replayed || second.Revision != first.Revision ||
		second.GuidePath != first.GuidePath || second.ExtensionPath != first.ExtensionPath ||
		second.OwnershipPath != first.OwnershipPath {
		t.Fatalf("replay InstallR7PiProjection() = (%#v, %v)", second, err)
	}
	for index, path := range paths {
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(identities[index], current) {
			t.Fatalf("replay replaced %s: %v", path, statErr)
		}
	}
}

func TestInstallR7PiProjectionRepairsOnlyMissingOwnedFiles(t *testing.T) {
	workspace, projection := newR7PiProjectionWorkspace(t)
	installed, err := InstallR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(installed.GuidePath); err != nil {
		t.Fatal(err)
	}
	repaired, err := InstallR7PiProjection(workspace, projection)
	if err != nil || repaired.Replayed {
		t.Fatalf("repair InstallR7PiProjection() = (%#v, %v)", repaired, err)
	}
	assertR7PiFile(t, installed.GuidePath, projection.Guide(), 0o644)
	if err := VerifyR7PiProjection(workspace, projection); err != nil {
		t.Fatalf("VerifyR7PiProjection(repaired) error = %v", err)
	}

	if err := os.WriteFile(installed.GuidePath, []byte("drift\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallR7PiProjection(workspace, projection); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("InstallR7PiProjection(drift) error = %v", err)
	}
	assertR7PiFile(t, installed.GuidePath, []byte("drift\n"), 0o644)
}

func TestInstallR7PiProjectionResumesEveryDurableBoundary(t *testing.T) {
	stages := []string{
		"after_journal",
		"after_file:.pi/extensions/mnemond.ts",
		"after_file:.pi/skills/mnemond/SKILL.md",
		"before_applied",
	}
	interrupted := errors.New("interrupted R7 Pi projection")
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			workspace, projection := newR7PiProjectionWorkspace(t)
			boundary := func(current string) error {
				if current == stage {
					return interrupted
				}
				return nil
			}
			if _, err := installR7PiProjection(workspace, projection, boundary); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted install error = %v", err)
			}
			if _, err := InstallR7PiProjection(workspace, projection); err != nil {
				t.Fatalf("resume InstallR7PiProjection() error = %v", err)
			}
			if err := VerifyR7PiProjection(workspace, projection); err != nil {
				t.Fatalf("resumed VerifyR7PiProjection() error = %v", err)
			}
		})
	}
}

func TestEjectR7PiProjectionIsExactResumableAndPreservesAdjacentAssets(t *testing.T) {
	workspace, projection := newR7PiProjectionWorkspace(t)
	custom := filepath.Join(workspace, ".pi", "extensions", "custom.ts")
	writeR7PiAdjacentFile(t, custom, "custom\n")
	installed, err := InstallR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	ejected, err := EjectR7PiProjection(workspace, projection)
	if err != nil || ejected.Replayed || ejected.RemovedFiles != 2 ||
		ejected.Revision != installed.Revision {
		t.Fatalf("EjectR7PiProjection() = (%#v, %v)", ejected, err)
	}
	for _, path := range []string{installed.GuidePath, installed.ExtensionPath,
		installed.OwnershipPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ejected path remains %s: %v", path, err)
		}
	}
	assertR7PiFile(t, custom, []byte("custom\n"), 0o644)
	replay, err := EjectR7PiProjection(workspace, projection)
	if err != nil || !replay.Replayed || replay.RemovedFiles != 0 ||
		replay.Revision != installed.Revision {
		t.Fatalf("replay EjectR7PiProjection() = (%#v, %v)", replay, err)
	}
}

func TestEjectR7PiProjectionRejectsDriftBeforeDeletingAnything(t *testing.T) {
	workspace, projection := newR7PiProjectionWorkspace(t)
	installed, err := InstallR7PiProjection(workspace, projection)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(installed.ExtensionPath, []byte("drift\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := EjectR7PiProjection(workspace, projection); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("EjectR7PiProjection(drift) error = %v", err)
	}
	assertR7PiFile(t, installed.GuidePath, projection.Guide(), 0o644)
	assertR7PiFile(t, installed.ExtensionPath, []byte("drift\n"), 0o644)
	assertR7PiMode(t, installed.OwnershipPath, 0o600)
}

func TestInstallR7PiProjectionRejectsUnownedAndSymlinkedTargets(t *testing.T) {
	t.Run("unowned target", func(t *testing.T) {
		workspace, projection := newR7PiProjectionWorkspace(t)
		target := filepath.Join(workspace, ".pi", "extensions", "mnemond.ts")
		writeR7PiAdjacentFile(t, target, "unowned\n")
		if _, err := InstallR7PiProjection(workspace, projection); !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("InstallR7PiProjection(unowned) error = %v", err)
		}
		assertR7PiFile(t, target, []byte("unowned\n"), 0o644)
	})

	t.Run("symlinked Pi directory", func(t *testing.T) {
		workspace, projection := newR7PiProjectionWorkspace(t)
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(workspace, ".pi")); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallR7PiProjection(workspace, projection); !errors.Is(err, ErrUnsafeProjection) {
			t.Fatalf("InstallR7PiProjection(symlink) error = %v", err)
		}
		if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
			t.Fatalf("symlink target changed: entries %v, error %v", entries, err)
		}
	})
}

func newR7PiProjectionWorkspace(t *testing.T) (string, assets.R7Projection) {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := assets.LoadR7Projection()
	if err != nil {
		t.Fatal(err)
	}
	return workspace, projection
}

func writeR7PiAdjacentFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertR7PiFile(t *testing.T, path string, want []byte, mode os.FileMode) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("file %s = %q, error %v; want %q", path, got, err, want)
	}
	assertR7PiMode(t, path, mode)
}

func assertR7PiMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != want || !ownedByCurrentUser(info) {
		t.Fatalf("path %s = (%#v, %v); want owner mode %04o", path, info, err, want)
	}
}
