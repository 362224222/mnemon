package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareR1AcceptanceRunRootResetsTestdataChild(t *testing.T) {
	cwd := t.TempDir()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldwd) })

	runRoot := filepath.Join(cwd, ".testdata", "r1-codex")
	if err := os.MkdirAll(filepath.Join(runRoot, "workspaces"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runRoot, "stale.txt"), []byte("old ledger"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := prepareR1AcceptanceRunRoot(runRoot); err != nil {
		t.Fatalf("prepareR1AcceptanceRunRoot() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(runRoot, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale artifact should be removed, err=%v", err)
	}
	if info, err := os.Stat(runRoot); err != nil || !info.IsDir() {
		t.Fatalf("run root should be recreated as a directory, info=%v err=%v", info, err)
	}
}

func TestPrepareR1AcceptanceRunRootRejectsNonEmptyExternalDir(t *testing.T) {
	cwd := t.TempDir()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldwd) })

	runRoot := filepath.Join(t.TempDir(), "r1-codex")
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	stalePath := filepath.Join(runRoot, "stale.txt")
	if err := os.WriteFile(stalePath, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	err = prepareR1AcceptanceRunRoot(runRoot)
	if err == nil || !strings.Contains(err.Error(), "already exists outside .testdata") {
		t.Fatalf("prepareR1AcceptanceRunRoot() error = %v, want non-empty external rejection", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("external stale artifact should not be removed: %v", err)
	}
}
