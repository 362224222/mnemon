package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSetupUninstallPreservesUserEditedStandardHook(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer
	opts := SetupOptions{Host: "codex", Principal: "codex@project", ProjectRoot: root}
	if _, err := h.Setup(context.Background(), &out, &out, opts); err != nil {
		t.Fatalf("setup: %v", err)
	}

	hook := filepath.Join(root, ".codex", "hooks", "mnemon-r1", "prime.sh")
	orig, err := os.ReadFile(hook)
	if err != nil {
		t.Fatalf("standard hook missing: %v", err)
	}
	if err := os.WriteFile(hook, append([]byte("# USER EDIT - keep me\n"), orig...), 0o755); err != nil {
		t.Fatalf("edit hook: %v", err)
	}

	if err := h.SetupUninstall(context.Background(), &out, &out, opts); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	after, err := os.ReadFile(hook)
	if err != nil {
		t.Fatalf("uninstall removed user-edited standard hook: %v", err)
	}
	if !bytes.Contains(after, []byte("USER EDIT")) {
		t.Fatal("uninstall clobbered the user edit")
	}
}

func TestSetupUninstallKeepsSharedShimUntilLastBinding(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer
	codex := SetupOptions{Host: "codex", Principal: "codex@project", ProjectRoot: root}
	human := SetupOptions{Host: "codex", Principal: "human@project", ProjectRoot: root}
	if _, err := h.Setup(context.Background(), &out, &out, codex); err != nil {
		t.Fatalf("setup codex: %v", err)
	}
	if _, err := h.Setup(context.Background(), &out, &out, human); err != nil {
		t.Fatalf("setup human: %v", err)
	}
	hookDir := filepath.Join(root, ".codex", "hooks", "mnemon-r1")
	if err := h.SetupUninstall(context.Background(), &out, &out, codex); err != nil {
		t.Fatalf("uninstall codex: %v", err)
	}
	if _, err := os.Stat(hookDir); err != nil {
		t.Fatalf("shared hook integration must remain while a sibling binding exists: %v", err)
	}
	if err := h.SetupUninstall(context.Background(), &out, &out, human); err != nil {
		t.Fatalf("uninstall human: %v", err)
	}
	if _, err := os.Stat(hookDir); !os.IsNotExist(err) {
		t.Fatalf("last binding uninstall must remove unedited standard hook dir; err=%v", err)
	}
}
