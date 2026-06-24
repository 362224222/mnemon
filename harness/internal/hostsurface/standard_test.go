package hostsurface

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallStandardHostWritesStaticShim(t *testing.T) {
	root := t.TempDir()
	report, err := InstallStandardHost(context.Background(), StandardHostOptions{Host: "codex", ProjectRoot: root})
	if err != nil {
		t.Fatalf("install standard host: %v", err)
	}
	if len(report.Conflicts) != 0 {
		t.Fatalf("fresh install must not report conflicts: %+v", report)
	}
	hook := string(mustReadHostSurface(t, filepath.Join(root, ".codex", "hooks", "mnemon-r1", "prime.sh")))
	for _, want := range []string{"control render", `--intent "teamwork.events"`, "continue only with local context"} {
		if !strings.Contains(hook, want) {
			t.Fatalf("standard hook missing %q:\n%s", want, hook)
		}
	}
	for _, blocked := range []string{"GUIDE.md", "MEMORY.md", "control observe", "control pull", "--mirror"} {
		if strings.Contains(hook, blocked) {
			t.Fatalf("standard hook must not contain legacy dynamic path %q:\n%s", blocked, hook)
		}
	}
	hooks := string(mustReadHostSurface(t, filepath.Join(root, ".codex", "hooks.json")))
	if !strings.Contains(hooks, "mnemon-r1") {
		t.Fatalf("codex hooks.json must register mnemon-r1:\n%s", hooks)
	}

	if _, err := UninstallStandardHost(context.Background(), StandardHostOptions{Host: "codex", ProjectRoot: root}); err != nil {
		t.Fatalf("uninstall standard host: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex", "hooks", "mnemon-r1")); !os.IsNotExist(err) {
		t.Fatalf("uninstall must remove standard hook dir; err=%v", err)
	}
}

func mustReadHostSurface(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
