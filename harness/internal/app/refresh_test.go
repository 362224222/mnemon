package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Refresh re-projects managed definition files under the no-clobber policy: a GUIDE the user has
// edited is preserved and reported, and the channel (bindings) is never touched.
func TestRefreshPreservesUserEditedGuideAndLeavesChannel(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer
	if _, err := h.Setup(context.Background(), &out, &out, SetupOptions{
		Host: "codex", Loops: []string{"memory"}, Principal: "codex@project", ProjectRoot: root,
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := h.Refresh(context.Background(), &out, &out, root, "codex", []string{"memory"}, nil); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}

	guide := filepath.Join(root, ".codex", "mnemon-memory", "GUIDE.md")
	orig, err := os.ReadFile(guide)
	if err != nil {
		t.Fatalf("read projected GUIDE: %v", err)
	}
	edited := append([]byte("# USER EDIT — keep me\n\n"), orig...)
	if err := os.WriteFile(guide, edited, 0o644); err != nil {
		t.Fatalf("edit GUIDE: %v", err)
	}

	bindingsPath := filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")
	bindingsBefore, err := os.ReadFile(bindingsPath)
	if err != nil {
		t.Fatalf("read bindings: %v", err)
	}

	conflicts, err := h.Refresh(context.Background(), &out, &out, root, "codex", []string{"memory"}, nil)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}

	after, err := os.ReadFile(guide)
	if err != nil {
		t.Fatalf("read GUIDE after refresh: %v", err)
	}
	if !bytes.Equal(after, edited) {
		t.Fatal("refresh clobbered the user-edited GUIDE")
	}
	reported := false
	for _, c := range conflicts {
		if strings.Contains(c, "GUIDE.md") {
			reported = true
		}
	}
	if !reported {
		t.Fatalf("refresh must report the preserved GUIDE; got %v", conflicts)
	}

	bindingsAfter, err := os.ReadFile(bindingsPath)
	if err != nil {
		t.Fatalf("read bindings after refresh: %v", err)
	}
	if !bytes.Equal(bindingsBefore, bindingsAfter) {
		t.Fatal("refresh must not touch the channel bindings")
	}
}
