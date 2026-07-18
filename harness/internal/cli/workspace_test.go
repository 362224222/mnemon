package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveManagedWorkspaceFindsPhysicalAncestorAndRejectsProjectionSymlink(t *testing.T) {
	workspace := t.TempDir()
	physical, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	workspace = physical
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(workspace, "nested", "child")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	root, node, err := resolveManagedWorkspace(func() (string, error) { return nested, nil })
	if err != nil || root != workspace || node != nodeState {
		t.Fatalf("resolveManagedWorkspace() = (%q, %q, %v)", root, node, err)
	}

	other := t.TempDir()
	linkedNode := filepath.Join(other, ".mnemon", "harness", "node")
	if err := os.MkdirAll(filepath.Dir(linkedNode), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(nodeState, linkedNode); err != nil {
		t.Fatal(err)
	}
	if root, node, err := resolveManagedWorkspace(func() (string, error) { return other, nil }); err == nil || root != "" || node != "" {
		t.Fatalf("symlink projection resolution = (%q, %q, %v)", root, node, err)
	}
}

func TestResolveManagedWorkspaceRejectsMissingAndInvalidWorkingDirectory(t *testing.T) {
	if root, node, err := resolveManagedWorkspace(nil); err == nil || root != "" || node != "" {
		t.Fatalf("nil dependency resolution = (%q, %q, %v)", root, node, err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if root, node, err := resolveManagedWorkspace(func() (string, error) { return missing, nil }); err == nil || root != "" || node != "" {
		t.Fatalf("missing directory resolution = (%q, %q, %v)", root, node, err)
	}
}
