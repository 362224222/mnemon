package main

import (
	"path/filepath"
	"testing"
)

func TestManagedRuntimeComponentsBindOnePhysicalWorkspace(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installation, factory, err := managedRuntimeComponents(workspace)
	if err != nil || installation == nil || installation.Revision() == "" || factory == nil {
		t.Fatalf("managedRuntimeComponents() = (%#v, %#v, %v)", installation, factory, err)
	}
	if installation, factory, err := managedRuntimeComponents("."); err == nil ||
		installation != nil || factory != nil {
		t.Fatalf("managedRuntimeComponents(relative) = (%#v, %#v, %v)",
			installation, factory, err)
	}
}
