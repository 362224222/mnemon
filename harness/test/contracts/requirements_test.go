package contracts_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestR7RequirementsRegistry(t *testing.T) {
	root := repositoryRoot(t)
	contract, err := corecontract.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := corecontract.LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := corecontract.ValidateBindings(root, contract, registry); err != nil {
		t.Fatal(err)
	}
}

func TestR7AuthorityCutover(t *testing.T) {
	if err := corecontract.ValidateAuthorityCutover(repositoryRoot(t)); err != nil {
		t.Fatal(err)
	}
}

func TestR7RequirementsPathIsTrackedAtTheOnlyCanonicalName(t *testing.T) {
	root := repositoryRoot(t)
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(corecontract.RegistryPath))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "harness", "test", "contracts", "requirements.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy requirements.json remains: %v", err)
	}
}
