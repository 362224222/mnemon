package architecture_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const dshGitHubInstallCommand = "dsh plugin --profile web add github:mnemon-dev/mnemon"

func TestDSHGitHubBundleContract(t *testing.T) {
	root := repositoryRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Name         string            `json:"name"`
		Dependencies map[string]string `json:"dependencies"`
		DSH          struct {
			Bundle struct {
				Patch string `json:"patch"`
			} `json:"bundle"`
		} `json:"dsh"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse package.json: %v", err)
	}
	if manifest.Name != "@mnemon-dev/dsh-mnemon" {
		t.Errorf("package name = %q", manifest.Name)
	}
	if manifest.Dependencies["dsh-mnemon"] != "0.1.0" {
		t.Errorf("dsh-mnemon pin = %q", manifest.Dependencies["dsh-mnemon"])
	}
	if manifest.DSH.Bundle.Patch != "./cordis.patch.yml" {
		t.Errorf("bundle patch = %q", manifest.DSH.Bundle.Patch)
	}
	assertFileContains(t, root, "cordis.patch.yml", "name: dsh-mnemon")
	assertFileContains(t, root, "README.md", dshGitHubInstallCommand)
	assertFileContains(t, root, "docs/zh/README.md", dshGitHubInstallCommand)
}

func assertFileContains(t *testing.T, root, name, want string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if !strings.Contains(string(raw), want) {
		t.Errorf("%s does not contain %q", name, want)
	}
}
