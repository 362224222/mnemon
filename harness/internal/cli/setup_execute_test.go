package cli

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestSetupRejectsActionPreflightBeforeCompanionOrNodeMutation(t *testing.T) {
	fixture := newSetupFixture(t, assets.HostCodex, true)
	fixture.fail["new-preflight"] = errors.New("injected canonical action policy failure")

	exit, stdout, stderr := fixture.run()
	if exit != 3 || stdout != "" || stderr !=
		"asset_revision_mismatch: canonical managed assets or projection are invalid\n" {
		t.Fatalf("early preflight = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	fixture.wantOrder(t, "cwd", "load-bundle", "new-preflight")
	for _, forbidden := range []string{"new-companion", "bootstrap", "lock", "deactivate:codex",
		"install-bundle", "install-projection:codex", "activate:codex", "ensure"} {
		if fixture.called(forbidden) {
			t.Fatalf("early preflight failure reached %s: %v", forbidden, fixture.order)
		}
	}
}

func TestSetupParsingWorkspaceAndStableExitClasses(t *testing.T) {
	for _, args := range [][]string{
		{"--host"}, {"--host", "other"}, {"--project-root"},
		{"--host", "auto", "--host", "codex"}, {"--json"},
	} {
		if _, apiErr := parseSetupRequest(args); apiErr == nil ||
			apiErr.Code != localapi.CodeInvalidArgument || apiErr.ExitStatus() != 2 {
			t.Fatalf("parseSetupRequest(%v) = %v", args, apiErr)
		}
	}
	request, apiErr := parseSetupRequest(nil)
	if apiErr != nil || request.host != "auto" || request.projectRoot != "" {
		t.Fatalf("default request = (%#v, %v)", request, apiErr)
	}

	physical := t.TempDir()
	physical, err := filepath.EvalSymlinks(physical)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "workspace")
	if err := os.Symlink(physical, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveSetupWorkspace(link, func() (string, error) { return "", errors.New("unused") })
	if err != nil || resolved != physical {
		t.Fatalf("resolveSetupWorkspace() = (%q, %v), want %q", resolved, err, physical)
	}
	if _, err := resolveSetupWorkspace(filepath.Join(physical, "missing"), os.Getwd); err == nil {
		t.Fatal("missing project root was accepted")
	}
}

func TestSetupFreshClassificationNeverReinitializesAnExistingDatabase(t *testing.T) {
	nodeState := setupFreshClassificationNodeState(t)
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("missing database fresh classification = (%t, %v)", allowed, err)
	}
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, 0o700); err != nil {
		t.Fatal(err)
	}
	credential := filepath.Join(profiles, model.TeamworkProfileID().String()+".token")
	if err := os.WriteFile(credential, []byte("corrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("corrupt credential fresh classification = (%t, %v)", allowed, err)
	}
	if err := os.Remove(credential); err != nil {
		t.Fatal(err)
	}
	if _, _, err := localapi.EnsureProfileCredential(nodeState); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("valid partial credential classification = (%t, %v)", allowed, err)
	}
	if err := os.WriteFile(filepath.Join(nodeState, "node.db"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("existing corrupt database fresh classification = (%t, %v)", allowed, err)
	}
	if err := os.WriteFile(credential, []byte("corrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("existing database plus corrupt credential classification = (%t, %v)", allowed, err)
	}
}

func setupFreshClassificationNodeState(t *testing.T) string {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
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
	return nodeState
}
