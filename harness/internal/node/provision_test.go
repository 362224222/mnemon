package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestProvisionCreatesAndReplaysOneDisabledWorkspaceAuthority(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 6, 0, 0, 0, time.UTC)
	options := ProvisionOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, Clock: controllerTestClock{at}}
	first, err := Provision(context.Background(), options)
	if err != nil || !first.Created || !first.CredentialCreated || first.Profile.Enabled() ||
		first.Profile.Host() != model.HostCodex || first.Profile.Runtime() != model.RuntimeCodexAppServer ||
		first.Profile.WorkspaceRoot() != workspace || first.Node.PeerID().IsZero() ||
		first.Node.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
		t.Fatalf("first Provision() = (%#v, %v)", first, err)
	}
	assertProvisionModes(t, workspace, first.NodeState)
	identityBefore, err := os.Lstat(filepath.Join(first.NodeState, identityKeyName))
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(first.NodeState, "profiles", model.TeamworkProfileID().String()+".token")
	tokenBefore, err := os.Lstat(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := localapi.VerifyProfileCredential(first.NodeState, first.Profile.CredentialHash()); err != nil {
		t.Fatal(err)
	}

	secondOptions := options
	secondOptions.Clock = controllerTestClock{at.Add(24 * time.Hour)}
	second, err := Provision(context.Background(), secondOptions)
	if err != nil || second.Created || second.CredentialCreated || second.Node.PeerID() != first.Node.PeerID() ||
		second.Node.OriginEpoch() != first.Node.OriginEpoch() || second.Profile.Principal() != first.Profile.Principal() ||
		!second.Profile.CreatedAt().Equal(first.Profile.CreatedAt()) {
		t.Fatalf("replayed Provision() = (%#v, %v)", second, err)
	}
	identityAfter, _ := os.Lstat(filepath.Join(first.NodeState, identityKeyName))
	tokenAfter, _ := os.Lstat(tokenPath)
	if !os.SameFile(identityBefore, identityAfter) || !os.SameFile(tokenBefore, tokenAfter) {
		t.Fatal("replayed Provision replaced identity or credential")
	}
	st, err := store.Open(context.Background(), filepath.Join(first.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if err != nil || closeErr != nil || authority.Node.PeerID() != first.Node.PeerID() || authority.Profile.Enabled() {
		t.Fatalf("durable authority = (%#v, %v, close %v)", authority, err, closeErr)
	}
}

func TestProvisionRejectsProjectionAndHostAuthorityDrift(t *testing.T) {
	t.Run("identity replacement", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(first.NodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(first.NodeState); err != nil {
			t.Fatal(err)
		}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("identity drift Provision() error = %v", err)
		}
	})
	t.Run("enabled Host switch", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		st, err := store.Open(context.Background(), filepath.Join(first.NodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		spec := first.Profile.Spec()
		spec.Enabled = true
		spec.UpdatedAt = first.Profile.UpdatedAt().Add(time.Second)
		enabled, _ := model.NewProfile(spec)
		if _, err := st.ActivateProfile(context.Background(), enabled, enabled.UpdatedAt()); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		options.Host = model.HostClaudeCode
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Host switch Provision() error = %v", err)
		}
	})
}

func TestProvisionRejectsUnsafeWorkspaceStateAndInvalidClock(t *testing.T) {
	t.Run("relative workspace", func(t *testing.T) {
		options := ProvisionOptions{Workspace: ".", Host: model.HostCodex, AssetRevision: "asset-r5"}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("Harness symlink", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		if err := os.Mkdir(filepath.Join(workspace, ".mnemon"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".mnemon", "harness")); err != nil {
			t.Fatal(err)
		}
		if _, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex)); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("invalid clock", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		options.Clock = controllerTestClock{}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("existing safe Mnemon directory", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		mnemonDir := filepath.Join(workspace, ".mnemon")
		if err := os.Mkdir(mnemonDir, 0o755); err != nil {
			t.Fatal(err)
		}
		result, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex))
		if err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(mnemonDir)
		if info.Mode().Perm() != 0o755 || result.NodeState == "" {
			t.Fatalf("existing .mnemon mode/result = %04o %#v", info.Mode().Perm(), result)
		}
	})
}

func provisionTestOptions(t *testing.T, workspace string, host model.HostKind) ProvisionOptions {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	return ProvisionOptions{Workspace: workspace, Host: host,
		AssetRevision: bundle.Manifest().AssetRevision,
		Clock:         controllerTestClock{time.Date(2026, 7, 17, 7, 0, 0, 0, time.UTC)}}
}

func newProvisionWorkspace(t *testing.T) string {
	t.Helper()
	workspace := t.TempDir()
	if err := os.Chmod(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func assertProvisionModes(t *testing.T, workspace, nodeState string) {
	t.Helper()
	for _, path := range []string{filepath.Join(workspace, ".mnemon", "harness"), nodeState,
		filepath.Join(nodeState, "profiles")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("owner directory %s = %v, %v", path, info, err)
		}
	}
	for _, path := range []string{filepath.Join(nodeState, identityKeyName), filepath.Join(nodeState, "node.db"),
		filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token")} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("private file %s = %v, %v", path, info, err)
		}
	}
}
