package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	r7authority "github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestPrepareNodeStateCreatesOnlyASerializableOwnerDirectorySkeleton(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	const callers = 20
	results := make(chan string, callers)
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			nodeState, err := PrepareNodeState(workspace)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- nodeState
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("PrepareNodeState() error = %v", err)
	}
	want := filepath.Join(workspace, ".mnemon", "harness", "node")
	for result := range results {
		if result != want {
			t.Errorf("PrepareNodeState() = %q, want %q", result, want)
		}
	}
	for _, path := range []string{filepath.Join(workspace, ".mnemon", "harness"), want} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("prepared directory %s = (%v, %v)", path, info, err)
		}
	}
	entries, err := os.ReadDir(want)
	if err != nil || len(entries) != 0 {
		t.Fatalf("prepared Node authority entries = (%v, %v)", entries, err)
	}
}

func TestPrepareNodeStateRejectsUnsafeParentsWithoutCreatingAuthority(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	if err := os.Mkdir(filepath.Join(workspace, ".mnemon"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".mnemon", "harness")); err != nil {
		t.Fatal(err)
	}
	if nodeState, err := PrepareNodeState(workspace); nodeState != "" || !errors.Is(err, ErrProvision) {
		t.Fatalf("PrepareNodeState() = (%q, %v)", nodeState, err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon", "harness", "node.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe prepare created authority: %v", err)
	}
}

func TestProvisionCreatesAndReplaysOneDisabledWorkspaceAuthority(t *testing.T) {
	t.Parallel()
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
	if err := VerifyProfileCredential(first.NodeState, first.Profile.CredentialHash()); err != nil {
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
	assertProvisionAgencyAuthority(t, first.NodeState, first.Profile.Principal())
}

func TestProvisionReplaysMissingAgencyAuthorityWithoutChangingNodeAuthority(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	first, err := Provision(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ".writer.lock", "-wal", "-shm"} {
		if err := os.Remove(filepath.Join(first.NodeState, "agency.db") + suffix); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	replayed, err := Provision(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Created || replayed.Node.PeerID() != first.Node.PeerID() ||
		replayed.Profile.Principal() != first.Profile.Principal() {
		t.Fatalf("replayed Provision() = %#v", replayed)
	}
	assertProvisionAgencyAuthority(t, first.NodeState, first.Profile.Principal())
}

func TestProvisionRejectsProjectionAndHostAuthorityDrift(t *testing.T) {
	t.Parallel()
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
	t.Run("unsupported Host", func(t *testing.T) {
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
		if _, err := st.ActivateProfile(context.Background(), enabled,
			first.Profile.UpdatedAt(), enabled.UpdatedAt()); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		options.Host = model.HostKind("claude-code")
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("unsupported Host Provision() error = %v", err)
		}
	})
}

func TestProvisionRejectsUnsafeWorkspaceStateAndInvalidClock(t *testing.T) {
	t.Parallel()
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
		if err := os.Chmod(mnemonDir, 0o755); err != nil {
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
		filepath.Join(nodeState, "agency.db"),
		filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token")} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("private file %s = %v, %v", path, info, err)
		}
	}
}

func assertProvisionAgencyAuthority(t *testing.T, nodeState, principalValue string) {
	t.Helper()
	principal, err := agencyPrincipalForValue(principalValue)
	if err != nil {
		t.Fatal(err)
	}
	verifier := r7authority.ArtifactVerifierFunc(func(context.Context, agency.Digest, int64) error {
		return nil
	})
	st, err := r7authority.OpenExistingWithArtifactVerifier(context.Background(),
		filepath.Join(nodeState, "agency.db"), verifier)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RequirePrincipal(context.Background(), principal); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}
