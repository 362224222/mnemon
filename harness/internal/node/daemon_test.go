package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestOpenDaemonBindsIdentityStoreCredentialAssetsAndSocket(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if daemon.Workspace() != fixture.workspace || daemon.NodeState() != fixture.nodeState ||
		daemon.PeerID() != fixture.identity.PeerID() {
		t.Fatalf("OpenDaemon() identity = %s %s %s", daemon.Workspace(), daemon.NodeState(), daemon.PeerID())
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(serveCtx) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, "control.sock"), served)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	health, apiErr := client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "ready" || health.AssetRevision != fixture.revision {
		t.Fatalf("ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatalf("Close() retained writer lock: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDaemonRejectsAuthorityDriftAndNeverCreatesMissingDatabase(t *testing.T) {
	t.Run("identity replacement", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		if err := os.Remove(filepath.Join(fixture.nodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(fixture.nodeState); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace, Install: fixture.install}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("credential replacement", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writeDaemonToken(t, fixture.nodeState, bytes.Repeat([]byte{0x99}, 32), true)
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace, Install: fixture.install}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("disabled Profile", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace, Install: fixture.install}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("Host projection drift", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.workspace, ".codex", "skills", "mnemon-harness", "SKILL.md")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("missing database", func(t *testing.T) {
		workspace := newDaemonWorkspace(t)
		nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
		if err := os.MkdirAll(nodeState, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(nodeState, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(nodeState); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: workspace}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
		if _, err := os.Lstat(filepath.Join(nodeState, "node.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("strict restart created node.db: %v", err)
		}
	})
	t.Run("empty database", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install}); daemon != nil || !errors.Is(err, store.ErrUnsupportedSchema) {
			t.Fatalf("OpenDaemon(empty node.db) = (%v, %v)", daemon, err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() != 0 {
			t.Fatalf("strict restart initialized empty node.db: (%v, %v)", info, err)
		}
	})
	t.Run("relative workspace", func(t *testing.T) {
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: "."}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
}

type daemonFixture struct {
	workspace string
	nodeState string
	identity  *Identity
	profile   model.Profile
	revision  string
	install   InstallationVerifier
}

func newDaemonFixture(t *testing.T, enabled bool) daemonFixture {
	t.Helper()
	workspace := newDaemonWorkspace(t)
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 5, 0, 0, 0, time.UTC)
	epoch, _ := model.ParseOriginEpoch("epoch-daemon-fixture")
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: bundle.Manifest().AssetRevision,
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	credential := bytes.Repeat([]byte{0x73}, 32)
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-daemon", WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum(credential),
		ActiveAssetRevision: bundle.Manifest().AssetRevision,
		HandlingBudget:      model.DefaultHandlingBudget().JSON(), CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
	if enabled {
		spec := profile.Spec()
		spec.Enabled = true
		spec.UpdatedAt = at.Add(time.Second)
		profile, err = model.NewProfile(spec)
		if err != nil {
			t.Fatal(err)
		}
		activated, err := st.ActivateProfile(context.Background(), profile, profile.UpdatedAt())
		if err != nil {
			t.Fatal(err)
		}
		profile = activated.Profile
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	writeDaemonToken(t, nodeState, credential, false)
	return daemonFixture{workspace: workspace, nodeState: nodeState, identity: identity,
		profile: profile, revision: bundle.Manifest().AssetRevision,
		install: testInstallationVerifier(workspace, nodeState, bundle)}
}

func newDaemonWorkspace(t *testing.T) string {
	t.Helper()
	workspace, err := os.MkdirTemp("/tmp", "mnemon-r5-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	return workspace
}

func writeDaemonToken(t *testing.T, nodeState string, credential []byte, replace bool) {
	t.Helper()
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	if err := os.Chmod(profiles, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profiles, model.TeamworkProfileID().String()+".token")
	if replace {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	raw := append([]byte(base64.RawURLEncoding.EncodeToString(credential)), '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}
