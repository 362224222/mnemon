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
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestActivatePublishesOnlyVerifiedInstallationAndReplaysExactly(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(provisioned.NodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, provisioned.NodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	at := provisioned.Profile.UpdatedAt().Add(time.Second)
	options := ActivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:   controllerTestClock{at},
		Install: testInstallationVerifier(workspace, provisioned.NodeState, bundle)}
	first, err := Activate(context.Background(), options)
	if err != nil || !first.Changed || !first.Profile.Enabled() || first.Profile.Host() != model.HostCodex ||
		first.Node.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
		t.Fatalf("first Activate() = (%#v, %v)", first, err)
	}
	options.Clock = controllerTestClock{at.Add(time.Hour)}
	options.ExpectedUpdatedAt = first.Profile.UpdatedAt()
	second, err := Activate(context.Background(), options)
	if err != nil || second.Changed || !second.Profile.UpdatedAt().Equal(first.Profile.UpdatedAt()) ||
		!second.Node.UpdatedAt().Equal(first.Node.UpdatedAt()) {
		t.Fatalf("replayed Activate() = (%#v, %v)", second, err)
	}
}

func TestActivateRejectsMissingActionAuthorityBeforeStoreOrProfileMutation(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 8, 30, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.OpenExisting(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	called := false
	install := InstallationVerifierFunc(func(context.Context, model.Profile) error {
		called = true
		return nil
	})
	_, activateErr := Activate(context.Background(), ActivateOptions{
		Workspace:         workspace,
		Host:              model.HostCodex,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           install,
	})
	if !errors.Is(activateErr, ErrActivate) || errors.Is(activateErr, store.ErrWriterActive) {
		t.Fatalf("Activate() error = %v", activateErr)
	}
	if called {
		t.Fatal("action authority failure reached installation verification")
	}
	authority, err := writer.ReadLocalAuthority(context.Background())
	if err != nil || authority.Profile.Enabled() {
		t.Fatalf("failed activation authority = (%#v, %v)", authority, err)
	}
}

func TestActivateFailureLeavesProfileDisabled(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	bundle, _ := assets.Load()
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	failed := errors.New("Host projection is not installed")
	install := testInstallationWithActions(InstallationVerifierFunc(func(context.Context, model.Profile) error {
		return failed
	}), bundle)
	options := ActivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           install}
	if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) || !errors.Is(err, failed) {
		t.Fatalf("Activate() error = %v", err)
	}
	st, err := store.Open(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if err != nil || closeErr != nil || authority.Profile.Enabled() {
		t.Fatalf("failed activation authority = (%#v, %v, close %v)", authority, err, closeErr)
	}
}

func TestActivateRejectsIdentityCredentialAndHostDrift(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		workspace, provisioned, bundle := activeTestProvision(t)
		if err := os.Remove(filepath.Join(provisioned.NodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(provisioned.NodeState); err != nil {
			t.Fatal(err)
		}
		options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
		if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
			t.Fatalf("Activate() error = %v", err)
		}
	})
	t.Run("credential", func(t *testing.T) {
		workspace, provisioned, bundle := activeTestProvision(t)
		replacement := append([]byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))), '\n')
		credentialPath := filepath.Join(provisioned.NodeState, "profiles", model.TeamworkProfileID().String()+".token")
		if err := os.WriteFile(credentialPath, replacement, 0o600); err != nil {
			t.Fatal(err)
		}
		options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
		if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
			t.Fatalf("Activate() error = %v", err)
		}
	})
	t.Run("enabled Host switch", func(t *testing.T) {
		workspace, provisioned, bundle := activeTestProvision(t)
		first, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle, model.HostCodex))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := integration.InstallHostProjection(workspace, provisioned.NodeState, assets.HostClaudeCode, bundle); err != nil {
			t.Fatal(err)
		}
		options := activeTestOptions(workspace, provisioned, bundle, model.HostClaudeCode)
		options.Clock = controllerTestClock{first.Profile.UpdatedAt().Add(time.Second)}
		options.Install = testInstallationVerifier(workspace, provisioned.NodeState, bundle)
		if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
			t.Fatalf("Activate() Host switch error = %v", err)
		}
	})
}

func activeTestProvision(t *testing.T) (string, ProvisionResult, assets.Bundle) {
	t.Helper()
	workspace := newProvisionWorkspace(t)
	bundle, _ := assets.Load()
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(provisioned.NodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, provisioned.NodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	return workspace, provisioned, bundle
}

func activeTestOptions(workspace string, provisioned ProvisionResult, bundle assets.Bundle,
	host model.HostKind,
) ActivateOptions {
	return ActivateOptions{Workspace: workspace, Host: host,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           testInstallationVerifier(workspace, provisioned.NodeState, bundle)}
}

func TestActivateRejectsStaleOrInvalidExpectedGenerationBeforeMutation(t *testing.T) {
	workspace, provisioned, bundle := activeTestProvision(t)
	for name, expected := range map[string]time.Time{
		"stale": provisioned.Profile.UpdatedAt().Add(-time.Nanosecond),
		"zero":  {},
	} {
		t.Run(name, func(t *testing.T) {
			options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
			options.ExpectedUpdatedAt = expected
			if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
				t.Fatalf("Activate() error = %v", err)
			}
		})
	}
	equalClock := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
	equalClock.Clock = controllerTestClock{provisioned.Profile.UpdatedAt()}
	if _, err := Activate(context.Background(), equalClock); !errors.Is(err, ErrActivate) {
		t.Fatalf("equal-clock Activate() error = %v", err)
	}
	st, err := store.Open(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, readErr := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if readErr != nil || closeErr != nil || authority.Profile.Enabled() ||
		!authority.Profile.UpdatedAt().Equal(provisioned.Profile.UpdatedAt()) {
		t.Fatalf("failed generation fences changed authority = (%#v, %v, close %v)",
			authority, readErr, closeErr)
	}
}
