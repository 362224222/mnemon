package cli

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestSetupEnsureOptionsPreservesInstallationCancellation(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	revision := bundle.Manifest().AssetRevision
	var install node.InstallationVerifier
	ctx, cancel := context.WithCancel(context.Background())
	app := &setupApp{deps: setupDependencies{
		verifyProjection: func(string, string, assets.Host, assets.Bundle) error {
			cancel()
			return nil
		},
		newPreflight: func(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			install = options.Install
			return node.DaemonEnsurePreflightFunc(func(context.Context) error { return nil }), nil
		},
		newHookGate: func(string, assets.Host) (node.DaemonReadyGate, error) {
			return node.DaemonReadyGateFunc(func(context.Context, localapi.HealthResponse) error {
				return nil
			}), nil
		},
	}}
	if _, apiErr := app.ensureOptions(workspace, nodeState, revision, bundle,
		assets.HostCodex, nil); apiErr != nil || install == nil {
		t.Fatalf("ensureOptions() = install %v, error %v", install != nil, apiErr)
	}
	at := time.Date(2026, time.July, 18, 1, 0, 0, 0, time.UTC)
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "setup-ensure", WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("setup-ensure")),
		ActiveAssetRevision: revision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := install.Verify(ctx, profile); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify(projection-cancelled) error = %v", err)
	}
}
