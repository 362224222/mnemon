package cli

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestSetupEnsureOptionsReusesTheEarlyValidatedPreflight(t *testing.T) {
	workspace := t.TempDir()
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	preflight := node.DaemonEnsurePreflightFunc(func(context.Context) error { return nil })
	app := &setupApp{deps: setupDependencies{
		newHookGate: func(string, assets.Host) (node.DaemonReadyGate, error) {
			return node.DaemonReadyGateFunc(func(context.Context, localapi.HealthResponse) error {
				return nil
			}), nil
		},
	}}
	options, apiErr := app.ensureOptions(workspace, nodeState, "asset-r5", preflight,
		assets.HostCodex, nil)
	if apiErr != nil || options.Preflight == nil {
		t.Fatalf("ensureOptions() = (%#v, %v)", options, apiErr)
	}
	if err := options.Preflight.Verify(context.Background()); err != nil {
		t.Fatalf("reused preflight Verify() error = %v", err)
	}
}
