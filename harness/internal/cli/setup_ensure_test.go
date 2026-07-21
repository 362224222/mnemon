package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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

func TestEnsureSetupDaemonRetriesTransientExistingNotReady(t *testing.T) {
	calls := 0
	want := node.DaemonEnsureResult{Health: localapi.HealthResponse{
		AssetRevision: "asset-r5", SchemaVersion: localapi.SchemaVersion, Status: "ready"}}
	got, err := ensureSetupDaemonWith(context.Background(), node.DaemonEnsureOptions{},
		func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
			calls++
			if calls == 1 {
				return node.DaemonEnsureResult{}, fmt.Errorf("%w: existing daemon is settling",
					node.ErrDaemonHealthAuthority)
			}
			return want, nil
		}, 200*time.Millisecond, time.Millisecond)
	if err != nil || got != want || calls != 2 {
		t.Fatalf("ensureSetupDaemonWith() = (%#v, %v), calls=%d", got, err, calls)
	}
}

func TestEnsureSetupDaemonDoesNotRetryNonAuthorityFailure(t *testing.T) {
	calls := 0
	failed := errors.New("transport failed")
	_, err := ensureSetupDaemonWith(context.Background(), node.DaemonEnsureOptions{},
		func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
			calls++
			return node.DaemonEnsureResult{}, failed
		}, 200*time.Millisecond, time.Millisecond)
	if !errors.Is(err, failed) || calls != 1 {
		t.Fatalf("ensureSetupDaemonWith() error = %v, calls=%d", err, calls)
	}
}
