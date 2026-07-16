package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestEnsureAgentDaemonComposesExactAuthorityAndDefersCompanionDiscovery(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeControlClient{}
	var preflightOptions node.DaemonPreflightOptions
	verified := 0
	companionLookups := 0
	dependencies := agentDaemonEnsureDependencies{
		loadBundle: func() (assets.Bundle, error) { return bundle, nil },
		currentExecutable: func() (string, error) {
			companionLookups++
			return "", errors.New("must stay lazy")
		},
		newPreflight: func(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			preflightOptions = options
			return node.DaemonEnsurePreflightFunc(func(context.Context) error {
				verified++
				return nil
			}), nil
		},
		newLauncher: func(node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			t.Fatal("ready path constructed the production launcher")
			return nil, nil
		},
		ensure: func(ctx context.Context, options node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
			if ctx == nil || options.NodeState != nodeState ||
				options.AssetRevision != bundle.Manifest().AssetRevision || options.Probe != client ||
				options.Preflight == nil || options.Launcher == nil || options.ReadyGate != nil {
				t.Fatalf("Ensure options = %#v", options)
			}
			if err := options.Preflight.Verify(ctx); err != nil {
				t.Fatal(err)
			}
			return node.DaemonEnsureResult{}, nil
		},
	}
	if apiErr := ensureAgentDaemonWith(context.Background(), workspace, nodeState, client,
		dependencies); apiErr != nil {
		t.Fatal(apiErr)
	}
	if preflightOptions.Workspace != workspace || preflightOptions.NodeState != nodeState ||
		preflightOptions.AssetRevision != bundle.Manifest().AssetRevision ||
		preflightOptions.Install == nil || verified != 1 || companionLookups != 0 {
		t.Fatalf("preflight = %#v verified=%d companion lookups=%d", preflightOptions,
			verified, companionLookups)
	}
}

func TestLazyAgentDaemonLauncherResolvesPhysicalSiblingOnlyWhenLaunched(t *testing.T) {
	directory := t.TempDir()
	harnessPath := filepath.Join(directory, "mnemon-harness")
	companionPath := filepath.Join(directory, "mnemond")
	for _, path := range []string{harnessPath, companionPath} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	physicalCompanion, err := filepath.EvalSymlinks(companionPath)
	if err != nil {
		t.Fatal(err)
	}
	workspace, nodeState := cliWorkspace(t)
	launched := 0
	wantHandle := &recordingCLIDaemonLaunch{}
	lazy := &lazyAgentDaemonLauncher{workspace: workspace, nodeState: nodeState,
		currentExecutable: func() (string, error) { return harnessPath, nil },
		newLauncher: func(options node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			if options.Executable != physicalCompanion || options.Workspace != workspace ||
				options.NodeState != nodeState {
				t.Fatalf("launcher options = %#v", options)
			}
			return node.DaemonLauncherFunc(func(context.Context, node.DaemonLaunchPermit) (node.DaemonLaunch, error) {
				launched++
				return wantHandle, nil
			}), nil
		}}
	got, err := lazy.Launch(context.Background(), node.DaemonLaunchPermit{})
	if err != nil || got != wantHandle || launched != 1 {
		t.Fatalf("Launch() = (%#v, %v), launches=%d", got, err, launched)
	}
	if err := os.Remove(companionPath); err != nil {
		t.Fatal(err)
	}
	if got, err := lazy.Launch(context.Background(), node.DaemonLaunchPermit{}); err == nil || got != nil || launched != 1 {
		t.Fatalf("missing companion Launch() = (%#v, %v), launches=%d", got, err, launched)
	}
	target := filepath.Join(t.TempDir(), "mnemond-target")
	if err := os.WriteFile(target, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, companionPath); err != nil {
		t.Fatal(err)
	}
	if got, err := lazy.Launch(context.Background(), node.DaemonLaunchPermit{}); err == nil || got != nil || launched != 1 {
		t.Fatalf("symlink companion Launch() = (%#v, %v), launches=%d", got, err, launched)
	}
}

func TestEnsureAgentDaemonMapsAuthorityFailureSeparatelyFromAvailability(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	base := agentDaemonEnsureDependencies{
		loadBundle:        func() (assets.Bundle, error) { return bundle, nil },
		currentExecutable: func() (string, error) { return "/unused/mnemon-harness", nil },
		newPreflight: func(node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			return node.DaemonEnsurePreflightFunc(func(context.Context) error { return nil }), nil
		},
		newLauncher: func(node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			return nil, errors.New("unused")
		},
	}
	for _, test := range []struct {
		name string
		err  error
		code localapi.ErrorCode
	}{
		{name: "authority", err: node.ErrDaemonPreflight, code: localapi.CodeAssetRevisionMismatch},
		{name: "health authority", err: node.ErrDaemonHealthAuthority, code: localapi.CodeAssetRevisionMismatch},
		{name: "transport", err: errors.New("timeout"), code: localapi.CodeMnemondUnavailable},
		{name: "authenticated", err: localapi.NewAPIError(localapi.CodeAuthenticationFailed,
			"authentication failed"), code: localapi.CodeAuthenticationFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies := base
			dependencies.ensure = func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
				return node.DaemonEnsureResult{}, test.err
			}
			apiErr := ensureAgentDaemonWith(context.Background(), workspace, nodeState,
				&fakeControlClient{}, dependencies)
			if apiErr == nil || apiErr.Code != test.code {
				t.Fatalf("ensure error = %#v, want %s", apiErr, test.code)
			}
		})
	}
}

type recordingCLIDaemonLaunch struct{}

func (*recordingCLIDaemonLaunch) Release() error                  { return nil }
func (*recordingCLIDaemonLaunch) Terminate(context.Context) error { return nil }
