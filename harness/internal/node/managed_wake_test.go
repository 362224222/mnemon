package node

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var errManagedWakeTest = errors.New("managed wake test failure")

type managedRuntimeInstallationFixture struct {
	executable        string
	executableErr     error
	verifyErr         error
	executableContext context.Context
	verifyContext     context.Context
	executableCalls   int
	verifyCalls       int
}

func (install *managedRuntimeInstallationFixture) RuntimeExecutable(ctx context.Context,
	_ model.Profile,
) (string, error) {
	install.executableCalls++
	install.executableContext = ctx
	return install.executable, install.executableErr
}

func (install *managedRuntimeInstallationFixture) Verify(ctx context.Context,
	_ model.Profile,
) error {
	install.verifyCalls++
	install.verifyContext = ctx
	return install.verifyErr
}

func TestNewManagedWakeAdapterFactoryRejectsInvalidCompositionAndAuthority(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	install := &managedRuntimeInstallationFixture{executable: "/usr/bin/codex"}
	if factory, err := NewManagedWakeAdapterFactory(".", install); factory != nil || err == nil {
		t.Fatalf("NewManagedWakeAdapterFactory(relative) = (%#v, %v)", factory, err)
	}
	if factory, err := NewManagedWakeAdapterFactory(fixture.workspace, nil); factory != nil || err == nil {
		t.Fatalf("NewManagedWakeAdapterFactory(nil) = (%#v, %v)", factory, err)
	}
	var typedNil *managedRuntimeInstallationFixture
	if factory, err := NewManagedWakeAdapterFactory(fixture.workspace, typedNil); factory != nil || err == nil {
		t.Fatalf("NewManagedWakeAdapterFactory(typed nil) = (%#v, %v)", factory, err)
	}

	factory, err := NewManagedWakeAdapterFactory(fixture.workspace, install)
	if err != nil {
		t.Fatal(err)
	}
	valid := WakeAdapterFactoryOptions{Workspace: fixture.workspace, NodeState: fixture.nodeState,
		Profile: fixture.profile}
	other := newDaemonFixture(t, true)
	otherProfile := fixture.profile.Spec()
	otherProfile.WorkspaceRoot = other.workspace
	foreignProfile, err := model.NewProfile(otherProfile)
	if err != nil {
		t.Fatal(err)
	}
	disabledProfile := fixture.profile.Spec()
	disabledProfile.Enabled = false
	disabled, err := model.NewProfile(disabledProfile)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		ctx     context.Context
		options WakeAdapterFactoryOptions
	}{
		{name: "nil context", options: valid},
		{name: "workspace", ctx: context.Background(), options: withManagedWakeWorkspace(valid, other.workspace)},
		{name: "node state", ctx: context.Background(), options: withManagedWakeNodeState(valid, filepath.Join(other.workspace, "node"))},
		{name: "foreign Profile", ctx: context.Background(), options: withManagedWakeProfile(valid, foreignProfile)},
		{name: "disabled Profile", ctx: context.Background(), options: withManagedWakeProfile(valid, disabled)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := install.executableCalls
			adapter, err := factory.NewWakeAdapter(test.ctx, test.options)
			if adapter != nil || err == nil {
				t.Fatalf("NewWakeAdapter() = (%#v, %v)", adapter, err)
			}
			if install.executableCalls != before {
				t.Fatal("invalid authority reached Runtime executable resolution")
			}
		})
	}
}

func TestManagedWakeAdapterFactoryUsesEachCallerContext(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	install := &managedRuntimeInstallationFixture{
		executable: "/usr/bin/codex",
		verifyErr:  errManagedWakeTest,
	}
	factory, err := NewManagedWakeAdapterFactory(fixture.workspace, install)
	if err != nil {
		t.Fatal(err)
	}
	type contextKey string
	factoryCtx, cancelFactory := context.WithCancel(context.WithValue(context.Background(),
		contextKey("phase"), "factory"))
	adapter, err := factory.NewWakeAdapter(factoryCtx, WakeAdapterFactoryOptions{
		Workspace: fixture.workspace, NodeState: fixture.nodeState, Profile: fixture.profile,
	})
	if err != nil || adapter == nil {
		t.Fatalf("NewWakeAdapter() = (%#v, %v)", adapter, err)
	}
	if install.executableCalls != 1 || install.executableContext != factoryCtx {
		t.Fatalf("RuntimeExecutable context = %#v after %d calls", install.executableContext,
			install.executableCalls)
	}
	cancelFactory()

	runCtx := context.WithValue(context.Background(), contextKey("phase"), "run")
	_, err = adapter.Run(runCtx, agent.CodexWakeRequest{
		RunAttachmentEnvironment: "MNEMON_HARNESS_RUN_ATTACHMENT=/tmp/mnemon-managed-wake-test",
		Callbacks: agent.CodexWakeCallbacks{
			RecordLaunch: func(context.Context, agent.CodexLaunchEvidence) error { return nil },
			RecordWake:   func(context.Context, agent.CodexWakeEvidence) error { return nil },
		},
	})
	if err == nil || install.verifyCalls != 1 || install.verifyContext != runCtx {
		t.Fatalf("Run() error/context/calls = (%v, %#v, %d)", err, install.verifyContext,
			install.verifyCalls)
	}
}

func TestManagedWakeEnvironmentIsClosedAndDeterministic(t *testing.T) {
	t.Parallel()
	input := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
		"R5_NODE_ALIAS=A", "R5_SCENARIO=payment-review",
		"PATH=/untrusted/duplicate", "OPENAI_API_KEY=private", "MNEMON_EVENT_BODY=private",
		"MNEMON_HARNESS_RUN_ATTACHMENT=/private/run.attach",
		"MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD=99", "MALFORMED",
	}
	want := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
	}
	first := managedWakeEnvironment(input)
	second := managedWakeEnvironment(input)
	if strings.Join(first, "\x00") != strings.Join(want, "\x00") ||
		strings.Join(second, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("managedWakeEnvironment() = %q then %q, want %q", first, second, want)
	}
	first[0] = "PATH=/mutated"
	if input[0] != "PATH=/managed/bin" || managedWakeEnvironment(input)[0] != "PATH=/managed/bin" {
		t.Fatal("managedWakeEnvironment shares mutable storage")
	}
}

func withManagedWakeWorkspace(options WakeAdapterFactoryOptions,
	workspace string,
) WakeAdapterFactoryOptions {
	options.Workspace = workspace
	return options
}

func withManagedWakeNodeState(options WakeAdapterFactoryOptions,
	nodeState string,
) WakeAdapterFactoryOptions {
	options.NodeState = nodeState
	return options
}

func withManagedWakeProfile(options WakeAdapterFactoryOptions,
	profile model.Profile,
) WakeAdapterFactoryOptions {
	options.Profile = profile
	return options
}
