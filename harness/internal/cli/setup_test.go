package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestSetupFreshRunsTheLockedStateMachineAndEmitsCanonicalReceipt(t *testing.T) {
	fixture := newSetupFixture(t, assets.HostCodex, false)
	fixture.clientFailures = 1
	fixture.ensureResult.Started = true

	exit, stdout, stderr := fixture.run()
	if exit != 0 || stderr != "" {
		t.Fatalf("setup = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	wantReceipt := setupReceipt{AssetRevision: fixture.revision, Host: "codex",
		PeerID: fixture.authority.PeerID, Replayed: false, SchemaVersion: localapi.SchemaVersion,
		Started: true, Status: "ready"}
	raw, err := model.CanonicalMarshal(wantReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if stdout != string(raw)+"\n" || strings.Contains(stdout, "token") ||
		strings.Count(stdout, "\n") != 1 {
		t.Fatalf("setup receipt = %q, want %q", stdout, string(raw)+"\n")
	}
	fixture.wantOrder(t,
		"cwd", "load-bundle", "new-companion", "bootstrap", "lock", "new-client",
		"can-initialize", "detect:auto", "initialize:codex", "inspect",
		"install-bundle", "install-projection:codex", "verify-projection:codex",
		"inspect-host:codex", "new-client", "new-preflight", "new-gate", "activate:codex",
		"ensure", "unlock")
}

func TestSetupEightConcurrentFreshCallersInitializeExactlyOnceUnderSetupLock(t *testing.T) {
	const callers = 8
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	revision := bundle.Manifest().AssetRevision
	state := &concurrentFreshSetupState{
		inactive: setupTestAuthority(t, assets.HostCodex, false, revision),
		active: setupTestAuthorityAt(t, assets.HostCodex, true, revision,
			setupTestActivationTime()),
	}
	companion := &concurrentFreshSetupCompanion{state: state, revision: revision}
	dependencies := concurrentFreshSetupDependencies(workspace, bundle, state, companion)
	start := make(chan struct{})
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-start
			app := &setupApp{stdout: io.Discard, stderr: io.Discard, version: "test-r5",
				deps: dependencies}
			receipt, apiErr := app.execute(context.Background(), setupRequest{host: "auto"})
			if apiErr != nil {
				errorsFound <- apiErr
				return
			}
			if receipt.Status != "ready" || receipt.Host != "codex" ||
				receipt.AssetRevision != revision {
				errorsFound <- fmt.Errorf("unexpected receipt: %#v", receipt)
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent setup: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.initializeCalls != 1 || state.maximumInitializers != 1 ||
		state.activeInitializers != 0 {
		t.Fatalf("initializers calls=%d maximum=%d active=%d", state.initializeCalls,
			state.maximumInitializers, state.activeInitializers)
	}
}

func TestSetupExistingAuthorityFreezesAutoAndFailsBeforeActiveMutations(t *testing.T) {
	t.Run("auto freezes durable Host and exact replay never activates", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostClaudeCode, true)
		exit, stdout, stderr := fixture.run()
		if exit != 0 || stderr != "" || !strings.Contains(stdout, `"host":"claude-code"`) ||
			!strings.Contains(stdout, `"replayed":true`) {
			t.Fatalf("replay = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t,
			"cwd", "load-bundle", "new-companion", "bootstrap", "lock",
			"new-client", "read-authority", "install-bundle",
			"install-projection:claude-code", "verify-projection:claude-code",
			"inspect-host:claude-code", "new-preflight", "new-gate", "ensure", "unlock")
	})

	t.Run("explicit different Host is a domain conflict", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, true)
		exit, stdout, stderr := fixture.run("--host", "claude-code")
		if exit != 4 || stdout != "" || stderr !=
			"profile_host_mismatch: managed Profile is bound to another Host; eject is required before switching\n" {
			t.Fatalf("conflict = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "bootstrap",
			"lock", "new-client", "read-authority", "unlock")
	})

	t.Run("different active revision requires explicit upgrade before projection", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, true)
		fixture.authority = setupTestAuthority(t, assets.HostCodex, true,
			model.Sum([]byte("previous managed revision")).String())
		exit, stdout, stderr := fixture.run()
		if exit != 3 || stdout != "" || stderr !=
			"asset_revision_mismatch: enabled managed Profile requires an explicit revision upgrade\n" {
			t.Fatalf("upgrade gate = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "bootstrap",
			"lock", "new-client", "read-authority", "unlock")
	})
}

func TestSetupRepairsDisabledAuthorityAndPartialInitialization(t *testing.T) {
	t.Run("disabled Profile still requires eject evidence before Host switch", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, false)
		exit, stdout, stderr := fixture.run("--host", "claude-code")
		if exit != 4 || stdout != "" || stderr !=
			"profile_host_mismatch: managed Profile is bound to another Host; eject is required before switching\n" {
			t.Fatalf("disabled switch = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "bootstrap", "lock",
			"new-client", "read-authority", "unlock")
	})

	t.Run("safe partial Node initializes only after taking setup lock", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, false)
		fixture.clientFailures = 1
		fixture.canInitialize = true
		exit, stdout, stderr := fixture.run()
		if exit != 0 || stdout == "" || stderr != "" {
			t.Fatalf("partial repair = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t,
			"cwd", "load-bundle", "new-companion", "bootstrap", "lock",
			"new-client", "can-initialize", "detect:auto", "initialize:codex", "inspect",
			"install-bundle", "install-projection:codex", "verify-projection:codex",
			"inspect-host:codex", "new-client", "new-preflight", "new-gate",
			"activate:codex", "ensure", "unlock")
	})

	t.Run("unsafe partial Node is not initialized", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, false)
		fixture.clientFailures = 1
		fixture.canInitialize = false
		exit, stdout, stderr := fixture.run()
		if exit != 3 || stdout != "" || stderr !=
			"authentication_failed: managed Profile credential is unavailable\n" {
			t.Fatalf("unsafe partial = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "bootstrap",
			"lock", "new-client", "can-initialize", "unlock")
	})
}

func TestSetupStaticAndHostGatesFailBeforeActivation(t *testing.T) {
	tests := []struct {
		name      string
		failure   string
		exit      int
		lastStage string
	}{
		{name: "projection", failure: "install-projection", exit: 3,
			lastStage: "install-projection:codex"},
		{name: "projection verification", failure: "verify-projection", exit: 3,
			lastStage: "verify-projection:codex"},
		{name: "Host", failure: "inspect-host", exit: 5,
			lastStage: "inspect-host:codex"},
		{name: "Hook gate construction", failure: "new-gate", exit: 3,
			lastStage: "new-gate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSetupFixture(t, assets.HostCodex, false)
			fixture.fail[test.failure] = errors.New("injected gate failure")
			exit, stdout, stderr := fixture.run()
			if exit != test.exit || stdout != "" || stderr == "" {
				t.Fatalf("failed setup = exit %d stdout %q stderr %q", exit, stdout, stderr)
			}
			if fixture.called("activate:codex") || fixture.called("ensure") ||
				fixture.called("deactivate:codex") {
				t.Fatalf("post-failure authority mutation: %v", fixture.order)
			}
			if len(fixture.order) < 2 || fixture.order[len(fixture.order)-2] != test.lastStage ||
				fixture.order[len(fixture.order)-1] != "unlock" {
				t.Fatalf("failure order = %v", fixture.order)
			}
		})
	}
}

func TestSetupRollsBackOnlyWithAClosedEnsureCompensationProof(t *testing.T) {
	tests := []struct {
		name         string
		enabled      bool
		started      bool
		outcome      node.DaemonEnsureFailureOutcome
		wantRollback bool
	}{
		{name: "fresh owned child cleaned", started: true,
			outcome: node.DaemonEnsureFailureOwnedChildCleaned, wantRollback: true},
		{name: "fresh prelaunch compensation fence",
			outcome: node.DaemonEnsureFailureCompensationFenced, wantRollback: true},
		{name: "fresh unproven no child", outcome: node.DaemonEnsureFailureUnproven},
		{name: "fresh failed child termination", started: true,
			outcome: node.DaemonEnsureFailureUnproven},
		{name: "active replay owned child", enabled: true, started: true,
			outcome: node.DaemonEnsureFailureOwnedChildCleaned},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSetupFixture(t, assets.HostCodex, test.enabled)
			fixture.ensureResult.Started = test.started
			fixture.ensureResult.FailureOutcome = test.outcome
			fixture.ensureError = fmt.Errorf("%w: injected projected Hook failure", errSetupHookGate)
			exit, stdout, stderr := fixture.run()
			if exit != 3 || stdout != "" || stderr !=
				"asset_revision_mismatch: canonical managed assets or projection are invalid\n" {
				t.Fatalf("gate failure = exit %d stdout %q stderr %q", exit, stdout, stderr)
			}
			if got := fixture.called("deactivate:codex"); got != test.wantRollback {
				t.Fatalf("deactivate called = %t, want %t; order %v", got, test.wantRollback,
					fixture.order)
			}
			if test.wantRollback {
				wantTail := []string{"activate:codex", "ensure", "deactivate:codex", "unlock"}
				if !reflect.DeepEqual(fixture.order[len(fixture.order)-len(wantTail):], wantTail) {
					t.Fatalf("rollback tail = %v", fixture.order)
				}
			}
		})
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

type concurrentFreshSetupState struct {
	mu                  sync.Mutex
	inactive            localapi.AuthorityResponse
	active              localapi.AuthorityResponse
	initialized         bool
	initializeCalls     int
	activeInitializers  int
	maximumInitializers int
}

type concurrentFreshSetupCompanion struct {
	state    *concurrentFreshSetupState
	revision string
}

func (companion *concurrentFreshSetupCompanion) Initialize(context.Context, model.HostKind,
	string,
) (companionInitializeReceipt, error) {
	state := companion.state
	state.mu.Lock()
	state.initializeCalls++
	state.activeInitializers++
	if state.activeInitializers > state.maximumInitializers {
		state.maximumInitializers = state.activeInitializers
	}
	state.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	state.mu.Lock()
	state.initialized = true
	state.activeInitializers--
	state.mu.Unlock()
	return companionInitializeReceipt{AssetRevision: companion.revision, Created: true,
		Host: "codex", SchemaVersion: model.SchemaVersion, Status: "initialized"}, nil
}

func (companion *concurrentFreshSetupCompanion) Inspect(context.Context) (localapi.AuthorityResponse,
	error,
) {
	companion.state.mu.Lock()
	defer companion.state.mu.Unlock()
	return companion.state.inactive, nil
}

func (companion *concurrentFreshSetupCompanion) Activate(context.Context, model.HostKind,
	string, time.Time,
) (companionLifecycleReceipt, error) {
	companion.state.mu.Lock()
	companion.state.initialized = true
	companion.state.mu.Unlock()
	return companionLifecycleReceipt{AssetRevision: companion.revision, Changed: true,
		Host: "codex", SchemaVersion: model.SchemaVersion, Status: "active",
		UpdatedAt: setupTestActivationTime().Format(time.RFC3339Nano)}, nil
}

func (*concurrentFreshSetupCompanion) Deactivate(context.Context, model.HostKind,
	string, time.Time,
) (companionLifecycleReceipt, error) {
	return companionLifecycleReceipt{}, errors.New("concurrent happy path cannot deactivate")
}

type concurrentFreshSetupClient struct{ state *concurrentFreshSetupState }

func (client *concurrentFreshSetupClient) ReadAuthority(context.Context) (localapi.AuthorityResponse,
	*localapi.APIError,
) {
	client.state.mu.Lock()
	defer client.state.mu.Unlock()
	return client.state.active, nil
}

func (client *concurrentFreshSetupClient) ProbeHealth(context.Context) (localapi.HealthResponse,
	*localapi.APIError,
) {
	return localapi.HealthResponse{}, nil
}

func concurrentFreshSetupDependencies(workspace string, bundle assets.Bundle,
	state *concurrentFreshSetupState, companion setupCompanion,
) setupDependencies {
	revision := bundle.Manifest().AssetRevision
	return setupDependencies{
		workingDirectory: func() (string, error) { return workspace, nil },
		loadBundle:       func() (assets.Bundle, error) { return bundle, nil },
		newCompanion: func(context.Context, string, string) (setupCompanion, error) {
			return companion, nil
		},
		detectHost: func(context.Context, string) (integration.HostObservation, error) {
			return integration.HostObservation{Host: assets.HostCodex}, nil
		},
		inspectHost: func(context.Context, assets.Host) (integration.HostObservation, error) {
			return integration.HostObservation{Host: assets.HostCodex}, nil
		},
		prepareNode: node.PrepareNodeState,
		canInitialize: func(string) (bool, error) {
			state.mu.Lock()
			defer state.mu.Unlock()
			return !state.initialized, nil
		},
		acquireLock: func(ctx context.Context, nodeState string) (io.Closer, error) {
			return acquireSetupLock(ctx, nodeState)
		},
		newClient: func(string) (setupAuthorityClient, error) {
			state.mu.Lock()
			defer state.mu.Unlock()
			if !state.initialized {
				return nil, errors.New("fresh authority is unavailable")
			}
			return &concurrentFreshSetupClient{state: state}, nil
		},
		installBundle:     func(string, assets.Bundle) error { return nil },
		installProjection: func(string, string, assets.Host, assets.Bundle) error { return nil },
		verifyProjection:  func(string, string, assets.Host, assets.Bundle) error { return nil },
		newPreflight: func(node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			return node.DaemonEnsurePreflightFunc(func(context.Context) error { return nil }), nil
		},
		currentExecutable: func() (string, error) { return "/unused/mnemon-harness", nil },
		newLauncher: func(node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			return nil, errors.New("happy-path fake never launches")
		},
		newHookGate: func(string, assets.Host) (node.DaemonReadyGate, error) {
			return node.DaemonReadyGateFunc(func(context.Context, localapi.HealthResponse) error {
				return nil
			}), nil
		},
		ensure: func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
			return node.DaemonEnsureResult{Health: localapi.HealthResponse{AssetRevision: revision,
				SchemaVersion: localapi.SchemaVersion, Status: "ready"}}, nil
		},
	}
}

type setupFixture struct {
	t              *testing.T
	workspace      string
	revision       string
	bundle         assets.Bundle
	authority      localapi.AuthorityResponse
	detectedHost   assets.Host
	canInitialize  bool
	clientFailures int
	clientCalls    int
	readError      *localapi.APIError
	ensureResult   node.DaemonEnsureResult
	ensureError    error
	fail           map[string]error
	order          []string
	stdout         bytes.Buffer
	stderr         bytes.Buffer
}

func newSetupFixture(t *testing.T, host assets.Host, enabled bool) *setupFixture {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	revision := bundle.Manifest().AssetRevision
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &setupFixture{t: t, workspace: workspace, revision: revision, bundle: bundle,
		authority: setupTestAuthority(t, host, enabled, revision), detectedHost: assets.HostCodex,
		canInitialize: true, fail: make(map[string]error),
		ensureResult: node.DaemonEnsureResult{Health: localapi.HealthResponse{
			SchemaVersion: localapi.SchemaVersion, Status: "ready", AssetRevision: revision}}}
}

func (fixture *setupFixture) app() *setupApp {
	t := fixture.t
	companion := &fakeSetupCompanion{fixture: fixture}
	dependencies := setupDependencies{
		workingDirectory: func() (string, error) {
			fixture.record("cwd")
			return fixture.workspace, nil
		},
		loadBundle: func() (assets.Bundle, error) {
			fixture.record("load-bundle")
			if err := fixture.fail["load-bundle"]; err != nil {
				return assets.Bundle{}, err
			}
			return fixture.bundle, nil
		},
		newCompanion: func(ctx context.Context, workspace, version string) (setupCompanion, error) {
			fixture.record("new-companion")
			if ctx == nil || workspace != fixture.workspace || version != "test-r5" {
				t.Fatalf("companion boundary = (%v, %q, %q)", ctx, workspace, version)
			}
			return companion, fixture.fail["new-companion"]
		},
		detectHost: func(ctx context.Context, selection string) (integration.HostObservation, error) {
			fixture.record("detect:" + selection)
			if ctx == nil {
				t.Fatal("nil Host detection context")
			}
			return integration.HostObservation{Host: fixture.detectedHost}, fixture.fail["detect-host"]
		},
		inspectHost: func(ctx context.Context, host assets.Host) (integration.HostObservation, error) {
			fixture.record("inspect-host:" + string(host))
			return integration.HostObservation{Host: host}, fixture.fail["inspect-host"]
		},
		prepareNode: func(workspace string) (string, error) {
			fixture.record("bootstrap")
			if workspace != fixture.workspace {
				t.Fatalf("bootstrap workspace = %q, want %q", workspace, fixture.workspace)
			}
			return filepath.Join(workspace, ".mnemon", "harness", "node"), fixture.fail["bootstrap"]
		},
		canInitialize: func(path string) (bool, error) {
			fixture.record("can-initialize")
			fixture.wantNodeState(t, path)
			return fixture.canInitialize, fixture.fail["can-initialize"]
		},
		acquireLock: func(ctx context.Context, path string) (io.Closer, error) {
			fixture.record("lock")
			fixture.wantNodeState(t, path)
			if err := fixture.fail["lock"]; err != nil {
				return nil, err
			}
			return fakeSetupCloser{fixture: fixture}, nil
		},
		newClient: func(path string) (setupAuthorityClient, error) {
			fixture.record("new-client")
			fixture.wantNodeState(t, path)
			fixture.clientCalls++
			if fixture.clientCalls <= fixture.clientFailures {
				return nil, errors.New("injected client construction failure")
			}
			return &fakeSetupAuthorityClient{fixture: fixture}, nil
		},
		installBundle: func(path string, bundle assets.Bundle) error {
			fixture.record("install-bundle")
			fixture.wantNodeState(t, path)
			return fixture.fail["install-bundle"]
		},
		installProjection: func(workspace, nodeState string, host assets.Host,
			bundle assets.Bundle,
		) error {
			fixture.record("install-projection:" + string(host))
			fixture.wantWorkspace(t, workspace, nodeState)
			return fixture.fail["install-projection"]
		},
		verifyProjection: func(workspace, nodeState string, host assets.Host,
			bundle assets.Bundle,
		) error {
			fixture.record("verify-projection:" + string(host))
			fixture.wantWorkspace(t, workspace, nodeState)
			return fixture.fail["verify-projection"]
		},
		newPreflight: func(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			fixture.record("new-preflight")
			fixture.wantWorkspace(t, options.Workspace, options.NodeState)
			if options.AssetRevision != fixture.revision || options.Install == nil {
				t.Fatalf("preflight authority = %#v", options)
			}
			if err := fixture.fail["new-preflight"]; err != nil {
				return nil, err
			}
			return node.DaemonEnsurePreflightFunc(func(context.Context) error { return nil }), nil
		},
		currentExecutable: func() (string, error) {
			fixture.record("current-executable")
			return "/unused/mnemon-harness", nil
		},
		newLauncher: func(node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			fixture.record("process-launcher")
			return nil, errors.New("fake ensure must preserve lazy launch")
		},
		newHookGate: func(workspace string, host assets.Host) (node.DaemonReadyGate, error) {
			fixture.record("new-gate")
			if workspace != fixture.workspace || !host.Valid() {
				t.Fatalf("Hook gate authority = (%q, %q)", workspace, host)
			}
			if err := fixture.fail["new-gate"]; err != nil {
				return nil, err
			}
			return node.DaemonReadyGateFunc(func(context.Context, localapi.HealthResponse) error {
				return nil
			}), nil
		},
		ensure: func(ctx context.Context, options node.DaemonEnsureOptions) (node.DaemonEnsureResult, error) {
			fixture.record("ensure")
			fixture.wantNodeState(t, options.NodeState)
			if ctx == nil || options.AssetRevision != fixture.revision || options.Probe == nil ||
				options.Preflight == nil || options.Launcher == nil || options.ReadyGate == nil {
				t.Fatalf("ensure options = %#v", options)
			}
			return fixture.ensureResult, fixture.ensureError
		},
	}
	return &setupApp{stdout: &fixture.stdout, stderr: &fixture.stderr, version: "test-r5",
		deps: dependencies}
}

func (fixture *setupFixture) run(args ...string) (int, string, string) {
	fixture.t.Helper()
	exit := fixture.app().run(context.Background(), args)
	return exit, fixture.stdout.String(), fixture.stderr.String()
}

func (fixture *setupFixture) record(stage string) { fixture.order = append(fixture.order, stage) }

func (fixture *setupFixture) wantOrder(t *testing.T, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(fixture.order, want) {
		t.Fatalf("setup order =\n%v\nwant\n%v", fixture.order, want)
	}
}

func (fixture *setupFixture) called(stage string) bool {
	for _, value := range fixture.order {
		if value == stage {
			return true
		}
	}
	return false
}

func (fixture *setupFixture) wantNodeState(t *testing.T, path string) {
	t.Helper()
	want := filepath.Join(fixture.workspace, ".mnemon", "harness", "node")
	if path != want {
		t.Fatalf("Node state = %q, want %q", path, want)
	}
}

func (fixture *setupFixture) wantWorkspace(t *testing.T, workspace, nodeState string) {
	t.Helper()
	if workspace != fixture.workspace {
		t.Fatalf("workspace = %q, want %q", workspace, fixture.workspace)
	}
	fixture.wantNodeState(t, nodeState)
}

type fakeSetupCompanion struct{ fixture *setupFixture }

func (companion *fakeSetupCompanion) Initialize(_ context.Context, host model.HostKind,
	revision string,
) (companionInitializeReceipt, error) {
	companion.fixture.record("initialize:" + string(host))
	if err := companion.fixture.fail["initialize"]; err != nil {
		return companionInitializeReceipt{}, err
	}
	companion.fixture.authority = setupTestAuthority(companion.fixture.t, assets.Host(host), false, revision)
	return companionInitializeReceipt{AssetRevision: revision, Created: true, Host: string(host),
		SchemaVersion: model.SchemaVersion, Status: "initialized"}, nil
}

func (companion *fakeSetupCompanion) Inspect(context.Context) (localapi.AuthorityResponse, error) {
	companion.fixture.record("inspect")
	if err := companion.fixture.fail["inspect"]; err != nil {
		return localapi.AuthorityResponse{}, err
	}
	return companion.fixture.authority, nil
}

func (companion *fakeSetupCompanion) Activate(_ context.Context, host model.HostKind,
	revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	companion.fixture.record("activate:" + string(host))
	if err := companion.fixture.fail["activate"]; err != nil {
		return companionLifecycleReceipt{}, err
	}
	wantExpected, err := parseSetupAuthorityTime(companion.fixture.authority.UpdatedAt)
	if err != nil || !expectedUpdatedAt.Equal(wantExpected) {
		return companionLifecycleReceipt{}, errors.New("activation generation mismatch")
	}
	companion.fixture.authority = setupTestAuthorityAt(companion.fixture.t, assets.Host(host), true,
		revision, setupTestActivationTime())
	return companionLifecycleReceipt{AssetRevision: revision, Changed: true, Host: string(host),
		SchemaVersion: model.SchemaVersion, Status: "active",
		UpdatedAt: setupTestActivationTime().Format(time.RFC3339Nano)}, nil
}

func (companion *fakeSetupCompanion) Deactivate(_ context.Context, host model.HostKind,
	revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	companion.fixture.record("deactivate:" + string(host))
	if err := companion.fixture.fail["deactivate"]; err != nil {
		return companionLifecycleReceipt{}, err
	}
	if !expectedUpdatedAt.Equal(setupTestActivationTime()) {
		return companionLifecycleReceipt{}, errors.New("deactivation generation mismatch")
	}
	deactivatedAt := setupTestActivationTime().Add(time.Second)
	companion.fixture.authority = setupTestAuthorityAt(companion.fixture.t, assets.Host(host), false,
		revision, deactivatedAt)
	return companionLifecycleReceipt{AssetRevision: revision, Changed: true, Host: string(host),
		SchemaVersion: model.SchemaVersion, Status: "inactive",
		UpdatedAt: deactivatedAt.Format(time.RFC3339Nano)}, nil
}

type fakeSetupAuthorityClient struct{ fixture *setupFixture }

func (client *fakeSetupAuthorityClient) ReadAuthority(context.Context) (localapi.AuthorityResponse,
	*localapi.APIError,
) {
	client.fixture.record("read-authority")
	return client.fixture.authority, client.fixture.readError
}

func (client *fakeSetupAuthorityClient) ProbeHealth(context.Context) (localapi.HealthResponse,
	*localapi.APIError,
) {
	return client.fixture.ensureResult.Health, nil
}

type fakeSetupCloser struct{ fixture *setupFixture }

func (closer fakeSetupCloser) Close() error {
	closer.fixture.record("unlock")
	return closer.fixture.fail["unlock"]
}

func setupTestAuthority(t *testing.T, host assets.Host, enabled bool,
	revision string,
) localapi.AuthorityResponse {
	return setupTestAuthorityAt(t, host, enabled, revision,
		time.Date(2026, time.July, 17, 4, 0, 0, 0, time.UTC))
}

func setupTestAuthorityAt(t *testing.T, host assets.Host, enabled bool,
	revision string, updatedAt time.Time,
) localapi.AuthorityResponse {
	t.Helper()
	runtimeKind, ok := model.RuntimeForHost(model.HostKind(host))
	if !ok {
		t.Fatalf("invalid test Host %q", host)
	}
	peerID, err := model.ParsePeerID("peer-setup-test")
	if err != nil {
		t.Fatal(err)
	}
	response, err := localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{Host: model.HostKind(host),
		Runtime: runtimeKind, Enabled: enabled, AssetRevision: revision,
		UpdatedAt: updatedAt, PeerID: peerID,
		ActiveAssetRevision: revision})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func setupTestActivationTime() time.Time {
	return time.Date(2026, time.July, 17, 5, 0, 0, 0, time.UTC)
}
