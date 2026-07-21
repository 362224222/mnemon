package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type setupCompanion interface {
	node.DaemonOfflineConfirmer
	Initialize(context.Context, model.HostKind, string) (companionInitializeReceipt, error)
	Inspect(context.Context) (localapi.AuthorityResponse, error)
	Activate(context.Context, model.HostKind, string, time.Time) (companionLifecycleReceipt, error)
	Deactivate(context.Context, model.HostKind, string, time.Time) (companionLifecycleReceipt, error)
}

type setupAuthorityClient interface {
	node.DaemonHealthProbe
	node.DaemonLifecycleClient
	ReadAuthority(context.Context) (localapi.AuthorityResponse, *localapi.APIError)
}

type setupDaemonLifecycle interface {
	Quiesce(context.Context, node.DaemonLifecycleClient, node.DaemonOfflineConfirmer,
		localapi.AuthorityResponse) (localapi.AuthorityResponse, error)
	Ensure(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error)
	Close() error
}

type setupDependencies struct {
	workingDirectory    func() (string, error)
	loadBundle          func() (assets.Bundle, error)
	newCompanion        func(context.Context, string, string) (setupCompanion, error)
	detectHost          func(context.Context, string) (integration.HostObservation, error)
	inspectHost         func(context.Context, assets.Host) (integration.HostObservation, error)
	activationSupported func(integration.HostObservation) bool
	prepareNode         func(string) (string, error)
	canInitialize       func(string) (bool, error)
	acquireLock         func(context.Context, string) (io.Closer, error)
	newClient           func(string) (setupAuthorityClient, error)
	installBundle       func(string, assets.Bundle) error
	installProjection   func(string, string, assets.Host, assets.Bundle) error
	verifyProjection    func(string, string, assets.Host, assets.Bundle) error
	verifyActivation    func(context.Context, string, string, integration.HostObservation,
		assets.Bundle) error
	verifyAbsent     func(string, string, assets.Host, assets.Bundle) error
	preflightUpgrade func(string, string, assets.Host, string,
		assets.Bundle) (integration.HostProjectionUpgradePreflight, error)
	newPreflight      func(node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error)
	currentExecutable func() (string, error)
	newLauncher       func(node.DaemonProcessOptions) (node.DaemonLauncher, error)
	newHookGate       func(string, assets.Host) (node.DaemonReadyGate, error)
	ensure            func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error)
	acquireLifecycle  func(context.Context, node.DaemonLifecycleOptions) (setupDaemonLifecycle, error)
}

type setupApp struct {
	stdout  io.Writer
	stderr  io.Writer
	version string
	deps    setupDependencies
}

type setupRequest struct {
	host        string
	projectRoot string
}

type setupReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Host          string `json:"host"`
	PeerID        string `json:"peer_id"`
	Replayed      bool   `json:"replayed"`
	SchemaVersion int    `json:"schema_version"`
	Started       bool   `json:"started"`
	Status        string `json:"status"`
}

type setupAuthorityObservation struct {
	authority localapi.AuthorityResponse
	client    setupAuthorityClient
	found     bool
	terminal  *localapi.APIError
	fallback  *localapi.APIError
}

func productionSetupDependencies() setupDependencies {
	return setupDependencies{
		workingDirectory: os.Getwd,
		loadBundle:       assets.Load,
		newCompanion: func(ctx context.Context, workspace, version string) (setupCompanion, error) {
			return newCompanionRunner(ctx, workspace, version)
		},
		detectHost:  integration.DetectHost,
		inspectHost: integration.InspectHost,
		activationSupported: func(observation integration.HostObservation) bool {
			return observation.Host.Valid()
		},
		prepareNode:   node.PrepareNodeState,
		canInitialize: setupCanInitialize,
		acquireLock: func(ctx context.Context, nodeState string) (io.Closer, error) {
			return acquireSetupLock(ctx, nodeState)
		},
		newClient: func(nodeState string) (setupAuthorityClient, error) {
			return localapi.NewClient(nodeState)
		},
		installBundle: func(nodeState string, bundle assets.Bundle) error {
			_, err := integration.InstallNodeBundle(nodeState, bundle)
			return err
		},
		installProjection: func(workspace, nodeState string, host assets.Host,
			bundle assets.Bundle,
		) error {
			_, err := integration.InstallHostProjection(workspace, nodeState, host, bundle)
			return err
		},
		verifyProjection: integration.VerifyHostProjection,
		verifyActivation: integration.VerifyHostActivation,
		verifyAbsent:     integration.VerifyHostProjectionAbsent,
		preflightUpgrade: integration.PreflightHostProjectionUpgrade,
		newPreflight: func(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			return node.NewDaemonPreflight(options)
		},
		currentExecutable: os.Executable,
		newLauncher: func(options node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			return node.NewDaemonProcessLauncher(options)
		},
		newHookGate: func(workspace string, host assets.Host) (node.DaemonReadyGate, error) {
			return newSetupHookGate(workspace, host)
		},
		ensure: ensureSetupDaemon,
		acquireLifecycle: func(ctx context.Context,
			options node.DaemonLifecycleOptions,
		) (setupDaemonLifecycle, error) {
			return node.AcquireDaemonLifecycle(ctx, options)
		},
	}
}

func parseSetupRequest(args []string) (setupRequest, *localapi.APIError) {
	request := setupRequest{host: "auto"}
	seen := make(map[string]bool)
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if flag != "--host" && flag != "--project-root" {
			return setupRequest{}, setupError(localapi.CodeInvalidArgument,
				"setup accepts only --host and --project-root")
		}
		if seen[flag] || index+1 >= len(args) || args[index+1] == "" ||
			strings.HasPrefix(args[index+1], "--") {
			return setupRequest{}, setupError(localapi.CodeInvalidArgument,
				flag+" requires exactly one value")
		}
		seen[flag] = true
		index++
		switch flag {
		case "--host":
			request.host = args[index]
		case "--project-root":
			request.projectRoot = args[index]
		}
	}
	if request.host != "auto" && request.host != string(assets.HostCodex) &&
		request.host != string(assets.HostClaudeCode) {
		return setupRequest{}, setupError(localapi.CodeInvalidArgument,
			"--host must be auto, codex, or claude-code")
	}
	return request, nil
}

func resolveSetupWorkspace(requested string, workingDirectory func() (string, error)) (string, error) {
	if workingDirectory == nil {
		return "", errors.New("working directory dependency is unavailable")
	}
	path := requested
	if path == "" {
		var err error
		path, err = workingDirectory()
		if err != nil {
			return "", err
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	physical, err := filepath.EvalSymlinks(absolute)
	if err != nil || !filepath.IsAbs(physical) || filepath.Clean(physical) != physical {
		return "", errors.New("project root is unavailable")
	}
	info, err := os.Lstat(physical)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("project root is not a physical directory")
	}
	return physical, nil
}

func setupCanInitialize(nodeState string) (bool, error) {
	state, err := openSetupLockNodeState(nodeState)
	if err != nil {
		return false, err
	}
	if err := state.close(); err != nil {
		return false, err
	}
	path := filepath.Join(nodeState, "node.db")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		credential := filepath.Join(nodeState, "profiles",
			model.TeamworkProfileID().String()+".token")
		if _, credentialErr := os.Lstat(credential); errors.Is(credentialErr, os.ErrNotExist) {
			return true, nil
		} else if credentialErr != nil {
			return false, credentialErr
		}
		// A projected credential may legitimately precede node.db after a
		// crashed initialize. Reuse it only when the normal closed client
		// validator accepts it; corrupted or unsafe credential state is never
		// treated as a fresh Node.
		if _, credentialErr := localapi.NewClient(nodeState); credentialErr != nil {
			return false, nil
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := validateSetupOwnerPath(info, 0o600, false); err != nil {
		return false, err
	}
	// An existing database is never classified as a fresh/partial initialize,
	// even when its outer file metadata is safe. Its schema and credential
	// bindings must be recovered through exact authority observation or doctor;
	// setup must not ask Provision to reinterpret corrupted durable state.
	return false, nil
}

func (app *setupApp) execute(ctx context.Context,
	request setupRequest,
) (setupReceipt, *localapi.APIError) {
	workspace, err := resolveSetupWorkspace(request.projectRoot, app.deps.workingDirectory)
	if err != nil {
		return setupReceipt{}, setupError(localapi.CodeInvalidArgument,
			"project root must be an existing physical directory")
	}
	bundle, err := app.deps.loadBundle()
	if err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	revision := bundle.Manifest().AssetRevision
	if _, err := model.ParseDigest(revision); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	installation, err := integration.NewManagedInstallationFromBundle(workspace, bundle)
	if err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	preflight, err := app.deps.newPreflight(node.DaemonPreflightOptions{
		Workspace: workspace, NodeState: nodeState, AssetRevision: revision,
		Install: installation, Credentials: localapi.NodeRuntime{},
	})
	if err != nil || preflight == nil {
		return setupReceipt{}, setupAssetsError()
	}
	companion, err := app.deps.newCompanion(ctx, workspace, app.version)
	if err != nil {
		return setupReceipt{}, setupUnavailableError("mnemond companion is unavailable")
	}
	preparedNodeState, err := app.deps.prepareNode(workspace)
	if err != nil || preparedNodeState != nodeState {
		return setupReceipt{}, setupAuthError("managed Node state is unsafe")
	}

	lock, err := app.deps.acquireLock(ctx, nodeState)
	if err != nil || lock == nil {
		return setupReceipt{}, setupUnavailableError("managed setup lock is unavailable")
	}
	receipt, apiErr := app.executeLocked(ctx, request, workspace, nodeState, revision,
		bundle, preflight, companion)
	if closeErr := lock.Close(); closeErr != nil {
		return setupReceipt{}, setupAuthError("managed setup lock changed during setup")
	}
	return receipt, apiErr
}
