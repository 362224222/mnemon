package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type controllerComposition struct {
	service    *agent.Service
	observer   controllerObserver
	wakeWorker managedWakeWorker
}

func validateControllerOptions(options *ControllerOptions) (string, error) {
	if options == nil {
		return "", errors.New("mnemond controller requires canonical Node, workspace, Profile, Store and signer")
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.Store == nil || options.Signer == nil || options.Install == nil ||
		options.Profile.ID() != model.TeamworkProfileID() ||
		!options.Profile.Enabled() || options.NodeState == "" || options.Workspace == "" ||
		!filepath.IsAbs(options.NodeState) || filepath.Clean(options.NodeState) != options.NodeState ||
		!filepath.IsAbs(options.Workspace) || filepath.Clean(options.Workspace) != options.Workspace ||
		options.Profile.WorkspaceRoot() != options.Workspace {
		return "", errors.New("mnemond controller requires canonical Node, workspace, Profile, Store and signer")
	}
	stateInfo, err := os.Lstat(options.NodeState)
	if err != nil || !stateInfo.IsDir() || stateInfo.Mode()&os.ModeSymlink != 0 ||
		stateInfo.Mode().Perm() != 0o700 || options.Store.Path() != filepath.Join(options.NodeState, "node.db") {
		return "", errors.New("mnemond controller Store is outside its owner-only Node state")
	}
	return options.Profile.ActiveAssetRevision(), nil
}

func composeController(ctx context.Context, options *ControllerOptions,
	admission *controllerAdmissionGate,
) (controllerComposition, error) {
	assetRevision := options.Profile.ActiveAssetRevision()
	if err := bindControllerActionPolicy(ctx, options.Profile, options.Install, assetRevision,
		&options.actionPolicy, &options.actionHandlers); err != nil {
		return controllerComposition{}, err
	}
	cas, err := artifact.NewCAS(filepath.Join(options.NodeState, "objects", "sha256"))
	if err != nil {
		return controllerComposition{}, err
	}
	capturer, err := artifact.NewCapturer(options.Workspace, cas, options.Clock.Now)
	if err != nil {
		return controllerComposition{}, err
	}
	capture, err := agent.NewArtifactCaptureCoordinator(capturer, cas, cas, options.Store, options.Clock)
	if err != nil {
		return controllerComposition{}, err
	}
	materializer, err := artifact.NewViewMaterializer(options.NodeState, cas)
	if err != nil {
		return controllerComposition{}, err
	}
	currentViews, err := agent.NewCurrentViewCoordinator(materializer)
	if err != nil {
		return controllerComposition{}, err
	}
	viewValidator, err := agent.NewReadonlyArtifactViewValidator(options.NodeState)
	if err != nil {
		return controllerComposition{}, err
	}
	artifactResolver, err := agent.NewArtifactResolver(capture, viewValidator)
	if err != nil {
		return controllerComposition{}, err
	}
	executor, err := agent.NewTeamworkActionExecutor(options.Store, agent.TeamworkActionExecutorOptions{
		Profile: options.Profile, Actions: options.actionHandlers, Signer: options.Signer,
		Artifacts: artifactResolver, Clock: options.Clock,
	})
	if err != nil {
		return controllerComposition{}, err
	}
	wakeWorker, err := composeControllerWakeWorker(options, admission)
	if err != nil {
		return controllerComposition{}, err
	}
	gate := controllerManagedActivationGate{
		install: controllerActivationGate{expected: options.Profile, install: options.Install},
		worker:  wakeWorker,
	}
	service, err := agent.NewService(options.Store, agent.ServiceOptions{Actions: options.actionHandlers,
		Clock: options.Clock, Executor: executor, CurrentViews: currentViews, ActivationGate: gate})
	if err != nil {
		return controllerComposition{}, err
	}
	channelRuntime, _ := options.Channels.(channelSessionObserver)
	observer := controllerObserver{assetRevision: assetRevision, store: options.Store,
		profile: options.Profile, install: options.Install, gate: gate, wakeWorker: wakeWorker}
	observer.channelRuntime = channelRuntime
	return controllerComposition{service: service, observer: observer, wakeWorker: wakeWorker}, nil
}

func composeControllerWakeWorker(options *ControllerOptions,
	admission *controllerAdmissionGate,
) (managedWakeWorker, error) {
	wakeWorker := options.wakeWorker
	if wakeWorker != nil && options.WakeAdapter != nil {
		return nil, errors.New("mnemond controller accepts only one managed wake worker")
	}
	if wakeWorker == nil && options.WakeAdapter != nil {
		var err error
		wakeWorker, err = newManagedWakeWorker(options.Store, options.NodeState, options.Profile,
			options.Clock, options.Install, options.WakeAdapter, admission, options.Control)
		if err != nil {
			return nil, fmt.Errorf("mnemond controller managed wake worker: %w", err)
		}
	}
	return wakeWorker, nil
}

type controllerObserver struct {
	assetRevision  string
	store          *store.Store
	profile        model.Profile
	install        InstallationVerifier
	gate           controllerManagedActivationGate
	wakeWorker     managedWakeWorker
	channelRuntime channelSessionObserver
}

func (observer controllerObserver) Health(ctx context.Context,
	metadata RequestMetadata,
) (HealthSnapshot, *APIError) {
	ready := observer.gate.Check(ctx, metadata.Profile) == nil
	return HealthSnapshot{AssetRevision: observer.assetRevision, WorkersReady: ready}, nil
}

func (observer controllerObserver) Status(ctx context.Context,
	metadata RequestMetadata,
) (StatusSnapshot, *APIError) {
	if ctx == nil || ctx.Err() != nil || metadata.Profile.ID() != model.TeamworkProfileID() {
		return StatusSnapshot{}, NewAPIError(CodeInternal,
			"operational status observation was cancelled")
	}
	current, err := observer.store.ReadLocalAuthority(ctx)
	if err != nil {
		return StatusSnapshot{}, NewAPIError(CodeInternal,
			"durable operational status is unavailable")
	}
	activationIssue := observer.activationIssue(ctx, current)
	worker := agent.WakeWorkerSnapshot{Healthy: true}
	if observer.wakeWorker != nil {
		worker = observer.wakeWorker.Snapshot()
	}
	channels, err := observer.store.ReadChannelObservation(ctx)
	if err != nil {
		return StatusSnapshot{}, NewAPIError(CodeInternal,
			"durable Channel progress is unavailable")
	}
	return StatusSnapshot{AssetRevision: observer.assetRevision,
		ActivationReady: activationIssue == "", ActivationIssue: activationIssue,
		Runtime: RuntimeStatusSnapshot{Running: worker.Running, Ready: worker.Ready,
			Healthy: worker.Healthy, Recovering: worker.Recovering, Issue: worker.LastError},
		Channels: projectStatusChannels(channels, observer.channelRuntime)}, nil
}

func (observer controllerObserver) activationIssue(ctx context.Context, current store.LocalAuthority) string {
	if !sameControllerProfile(current.Profile, observer.profile) {
		return "durable_authority_mismatch"
	}
	if current.Node.ActiveAssetRevision() != observer.assetRevision ||
		observer.install.Verify(ctx, observer.profile) != nil {
		return "asset_revision_mismatch"
	}
	return ""
}

func (observer controllerObserver) Authority(ctx context.Context,
	metadata RequestMetadata,
) (AuthoritySnapshot, *APIError) {
	if ctx == nil || ctx.Err() != nil || metadata.Profile.ID() != model.TeamworkProfileID() {
		return AuthoritySnapshot{}, NewAPIError(CodeInternal,
			"durable authority observation was cancelled")
	}
	current, err := observer.store.ReadLocalAuthority(ctx)
	if err != nil {
		return AuthoritySnapshot{}, NewAPIError(CodeInternal,
			"durable authority is unavailable")
	}
	return authoritySnapshot(current), nil
}

type controllerActivationGate struct {
	expected model.Profile
	install  InstallationVerifier
}

// controllerManagedActivationGate keeps the four Agent routes closed until
// both the canonical installation and the single managed Runtime worker are
// ready. A nil worker preserves the explicit low-level composition seam.
type controllerManagedActivationGate struct {
	install controllerActivationGate
	worker  managedWakeWorker
}

func (gate controllerManagedActivationGate) Check(ctx context.Context,
	profile model.Profile,
) *agent.ControlError {
	if apiErr := gate.install.Check(ctx, profile); apiErr != nil {
		return apiErr
	}
	if gate.worker == nil {
		return nil
	}
	snapshot := gate.worker.Snapshot()
	if !snapshot.Running || !snapshot.Ready || !snapshot.Healthy || snapshot.Recovering {
		return agent.NewControlError(agent.CodeMnemondUnavailable,
			"managed Runtime worker is not ready")
	}
	return nil
}

func (gate controllerActivationGate) Check(ctx context.Context, profile model.Profile) *agent.ControlError {
	if ctx == nil || ctx.Err() != nil {
		return agent.NewControlError(agent.CodeInternal, "managed activation check was cancelled")
	}
	if gate.install == nil || !sameControllerProfile(profile, gate.expected) ||
		gate.install.Verify(ctx, profile) != nil {
		return agent.NewControlError(agent.CodeAssetRevisionMismatch,
			"managed Node assets or Host projection differ from the active Profile")
	}
	return nil
}

func sameControllerProfile(got, want model.Profile) bool {
	return got.ID() == want.ID() && got.Principal() == want.Principal() &&
		got.WorkspaceRoot() == want.WorkspaceRoot() && got.Host() == want.Host() &&
		got.Runtime() == want.Runtime() && got.CredentialHash() == want.CredentialHash() &&
		got.ActiveAssetRevision() == want.ActiveAssetRevision() && got.UpdatedAt().Equal(want.UpdatedAt()) &&
		got.HandlingBudget().String() == want.HandlingBudget().String() && got.Enabled() && want.Enabled()
}

// bindControllerActionPolicy verifies the physical installation and fills the
// local options snapshot with the immutable semantic policy for that exact
// revision. A daemon may supply its precomposed snapshot; standalone
// controllers compose one only after installation verification succeeds.
func bindControllerActionPolicy(ctx context.Context, profile model.Profile,
	install InstallationVerifier, revision string, policy *agent.ActionPolicy,
	handlers *agent.ActionHandlers,
) error {
	if isNilNodeInterface(install) || policy == nil || handlers == nil {
		return errors.New("mnemond controller installation or action policy is unavailable")
	}
	if err := install.Verify(ctx, profile); err != nil {
		return fmt.Errorf("mnemond controller managed installation: %w", err)
	}
	if policy.AssetRevision().IsZero() {
		composed, err := actionPolicyForInstallation(install)
		if err != nil {
			return fmt.Errorf("mnemond controller action policy: %w", err)
		}
		*policy = composed
	}
	if policy.AssetRevision().String() != revision {
		return errors.New("mnemond controller action policy differs from the active asset revision")
	}
	composed, err := agent.NewActionHandlers(*policy)
	if err != nil {
		return fmt.Errorf("mnemond controller Action handlers: %w", err)
	}
	*handlers = composed
	if handlers.AssetRevision().String() != revision {
		return errors.New("mnemond controller Action handlers differ from the active asset revision")
	}
	return nil
}
