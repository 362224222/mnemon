package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
)

type managedMeshTransport interface {
	Run(context.Context) error
	Readiness(context.Context) error
	Close() error
}

type managedChannelRuntime interface {
	Run(context.Context) error
	Readiness(context.Context) error
}

var _ managedChannelRuntime = (*ChannelRuntime)(nil)

type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

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

// prepareControllerOptions performs every validation that must precede
// daemon-owned filesystem composition. It deliberately does not construct or
// validate Artifact CAS: the Daemon installs that exact-root capability only
// after this preflight succeeds.
func prepareControllerOptions(ctx context.Context,
	options ControllerOptions,
) (ControllerOptions, error) {
	var err error
	options, err = validateControllerOptions(ctx, options)
	if err != nil {
		return ControllerOptions{}, err
	}
	stateInfo, err := os.Lstat(options.NodeState)
	if err != nil || !stateInfo.IsDir() || stateInfo.Mode()&os.ModeSymlink != 0 ||
		stateInfo.Mode().Perm() != 0o700 ||
		options.Store.Path() != filepath.Join(options.NodeState, "node.db") {
		return ControllerOptions{}, errors.New(
			"mnemond controller Store is outside its owner-only Node state")
	}
	assetRevision := options.Profile.ActiveAssetRevision()
	if err := bindControllerActionPolicy(ctx, options.Profile, options.Install, assetRevision,
		&options.actionPolicy, &options.actionHandlers); err != nil {
		return ControllerOptions{}, err
	}
	return options, nil
}

func newController(options ControllerOptions) (*Controller, error) {
	casRoot := filepath.Join(options.NodeState, "objects", "sha256")
	if options.ArtifactCAS == nil || options.ArtifactCAS.Root() != casRoot {
		return nil, errors.New("mnemond controller requires the exact Node Artifact CAS")
	}
	if err := validateControllerManagedRuntimePair(
		options.meshTransport, options.channelRuntime); err != nil {
		return nil, err
	}
	cas := options.ArtifactCAS
	assetRevision := options.Profile.ActiveAssetRevision()
	capturer, err := artifact.NewCapturer(options.Workspace, cas, options.Clock.Now)
	if err != nil {
		return nil, err
	}
	capture, err := agent.NewArtifactCaptureCoordinator(capturer, cas, cas,
		options.Store, options.Clock)
	if err != nil {
		return nil, err
	}
	materializer, err := artifact.NewViewMaterializer(options.NodeState, cas)
	if err != nil {
		return nil, err
	}
	currentViews, err := agent.NewCurrentViewCoordinator(materializer)
	if err != nil {
		return nil, err
	}
	viewValidator, err := agent.NewReadonlyArtifactViewValidator(options.NodeState)
	if err != nil {
		return nil, err
	}
	artifactResolver, err := agent.NewArtifactResolver(capture, viewValidator)
	if err != nil {
		return nil, err
	}
	executor, err := agent.NewTeamworkActionExecutor(options.Store,
		agent.TeamworkActionExecutorOptions{Profile: options.Profile,
			Actions: options.actionHandlers, Signer: options.Signer,
			Artifacts: artifactResolver, Clock: options.Clock})
	if err != nil {
		return nil, err
	}
	installGate := controllerActivationGate{expected: options.Profile, install: options.Install}
	admission := newControllerAdmissionGate()
	wakeWorker := options.wakeWorker
	if wakeWorker == nil && options.WakeAdapter != nil {
		wakeWorker, err = newManagedWakeWorker(options.Store, options.Profile,
			options.Clock, options.Install, options.WakeAdapter, options.Attachments, admission)
		if err != nil {
			return nil, fmt.Errorf("mnemond controller managed wake worker: %w", err)
		}
	}
	gate := controllerManagedActivationGate{install: installGate, worker: wakeWorker}
	service, err := agent.NewService(options.Store, agent.ServiceOptions{
		Actions: options.actionHandlers, Clock: options.Clock, Executor: executor,
		CurrentViews: currentViews, ActivationGate: gate})
	if err != nil {
		return nil, err
	}
	controller := &Controller{nodeState: options.NodeState, assetRevision: assetRevision,
		store: options.Store, profile: options.Profile, artifactCAS: cas, admission: admission,
		activation: gate, controlFactory: options.Control, wakeWorker: wakeWorker,
		meshTransport: options.meshTransport, channelRuntime: options.channelRuntime,
		shutdownRequested: make(chan struct{}),
		beforeAccept:      options.BeforeAccept}
	controller.controlService = controllerAdmissionService{gate: admission, next: service}
	return controller, nil
}

func validateControllerManagedRuntimePair(mesh managedMeshTransport,
	channelRuntime managedChannelRuntime,
) error {
	if isNilNodeInterface(mesh) != isNilNodeInterface(channelRuntime) {
		return errors.New(
			"mnemond controller requires paired mesh transport and Channel runtime")
	}
	return nil
}

// runtimeComponents freezes the only Supervisor graph for a Controller. A
// managed mesh and its local Channel topics must become ready before local
// control can release the inherited launch permit and accept. Remote baseline
// convergence is deliberately not startup readiness; managed wake remains the
// final dependent.
func (controller *Controller) runtimeComponents(
	transport PreparedControlTransport,
) []componentSpec {
	localDependencies := []string(nil)
	components := make([]componentSpec, 0, 4)
	var retainMeshAuthority atomic.Bool
	if !isNilNodeInterface(controller.meshTransport) {
		components = append(components,
			managedMeshComponent(controller.meshTransport, &retainMeshAuthority))
		localDependencies = []string{"mesh-transport"}
	}
	if !isNilNodeInterface(controller.channelRuntime) {
		dependencies := []string(nil)
		if !isNilNodeInterface(controller.meshTransport) {
			dependencies = []string{"mesh-transport"}
		}
		components = append(components,
			managedChannelRuntimeComponent(controller.channelRuntime, dependencies))
		localDependencies = []string{"channel-runtime"}
	}
	components = append(components, componentSpec{Name: "local-control",
		Dependencies: localDependencies, Readiness: transport.Readiness,
		Run: func(ctx context.Context) error {
			if err := controller.releaseBeforeAccept(); err != nil {
				return err
			}
			return transport.Run(ctx)
		},
		Shutdown: func(ctx context.Context) error {
			err := transport.Shutdown(ctx)
			if errors.Is(err, ErrControlTransportUndrained) {
				retainMeshAuthority.Store(true)
			}
			return err
		}, Restart: componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: controllerControlConnectionLimit}})
	if controller.wakeWorker != nil {
		workerStarted := make(chan struct{})
		components = append(components, componentSpec{Name: "managed-wake",
			Dependencies: []string{"local-control"},
			Readiness: func(readyCtx context.Context) error {
				select {
				case <-workerStarted:
					return nil
				case <-readyCtx.Done():
					return readyCtx.Err()
				}
			},
			Run: func(workerCtx context.Context) error {
				close(workerStarted)
				_ = controller.wakeWorker.Run(workerCtx)
				// Domain failure remains visible through WakeWorker.Snapshot.
				if workerCtx.Err() == nil {
					<-workerCtx.Done()
				}
				return nil
			},
			Shutdown: func(context.Context) error { return nil }, Restart: componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}})
	}
	return components
}

func managedMeshComponent(mesh managedMeshTransport,
	retainAuthority *atomic.Bool,
) componentSpec {
	meshRunDone := make(chan struct{})
	var meshRunErr error
	return componentSpec{Name: "mesh-transport", Readiness: mesh.Readiness,
		Run: func(componentCtx context.Context) error {
			// Detachment lets an unsafe local-control drain retain every dependency
			// beneath an admitted handler. Normal shutdown still Close+waits.
			go func() {
				meshRunErr = mesh.Run(context.WithoutCancel(componentCtx))
				close(meshRunDone)
			}()
			select {
			case <-meshRunDone:
				return meshRunErr
			case <-componentCtx.Done():
				select {
				case <-meshRunDone:
					return meshRunErr
				default:
				}
				if retainAuthority.Load() {
					return nil
				}
				<-meshRunDone
				return meshRunErr
			}
		},
		Shutdown: func(context.Context) error {
			if retainAuthority.Load() {
				return nil
			}
			return mesh.Close()
		},
		Restart: componentRestartNever,
		Resources: componentResourceBudget{
			MaxConcurrent: uint32(peer.HermeticLimits().NodeStreams),
		}}
}

func managedChannelRuntimeComponent(channelRuntime managedChannelRuntime,
	dependencies []string,
) componentSpec {
	return componentSpec{Name: "channel-runtime", Dependencies: dependencies,
		Readiness: channelRuntime.Readiness, Run: channelRuntime.Run,
		Shutdown: func(context.Context) error { return nil },
		Restart:  componentRestartNever,
		Resources: componentResourceBudget{
			MaxConcurrent: uint32(peer.HermeticLimits().ApplicationProtocolStreams),
		}}
}
