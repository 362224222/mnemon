package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// InstallationVerifier is supplied by the outer composition layer. Node does
// not import Host integration; it only requires a current Profile to remain
// bound to its canonical Node bundle and Host projection.
type InstallationVerifier interface {
	Verify(model.Profile) error
}

type InstallationVerifierFunc func(model.Profile) error

func (verify InstallationVerifierFunc) Verify(profile model.Profile) error {
	if verify == nil {
		return errors.New("managed installation verifier is unavailable")
	}
	return verify(profile)
}

type ControllerOptions struct {
	NodeState string
	Workspace string
	Store     *store.Store
	Profile   model.Profile
	Signer    event.PublicationSigner
	Clock     Clock
	Install   InstallationVerifier
}

// Controller is the single local composition root for mnemond-managed Agent
// traffic. It owns no second domain state: every route reaches the one Store,
// while CAS and readonly views stay beneath the same Node state directory.
type Controller struct {
	nodeState string
	server    *localapi.Server
	serveMu   sync.Mutex
	served    bool
}

func NewController(options ControllerOptions) (*Controller, error) {
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.Store == nil || options.Signer == nil || options.Install == nil ||
		options.Profile.ID() != model.TeamworkProfileID() ||
		!options.Profile.Enabled() || options.NodeState == "" || options.Workspace == "" ||
		!filepath.IsAbs(options.NodeState) || filepath.Clean(options.NodeState) != options.NodeState ||
		!filepath.IsAbs(options.Workspace) || filepath.Clean(options.Workspace) != options.Workspace ||
		options.Profile.WorkspaceRoot() != options.Workspace {
		return nil, errors.New("mnemond controller requires canonical Node, workspace, Profile, Store and signer")
	}
	stateInfo, err := os.Lstat(options.NodeState)
	if err != nil || !stateInfo.IsDir() || stateInfo.Mode()&os.ModeSymlink != 0 ||
		stateInfo.Mode().Perm() != 0o700 || options.Store.Path() != filepath.Join(options.NodeState, "node.db") {
		return nil, errors.New("mnemond controller Store is outside its owner-only Node state")
	}
	assetRevision := options.Profile.ActiveAssetRevision()
	if err := options.Install.Verify(options.Profile); err != nil {
		return nil, fmt.Errorf("mnemond controller managed installation: %w", err)
	}
	cas, err := artifact.NewCAS(filepath.Join(options.NodeState, "objects", "sha256"))
	if err != nil {
		return nil, err
	}
	capturer, err := artifact.NewCapturer(options.Workspace, cas, options.Clock.Now)
	if err != nil {
		return nil, err
	}
	capture, err := agent.NewArtifactCaptureCoordinator(capturer, cas, options.Store, options.Clock)
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
	executor, err := agent.NewTeamworkActionExecutor(options.Store, agent.TeamworkActionExecutorOptions{
		Profile: options.Profile, Signer: options.Signer, Artifacts: artifactResolver, Clock: options.Clock,
	})
	if err != nil {
		return nil, err
	}
	gate := controllerActivationGate{expected: options.Profile, install: options.Install}
	service, err := agent.NewService(options.Store, agent.ServiceOptions{AssetRevision: assetRevision,
		Clock: options.Clock, Executor: executor, CurrentViews: currentViews, ActivationGate: gate})
	if err != nil {
		return nil, err
	}
	health := localapi.HealthProviderFunc(func(healthCtx context.Context,
		metadata localapi.RequestMetadata,
	) (localapi.HealthSnapshot, *localapi.APIError) {
		ready := gate.Check(healthCtx, metadata.Profile) == nil
		return localapi.HealthSnapshot{AssetRevision: assetRevision, WorkersReady: ready}, nil
	})
	server, err := localapi.NewServer(options.Store, service, health)
	if err != nil {
		return nil, err
	}
	return &Controller{nodeState: options.NodeState, server: server}, nil
}

type controllerActivationGate struct {
	expected model.Profile
	install  InstallationVerifier
}

func (gate controllerActivationGate) Check(ctx context.Context, profile model.Profile) *localapi.APIError {
	if ctx == nil || ctx.Err() != nil {
		return localapi.NewAPIError(localapi.CodeInternal, "managed activation check was cancelled")
	}
	if gate.install == nil || !sameControllerProfile(profile, gate.expected) || gate.install.Verify(profile) != nil {
		return localapi.NewAPIError(localapi.CodeAssetRevisionMismatch,
			"managed Node assets or Host projection differ from the active Profile")
	}
	return nil
}

func sameControllerProfile(got, want model.Profile) bool {
	return got.ID() == want.ID() && got.Principal() == want.Principal() &&
		got.WorkspaceRoot() == want.WorkspaceRoot() && got.Host() == want.Host() &&
		got.Runtime() == want.Runtime() && got.CredentialHash() == want.CredentialHash() &&
		got.ActiveAssetRevision() == want.ActiveAssetRevision() &&
		got.HandlingBudget().String() == want.HandlingBudget().String() && got.Enabled() && want.Enabled()
}

func (controller *Controller) Serve(ctx context.Context) error {
	if controller == nil || controller.server == nil || ctx == nil {
		return errors.New("mnemond controller is unavailable")
	}
	controller.serveMu.Lock()
	if controller.served {
		controller.serveMu.Unlock()
		return errors.New("mnemond controller can serve only once")
	}
	controller.served = true
	controller.serveMu.Unlock()

	listener, err := localapi.ListenOwnerUnix(filepath.Join(controller.nodeState, "control.sock"))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: controller.server.Handler(), ReadHeaderTimeout: 5 * time.Second}
	shutdownDone := make(chan struct{})
	serveDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-serveDone:
		}
	}()
	serveErr := server.Serve(listener)
	close(serveDone)
	<-shutdownDone
	if errors.Is(serveErr, http.ErrServerClosed) || ctx.Err() != nil && errors.Is(serveErr, os.ErrClosed) {
		return nil
	}
	return serveErr
}
