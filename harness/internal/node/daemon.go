package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrDaemonAuthority = errors.New("mnemond durable authority is invalid")

// WakeAdapterFactory is the outer composition boundary between the managed
// Node and a Host-specific Agent Runtime. Node owns worker lifecycle and
// durable authority, while the caller supplies the already-inspected Runtime
// adapter without importing Host integration into this package.
type WakeAdapterFactory interface {
	NewWakeAdapter(context.Context, WakeAdapterFactoryOptions) (agent.WakeWorkerAdapter, error)
}

type WakeAdapterFactoryFunc func(context.Context, WakeAdapterFactoryOptions) (agent.WakeWorkerAdapter, error)

func (factory WakeAdapterFactoryFunc) NewWakeAdapter(ctx context.Context,
	options WakeAdapterFactoryOptions,
) (agent.WakeWorkerAdapter, error) {
	if factory == nil {
		return nil, errors.New("managed wake adapter factory is unavailable")
	}
	return factory(ctx, options)
}

type WakeAdapterFactoryOptions struct {
	Workspace string
	NodeState string
	Profile   model.Profile
}

type DaemonOptions struct {
	Workspace          string
	Clock              Clock
	Install            InstallationVerifier
	WakeAdapterFactory WakeAdapterFactory
}

// Daemon owns the strict restart path for one workspace-local Node. It binds
// the existing DB, identity key, Profile token and canonical assets before the
// controller is allowed to create its socket. It never initializes missing
// state or silently rotates identity.
type Daemon struct {
	workspace  string
	nodeState  string
	identity   *Identity
	store      *store.Store
	controller *Controller

	lifecycleMu  sync.Mutex
	serveStarted bool
	serveDone    chan struct{}
	closing      bool
	closed       bool
	closeOnce    sync.Once
	closeErr     error
}

func OpenDaemon(ctx context.Context, options DaemonOptions) (*Daemon, error) {
	return openDaemon(ctx, options, nil)
}

// OpenManagedDaemon is the production serve boundary. Unlike OpenDaemon's
// in-process composition surface, it requires the exact ensure.lock descriptor
// inherited by DaemonProcessLauncher and consumes that descriptor before the
// controller begins accepting requests.
func OpenManagedDaemon(ctx context.Context, options DaemonOptions) (*Daemon, error) {
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return nil, err
	}
	options.Workspace = workspace
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	permit, err := openInheritedDaemonLaunchPermit(nodeState)
	if err != nil {
		return nil, err
	}
	if isNilWakeAdapterFactory(options.WakeAdapterFactory) {
		return nil, errors.Join(
			fmt.Errorf("%w: managed wake adapter factory is unavailable", ErrDaemonAuthority),
			permit.close(),
		)
	}
	daemon, openErr := openDaemon(ctx, options, permit.close)
	if openErr != nil {
		return nil, errors.Join(openErr, permit.close())
	}
	return daemon, nil
}

func openDaemon(ctx context.Context, options DaemonOptions,
	beforeAccept func() error,
) (*Daemon, error) {
	nodeState := filepath.Join(options.Workspace, ".mnemon", "harness", "node")
	authority, err := openExistingDaemonAuthority(ctx, options.Workspace, nodeState)
	if err != nil {
		return nil, err
	}
	workspace := options.Workspace
	identity := authority.identity
	st := authority.store
	fail := func(cause error) (*Daemon, error) {
		if closeErr := st.Close(); closeErr != nil {
			cause = errors.Join(cause,
				fmt.Errorf("%w: close Store after composition failure: %v", ErrDaemonAuthority, closeErr))
		}
		return nil, cause
	}
	var wakeAdapter agent.WakeWorkerAdapter
	if !isNilWakeAdapterFactory(options.WakeAdapterFactory) {
		wakeAdapter, err = options.WakeAdapterFactory.NewWakeAdapter(ctx, WakeAdapterFactoryOptions{
			Workspace: workspace,
			NodeState: nodeState,
			Profile:   authority.authority.Profile,
		})
		if err != nil {
			return fail(fmt.Errorf("%w: compose managed wake adapter: %w", ErrDaemonAuthority, err))
		}
		if isNilWakeAdapter(wakeAdapter) {
			return fail(fmt.Errorf("%w: managed wake adapter factory returned no adapter", ErrDaemonAuthority))
		}
	}
	controller, err := NewController(ctx, ControllerOptions{NodeState: nodeState, Workspace: workspace,
		Store: st, Profile: authority.authority.Profile, Signer: identity.PublicationSigner(), Clock: options.Clock,
		Install: options.Install, WakeAdapter: wakeAdapter, BeforeAccept: beforeAccept})
	if err != nil {
		return fail(fmt.Errorf("%w: compose controller: %v", ErrDaemonAuthority, err))
	}
	return &Daemon{workspace: workspace, nodeState: nodeState, identity: identity,
		store: st, controller: controller, serveDone: make(chan struct{})}, nil
}

func (daemon *Daemon) Serve(ctx context.Context) error {
	if daemon == nil || daemon.controller == nil || daemon.store == nil {
		return fmt.Errorf("%w: daemon is unavailable", ErrDaemonAuthority)
	}
	if ctx == nil {
		return fmt.Errorf("%w: daemon serve context is unavailable", ErrDaemonAuthority)
	}
	daemon.lifecycleMu.Lock()
	if daemon.closing || daemon.closed {
		daemon.lifecycleMu.Unlock()
		return fmt.Errorf("%w: daemon is closed", ErrDaemonAuthority)
	}
	if daemon.serveStarted {
		daemon.lifecycleMu.Unlock()
		return fmt.Errorf("%w: daemon can serve only once", ErrDaemonAuthority)
	}
	daemon.serveStarted = true
	serveDone := daemon.serveDone
	daemon.lifecycleMu.Unlock()

	err := daemon.controller.Serve(ctx)
	daemon.lifecycleMu.Lock()
	close(serveDone)
	daemon.lifecycleMu.Unlock()
	return err
}

func (daemon *Daemon) Close() error {
	if daemon == nil {
		return nil
	}
	daemon.closeOnce.Do(func() {
		daemon.lifecycleMu.Lock()
		daemon.closing = true
		serveStarted := daemon.serveStarted
		serveDone := daemon.serveDone
		controller := daemon.controller
		daemon.lifecycleMu.Unlock()

		if controller != nil {
			if serveStarted {
				controller.requestShutdown()
				if serveDone != nil {
					<-serveDone
				}
			}
			// Serve normally releases the launch permit immediately before HTTP
			// admission. Keep this idempotent fallback for every early-return
			// path so Close can never strand inherited ensure.lock authority.
			daemon.closeErr = controller.releaseBeforeAccept()
		}
		if daemon.store != nil {
			daemon.closeErr = errors.Join(daemon.closeErr, daemon.store.Close())
		}
		daemon.lifecycleMu.Lock()
		daemon.closed = true
		daemon.lifecycleMu.Unlock()
	})
	daemon.lifecycleMu.Lock()
	closeErr := daemon.closeErr
	daemon.lifecycleMu.Unlock()
	return closeErr
}

func isNilWakeAdapterFactory(factory WakeAdapterFactory) bool {
	return isNilDaemonInterface(factory)
}

func isNilWakeAdapter(adapter agent.WakeWorkerAdapter) bool {
	return isNilDaemonInterface(adapter)
}

func isNilDaemonInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (daemon *Daemon) Workspace() string {
	if daemon == nil {
		return ""
	}
	return daemon.workspace
}

func (daemon *Daemon) NodeState() string {
	if daemon == nil {
		return ""
	}
	return daemon.nodeState
}

func (daemon *Daemon) PeerID() model.PeerID {
	if daemon == nil || daemon.identity == nil {
		return model.PeerID{}
	}
	return daemon.identity.PeerID()
}

func validateDaemonWorkspace(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("%w: workspace must be absolute and canonical", ErrDaemonAuthority)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ownedByEffectiveUser(info) {
		return "", fmt.Errorf("%w: workspace must be a real current-owner directory", ErrDaemonAuthority)
	}
	return path, nil
}

func ownedByEffectiveUser(info os.FileInfo) bool {
	if info == nil {
		return false
	}
	owner, err := validateIdentityOwnerPath(info, info.Mode().Perm(), true)
	return err == nil && owner == uint32(os.Geteuid())
}
