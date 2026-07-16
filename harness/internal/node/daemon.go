package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrDaemonAuthority = errors.New("mnemond durable authority is invalid")

type DaemonOptions struct {
	Workspace string
	Clock     Clock
	Install   InstallationVerifier
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
	closeOnce  sync.Once
	closeErr   error
}

func OpenDaemon(ctx context.Context, options DaemonOptions) (*Daemon, error) {
	nodeState := filepath.Join(options.Workspace, ".mnemon", "harness", "node")
	authority, err := openExistingDaemonAuthority(ctx, options.Workspace, nodeState)
	if err != nil {
		return nil, err
	}
	workspace := options.Workspace
	identity := authority.identity
	st := authority.store
	fail := func(cause error) (*Daemon, error) {
		_ = st.Close()
		return nil, cause
	}
	controller, err := NewController(ControllerOptions{NodeState: nodeState, Workspace: workspace,
		Store: st, Profile: authority.authority.Profile, Signer: identity.PublicationSigner(), Clock: options.Clock,
		Install: options.Install})
	if err != nil {
		return fail(fmt.Errorf("%w: compose controller: %v", ErrDaemonAuthority, err))
	}
	return &Daemon{workspace: workspace, nodeState: nodeState, identity: identity,
		store: st, controller: controller}, nil
}

func (daemon *Daemon) Serve(ctx context.Context) error {
	if daemon == nil || daemon.controller == nil || daemon.store == nil {
		return fmt.Errorf("%w: daemon is unavailable", ErrDaemonAuthority)
	}
	return daemon.controller.Serve(ctx)
}

func (daemon *Daemon) Close() error {
	if daemon == nil {
		return nil
	}
	daemon.closeOnce.Do(func() {
		if daemon.store != nil {
			daemon.closeErr = daemon.store.Close()
		}
	})
	return daemon.closeErr
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
