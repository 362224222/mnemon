package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is unavailable", ErrDaemonAuthority)
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return nil, err
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	identity, err := LoadIdentity(nodeState)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonAuthority, err)
	}
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect node.db: %v", ErrDaemonAuthority, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return nil, fmt.Errorf("%w: node.db: %v", ErrDaemonAuthority, err)
	}
	st, err := store.Open(ctx, databasePath)
	if err != nil {
		return nil, fmt.Errorf("%w: open Store: %v", ErrDaemonAuthority, err)
	}
	fail := func(cause error) (*Daemon, error) {
		_ = st.Close()
		return nil, cause
	}
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return fail(fmt.Errorf("%w: %v", ErrDaemonAuthority, err))
	}
	if authority.Node.PeerID() != identity.PeerID() {
		return fail(fmt.Errorf("%w: identity key and Store PeerID differ", ErrDaemonAuthority))
	}
	if authority.Profile.WorkspaceRoot() != workspace {
		return fail(fmt.Errorf("%w: Profile belongs to another workspace", ErrDaemonAuthority))
	}
	if !authority.Profile.Enabled() {
		return fail(fmt.Errorf("%w: Teamwork Profile is disabled", ErrDaemonAuthority))
	}
	if err := localapi.VerifyProfileCredential(nodeState, authority.Profile.CredentialHash()); err != nil {
		return fail(fmt.Errorf("%w: %v", ErrDaemonAuthority, err))
	}
	controller, err := NewController(ControllerOptions{NodeState: nodeState, Workspace: workspace,
		Store: st, Profile: authority.Profile, Signer: identity.PublicationSigner(), Clock: options.Clock,
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
