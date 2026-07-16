package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrDaemonPreflight = errors.New("strict mnemond preflight")

type DaemonPreflightOptions struct {
	Workspace     string
	NodeState     string
	AssetRevision string
	Install       InstallationVerifier
}

// DaemonPreflight is the production DaemonEnsurePreflight. Its constructor
// freezes the caller's intended workspace, Node and asset authority; Verify
// re-reads every physical and durable binding immediately before launch.
type DaemonPreflight struct {
	workspace     string
	nodeState     string
	assetRevision string
	install       InstallationVerifier
}

func NewDaemonPreflight(options DaemonPreflightOptions) (*DaemonPreflight, error) {
	if options.Workspace == "" || !filepath.IsAbs(options.Workspace) ||
		filepath.Clean(options.Workspace) != options.Workspace {
		return nil, fmt.Errorf("%w: workspace must be absolute and canonical", ErrDaemonPreflight)
	}
	wantedNodeState := filepath.Join(options.Workspace, ".mnemon", "harness", "node")
	if options.NodeState != wantedNodeState {
		return nil, fmt.Errorf("%w: Node state is outside the managed workspace", ErrDaemonPreflight)
	}
	if _, err := model.ParseDigest(options.AssetRevision); err != nil {
		return nil, fmt.Errorf("%w: asset revision is invalid", ErrDaemonPreflight)
	}
	if options.Install == nil {
		return nil, fmt.Errorf("%w: installation verifier is unavailable", ErrDaemonPreflight)
	}
	return &DaemonPreflight{workspace: options.Workspace, nodeState: options.NodeState,
		assetRevision: options.AssetRevision, install: options.Install}, nil
}

// Verify is validation-only. It never provisions identity or credentials,
// creates or migrates node.db, repairs modes, or projects managed assets. The
// temporary Store writer authority is released before Verify returns on every
// success and failure path.
func (preflight *DaemonPreflight) Verify(ctx context.Context) (err error) {
	if preflight == nil || ctx == nil {
		return fmt.Errorf("%w: verifier or context is unavailable", ErrDaemonPreflight)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrDaemonPreflight, err)
	}
	authority, err := openExistingDaemonAuthority(ctx, preflight.workspace, preflight.nodeState)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDaemonPreflight, err)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrDaemonPreflight, closeErr))
		}
	}()
	if authority.authority.Node.ActiveAssetRevision() != preflight.assetRevision ||
		authority.authority.Profile.ActiveAssetRevision() != preflight.assetRevision {
		return fmt.Errorf("%w: durable asset revision differs from expected %q",
			ErrDaemonPreflight, preflight.assetRevision)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrDaemonPreflight, err)
	}
	if err := verifyDaemonInstallation(ctx, preflight.install, authority.authority.Profile); err != nil {
		return fmt.Errorf("%w: managed installation: %w", ErrDaemonPreflight, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrDaemonPreflight, err)
	}
	return nil
}

// Installation verification is filesystem-facing and supplied by the outer
// composition layer. Keep that boundary behind the same hard ensure context:
// cancellation releases the temporary Store writer even if a defective or
// stalled verifier has not returned yet. The verifier receives immutable
// Profile data and no Store handle, so it cannot retain writer authority.
func verifyDaemonInstallation(ctx context.Context, install InstallationVerifier,
	profile model.Profile,
) error {
	if ctx == nil || install == nil {
		return errors.New("managed installation verifier is unavailable")
	}
	result := make(chan error, 1)
	go func() { result <- install.Verify(profile) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type existingDaemonAuthority struct {
	identity  *Identity
	store     *store.Store
	authority store.LocalAuthority
}

func openExistingDaemonAuthority(ctx context.Context, workspace, nodeState string) (existingDaemonAuthority, error) {
	if ctx == nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: context is unavailable", ErrDaemonAuthority)
	}
	if err := ctx.Err(); err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: %w", ErrDaemonAuthority, err)
	}
	validatedWorkspace, err := validateDaemonWorkspace(workspace)
	if err != nil {
		return existingDaemonAuthority{}, err
	}
	wantedNodeState := filepath.Join(validatedWorkspace, ".mnemon", "harness", "node")
	if nodeState != wantedNodeState {
		return existingDaemonAuthority{}, fmt.Errorf("%w: Node state is outside the managed workspace", ErrDaemonAuthority)
	}
	identity, err := loadExistingDaemonIdentity(ctx, nodeState)
	if err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: %w", ErrDaemonAuthority, err)
	}
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: inspect node.db: %v", ErrDaemonAuthority, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: node.db: %v", ErrDaemonAuthority, err)
	}
	st, err := store.OpenExisting(ctx, databasePath)
	if err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: open Store: %w", ErrDaemonAuthority, err)
	}
	fail := func(cause error) (existingDaemonAuthority, error) {
		if closeErr := st.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("%w: close Store: %v", ErrDaemonAuthority, closeErr))
		}
		return existingDaemonAuthority{}, cause
	}
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return fail(fmt.Errorf("%w: %w", ErrDaemonAuthority, err))
	}
	if authority.Profile.ID() != model.TeamworkProfileID() || !authority.Profile.Enabled() {
		return fail(fmt.Errorf("%w: the unique Teamwork Profile is disabled or unavailable", ErrDaemonAuthority))
	}
	if authority.Node.PeerID() != identity.PeerID() {
		return fail(fmt.Errorf("%w: identity key and Store PeerID differ", ErrDaemonAuthority))
	}
	if authority.Profile.WorkspaceRoot() != validatedWorkspace {
		return fail(fmt.Errorf("%w: Profile belongs to another workspace", ErrDaemonAuthority))
	}
	if err := localapi.VerifyProfileCredential(nodeState, authority.Profile.CredentialHash()); err != nil {
		return fail(fmt.Errorf("%w: %w", ErrDaemonAuthority, err))
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("%w: %w", ErrDaemonAuthority, err))
	}
	return existingDaemonAuthority{identity: identity, store: st, authority: authority}, nil
}

func loadExistingDaemonIdentity(ctx context.Context, nodeState string) (*Identity, error) {
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		return nil, err
	}
	defer state.close()
	if err := state.lockContext(ctx); err != nil {
		return nil, err
	}
	defer state.unlock()
	if err := ctx.Err(); err != nil {
		return nil, identityError("load existing key", err)
	}
	identity, err := state.load()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, identityError("load existing key", err)
	}
	return identity, nil
}
