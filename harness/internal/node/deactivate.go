package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrDeactivate = errors.New("deactivate mnemond Profile")

type DeactivateOptions struct {
	Workspace     string
	Host          model.HostKind
	AssetRevision string
	Clock         Clock
}

type DeactivateResult struct {
	Node    model.Node
	Profile model.Profile
	Changed bool
}

// Deactivate withdraws one exact durable Profile authority for setup rollback
// or eject. It intentionally does not verify the Host projection: drifted
// managed assets must still be removable after Agent authority is quiescent.
func Deactivate(ctx context.Context, options DeactivateOptions) (result DeactivateResult, err error) {
	if ctx == nil {
		return DeactivateResult{}, fmt.Errorf("%w: context is unavailable", ErrDeactivate)
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	runtimeKind, hostOK := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !hostOK || digestErr != nil {
		return DeactivateResult{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrDeactivate)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at := options.Clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return DeactivateResult{}, fmt.Errorf("%w: clock is invalid", ErrDeactivate)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	identity, err := LoadIdentity(nodeState)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: inspect node.db: %v", ErrDeactivate, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: node.db: %v", ErrDeactivate, err)
	}
	st, err := store.Open(ctx, databasePath)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrDeactivate, closeErr))
		}
	}()
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	if authority.Node.PeerID() != identity.PeerID() || authority.Profile.WorkspaceRoot() != workspace {
		return DeactivateResult{}, fmt.Errorf("%w: durable Node differs from workspace identity", ErrDeactivate)
	}
	if authority.Profile.Host() != options.Host || authority.Profile.Runtime() != runtimeKind ||
		authority.Profile.ActiveAssetRevision() != options.AssetRevision ||
		authority.Node.ActiveAssetRevision() != options.AssetRevision {
		return DeactivateResult{}, fmt.Errorf("%w: requested authority differs from durable Profile", ErrDeactivate)
	}
	if err := localapi.VerifyProfileCredential(nodeState, authority.Profile.CredentialHash()); err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	deactivated, err := st.DeactivateProfile(ctx, authority.Profile, at)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	return DeactivateResult{Node: deactivated.Node, Profile: deactivated.Profile,
		Changed: deactivated.Changed}, nil
}
