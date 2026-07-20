package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrDeactivate = errors.New("deactivate mnemond Profile")

type DeactivateOptions struct {
	Workspace         string
	Host              model.HostKind
	AssetRevision     string
	ExpectedUpdatedAt time.Time
	Clock             Clock
	Credentials       ProfileCredentialVerifier
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
	if ctx == nil || isNilNodeInterface(options.Credentials) {
		return DeactivateResult{}, fmt.Errorf("%w: context or credential verifier is unavailable",
			ErrDeactivate)
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	runtimeKind, hostOK := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !hostOK || digestErr != nil {
		return DeactivateResult{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrDeactivate)
	}
	expectedUpdatedAt := options.ExpectedUpdatedAt.Round(0).UTC()
	if expectedUpdatedAt.IsZero() || expectedUpdatedAt.UnixNano() <= 0 ||
		!time.Unix(0, expectedUpdatedAt.UnixNano()).UTC().Equal(expectedUpdatedAt) {
		return DeactivateResult{}, fmt.Errorf("%w: expected authority update time is invalid", ErrDeactivate)
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
	if err := validateDeactivationAuthority(authority, identity, workspace, nodeState,
		options, runtimeKind, expectedUpdatedAt); err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	expectedSpec := authority.Profile.Spec()
	expectedSpec.UpdatedAt = expectedUpdatedAt
	expected, err := model.NewProfile(expectedSpec)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: expected durable Profile: %v", ErrDeactivate, err)
	}
	deactivated, err := st.DeactivateProfile(ctx, expected, at)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	return DeactivateResult{Node: deactivated.Node, Profile: deactivated.Profile,
		Changed: deactivated.Changed}, nil
}

func validateDeactivationAuthority(authority store.LocalAuthority, identity *Identity,
	workspace, nodeState string, options DeactivateOptions, runtimeKind model.RuntimeKind,
	expectedUpdatedAt time.Time,
) error {
	if authority.Node.PeerID() != identity.PeerID() || authority.Profile.WorkspaceRoot() != workspace {
		return errors.New("durable Node differs from workspace identity")
	}
	if authority.Profile.Host() != options.Host || authority.Profile.Runtime() != runtimeKind ||
		authority.Profile.ActiveAssetRevision() != options.AssetRevision ||
		authority.Node.ActiveAssetRevision() != options.AssetRevision {
		return errors.New("requested authority differs from durable Profile")
	}
	if !authority.Profile.UpdatedAt().Equal(expectedUpdatedAt) {
		return errors.New("requested authority generation differs from durable Profile")
	}
	return options.Credentials.Verify(nodeState, authority.Profile.CredentialHash())
}
