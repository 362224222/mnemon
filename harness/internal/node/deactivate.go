package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"os"
	"path/filepath"
	"time"
)

var ErrDeactivate = errors.New("deactivate mnemond Profile")

type DeactivateOptions struct {
	Workspace         string
	Host              model.HostKind
	AssetRevision     string
	ExpectedUpdatedAt time.Time
	Clock             Clock
	Credentials       ProfileCredentialAuthority
}

type DeactivateResult struct {
	Node    model.Node
	Profile model.Profile
	Changed bool
}

type deactivationPlan struct {
	workspace         string
	runtime           model.RuntimeKind
	expectedUpdatedAt time.Time
	at                time.Time
}

// Deactivate withdraws one exact durable Profile authority for setup rollback
// or eject. It intentionally does not verify the Host projection: drifted
// managed assets must still be removable after Agent authority is quiescent.
func Deactivate(ctx context.Context, options DeactivateOptions) (result DeactivateResult, err error) {
	if ctx == nil {
		return DeactivateResult{}, fmt.Errorf("%w: context is unavailable", ErrDeactivate)
	}
	credentials, err := requireProfileCredentialAuthority(options.Credentials)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %w", ErrDeactivate, err)
	}
	plan, err := prepareDeactivation(&options)
	if err != nil {
		return DeactivateResult{}, err
	}
	nodeState := filepath.Join(plan.workspace, ".mnemon", "harness", "node")
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
	if err := validateDeactivationAuthority(authority, identity, nodeState, plan, options,
		credentials); err != nil {
		return DeactivateResult{}, err
	}
	expectedSpec := authority.Profile.Spec()
	expectedSpec.UpdatedAt = plan.expectedUpdatedAt
	expected, err := model.NewProfile(expectedSpec)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: expected durable Profile: %v", ErrDeactivate, err)
	}
	deactivated, err := st.DeactivateProfile(ctx, expected, plan.at)
	if err != nil {
		return DeactivateResult{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	return DeactivateResult{Node: deactivated.Node, Profile: deactivated.Profile,
		Changed: deactivated.Changed}, nil
}

func prepareDeactivation(options *DeactivateOptions) (deactivationPlan, error) {
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return deactivationPlan{}, fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	runtimeKind, hostOK := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !hostOK || digestErr != nil {
		return deactivationPlan{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrDeactivate)
	}
	expectedUpdatedAt, ok := canonicalAuthorityTime(options.ExpectedUpdatedAt)
	if !ok {
		return deactivationPlan{}, fmt.Errorf("%w: expected authority update time is invalid", ErrDeactivate)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at, ok := canonicalAuthorityTime(options.Clock.Now())
	if !ok {
		return deactivationPlan{}, fmt.Errorf("%w: clock is invalid", ErrDeactivate)
	}
	return deactivationPlan{workspace: workspace, runtime: runtimeKind,
		expectedUpdatedAt: expectedUpdatedAt, at: at}, nil
}

func validateDeactivationAuthority(authority store.LocalAuthority, identity *Identity, nodeState string,
	plan deactivationPlan, options DeactivateOptions, credentials ProfileCredentialAuthority,
) error {
	if authority.Node.PeerID() != identity.PeerID() ||
		authority.Profile.WorkspaceRoot() != plan.workspace {
		return fmt.Errorf("%w: durable Node differs from workspace identity", ErrDeactivate)
	}
	if authority.Profile.Host() != options.Host || authority.Profile.Runtime() != plan.runtime ||
		authority.Profile.ActiveAssetRevision() != options.AssetRevision ||
		authority.Node.ActiveAssetRevision() != options.AssetRevision {
		return fmt.Errorf("%w: requested authority differs from durable Profile", ErrDeactivate)
	}
	if !authority.Profile.UpdatedAt().Equal(plan.expectedUpdatedAt) {
		return fmt.Errorf("%w: requested authority generation differs from durable Profile", ErrDeactivate)
	}
	if err := credentials.VerifyProfileCredential(nodeState, authority.Profile.CredentialHash()); err != nil {
		return fmt.Errorf("%w: %v", ErrDeactivate, err)
	}
	return nil
}
