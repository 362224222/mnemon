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

var ErrActivate = errors.New("activate mnemond Profile")

type ActivateOptions struct {
	Workspace         string
	Host              model.HostKind
	AssetRevision     string
	ExpectedUpdatedAt time.Time
	Clock             Clock
	Install           InstallationVerifier
}

type ActivateResult struct {
	Node    model.Node
	Profile model.Profile
	Changed bool
}

// Activate publishes already-installed Host authority. It never projects
// assets or starts a Runtime; the injected verifier must prove the exact Node
// bundle and Host projection before Store grants managed admission.
func Activate(ctx context.Context, options ActivateOptions) (result ActivateResult, err error) {
	if ctx == nil || options.Install == nil {
		return ActivateResult{}, fmt.Errorf("%w: context or installation verifier is unavailable", ErrActivate)
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %v", ErrActivate, err)
	}
	runtimeKind, hostOK := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !hostOK || digestErr != nil {
		return ActivateResult{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrActivate)
	}
	expectedUpdatedAt := options.ExpectedUpdatedAt.Round(0).UTC()
	if expectedUpdatedAt.IsZero() || expectedUpdatedAt.UnixNano() <= 0 ||
		!time.Unix(0, expectedUpdatedAt.UnixNano()).UTC().Equal(expectedUpdatedAt) {
		return ActivateResult{}, fmt.Errorf("%w: expected authority update time is invalid", ErrActivate)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at := options.Clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return ActivateResult{}, fmt.Errorf("%w: clock is invalid", ErrActivate)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	identity, err := loadActivationIdentity(nodeState, options.Install, options.AssetRevision)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %w", ErrActivate, err)
	}
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: inspect node.db: %v", ErrActivate, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return ActivateResult{}, fmt.Errorf("%w: node.db: %v", ErrActivate, err)
	}
	st, err := store.Open(ctx, databasePath)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %v", ErrActivate, err)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrActivate, closeErr))
		}
	}()
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %v", ErrActivate, err)
	}
	if authority.Node.PeerID() != identity.PeerID() || authority.Profile.WorkspaceRoot() != workspace {
		return ActivateResult{}, fmt.Errorf("%w: durable Node differs from workspace identity", ErrActivate)
	}
	if err := localapi.VerifyProfileCredential(nodeState, authority.Profile.CredentialHash()); err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %v", ErrActivate, err)
	}
	spec := authority.Profile.Spec()
	spec.Host = options.Host
	spec.Runtime = runtimeKind
	spec.ActiveAssetRevision = options.AssetRevision
	spec.HandlingBudget = model.DefaultHandlingBudget().JSON()
	spec.Enabled = true
	spec.UpdatedAt = at
	desired, err := model.NewProfile(spec)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: desired Profile: %v", ErrActivate, err)
	}
	if err := options.Install.Verify(ctx, desired); err != nil {
		return ActivateResult{}, fmt.Errorf("%w: managed installation: %w", ErrActivate, err)
	}
	if !authority.Profile.UpdatedAt().Equal(expectedUpdatedAt) {
		return ActivateResult{}, fmt.Errorf("%w: requested authority generation differs from durable Profile", ErrActivate)
	}
	activated, err := st.ActivateProfile(ctx, desired, expectedUpdatedAt, at)
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: %v", ErrActivate, err)
	}
	return ActivateResult{Node: activated.Node, Profile: activated.Profile, Changed: activated.Changed}, nil
}
