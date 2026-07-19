package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrManagedInstallation = errors.New("managed installation is unavailable")

type managedHostInspector func(context.Context, assets.Host) (HostObservation, error)

// ManagedInstallation freezes one validated canonical asset bundle and one
// physical workspace. It is the integration-owned authority used to verify
// the installed projections and resolve the exact Runtime selected by a
// Profile; callers do not need to import the asset package.
type ManagedInstallation struct {
	bundle        assets.Bundle
	inspectHost   managedHostInspector
	nodeState     string
	revision      string
	workspace     string
	workspaceInfo os.FileInfo
}

// NewManagedInstallation loads the canonical managed assets once and binds
// them to an existing absolute, physical, current-owner workspace.
func NewManagedInstallation(workspace string) (*ManagedInstallation, error) {
	bundle, err := assets.Load()
	if err != nil {
		return nil, fmt.Errorf("%w: load canonical assets: %v", ErrManagedInstallation, err)
	}
	return newManagedInstallation(workspace, bundle, InspectHost)
}

func newManagedInstallation(workspace string, bundle assets.Bundle,
	inspect managedHostInspector,
) (*ManagedInstallation, error) {
	info, err := inspectManagedWorkspace(workspace)
	manifest := bundle.Manifest()
	if err != nil || inspect == nil || manifest.AssetRevision == "" {
		if err == nil {
			err = errors.New("canonical bundle or Host inspector is unavailable")
		}
		return nil, fmt.Errorf("%w: %v", ErrManagedInstallation, err)
	}
	return &ManagedInstallation{
		bundle: bundle, inspectHost: inspect,
		nodeState:     filepath.Join(workspace, ".mnemon", "harness", "node"),
		revision:      manifest.AssetRevision,
		workspace:     workspace,
		workspaceInfo: info,
	}, nil
}

// Revision returns the one canonical asset revision frozen at construction.
func (installation *ManagedInstallation) Revision() string {
	if installation == nil {
		return ""
	}
	return installation.revision
}

// Verify proves that the Profile selects this workspace and canonical bundle,
// then checks the immutable Node bundle and the selected Host projection.
func (installation *ManagedInstallation) Verify(ctx context.Context, profile model.Profile) error {
	host, err := installation.authorizeProfile(ctx, profile)
	if err != nil {
		return err
	}
	if err := VerifyNodeBundle(installation.nodeState, installation.bundle); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := VerifyHostProjection(installation.workspace, installation.nodeState,
		host, installation.bundle); err != nil {
		return err
	}
	return ctx.Err()
}

// RuntimeExecutable resolves the bounded Host preflight for exactly the
// Runtime selected by the supplied Profile. Installation verification remains
// an explicit, separate gate so callers can place it at their authority edge.
func (installation *ManagedInstallation) RuntimeExecutable(ctx context.Context,
	profile model.Profile,
) (string, error) {
	host, err := installation.authorizeProfile(ctx, profile)
	if err != nil {
		return "", err
	}
	observation, err := installation.inspectHost(ctx, host)
	if err != nil {
		return "", err
	}
	if observation.Host != host || observation.Executable == "" {
		return "", fmt.Errorf("%w: Host preflight returned different authority",
			ErrManagedInstallation)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return observation.Executable, nil
}

func (installation *ManagedInstallation) authorizeProfile(ctx context.Context,
	profile model.Profile,
) (assets.Host, error) {
	if ctx == nil {
		return "", fmt.Errorf("%w: verification context is unavailable", ErrManagedInstallation)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if installation == nil || installation.inspectHost == nil ||
		installation.revision == "" || installation.workspaceInfo == nil {
		return "", ErrManagedInstallation
	}
	info, err := inspectManagedWorkspace(installation.workspace)
	if err != nil || !os.SameFile(installation.workspaceInfo, info) {
		return "", fmt.Errorf("%w: workspace identity changed", ErrManagedInstallation)
	}
	host := assets.Host(profile.Host())
	runtimeKind, runtimeOK := model.RuntimeForHost(profile.Host())
	if profile.ID() != model.TeamworkProfileID() ||
		!profile.Enabled() ||
		profile.WorkspaceRoot() != installation.workspace ||
		!host.Valid() || !runtimeOK || profile.Runtime() != runtimeKind ||
		profile.ActiveAssetRevision() != installation.revision {
		return "", fmt.Errorf("%w: Profile does not select this workspace and asset bundle",
			ErrManagedInstallation)
	}
	return host, nil
}

func inspectManagedWorkspace(workspace string) (os.FileInfo, error) {
	if workspace == "" || !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
		return nil, errors.New("workspace path is not absolute and canonical")
	}
	physical, err := filepath.EvalSymlinks(workspace)
	if err != nil || physical != workspace {
		return nil, errors.New("workspace path is not physical")
	}
	if err := requireOwnedRealDirectory(workspace); err != nil {
		return nil, err
	}
	info, err := os.Lstat(workspace)
	if err != nil {
		return nil, errors.New("workspace identity is unavailable")
	}
	return info, nil
}
