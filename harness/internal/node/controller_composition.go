package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// bindControllerActionPolicy verifies the physical installation and fills the
// local options snapshot with the immutable semantic policy for that exact
// revision. A daemon may supply its precomposed snapshot; standalone
// controllers compose one only after installation verification succeeds.
func bindControllerActionPolicy(ctx context.Context, profile model.Profile,
	install InstallationVerifier, revision string, policy *agent.ActionPolicy,
) error {
	if isNilNodeInterface(install) || policy == nil {
		return errors.New("mnemond controller installation or action policy is unavailable")
	}
	if err := install.Verify(ctx, profile); err != nil {
		return fmt.Errorf("mnemond controller managed installation: %w", err)
	}
	if policy.AssetRevision().IsZero() {
		composed, err := actionPolicyForInstallation(install)
		if err != nil {
			return fmt.Errorf("mnemond controller action policy: %w", err)
		}
		*policy = composed
	}
	if policy.AssetRevision().String() != revision {
		return errors.New("mnemond controller action policy differs from the active asset revision")
	}
	return nil
}
