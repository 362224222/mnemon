package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// InstallationVerifier is supplied by the outer composition layer. Node does
// not import Host integration; it only requires a current Profile to remain
// bound to its canonical Node bundle and Host projection.
type InstallationVerifier interface {
	Verify(context.Context, model.Profile) error
}

type InstallationVerifierFunc func(context.Context, model.Profile) error

func (verify InstallationVerifierFunc) Verify(ctx context.Context, profile model.Profile) error {
	if verify == nil || ctx == nil {
		return errors.New("managed installation verifier is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verify(ctx, profile); err != nil {
		return err
	}
	return ctx.Err()
}

func actionPolicyForInstallation(installation InstallationVerifier) (agent.ActionPolicy, error) {
	if isNilNodeInterface(installation) {
		return agent.ActionPolicy{}, errors.New("managed installation is unavailable")
	}
	provider, ok := installation.(agent.ActionAssetProvider)
	if !ok {
		return agent.ActionPolicy{}, errors.New("managed installation does not provide Teamwork action assets")
	}
	policy, err := agent.NewActionPolicy(provider)
	if err != nil {
		return agent.ActionPolicy{}, fmt.Errorf("managed installation action policy: %w", err)
	}
	if _, err := agent.NewActionHandlers(policy); err != nil {
		return agent.ActionPolicy{}, fmt.Errorf("managed installation typed action policy: %w", err)
	}
	return policy, nil
}

func loadActivationIdentity(nodeState string, installation InstallationVerifier,
	revision string,
) (*Identity, error) {
	policy, err := actionPolicyForInstallation(installation)
	if err != nil {
		return nil, fmt.Errorf("action policy: %w", err)
	}
	if policy.AssetRevision().String() != revision {
		return nil, errors.New("action policy differs from requested asset revision")
	}
	return LoadIdentity(nodeState)
}
