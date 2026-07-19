//go:build darwin || linux

package process_test

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type setupProcessInstallation struct {
	node.InstallationVerifier
	bundle   assets.Bundle
	revision string
}

func setupProcessInstallationFor(revision string, bundle assets.Bundle,
	verify func(context.Context, model.Profile) error,
) node.InstallationVerifier {
	return setupProcessInstallation{InstallationVerifier: node.InstallationVerifierFunc(verify),
		bundle: bundle, revision: revision}
}

func (installation setupProcessInstallation) Revision() string {
	return installation.revision
}

func (installation setupProcessInstallation) TeamworkActionPaths() []string {
	return installation.bundle.TeamworkActionPaths()
}

func (installation setupProcessInstallation) ReadTeamworkAction(path string) ([]byte, error) {
	return installation.bundle.ReadTeamworkAction(path)
}
