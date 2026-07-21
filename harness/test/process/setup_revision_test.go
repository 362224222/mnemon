//go:build darwin || linux

package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func setupProcessProvisionDisabledRevision(t *testing.T, workspace, nodeState,
	revision string, at time.Time,
) node.ProvisionResult {
	t.Helper()
	provisioned, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: revision,
		Clock: setupProcessClock{at: at}, Credentials: localapi.NodeRuntime{},
	})
	if err != nil || provisioned.NodeState != nodeState || provisioned.Profile.Enabled() {
		t.Fatalf("provision old revision = (%#v, %v)", provisioned, err)
	}
	return provisioned
}
