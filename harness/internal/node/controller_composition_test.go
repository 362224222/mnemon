package node

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestBindControllerActionPolicyVerifiesAndFreezesTheInstallationRevision(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	install := testInstallationWithActions(InstallationVerifierFunc(func(context.Context, model.Profile) error {
		verified++
		return nil
	}), bundle)
	policy := agent.ActionPolicy{}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		bundle.Revision(), &policy); err != nil {
		t.Fatal(err)
	}
	if verified != 1 || policy.AssetRevision().String() != bundle.Revision() ||
		len(policy.Actions()) != teamwork.TeamworkActionCount {
		t.Fatalf("bound policy = (%d, %s, %d)", verified,
			policy.AssetRevision(), len(policy.Actions()))
	}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		model.Sum([]byte("other controller revision")).String(), &policy); err == nil {
		t.Fatal("bindControllerActionPolicy accepted a different active revision")
	}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		bundle.Revision(), nil); err == nil {
		t.Fatal("bindControllerActionPolicy accepted a missing policy destination")
	}
}
