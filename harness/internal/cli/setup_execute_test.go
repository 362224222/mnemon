package cli

import (
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestSetupRejectsActionPreflightBeforeCompanionOrNodeMutation(t *testing.T) {
	fixture := newSetupFixture(t, assets.HostCodex, true)
	fixture.fail["new-preflight"] = errors.New("injected canonical action policy failure")

	exit, stdout, stderr := fixture.run()
	if exit != 3 || stdout != "" || stderr !=
		"asset_revision_mismatch: canonical managed assets or projection are invalid\n" {
		t.Fatalf("early preflight = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	fixture.wantOrder(t, "cwd", "load-bundle", "new-preflight")
	for _, forbidden := range []string{"new-companion", "bootstrap", "lock", "deactivate:codex",
		"install-bundle", "install-projection:codex", "activate:codex", "ensure"} {
		if fixture.called(forbidden) {
			t.Fatalf("early preflight failure reached %s: %v", forbidden, fixture.order)
		}
	}
}
