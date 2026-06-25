package state

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// AuthorityRules.Enforce takes NO Version (authorization is not concurrency, Invariant #11).
type AuthorityRules struct {
	Allow map[contract.ActorID][]contract.ResourceKind
}

func (r AuthorityRules) Enforce(actor contract.ActorID, kind contract.ResourceKind) error {
	for _, k := range r.Allow[actor] {
		if k == kind {
			return nil
		}
	}
	return fmt.Errorf("%w: %q may not write %q", errAuthz, actor, kind)
}
