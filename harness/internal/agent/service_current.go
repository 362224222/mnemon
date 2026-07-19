package agent

import (
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (s *Service) currentReadSpec(profile model.Profile, runID model.RunID,
	claimHash model.Digest, at time.Time,
) store.AgentCurrentReadSpec {
	return store.AgentCurrentReadSpec{ProfileID: profile.ID(), ExpectedAssetRevision: s.assetRevision,
		RunID: runID, ClaimTokenHash: claimHash, At: at, ActionPolicy: s.actions.RuntimePolicy()}
}
