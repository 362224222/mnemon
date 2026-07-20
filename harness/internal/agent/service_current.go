package agent

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type agentCurrentClaim struct {
	result store.AgentClaimResult
	hash   model.Digest
	secret []byte
}

func (s *Service) claimAgentCurrent(ctx context.Context, metadata ControlMetadata,
	at time.Time,
) (agentCurrentClaim, *ControlError) {
	if metadata.HasRunAttachment {
		claimed, err := s.store.ConsumeAgentRunAttachment(ctx, store.AgentAttachmentSpec{
			ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
			AttachmentTokenHash: metadata.RunAttachmentHash, At: at,
		})
		if err != nil {
			return agentCurrentClaim{}, mapControlError(err)
		}
		claimHash, _ := claimed.Run.ClaimFenceHash()
		return agentCurrentClaim{result: claimed, hash: claimHash}, nil
	}

	budget, err := model.ParseHandlingBudget(metadata.Profile.HandlingBudget())
	if err != nil {
		return agentCurrentClaim{}, NewControlError(CodeInternal,
			"Profile handling budget is invalid")
	}
	leaseUntil := at.Add(time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second)
	if !leaseUntil.After(at) {
		return agentCurrentClaim{}, NewControlError(CodeInternal,
			"managed claim lease cannot be represented")
	}
	claimSecret, err := s.drawSecret()
	if err != nil {
		return agentCurrentClaim{}, NewControlError(CodeInternal,
			"managed claim capability cannot be generated")
	}
	claimOwnerBytes, err := s.drawSecret()
	if err != nil {
		clear(claimSecret)
		return agentCurrentClaim{}, NewControlError(CodeInternal,
			"managed claim owner cannot be generated")
	}
	claimOwner := "claim-" + base64.RawURLEncoding.EncodeToString(claimOwnerBytes)
	clear(claimOwnerBytes)
	claimHash := model.Sum(claimSecret)
	claimed, err := s.store.ClaimAgentCurrent(ctx, store.AgentClaimSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
		ClaimOwner: claimOwner, ClaimTokenHash: claimHash, At: at, LeaseUntil: leaseUntil,
	})
	if err != nil {
		clear(claimSecret)
		return agentCurrentClaim{}, mapControlError(err)
	}
	return agentCurrentClaim{result: claimed, hash: claimHash, secret: claimSecret}, nil
}
