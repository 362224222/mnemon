package agent

import (
	"context"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type committedTeamworkCaptureRoot struct {
	ManifestDigest string `json:"manifest_digest"`
	RootDigest     string `json:"root_digest"`
}

func (e *TeamworkActionExecutor) projectFreshCommit(ctx context.Context,
	operation model.Operation, handler ActionHandler, artifacts []model.ArtifactRef,
	result store.LocalAcceptanceResult,
) (OperationResponse, *ControlError) {
	if hasProducedArtifact(artifacts) {
		if apiErr := e.publishAccepted(ctx, operation.ID()); apiErr != nil {
			return OperationResponse{}, apiErr
		}
	}
	response, err := decodeCommittedTeamworkReceipt(handler, result.Receipt,
		operation, result.Replayed)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"committed Teamwork receipt cannot be projected")
	}
	return response, nil
}

func hasProducedArtifact(artifacts []model.ArtifactRef) bool {
	for _, ref := range artifacts {
		if ref.Role() == model.ArtifactProduced {
			return true
		}
	}
	return false
}

func (e *TeamworkActionExecutor) publishAccepted(ctx context.Context,
	operation model.OperationID,
) *ControlError {
	if e == nil || e.artifacts == nil || ctx == nil || operation.IsZero() {
		return NewControlError(CodeInternal,
			"Artifact publication coordinator is unavailable")
	}
	return e.artifacts.PublishAccepted(ctx, operation)
}

func validateCommittedTeamworkCapture(operation model.Operation,
	rows []committedTeamworkCaptureRoot,
) (map[model.Digest]struct{}, error) {
	var checkpointRoots []ArtifactCaptureRoot
	switch operation.Status() {
	case model.OperationCommitted:
		var err error
		checkpointRoots, err = operationArtifactCaptureRoots(operation)
		if err != nil || len(checkpointRoots) != len(rows) {
			return nil, errors.New("committed Teamwork capture checkpoint differs")
		}
	case model.OperationStarted:
	default:
		return nil, errors.New("invalid Teamwork operation receipt state")
	}

	captured := make(map[model.Digest]struct{}, len(rows))
	previous := ""
	for index, row := range rows {
		root, rootErr := model.ParseDigest(row.RootDigest)
		manifest, manifestErr := model.ParseDigest(row.ManifestDigest)
		if rootErr != nil || manifestErr != nil ||
			(previous != "" && previous >= row.RootDigest) {
			return nil, errors.New("invalid committed Teamwork capture roots")
		}
		if operation.Status() == model.OperationCommitted &&
			(checkpointRoots[index].RootDigest != root ||
				checkpointRoots[index].ManifestDigest != manifest) {
			return nil, errors.New("committed Teamwork capture checkpoint differs")
		}
		previous = row.RootDigest
		captured[root] = struct{}{}
	}
	return captured, nil
}
