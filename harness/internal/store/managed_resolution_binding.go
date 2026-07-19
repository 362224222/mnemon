package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ManagedResolutionRequestDigest is the closed semantic digest used both when
// reserving and committing an Agent resolution. Including the server-resolved
// context hash and managed asset revision prevent semantic substitution behind
// one client key.
func ManagedResolutionRequestDigest(assetRevision, contextHash model.Digest,
	kind model.OperationKind, content string,
) (model.Digest, error) {
	if assetRevision.IsZero() || contextHash.IsZero() || !managedResolutionKind(kind) {
		return model.Digest{}, fmt.Errorf("%w: revision, context and closed resolution kind are required",
			ErrManagedResolutionInput)
	}
	if err := validateManagedResolutionContent(kind, content); err != nil {
		return model.Digest{}, err
	}
	request, err := model.JSONFrom(struct {
		SchemaVersion int                 `json:"schema_version"`
		Domain        string              `json:"domain"`
		AssetRevision model.Digest        `json:"asset_revision"`
		Content       string              `json:"content"`
		ContextHash   model.Digest        `json:"context_hash"`
		Kind          model.OperationKind `json:"kind"`
	}{1, "mnemon/r5/managed-resolution-request/v1", assetRevision, content, contextHash, kind})
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: canonical request: %v", ErrManagedResolutionInput, err)
	}
	return model.Sum(request.Bytes()), nil
}

func readManagedResolutionBinding(ctx context.Context, tx *sql.Tx, supplied model.Operation,
	contextHash model.Digest, content string,
) (model.Operation, model.Profile, error) {
	operation, err := readOperationByID(ctx, tx, supplied.ID())
	if err != nil {
		return model.Operation{}, model.Profile{}, fmt.Errorf("commit managed resolution: operation: %w", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil || profile.ID() != operation.ProfileID() {
		return model.Operation{}, model.Profile{}, fmt.Errorf("%w: Profile is unavailable",
			ErrManagedResolutionInvariant)
	}
	assetRevision := managedAssetRevisionDigest(profile.ActiveAssetRevision())
	requestDigest, err := ManagedResolutionRequestDigest(assetRevision, contextHash,
		supplied.Kind(), content)
	if err != nil {
		return model.Operation{}, model.Profile{}, err
	}
	if !sameManagedResolutionOperation(operation, supplied) || operation.RequestDigest() != requestDigest {
		return model.Operation{}, model.Profile{}, ErrOperationMismatch
	}
	return operation, profile, nil
}

func managedAssetRevisionDigest(value string) model.Digest {
	if value == "" {
		return model.Digest{}
	}
	if revision, err := model.ParseDigest(value); err == nil {
		return revision
	}
	return model.Sum([]byte("mnemon/r5/managed-asset-revision/v1\x00" + value))
}
