package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type peerInboxArtifactPrepareInput struct {
	at        time.Time
	expiresAt time.Time
	closure   VerifiedArtifactClosure
}

// PreparePeerInboxArtifactPublish atomically installs the exact immutable
// relational closure and moves its pre-registered owner to publishing. It
// deliberately does not promote roots to verified authority.
func (s *Store) PreparePeerInboxArtifactPublish(ctx context.Context,
	spec PreparePeerInboxArtifactPublishSpec,
) (PeerInboxArtifactStage, error) {
	input, err := validatePeerInboxArtifactPrepareInput(s, ctx, spec)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"stage Peer Inbox Artifact closure: begin: %w", err)
	}
	defer tx.Rollback()
	row, stage, err := readPeerInboxArtifactPrepareState(ctx, tx, spec, input)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if stage.state != ArtifactStageStaged {
		return replayPeerInboxArtifactPrepare(ctx, tx, row, stage, input)
	}
	if stage.cleanupClaimed {
		return PeerInboxArtifactStage{}, ErrArtifactStageFence
	}
	replayed, err := installPeerInboxArtifactPrepare(ctx, tx, row, stage, spec, input)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"prepare Peer Inbox Artifact publish: commit: %w", err)
	}
	return PeerInboxArtifactStage{changed: !replayed, replayed: replayed}, nil
}

func validatePeerInboxArtifactPrepareInput(s *Store, ctx context.Context,
	spec PreparePeerInboxArtifactPublishSpec,
) (peerInboxArtifactPrepareInput, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return peerInboxArtifactPrepareInput{}, err
	}
	if err := requirePeerInboxArtifactPublishOwner(spec.Owner, spec.Fence); err != nil {
		return peerInboxArtifactPrepareInput{}, err
	}
	closure, err := validateVerifiedArtifactClosure(spec.Closure)
	if err != nil {
		return peerInboxArtifactPrepareInput{}, fmt.Errorf("%w: staged closure: %v",
			ErrPeerInboxArtifactInput, err)
	}
	if err := requirePeerInboxArtifactPrepareTimes(closure, at); err != nil {
		return peerInboxArtifactPrepareInput{}, err
	}
	expiresAt, err := canonicalStoreTime(spec.Fence.leaseUntil.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return peerInboxArtifactPrepareInput{}, fmt.Errorf("%w: stage expiry: %v",
			ErrPeerInboxArtifactInput, err)
	}
	return peerInboxArtifactPrepareInput{at: at, expiresAt: expiresAt, closure: closure}, nil
}

func requirePeerInboxArtifactPrepareTimes(closure VerifiedArtifactClosure, at time.Time) error {
	for _, root := range closure.Roots {
		if root.CreatedAt.After(at) || root.VerifiedAt.After(at) {
			return fmt.Errorf("%w: staged root time exceeds trusted time",
				ErrPeerInboxArtifactInput)
		}
	}
	for _, block := range closure.Blocks {
		if block.CreatedAt.After(at) {
			return fmt.Errorf("%w: staged block time exceeds trusted time",
				ErrPeerInboxArtifactInput)
		}
	}
	return nil
}

func readPeerInboxArtifactPrepareState(ctx context.Context, tx *sql.Tx,
	spec PreparePeerInboxArtifactPublishSpec, input peerInboxArtifactPrepareInput,
) (peerInboxArtifactRow, durableArtifactStage, error) {
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, err
	}
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, input.at); err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, input.at); err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, err
	}
	stage, found, err := readPeerInboxArtifactStage(ctx, tx, row.inboxID)
	if err != nil || !found || !exactPeerInboxArtifactPublishStage(row, stage,
		spec.Fence, spec.Owner) {
		return peerInboxArtifactRow{}, durableArtifactStage{}, ErrArtifactStageFence
	}
	if input.at.Before(stage.updatedAt) {
		return peerInboxArtifactRow{}, durableArtifactStage{}, ErrPeerInboxArtifactStale
	}
	if !equalPeerInboxArtifactClosureRoots(row.requiredRoots, input.closure.Roots) {
		return peerInboxArtifactRow{}, durableArtifactStage{}, fmt.Errorf(
			"%w: staged roots differ from immutable Inbox roots", ErrPeerInboxArtifactInput)
	}
	return row, stage, nil
}

func replayPeerInboxArtifactPrepare(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage, input peerInboxArtifactPrepareInput,
) (PeerInboxArtifactStage, error) {
	if stage.state != ArtifactStagePublishing {
		return PeerInboxArtifactStage{}, ErrArtifactStageConflict
	}
	projected, err := readPeerInboxArtifactClosureProjection(ctx, tx, row.inboxID)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := requirePeerInboxArtifactRootProjection(ctx, tx, row.inboxID,
		projected.Roots); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := requirePeerInboxArtifactPrepareDigest(input.closure, projected, stage); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	hasPins, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, input.at, input.expiresAt)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if len(row.requiredRoots) != 0 && !hasPins {
		return PeerInboxArtifactStage{}, ErrPeerInboxArtifactInvariant
	}
	if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
		row.requiredRoots, input.at, input.expiresAt, false); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"prepare Peer Inbox Artifact publish: replay commit: %w", err)
	}
	return PeerInboxArtifactStage{replayed: true}, nil
}

func requirePeerInboxArtifactPrepareDigest(requested, projected VerifiedArtifactClosure,
	stage durableArtifactStage,
) error {
	requestedDigest, requestedErr := verifiedClosureDigest(requested)
	projectedDigest, projectedErr := verifiedClosureDigest(projected)
	if requestedErr != nil || projectedErr != nil ||
		requestedDigest != projectedDigest || stage.payloadDigest != projectedDigest {
		return ErrArtifactStageConflict
	}
	return nil
}

func installPeerInboxArtifactPrepare(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage,
	spec PreparePeerInboxArtifactPublishSpec, input peerInboxArtifactPrepareInput,
) (bool, error) {
	durableClosure := cloneVerifiedArtifactClosureValue(input.closure)
	replayed, err := checkpointPeerInboxArtifactPrepareBlocks(ctx, tx,
		&durableClosure, input.at)
	if err != nil {
		return false, err
	}
	rootStates, rootsReplayed, err := checkpointPeerInboxArtifactPrepareRoots(
		ctx, tx, &durableClosure, input.at)
	if err != nil {
		return false, err
	}
	mapsReplayed, err := checkpointPeerInboxArtifactPrepareRootMaps(ctx, tx,
		input.closure, rootStates)
	if err != nil {
		return false, err
	}
	pinsReplayed, err := stagePeerInboxArtifactPins(ctx, tx, row.inboxID,
		row.requiredRoots, input.at, input.expiresAt)
	if err != nil {
		return false, err
	}
	if err := finishPeerInboxArtifactPrepare(ctx, tx, row, spec,
		durableClosure, input.at); err != nil {
		return false, err
	}
	return replayed && rootsReplayed && mapsReplayed && pinsReplayed, nil
}

func checkpointPeerInboxArtifactPrepareBlocks(ctx context.Context, tx *sql.Tx,
	closure *VerifiedArtifactClosure, at time.Time,
) (bool, error) {
	replayed := true
	for index, block := range closure.Blocks {
		stored, found, err := checkpointArtifactBlock(ctx, tx, block)
		if err != nil {
			return false, err
		}
		if stored.CreatedAt.After(at) {
			return false, fmt.Errorf("%w: shared staged block observation is newer",
				ErrPeerInboxArtifactStale)
		}
		closure.Blocks[index] = stored
		replayed = replayed && found
	}
	return replayed, nil
}

func checkpointPeerInboxArtifactPrepareRoots(ctx context.Context, tx *sql.Tx,
	closure *VerifiedArtifactClosure, at time.Time,
) (map[model.Digest]string, bool, error) {
	states := make(map[model.Digest]string, len(closure.Roots))
	replayed := true
	for index, root := range closure.Roots {
		stored, state, found, err := stageArtifactClosureRoot(ctx, tx, root)
		if err != nil {
			return nil, false, err
		}
		if stored.CreatedAt.After(at) || state == "verified" && stored.VerifiedAt.After(at) {
			return nil, false, fmt.Errorf("%w: shared staged root observation is newer",
				ErrPeerInboxArtifactStale)
		}
		stored.VerifiedAt = root.VerifiedAt
		if stored.VerifiedAt.Before(stored.CreatedAt) {
			stored.VerifiedAt = stored.CreatedAt
		}
		closure.Roots[index] = stored
		states[stored.RootDigest] = state
		replayed = replayed && found
	}
	return states, replayed, nil
}

func checkpointPeerInboxArtifactPrepareRootMaps(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure, rootStates map[model.Digest]string,
) (bool, error) {
	replayed := true
	for _, root := range closure.Roots {
		state, err := checkpointArtifactRootBlockMap(ctx, tx, root.RootDigest,
			rootBlocksForDigest(closure.RootBlocks, root.RootDigest))
		if err != nil {
			return false, err
		}
		if state.rootState != rootStates[root.RootDigest] {
			return false, fmt.Errorf("%w: staged root state changed",
				ErrPeerInboxArtifactInvariant)
		}
		replayed = replayed && !state.changed
	}
	return replayed, nil
}

func finishPeerInboxArtifactPrepare(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, spec PreparePeerInboxArtifactPublishSpec,
	closure VerifiedArtifactClosure, at time.Time,
) error {
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return err
	}
	durableClosure, err := validateVerifiedArtifactClosure(closure)
	if err != nil {
		return ErrArtifactStageConflict
	}
	if err := insertPeerInboxArtifactRootProjection(ctx, tx, row.inboxID,
		durableClosure.Roots); err != nil {
		return err
	}
	closureDigest, err := verifiedClosureDigest(durableClosure)
	if err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE peer_inbox_artifact_stages
		SET state='publishing',closure_digest=?,updated_at=?
		WHERE inbox_id=? AND generation=? AND state='staged' AND attempt=?
		AND lease_owner=? AND lease_until=? AND semantic_nonce=?
		AND cleanup_started_at IS NULL`,
		closureDigest.Bytes(), storeTime(at), row.inboxID.String(), spec.Owner.Generation(),
		row.attempts, row.leaseOwner, storeTime(row.leaseUntil), row.semanticNonce[:])
	if err != nil || exactlyOne(updated) != nil {
		return fmt.Errorf("%w: publish Inbox stage: %v", ErrArtifactStageFence, err)
	}
	return nil
}
