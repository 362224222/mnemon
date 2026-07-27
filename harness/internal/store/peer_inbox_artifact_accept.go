package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// AcceptPeerInboxArtifactPublish establishes durable ownership before any
// staged byte is promoted into final CAS. The Inbox remains non-ready until
// the filesystem publication is complete.
func (s *Store) AcceptPeerInboxArtifactPublish(ctx context.Context,
	spec AcceptPeerInboxArtifactPublishSpec,
) (PeerInboxArtifactStage, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := requirePeerInboxArtifactPublishOwner(spec.Owner, spec.Fence); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"accept Peer Inbox Artifact publish: begin: %w", err)
	}
	defer tx.Rollback()
	row, stage, hasRenewReceipt, accepted, err := readPeerInboxArtifactAcceptState(
		ctx, tx, spec, at)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if accepted {
		return replayPeerInboxArtifactAccept(tx)
	}
	terminal, err := settleTerminalPeerInboxArtifactAccept(ctx, tx, row, stage,
		spec, at, hasRenewReceipt)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if terminal {
		return PeerInboxArtifactStage{}, ErrPeerInboxArtifactStale
	}
	if err := acceptPeerInboxArtifactPublish(ctx, tx, row, at, hasRenewReceipt); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"accept Peer Inbox Artifact publish: commit: %w", err)
	}
	return PeerInboxArtifactStage{changed: true}, nil
}

func readPeerInboxArtifactAcceptState(ctx context.Context, tx *sql.Tx,
	spec AcceptPeerInboxArtifactPublishSpec, at time.Time,
) (peerInboxArtifactRow, durableArtifactStage, bool, bool, error) {
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, false, false, err
	}
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return peerInboxArtifactRow{}, durableArtifactStage{}, false, false, err
	}
	stage, found, err := readPeerInboxArtifactStage(ctx, tx, row.inboxID)
	if err != nil || !found || stage.cleanupClaimed ||
		!exactPeerInboxArtifactPublishStage(row, stage, spec.Fence, spec.Owner) {
		return peerInboxArtifactRow{}, durableArtifactStage{}, false, false,
			ErrArtifactStageFence
	}
	accepted, err := peerInboxArtifactPublishAccepted(ctx, tx, row, stage, at)
	return row, stage, hasRenewReceipt, accepted, err
}

func replayPeerInboxArtifactAccept(tx *sql.Tx) (PeerInboxArtifactStage, error) {
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf(
			"accept Peer Inbox Artifact publish: replay commit: %w", err)
	}
	return PeerInboxArtifactStage{replayed: true}, nil
}

func settleTerminalPeerInboxArtifactAccept(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage,
	spec AcceptPeerInboxArtifactPublishSpec, at time.Time, hasRenewReceipt bool,
) (bool, error) {
	terminal, err := classifyPeerInboxArtifactAcceptAuthority(
		ctx, tx, row, spec.Fence, at)
	if err != nil || !terminal {
		return terminal, err
	}
	if err := quarantineTerminalPeerInboxArtifactPublish(ctx, tx, row, stage,
		spec.Fence, spec.Owner, at, hasRenewReceipt); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf(
			"accept Peer Inbox Artifact publish: terminal commit: %w", err)
	}
	return true, nil
}

func acceptPeerInboxArtifactPublish(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time, hasRenewReceipt bool,
) error {
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx,
		row.requiredRoots, at); err != nil {
		return err
	}
	if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
		row.requiredRoots, at, time.Time{}, false); err != nil {
		return err
	}
	if err := requirePeerInboxStageProjectionRoots(ctx, tx, row.inboxID,
		row.requiredRoots); err != nil {
		return err
	}
	if err := makePeerInboxArtifactPinsPermanent(ctx, tx, row); err != nil {
		return err
	}
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx,
		row.requiredRoots, at); err != nil {
		return err
	}
	if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID,
		row.requiredRoots, at); err != nil {
		return err
	}
	return deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID, hasRenewReceipt)
}

func promotePeerInboxArtifactRoots(ctx context.Context, tx *sql.Tx,
	roots []model.Digest, at time.Time,
) error {
	for _, root := range roots {
		result, err := tx.ExecContext(ctx, `UPDATE artifact_roots SET state='verified',verified_at=?
			WHERE root_digest=? AND state='staged' AND verified_at IS NULL`, storeTime(at), root.String())
		if err != nil {
			return fmt.Errorf("%w: promote staged Inbox root: %v",
				ErrPeerInboxArtifactInvariant, err)
		}
		if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected > 1 {
			return fmt.Errorf("%w: promote staged Inbox root cardinality",
				ErrPeerInboxArtifactInvariant)
		}
	}
	return nil
}

func makePeerInboxArtifactPinsPermanent(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow,
) error {
	if len(row.requiredRoots) == 0 {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE artifact_pins SET expires_at=NULL
		WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NOT NULL`, row.inboxID.String())
	if err != nil {
		return fmt.Errorf("%w: make Inbox pins permanent: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	affected, affectedErr := result.RowsAffected()
	if affectedErr != nil || affected != int64(len(row.requiredRoots)) {
		return fmt.Errorf("%w: make Inbox pins permanent cardinality",
			ErrPeerInboxArtifactInvariant)
	}
	return nil
}

func peerInboxArtifactPublishAccepted(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage, at time.Time,
) (bool, error) {
	if stage.state != ArtifactStagePublishing && stage.state != ArtifactStageReady {
		return false, ErrArtifactStageFence
	}
	permanent, err := peerInboxArtifactPinsPermanent(ctx, tx, row, at)
	if err != nil || !permanent {
		return false, err
	}
	if err := requirePeerInboxStageProjectionRoots(ctx, tx, row.inboxID,
		row.requiredRoots); err != nil {
		return false, err
	}
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx,
		row.requiredRoots, at); err != nil {
		return false, err
	}
	return true, nil
}

func peerInboxArtifactPinsPermanent(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time,
) (bool, error) {
	pins, err := readPeerInboxArtifactPins(ctx, tx, row.inboxID)
	if err != nil {
		return false, err
	}
	if err := requireExactPeerInboxArtifactPinRoots(pins, row.requiredRoots, at); err != nil {
		return false, err
	}
	if len(pins) == 0 {
		return false, nil
	}
	hasExpiry := pins[0].hasExpiry
	for _, pin := range pins[1:] {
		if pin.hasExpiry != hasExpiry {
			return false, ErrPeerInboxArtifactInvariant
		}
	}
	return !hasExpiry, nil
}

func requirePeerInboxArtifactPublishOwner(owner artifactdomain.StageOwner,
	fence PeerInboxArtifactFence,
) error {
	if owner.IsZero() || owner.Kind() != artifactdomain.StageOwnerInbox ||
		owner.CanonicalID() != fence.inboxID.String() {
		return ErrArtifactStageFence
	}
	return nil
}

func exactPeerInboxArtifactPublishStage(row peerInboxArtifactRow,
	stage durableArtifactStage, fence PeerInboxArtifactFence,
	owner artifactdomain.StageOwner,
) bool {
	return stage.generation == owner.Generation() &&
		stage.attempt == fence.attempt &&
		stage.leaseOwner == fence.leaseOwner &&
		stage.leaseUntil.Equal(fence.leaseUntil) &&
		stage.semanticNonce == row.semanticNonce
}
