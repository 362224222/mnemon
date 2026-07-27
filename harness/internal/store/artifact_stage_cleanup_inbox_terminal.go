package store

import (
	"context"
	"database/sql"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func peerInboxArtifactStageTerminalUnaccepted(ctx context.Context, tx *sql.Tx,
	id model.InboxID, stage durableArtifactStage, owner artifactdomain.StageOwner,
	cutoff, at time.Time,
) (bool, error) {
	if stage.updatedAt.After(cutoff) {
		return false, nil
	}
	row, err := readPeerInboxArtifactRow(ctx, tx, id)
	if err != nil {
		return false, ErrArtifactStageConflict
	}
	permanent, err := peerInboxArtifactPinsPermanent(ctx, tx, row, at)
	if err != nil {
		return false, err
	}
	if permanent {
		return false, nil
	}
	switch row.status {
	case model.InboxQuarantined:
		return !row.updatedAt.After(cutoff), nil
	case model.InboxWaitingArtifact:
		return settleExpiredTerminalPeerInboxArtifactStage(
			ctx, tx, row, stage, owner, at)
	default:
		return false, nil
	}
}

func settleExpiredTerminalPeerInboxArtifactStage(
	ctx context.Context, tx *sql.Tx, row peerInboxArtifactRow,
	stage durableArtifactStage, owner artifactdomain.StageOwner, at time.Time,
) (bool, error) {
	fence := PeerInboxArtifactFence{
		inboxID: row.inboxID, leaseOwner: stage.leaseOwner,
		leaseUntil: stage.leaseUntil, attempt: stage.attempt,
	}
	if !exactExpiredPeerInboxArtifactFence(row, fence, at) {
		return false, nil
	}
	terminal, err := peerInboxArtifactAuthorityTerminal(ctx, tx, row)
	if err != nil || !terminal {
		return false, err
	}
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(
		ctx, tx, row)
	if err != nil {
		return false, err
	}
	if err := quarantineTerminalPeerInboxArtifactPublish(ctx, tx, row, stage,
		fence, owner, at, hasRenewReceipt); err != nil {
		return false, err
	}
	return true, nil
}
