package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const peerInboxArtifactTerminalAuthorityDiagnostic = "artifact_authority_terminal"

func classifyPeerInboxArtifactAcceptAuthority(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, fence PeerInboxArtifactFence, at time.Time,
) (bool, error) {
	fenceErr := requireLivePeerInboxArtifactFence(row, fence, at)
	if fenceErr != nil && !exactExpiredPeerInboxArtifactFence(row, fence, at) {
		return false, fenceErr
	}
	if fenceErr == nil {
		authorityErr := requirePeerInboxArtifactAuthority(ctx, tx, row, at)
		if authorityErr == nil {
			return false, nil
		}
		if !errors.Is(authorityErr, ErrPeerInboxArtifactAuthority) {
			return false, authorityErr
		}
		terminal, err := peerInboxArtifactAuthorityTerminal(ctx, tx, row)
		if err != nil || terminal {
			return terminal, err
		}
		return false, authorityErr
	}
	terminal, err := peerInboxArtifactAuthorityTerminal(ctx, tx, row)
	if err != nil || terminal {
		return terminal, err
	}
	return false, fenceErr
}

func exactExpiredPeerInboxArtifactFence(row peerInboxArtifactRow,
	fence PeerInboxArtifactFence, at time.Time,
) bool {
	return row.inboxID == fence.inboxID &&
		row.status == model.InboxWaitingArtifact && row.hasLease &&
		row.attempts == fence.attempt && row.leaseOwner == fence.leaseOwner &&
		row.leaseUntil.Equal(fence.leaseUntil) &&
		!at.Before(row.updatedAt) && !at.Before(row.leaseUntil)
}

func peerInboxArtifactAuthorityTerminal(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow,
) (bool, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return false, fmt.Errorf("%w: Node: %v", ErrPeerInboxArtifactInvariant, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), row.channelID)
	if err != nil {
		return false, fmt.Errorf("%w: current Channel projection: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if authority.channel.Status().Terminal() {
		return true, nil
	}
	for _, binding := range authority.bindings {
		if binding.PeerID() == row.originPeerID &&
			binding.OriginEpoch() == row.originEpoch &&
			binding.State() == model.BindingRevoked {
			return true, nil
		}
	}
	if member, found := authority.roster.CurrentMember(row.originPeerID); found &&
		member.OriginEpoch() == row.originEpoch && member.Status().Terminal() {
		return true, nil
	}
	var quarantined int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM origin_quarantines
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?)`,
		row.channelID.String(), row.originPeerID.String(), row.originEpoch.String()).
		Scan(&quarantined); err != nil {
		return false, fmt.Errorf("%w: origin quarantine: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return quarantined != 0, nil
}

func quarantineTerminalPeerInboxArtifactPublish(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage,
	fence PeerInboxArtifactFence, owner artifactdomain.StageOwner,
	at time.Time, hasRenewReceipt bool,
) error {
	if stage.state != ArtifactStagePublishing ||
		!exactPeerInboxArtifactPublishStage(row, stage, fence, owner) {
		return ErrArtifactStageFence
	}
	expiresAt, err := canonicalStoreTime(at.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return fmt.Errorf("%w: terminal stage expiry: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	hasPins, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, at, expiresAt)
	if err != nil {
		return err
	}
	if len(row.requiredRoots) != 0 && !hasPins {
		return fmt.Errorf("%w: terminal publish has no expiring stage pins",
			ErrPeerInboxArtifactInvariant)
	}
	if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
		row.requiredRoots, at, expiresAt, false); err != nil {
		return err
	}
	if err := deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID,
		hasRenewReceipt); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox
		SET status='quarantined',next_attempt_at=?,lease_owner=NULL,lease_until=NULL,
			diagnostic=?,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=?
			AND lease_owner=? AND lease_until=? AND updated_at=?`,
		storeTime(at), peerInboxArtifactTerminalAuthorityDiagnostic, storeTime(at),
		row.inboxID.String(), row.attempts, row.leaseOwner,
		storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return fmt.Errorf("%w: terminal quarantine update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result,
		"terminal authority Peer Inbox Artifact CAS"); err != nil {
		return fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	return nil
}
