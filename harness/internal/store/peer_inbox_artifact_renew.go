package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type peerInboxArtifactRenewTimes struct {
	at             time.Time
	leaseUntil     time.Time
	stageExpiresAt time.Time
}

// RenewPeerInboxArtifactLease extends one still-live fence to trusted At+120s.
// Repeating the same renewal after response loss returns the already installed
// fence without another write.
func (s *Store) RenewPeerInboxArtifactLease(ctx context.Context,
	spec RenewPeerInboxArtifactSpec,
) (PeerInboxArtifactRenewal, error) {
	times, err := peerInboxArtifactRenewalTimes(s, ctx, spec)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf(
			"renew Peer Inbox Artifact: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	receipt, hasReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if hasReceipt && receipt.matchesRequest(spec.Fence, times.at) {
		return replayPeerInboxArtifactRenewal(ctx, tx, row, receipt, spec, times)
	}
	return renewPeerInboxArtifactLease(ctx, tx, row, spec, times)
}

func peerInboxArtifactRenewalTimes(s *Store, ctx context.Context,
	spec RenewPeerInboxArtifactSpec,
) (peerInboxArtifactRenewTimes, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return peerInboxArtifactRenewTimes{}, err
	}
	leaseUntil, err := canonicalStoreTime(at.Add(peerInboxArtifactLease))
	if err != nil {
		return peerInboxArtifactRenewTimes{}, fmt.Errorf("%w: derived renewal: %v",
			ErrPeerInboxArtifactInput, err)
	}
	stageExpiresAt, err := canonicalStoreTime(leaseUntil.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return peerInboxArtifactRenewTimes{}, fmt.Errorf("%w: renewal stage expiry: %v",
			ErrPeerInboxArtifactInput, err)
	}
	return peerInboxArtifactRenewTimes{
		at: at, leaseUntil: leaseUntil, stageExpiresAt: stageExpiresAt,
	}, nil
}

func replayPeerInboxArtifactRenewal(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, receipt peerInboxArtifactRenewReceipt,
	spec RenewPeerInboxArtifactSpec, times peerInboxArtifactRenewTimes,
) (PeerInboxArtifactRenewal, error) {
	if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
		row.requiredRoots, times.leaseUntil, times.stageExpiresAt, true); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := renewPeerInboxArtifactStageFence(ctx, tx, row, spec.Fence,
		times.leaseUntil, times.at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf(
			"renew Peer Inbox Artifact: replay commit: %w", err)
	}
	return PeerInboxArtifactRenewal{fence: PeerInboxArtifactFence{
		inboxID: row.inboxID, leaseOwner: receipt.output.leaseOwner,
		leaseUntil: receipt.output.leaseUntil, attempt: receipt.output.attempt,
	}, replayed: true}, nil
}

func renewPeerInboxArtifactLease(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, spec RenewPeerInboxArtifactSpec,
	times peerInboxArtifactRenewTimes,
) (PeerInboxArtifactRenewal, error) {
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, times.at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, times.at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if _, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, times.at, times.stageExpiresAt); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := renewPeerInboxArtifactStageFence(ctx, tx, row, spec.Fence,
		times.leaseUntil, times.at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := updatePeerInboxArtifactRenewal(ctx, tx, row, times); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := recordPeerInboxArtifactRenewal(ctx, tx, row, spec, times); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf(
			"renew Peer Inbox Artifact: commit: %w", err)
	}
	next := spec.Fence
	next.leaseUntil = times.leaseUntil
	return PeerInboxArtifactRenewal{fence: next, changed: true}, nil
}

func updatePeerInboxArtifactRenewal(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, times peerInboxArtifactRenewTimes,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET lease_until=?,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(times.leaseUntil), storeTime(times.at),
		row.inboxID.String(), row.attempts, row.leaseOwner, storeTime(row.leaseUntil),
		storeTime(row.updatedAt))
	if err != nil {
		return fmt.Errorf("%w: renew update: %v", ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "renew Peer Inbox Artifact CAS"); err != nil {
		return fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	return nil
}

func recordPeerInboxArtifactRenewal(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, spec RenewPeerInboxArtifactSpec,
	times peerInboxArtifactRenewTimes,
) error {
	output := peerInboxArtifactResultFromRow(row)
	output.leaseUntil = times.leaseUntil
	output.updatedAt = times.at
	receipt, err := newPeerInboxArtifactRenewReceipt(spec.Fence, row.semanticNonce,
		times.at, output)
	if err != nil {
		return err
	}
	return upsertPeerInboxArtifactRenewReceipt(ctx, tx, receipt)
}
