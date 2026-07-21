package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// RecordPeerInboxArtifactSourceSpec records the authenticated serving peer for
// the first successful network exchange made under one exact Artifact fence.
// SourcePeerID must be the signed publication origin.
type RecordPeerInboxArtifactSourceSpec struct {
	Fence        PeerInboxArtifactFence
	SourcePeerID model.PeerID
	At           time.Time
}

// PeerInboxArtifactSourceReceipt is immutable, restart-safe path evidence.
// Lease details are intentionally not exposed by this public Store value.
type PeerInboxArtifactSourceReceipt struct {
	inboxID      model.InboxID
	sourcePeerID model.PeerID
	recordedAt   time.Time
	changed      bool
	replayed     bool
}

func (receipt PeerInboxArtifactSourceReceipt) InboxID() model.InboxID { return receipt.inboxID }
func (receipt PeerInboxArtifactSourceReceipt) SourcePeerID() model.PeerID {
	return receipt.sourcePeerID
}
func (receipt PeerInboxArtifactSourceReceipt) RecordedAt() time.Time { return receipt.recordedAt }
func (receipt PeerInboxArtifactSourceReceipt) Changed() bool         { return receipt.changed }
func (receipt PeerInboxArtifactSourceReceipt) Replayed() bool        { return receipt.replayed }

type storedPeerInboxArtifactSourceReceipt struct {
	inboxID      model.InboxID
	sourcePeerID model.PeerID
	attempt      uint32
	leaseOwner   string
	leaseUntil   time.Time
	recordedAt   time.Time
}

// RecordPeerInboxArtifactSource installs at most one direct-source receipt.
// Repeating a successful call after response loss returns the original receipt
// without requiring that its old fence still be live.
func (s *Store) RecordPeerInboxArtifactSource(ctx context.Context,
	spec RecordPeerInboxArtifactSourceSpec,
) (PeerInboxArtifactSourceReceipt, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil || spec.SourcePeerID.IsZero() {
		return PeerInboxArtifactSourceReceipt{}, ErrPeerInboxArtifactInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactSourceReceipt{}, fmt.Errorf("record Peer Inbox Artifact source: begin: %w", err)
	}
	defer tx.Rollback()

	stored, found, err := readPeerInboxArtifactSourceReceipt(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactSourceReceipt{}, err
	}
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactSourceReceipt{}, err
	}
	if found {
		err = validatePeerInboxArtifactSourceReplay(stored, row, spec)
	} else {
		stored, err = insertPeerInboxArtifactSource(ctx, tx, row, spec, at)
	}
	if err != nil {
		return PeerInboxArtifactSourceReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactSourceReceipt{}, fmt.Errorf("record Peer Inbox Artifact source: commit: %w", err)
	}
	return projectPeerInboxArtifactSourceReceipt(stored, !found, found), nil
}

func validatePeerInboxArtifactSourceReplay(stored storedPeerInboxArtifactSourceReceipt,
	row peerInboxArtifactRow, spec RecordPeerInboxArtifactSourceSpec,
) error {
	if stored.sourcePeerID != spec.SourcePeerID || stored.sourcePeerID != row.originPeerID {
		return fmt.Errorf("%w: Artifact source receipt origin differs", ErrPeerInboxArtifactInvariant)
	}
	if stored.attempt != spec.Fence.attempt || stored.leaseOwner != spec.Fence.leaseOwner ||
		!stored.leaseUntil.Equal(spec.Fence.leaseUntil) {
		return ErrPeerInboxArtifactStale
	}
	return nil
}

func insertPeerInboxArtifactSource(ctx context.Context, tx *sql.Tx, row peerInboxArtifactRow,
	spec RecordPeerInboxArtifactSourceSpec, at time.Time,
) (storedPeerInboxArtifactSourceReceipt, error) {
	if spec.SourcePeerID != row.originPeerID {
		return storedPeerInboxArtifactSourceReceipt{}, ErrPeerInboxArtifactInput
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return storedPeerInboxArtifactSourceReceipt{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return storedPeerInboxArtifactSourceReceipt{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox_artifact_source_receipts(
		inbox_id,source_peer_id,attempt,lease_owner,lease_until,recorded_at) VALUES(?,?,?,?,?,?)`,
		spec.Fence.inboxID.String(), spec.SourcePeerID.String(), spec.Fence.attempt,
		spec.Fence.leaseOwner, storeTime(spec.Fence.leaseUntil), storeTime(at))
	if err != nil {
		return storedPeerInboxArtifactSourceReceipt{}, fmt.Errorf("%w: insert Artifact source receipt: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "record Peer Inbox Artifact source"); err != nil {
		return storedPeerInboxArtifactSourceReceipt{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactInvariant, err)
	}
	stored, found, err := readPeerInboxArtifactSourceReceipt(ctx, tx, spec.Fence.inboxID)
	if err != nil || !found || stored.sourcePeerID != row.originPeerID ||
		stored.attempt != spec.Fence.attempt || stored.leaseOwner != spec.Fence.leaseOwner ||
		!stored.leaseUntil.Equal(spec.Fence.leaseUntil) || !stored.recordedAt.Equal(at) {
		return storedPeerInboxArtifactSourceReceipt{}, fmt.Errorf(
			"%w: installed Artifact source receipt differs: %v", ErrPeerInboxArtifactInvariant, err)
	}
	return stored, nil
}

func readPeerInboxArtifactSourceReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (storedPeerInboxArtifactSourceReceipt, bool, error) {
	var inboxText, sourceText, owner, leaseText, recordedText string
	var attempt int64
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,source_peer_id,attempt,lease_owner,
		lease_until,recorded_at FROM peer_inbox_artifact_source_receipts WHERE inbox_id=?`,
		inboxID.String()).Scan(&inboxText, &sourceText, &attempt, &owner, &leaseText, &recordedText)
	if errors.Is(err, sql.ErrNoRows) {
		return storedPeerInboxArtifactSourceReceipt{}, false, nil
	}
	if err != nil {
		return storedPeerInboxArtifactSourceReceipt{}, false,
			fmt.Errorf("%w: read Artifact source receipt: %v", ErrPeerInboxArtifactInvariant, err)
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	source, sourceErr := model.ParsePeerID(sourceText)
	leaseUntil, leaseErr := parseCanonicalStoreTime(leaseText)
	recordedAt, recordedErr := parseCanonicalStoreTime(recordedText)
	if inboxErr != nil || parsedInbox != inboxID || sourceErr != nil || attempt <= 0 ||
		attempt > math.MaxUint32 || !validPublicationIdentifier(owner) || leaseErr != nil ||
		recordedErr != nil || !recordedAt.Before(leaseUntil) {
		return storedPeerInboxArtifactSourceReceipt{}, false,
			fmt.Errorf("%w: malformed Artifact source receipt", ErrPeerInboxArtifactInvariant)
	}
	return storedPeerInboxArtifactSourceReceipt{inboxID: parsedInbox, sourcePeerID: source,
		attempt: uint32(attempt), leaseOwner: owner, leaseUntil: leaseUntil, recordedAt: recordedAt}, true, nil
}

func projectPeerInboxArtifactSourceReceipt(stored storedPeerInboxArtifactSourceReceipt,
	changed, replayed bool,
) PeerInboxArtifactSourceReceipt {
	return PeerInboxArtifactSourceReceipt{inboxID: stored.inboxID, sourcePeerID: stored.sourcePeerID,
		recordedAt: stored.recordedAt, changed: changed, replayed: replayed}
}
