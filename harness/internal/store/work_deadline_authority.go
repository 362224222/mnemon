package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// validateWorkDeadlineAdmissionAuthority rechecks the Store-prepared scope at
// commit. It intentionally permits topic recovery and an unavailable audience;
// the resulting delivery is recorded blocked instead of suppressing expiry.
func validateWorkDeadlineAdmissionAuthority(ctx context.Context, tx *sql.Tx,
	spec LocalAcceptanceSpec, acceptedAt time.Time,
) ([]byte, error) {
	snapshot := spec.Scope
	if !spec.Controller || spec.Operation != nil || len(spec.Items) != 1 {
		return nil, fmt.Errorf("%w: invalid Work deadline controller batch", ErrAdmissionConflict)
	}
	authority, err := requireFrozenAdmissionAuthority(ctx, tx, snapshot, acceptedAt, 1)
	if err != nil {
		return nil, err
	}
	if err := requireFrozenWorkDeadlineHead(ctx, tx, authority.node, snapshot); err != nil {
		return nil, err
	}
	return requireFrozenOriginMemberHead(ctx, tx, authority.node, snapshot)
}

func requireFrozenWorkDeadlineHead(ctx context.Context, tx *sql.Tx, node model.Node,
	snapshot LocalAdmissionScope,
) error {
	var status string
	var rosterRevision, sourceHead uint64
	var rosterHash []byte
	err := tx.QueryRowContext(ctx, `SELECT c.status,c.roster_head_revision,c.roster_head_hash,
		p.source_head_channel_seq FROM channels c JOIN publication_epochs p ON p.channel_id=c.channel_id
		AND p.origin_peer_id=? AND p.origin_epoch=? WHERE c.channel_id=?`, node.PeerID().String(),
		node.OriginEpoch().String(), snapshot.ChannelID().String()).
		Scan(&status, &rosterRevision, &rosterHash, &sourceHead)
	if err != nil || status != string(model.ChannelActive) ||
		rosterRevision != snapshot.PublicationRoster().Revision() ||
		!bytes.Equal(rosterHash, snapshot.PublicationRoster().Digest().Bytes()) ||
		sourceHead+1 != snapshot.FirstChannelSequence() || sourceHead >= model.MaxSQLiteInteger {
		return fmt.Errorf("%w: Channel or publication head changed", ErrAdmissionConflict)
	}
	return nil
}
