package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func advancePeerCursor(ctx context.Context, tx *sql.Tx, cursor PeerCursorProjection,
	observed uint64, at time.Time, scheduleLiveRepair bool,
) (PeerCursorProjection, error) {
	if observed > cursor.ObservedChannelSequence {
		cursor.ObservedChannelSequence = observed
	}
	for cursor.ContiguousChannelSequence < cursor.ObservedChannelSequence {
		next := cursor.ContiguousChannelSequence + 1
		var covered int
		err := tx.QueryRowContext(ctx, `SELECT CASE WHEN
			EXISTS(SELECT 1 FROM peer_inbox WHERE channel_id=? AND origin_peer_id=?
				AND origin_epoch=? AND channel_seq=?) OR
			EXISTS(SELECT 1 FROM publication_conflicts WHERE channel_id=? AND origin_peer_id=?
				AND origin_epoch=? AND claimed_channel_seq=?)
			THEN 1 ELSE 0 END`, cursor.ChannelID.String(), cursor.OriginPeerID.String(),
			cursor.OriginEpoch.String(), next, cursor.ChannelID.String(),
			cursor.OriginPeerID.String(), cursor.OriginEpoch.String(), next).
			Scan(&covered)
		if err != nil {
			return PeerCursorProjection{}, fmt.Errorf("%w: derive cursor coverage: %v", ErrPeerInboxConflict, err)
		}
		if covered == 0 {
			break
		}
		cursor.ContiguousChannelSequence = next
	}
	var durableContiguous, durableObserved uint64
	var updatedText string
	err := tx.QueryRowContext(ctx, `SELECT contiguous_channel_seq,observed_channel_seq,updated_at
		FROM peer_cursors WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`,
		cursor.ChannelID.String(), cursor.OriginPeerID.String(), cursor.OriginEpoch.String()).
		Scan(&durableContiguous, &durableObserved, &updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: reread cursor: %v", ErrPeerInboxConflict, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: cursor time: %v", ErrPeerInboxConflict, err)
	}
	if cursor.ContiguousChannelSequence == durableContiguous && cursor.ObservedChannelSequence == durableObserved {
		cursor.UpdatedAt = updatedAt
		return cursor, nil
	}
	if at.Before(updatedAt) {
		at = updatedAt
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_cursors SET contiguous_channel_seq=?,
		observed_channel_seq=?,updated_at=? WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?
		AND contiguous_channel_seq=? AND observed_channel_seq=? AND updated_at=?`,
		cursor.ContiguousChannelSequence, cursor.ObservedChannelSequence, storeTime(at),
		cursor.ChannelID.String(), cursor.OriginPeerID.String(), cursor.OriginEpoch.String(),
		durableContiguous, durableObserved, updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("update Peer Inbox cursor: %w", err)
	}
	if err := exactlyOne(result); err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: update cursor: %v", ErrPeerInboxConflict, err)
	}
	cursor.UpdatedAt = at
	if scheduleLiveRepair {
		if err := schedulePeerRepairAfterLiveCursorAdvance(ctx, tx, cursor, at); err != nil {
			return PeerCursorProjection{}, err
		}
	}
	return cursor, nil
}

func schedulePeerRepairAfterLiveCursorAdvance(ctx context.Context, tx *sql.Tx,
	cursor PeerCursorProjection, at time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE peer_repairs
		SET generation=generation+1,next_attempt_at=?,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?
		AND status IN ('progress','caught_up') AND next_attempt_at>? AND updated_at<=?`,
		storeTime(at), storeTime(at), cursor.ChannelID.String(), cursor.OriginPeerID.String(),
		cursor.OriginEpoch.String(), storeTime(at), storeTime(at))
	if err != nil {
		return fmt.Errorf("%w: schedule live cursor repair: %v", ErrPeerInboxConflict, err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("%w: schedule live cursor repair rows: %v", ErrPeerInboxConflict, err)
	}
	return nil
}
