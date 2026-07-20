package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readChannelRosterConflictReplay(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, challenger model.Member, conflictID string,
) (MergeChannelRosterResult, bool, error) {
	var existingDigest []byte
	err := tx.QueryRowContext(ctx, `SELECT challenger_record_hash FROM channel_conflicts
		WHERE conflict_id=? AND channel_id=?`, conflictID,
		authority.channel.ID().String()).Scan(&existingDigest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return MergeChannelRosterResult{}, false, nil
	case err != nil:
		return MergeChannelRosterResult{}, false, mapChannelRosterError(err)
	case !bytes.Equal(existingDigest, challenger.Head().Digest().Bytes()) ||
		authority.channel.Status() != model.ChannelConflicted ||
		authority.channel.TopicState() != model.TopicBlocked:
		return MergeChannelRosterResult{}, false, ErrChannelRosterConflict
	default:
		return channelRosterConflictResult(authority), true, nil
	}
}

func channelRosterConflictResult(authority verifiedChannelAuthority) MergeChannelRosterResult {
	return MergeChannelRosterResult{Status: ChannelRosterConflicted, Channel: authority.channel,
		Roster: authority.roster, ExpectedNextRevision: authority.roster.Head().Revision() + 1}
}
