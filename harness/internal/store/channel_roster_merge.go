package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// MergeChannelRoster is the single durable boundary for owner-signed roster
// repair. It accepts an overlapping prefix or suffix, records a valid fork as
// permanent evidence, and never performs network I/O inside the transaction.
func (s *Store) MergeChannelRoster(ctx context.Context,
	spec MergeChannelRosterSpec,
) (MergeChannelRosterResult, error) {
	records, firstRevision, at, err := validateMergeChannelRosterInput(s, ctx, spec)
	if err != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MergeChannelRosterResult{}, fmt.Errorf("merge Channel roster: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return MergeChannelRosterResult{}, fmt.Errorf("merge Channel roster: Node: %w", err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), spec.ChannelID)
	if err != nil {
		return MergeChannelRosterResult{}, fmt.Errorf("%w: %v", ErrChannelRosterConflict, err)
	}
	if authority.channel.Status() == model.ChannelAbandoned {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	if err := validateChannelRosterPage(authority.channel.Descriptor(), records); err != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	existing := authority.roster.Members()
	expected := uint64(len(existing)) + 1
	if firstRevision > expected {
		return commitChannelRosterGap(tx, authority, spec.AuthenticatedTransportPeerID, expected)
	}
	candidate, prefixLength, err := prepareChannelRosterCandidate(authority, spec, records, firstRevision)
	if err != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	if incumbent, challenger, ok := channelRosterConflict(existing, records, prefixLength); ok {
		return s.commitChannelRosterConflict(ctx, tx, node.PeerID(), authority, candidate, incumbent,
			challenger, spec.AuthenticatedTransportPeerID, at)
	}
	if len(candidate.Members()) <= len(existing) {
		if err := tx.Commit(); err != nil {
			return MergeChannelRosterResult{}, mapChannelRosterError(err)
		}
		return MergeChannelRosterResult{Status: ChannelRosterDuplicate,
			Channel: authority.channel, Roster: authority.roster,
			ExpectedNextRevision: expected}, nil
	}
	return s.applyChannelRosterCandidate(ctx, tx, node.PeerID(), authority, candidate, at,
		spec.AuthenticatedTransportPeerID, spec.LeaveOperation)
}

func validateMergeChannelRosterInput(s *Store, ctx context.Context,
	spec MergeChannelRosterSpec,
) ([]model.Member, uint64, time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		spec.AuthenticatedTransportPeerID.IsZero() || len(spec.Records) == 0 ||
		len(spec.Records) > model.MaxMemberRecordsPerChannel ||
		spec.LeaveOperation != nil && !spec.LeaveOperation.valid() {
		return nil, 0, time.Time{}, ErrChannelRosterInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil {
		return nil, 0, time.Time{}, ErrChannelRosterInput
	}
	records := append([]model.Member(nil), spec.Records...)
	firstRevision := records[0].Head().Revision()
	if firstRevision == 0 {
		return nil, 0, time.Time{}, ErrChannelRosterInput
	}
	for index, record := range records {
		if record.IsZero() || record.ChannelID() != spec.ChannelID ||
			record.Head().Revision() != firstRevision+uint64(index) ||
			record.Head().Revision() > model.MaxMemberRecordsPerChannel {
			return nil, 0, time.Time{}, ErrChannelRosterInput
		}
	}
	return records, firstRevision, at, nil
}

func commitChannelRosterGap(tx *sql.Tx, authority verifiedChannelAuthority, transport model.PeerID,
	expected uint64,
) (MergeChannelRosterResult, error) {
	if !rosterTransportAuthorized(transport, authority.roster) {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	if err := tx.Commit(); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	return MergeChannelRosterResult{Status: ChannelRosterGap, Channel: authority.channel,
		Roster: authority.roster, ExpectedNextRevision: expected}, nil
}

func prepareChannelRosterCandidate(authority verifiedChannelAuthority, spec MergeChannelRosterSpec,
	records []model.Member, firstRevision uint64,
) (model.VerifiedRoster, int, error) {
	existing := authority.roster.Members()
	prefixLength := int(firstRevision - 1)
	if prefixLength > len(existing) {
		return model.VerifiedRoster{}, 0, ErrChannelRosterInput
	}
	candidateMembers := append([]model.Member(nil), existing[:prefixLength]...)
	candidateMembers = append(candidateMembers, records...)
	candidate, err := model.NewVerifiedRoster(authority.channel.Descriptor(), candidateMembers)
	if err != nil || !rosterTransportAuthorized(spec.AuthenticatedTransportPeerID, authority.roster, candidate) {
		return model.VerifiedRoster{}, 0, ErrChannelRosterInput
	}
	return candidate, prefixLength, nil
}

func channelRosterConflict(existing, records []model.Member,
	prefixLength int,
) (model.Member, model.Member, bool) {
	overlap := len(existing) - prefixLength
	if len(records) < overlap {
		overlap = len(records)
	}
	for index := 0; index < overlap; index++ {
		incumbent, challenger := existing[prefixLength+index], records[index]
		if !bytes.Equal(incumbent.WireJSON().Bytes(), challenger.WireJSON().Bytes()) {
			return incumbent, challenger, true
		}
	}
	return model.Member{}, model.Member{}, false
}

func (s *Store) applyChannelRosterCandidate(ctx context.Context, tx *sql.Tx,
	localPeer model.PeerID, authority verifiedChannelAuthority, candidate model.VerifiedRoster,
	at time.Time, transport model.PeerID, leaveOperation *ChannelLeaveOperation,
) (MergeChannelRosterResult, error) {
	result, err := s.applyChannelRosterCandidateTx(ctx, tx, localPeer, authority, candidate, at)
	if err != nil {
		return MergeChannelRosterResult{}, err
	}
	if leaveOperation != nil {
		owner, ok := candidate.CurrentMember(localPeer)
		if !ok || transport != localPeer || authority.channel.OwnerPeerID() != localPeer ||
			owner.Status() != model.MemberLeft || result.Channel.Status() != model.ChannelClosed {
			return MergeChannelRosterResult{}, ErrChannelRosterInput
		}
		if err := insertChannelLeaveOperation(ctx, tx, *leaveOperation, result.Channel.ID(),
			model.ChannelLeaveRequestID{}, 0, at); err != nil {
			return MergeChannelRosterResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	return result, nil
}

// applyChannelRosterCandidateTx installs one already-verified extension but
// deliberately leaves commit ownership with its caller. Voluntary leave uses
// this boundary to make the terminal roster merge and request receipt one
// indivisible durable transition.
func (s *Store) applyChannelRosterCandidateTx(ctx context.Context, tx *sql.Tx,
	localPeer model.PeerID, authority verifiedChannelAuthority, candidate model.VerifiedRoster, at time.Time,
) (MergeChannelRosterResult, error) {
	existing := authority.roster.Members()
	if authority.channel.Status() == model.ChannelConflicted ||
		at.Before(authority.channel.UpdatedAt()) || at.Before(candidate.Members()[len(candidate.Members())-1].CreatedAt()) {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	for _, record := range candidate.Members()[len(existing):] {
		if err := insertChannelMember(ctx, tx, record); err != nil {
			return MergeChannelRosterResult{}, mapChannelRosterError(err)
		}
	}
	channel, err := mergedChannelProjection(localPeer, authority.channel, candidate, at)
	if err != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		status=?,topic_state=?,updated_at=? WHERE channel_id=? AND roster_head_revision=?
		AND roster_head_hash=? AND status=? AND topic_state=?`, channel.RosterHead().Revision(),
		channel.RosterHead().Digest().Bytes(), string(channel.Status()), string(channel.TopicState()),
		storeTime(channel.UpdatedAt()), channel.ID().String(), authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes(), string(authority.channel.Status()),
		string(authority.channel.TopicState()))
	if err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	if err := syncChannelRosterBindings(ctx, tx, localPeer, channel, candidate,
		authority.bindings, at); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	if err := applyChannelRosterEgressEffects(ctx, tx, localPeer, authority.roster,
		channel, candidate, at); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, localPeer, channel.ID())
	if err != nil || committed.channel.RosterHead() != candidate.Head() {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	return MergeChannelRosterResult{Status: ChannelRosterApplied, Channel: committed.channel,
		Roster: committed.roster, ExpectedNextRevision: committed.roster.Head().Revision() + 1}, nil
}
