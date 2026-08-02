package selector

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// FreezeRound durably prepares one exact sample, nonce, and deadline before
// any network I/O. Exact retry returns the existing pending round.
func (s *Store) FreezeRound(ctx context.Context, id SelectionID) (PendingRound, error) {
	if ctx == nil {
		return PendingRound{}, errors.New("freeze selector round: nil context")
	}
	before, err := s.Selection(ctx, id)
	if err != nil {
		return PendingRound{}, err
	}
	if pending, present := before.PendingRound(); present {
		return pending, nil
	}
	if before.phase != PhaseActive {
		return PendingRound{}, ErrNotActive
	}
	sample, nonce, err := prepareSecureSample(s.entropy, before.descriptor, before.self)
	if err != nil {
		return PendingRound{}, err
	}
	sampleJSON, err := canonicalSample(sample)
	if err != nil {
		return PendingRound{}, err
	}
	candidate := frozenRoundCandidate{before: before, sample: sample, nonce: nonce,
		sampleJSON: sampleJSON}
	return s.commitFrozenRound(ctx, candidate)
}

type frozenRoundCandidate struct {
	before     SelectionSnapshot
	sample     []ParticipantID
	nonce      agency.Digest
	sampleJSON []byte
}

func (s *Store) commitFrozenRound(ctx context.Context,
	candidate frozenRoundCandidate,
) (PendingRound, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return PendingRound{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PendingRound{}, fmt.Errorf("freeze selector round: begin: %w", err)
	}
	defer tx.Rollback()
	current, err := loadSelectionTx(ctx, tx, candidate.before.descriptor.id)
	if err != nil {
		return PendingRound{}, err
	}
	if pending, present := current.PendingRound(); present {
		return pending, nil
	}
	if current.phase != PhaseActive || current.revision != candidate.before.revision ||
		current.state != candidate.before.state {
		return PendingRound{}, ErrConflict
	}
	now, err := s.trustedNow()
	if err != nil {
		return PendingRound{}, err
	}
	if !now.Before(current.descriptor.expiresAt) {
		return PendingRound{}, commitFrozenRoundExpiry(ctx, tx, current, now)
	}
	pending, err := persistFrozenRoundTx(ctx, tx, current, candidate, now)
	if err != nil {
		return PendingRound{}, err
	}
	if err := tx.Commit(); err != nil {
		return PendingRound{}, fmt.Errorf("freeze selector round: commit: %w", err)
	}
	return pending, nil
}

func commitFrozenRoundExpiry(ctx context.Context, tx *sql.Tx, current SelectionSnapshot,
	now time.Time,
) error {
	if err := settleDueSelectionTx(ctx, tx, current, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("freeze selector round: expire: %w", err)
	}
	return ErrNotActive
}

func persistFrozenRoundTx(ctx context.Context, tx *sql.Tx, current SelectionSnapshot,
	candidate frozenRoundCandidate, now time.Time,
) (PendingRound, error) {
	if err := settleDueSelectionsTx(ctx, tx, now); err != nil {
		return PendingRound{}, err
	}
	var pendingCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pending_rounds").Scan(&pendingCount); err != nil {
		return PendingRound{}, fmt.Errorf("freeze selector round: count pending: %w", err)
	}
	if pendingCount >= MaxPendingRounds {
		return PendingRound{}, ErrStoreCapacity
	}
	nextRevision := current.revision + 1
	round := current.state.round + 1
	deadline := now.Add(current.descriptor.profile.roundTimeout)
	if deadline.After(current.descriptor.expiresAt) {
		deadline = current.descriptor.expiresAt
	}
	result, err := tx.ExecContext(ctx, `UPDATE selections SET revision = ?, updated_at = ?
		WHERE selection_id = ? AND phase = 'active' AND revision = ?`, nextRevision,
		formatProviderTime(now), current.descriptor.id.String(), current.revision)
	if err != nil {
		return PendingRound{}, fmt.Errorf("freeze selector round: advance revision: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return PendingRound{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pending_rounds(
		selection_id, round, nonce_digest, sample_json, deadline, state_revision, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, current.descriptor.id.String(), round,
		candidate.nonce.String(), candidate.sampleJSON, formatProviderTime(deadline),
		nextRevision, formatProviderTime(now)); err != nil {
		return PendingRound{}, fmt.Errorf("freeze selector round: insert pending: %w", err)
	}
	query, _ := NewSampleQuery(current.descriptor.id, round, candidate.nonce)
	return PendingRound{query: query, sample: candidate.sample, deadline: deadline,
		stateRevision: nextRevision}, nil
}

// ApplyObservations settles one exact durable pending round. If its deadline
// has passed, supplied votes are ignored and the round is applied as
// no-majority. Network I/O therefore always occurs between FreezeRound and
// this fenced transaction.
func (s *Store) ApplyObservations(ctx context.Context, pending PendingRound,
	votes []SampleVote,
) (SelectionSnapshot, error) {
	if ctx == nil || !pending.valid() {
		return SelectionSnapshot{}, fmt.Errorf("apply selector observations: invalid pending round: %w", ErrInvalid)
	}
	if len(votes) > MaxSampleSize*2 {
		return SelectionSnapshot{}, fmt.Errorf("apply selector observations: vote frames exceed fixed bound: %w", ErrLimit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return SelectionSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("apply selector observations: begin: %w", err)
	}
	defer tx.Rollback()
	snapshot, err := loadSelectionTx(ctx, tx, pending.query.selectionID)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	stored, present := snapshot.PendingRound()
	if !present {
		return replaySettledSelectionTx(ctx, tx, snapshot, pending, votes)
	}
	if snapshot.phase != PhaseActive || !samePending(stored, pending) {
		return SelectionSnapshot{}, ErrConflict
	}
	now, err := s.trustedNow()
	if err != nil {
		return SelectionSnapshot{}, err
	}
	settlement, err := settlePendingRoundTx(ctx, tx, snapshot, stored, votes, now)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return SelectionSnapshot{}, fmt.Errorf("apply selector observations: commit: %w", err)
	}
	return appliedSelectionSnapshot(snapshot, settlement), nil
}

func replaySettledSelectionTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshot,
	pending PendingRound, votes []SampleVote,
) (SelectionSnapshot, error) {
	replay, found, err := settledRoundReplayTx(ctx, tx, pending, votes, snapshot.revision)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if found && replay {
		return snapshot, nil
	}
	return SelectionSnapshot{}, ErrConflict
}

func settlePendingRoundTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshot,
	stored PendingRound, votes []SampleVote, now time.Time,
) (roundSettlement, error) {
	effectiveVotes := votes
	if !now.Before(stored.deadline) {
		effectiveVotes = nil
	}
	voteSet, err := canonicalVoteSet(effectiveVotes)
	if err != nil {
		return roundSettlement{}, err
	}
	voteSetDigest := agency.Sum(voteSet)
	settlement, err := deriveRoundSettlement(snapshot, stored, effectiveVotes, now)
	if err != nil {
		return roundSettlement{}, err
	}
	if err := commitRoundSettlementTx(ctx, tx, snapshot, stored, settlement,
		voteSetDigest, now); err != nil {
		return roundSettlement{}, err
	}
	return settlement, nil
}

func appliedSelectionSnapshot(snapshot SelectionSnapshot,
	settlement roundSettlement,
) SelectionSnapshot {
	snapshot.state, snapshot.phase = settlement.state, settlement.phase
	snapshot.revision++
	snapshot.pending = PendingRound{}
	if settlement.ready {
		snapshot.observation = settlement.observation
	}
	return snapshot
}

func settledRoundReplayTx(ctx context.Context, tx *sql.Tx, requested PendingRound,
	votes []SampleVote, currentRevision uint64,
) (bool, bool, error) {
	var round, stateRevision, resultRevision int64
	var nonceValue, deadlineValue, storedVoteSetDigest, settledAtValue string
	var sampleJSON []byte
	err := tx.QueryRowContext(ctx, `SELECT round, nonce_digest, sample_json, deadline,
		state_revision, vote_set_digest, result_revision, settled_at FROM settled_rounds
		WHERE selection_id = ? AND round = ?`, requested.query.selectionID.String(),
		requested.query.round).Scan(&round, &nonceValue, &sampleJSON, &deadlineValue,
		&stateRevision, &storedVoteSetDigest, &resultRevision, &settledAtValue)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("apply selector observations: inspect settlement: %w", err)
	}
	nonce, err := agency.ParseDigest(nonceValue)
	if err != nil || round <= 0 || round > int64(^uint32(0)) || stateRevision <= 0 ||
		resultRevision <= stateRevision {
		return false, false, fmt.Errorf("apply selector observations: corrupt settlement: %w", ErrState)
	}
	sample, err := parseSampleCanonical(sampleJSON)
	if err != nil {
		return false, false, err
	}
	deadline, err := parseProviderTime(deadlineValue)
	if err != nil {
		return false, false, err
	}
	query, err := NewSampleQuery(requested.query.selectionID, uint32(round), nonce)
	if err != nil {
		return false, false, err
	}
	stored := PendingRound{query: query, sample: sample, deadline: deadline,
		stateRevision: uint64(stateRevision)}
	settledAt, err := parseProviderTime(settledAtValue)
	if err != nil {
		return false, false, err
	}
	effectiveVotes := votes
	if !settledAt.Before(deadline) {
		effectiveVotes = nil
	}
	voteSet, err := canonicalVoteSet(effectiveVotes)
	if err != nil {
		return false, false, err
	}
	voteSetDigest := agency.Sum(voteSet)
	return samePending(stored, requested) && storedVoteSetDigest == voteSetDigest.String() &&
		uint64(resultRevision) <= currentRevision, true, nil
}

func prepareSecureSample(entropy io.Reader, descriptor SelectionDescriptor,
	self ParticipantID,
) ([]ParticipantID, agency.Digest, error) {
	if entropy == nil {
		return nil, agency.Digest{}, errors.New("selector entropy is unavailable")
	}
	eligible := make([]ParticipantID, 0, len(descriptor.roster)-1)
	for _, peer := range descriptor.roster {
		if peer != self {
			eligible = append(eligible, peer)
		}
	}
	for index := 0; index < int(descriptor.profile.sampleSize); index++ {
		offset, err := cryptorand.Int(entropy, big.NewInt(int64(len(eligible)-index)))
		if err != nil {
			return nil, agency.Digest{}, fmt.Errorf("freeze selector round: secure sample: %w", err)
		}
		chosen := index + int(offset.Int64())
		eligible[index], eligible[chosen] = eligible[chosen], eligible[index]
	}
	sample := append([]ParticipantID(nil), eligible[:descriptor.profile.sampleSize]...)
	sort.Slice(sample, func(left, right int) bool { return sample[left].String() < sample[right].String() })
	rawNonce := make([]byte, 32)
	if _, err := io.ReadFull(entropy, rawNonce); err != nil {
		return nil, agency.Digest{}, fmt.Errorf("freeze selector round: secure nonce: %w", err)
	}
	return sample, agency.Sum(rawNonce), nil
}

func samePending(left, right PendingRound) bool {
	if left.query != right.query || !left.deadline.Equal(right.deadline) ||
		left.stateRevision != right.stateRevision || len(left.sample) != len(right.sample) {
		return false
	}
	for index := range left.sample {
		if left.sample[index] != right.sample[index] {
			return false
		}
	}
	return true
}
