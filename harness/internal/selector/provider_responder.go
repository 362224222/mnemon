package selector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SampleResponse is either a vote or an explicit no-vote. All unavailable
// cases intentionally share one no-vote shape so callers cannot use the
// responder as a selection-existence or lifecycle oracle.
type SampleResponse struct {
	vote    SampleVote
	hasVote bool
}

func (r SampleResponse) Vote() (SampleVote, bool) { return r.vote, r.hasVote }
func (r SampleResponse) IsNoVote() bool           { return !r.hasVote }

// RespondSampleQuery synchronously reads one local preference. requester must
// be the independently authenticated transport identity; query contains no
// authority. The method performs no write, network I/O, R7 admission, or
// Reference mutation. A network adapter must issue each frozen query at most
// once per sampled peer: this stateless responder may return a later color if
// the same query is retried after local progress. Response loss is therefore a
// no-vote, not a retry. The adapter also owns bounded concurrency and per-peer
// message budgets.
func (s *Store) RespondSampleQuery(ctx context.Context, requester ParticipantID,
	query SampleQuery,
) (SampleResponse, error) {
	if ctx == nil || requester.IsZero() {
		return SampleResponse{}, fmt.Errorf("respond sample query input is incomplete: %w", ErrInvalid)
	}
	if err := validateSampleQueryFrame(query); err != nil {
		return SampleResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return SampleResponse{}, err
	}
	now, err := s.trustedNow()
	if err != nil {
		return SampleResponse{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SampleResponse{}, fmt.Errorf("respond sample query: begin: %w", err)
	}
	defer tx.Rollback()
	snapshot, err := loadSelectionTx(ctx, tx, query.selectionID)
	if errors.Is(err, ErrNotFound) {
		return SampleResponse{}, nil
	}
	if err != nil {
		return SampleResponse{}, err
	}
	if !sampleRequesterEligible(snapshot, requester, query, now) {
		return SampleResponse{}, nil
	}
	state, ok := snapshot.State()
	if !ok {
		return SampleResponse{}, nil
	}
	vote, err := NewSampleVote(query.selectionID, query.round, query.nonce,
		state.preference, snapshot.self)
	if err != nil {
		return SampleResponse{}, fmt.Errorf("respond sample query: construct vote: %w", err)
	}
	return SampleResponse{vote: vote, hasVote: true}, nil
}

func sampleRequesterEligible(snapshot SelectionSnapshot, requester ParticipantID,
	query SampleQuery, now time.Time,
) bool {
	if snapshot.phase != PhaseActive && snapshot.phase != PhaseObserved {
		return false
	}
	if requester == snapshot.self || !snapshot.descriptor.contains(requester) {
		return false
	}
	if query.selectionID != snapshot.descriptor.id ||
		query.round > snapshot.descriptor.profile.maxRounds {
		return false
	}
	return !now.Before(snapshot.descriptor.createdAt) && now.Before(snapshot.descriptor.expiresAt)
}
