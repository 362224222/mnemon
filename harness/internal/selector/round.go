package selector

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// SelectionState is the complete module-private state needed between rounds.
type SelectionState struct {
	selectionID SelectionID
	preference  Preference
	margin      int64
	round       uint32
}

func NewSelectionState(selectionID SelectionID, initial Preference) (SelectionState, error) {
	if selectionID.IsZero() || !validPreference(initial) {
		return SelectionState{}, fmt.Errorf("selection ID and initial preference are required: %w", ErrInvalid)
	}
	return SelectionState{selectionID: selectionID, preference: initial}, nil
}

func (s SelectionState) SelectionID() SelectionID { return s.selectionID }
func (s SelectionState) Preference() Preference   { return s.preference }
func (s SelectionState) Margin() int64            { return s.margin }
func (s SelectionState) Round() uint32            { return s.round }

func (s SelectionState) validate(descriptor SelectionDescriptor) error {
	if s.selectionID.IsZero() || s.selectionID != descriptor.id || !validPreference(s.preference) {
		return fmt.Errorf("state does not belong to descriptor: %w", ErrState)
	}
	if s.round > descriptor.profile.maxRounds || abs64(s.margin) > int64(s.round) {
		return fmt.Errorf("round %d and margin %d are inconsistent: %w", s.round, s.margin, ErrState)
	}
	if s.margin >= int64(descriptor.profile.threshold) && s.preference != PreferenceA {
		return fmt.Errorf("A threshold margin has preference %s: %w", s.preference, ErrState)
	}
	if s.margin <= -int64(descriptor.profile.threshold) && s.preference != PreferenceB {
		return fmt.Errorf("B threshold margin has preference %s: %w", s.preference, ErrState)
	}
	return nil
}

type SampleQuery struct {
	selectionID SelectionID
	round       uint32
	nonce       agency.Digest
}

func NewSampleQuery(selectionID SelectionID, round uint32, nonce agency.Digest) (SampleQuery, error) {
	if selectionID.IsZero() || round == 0 || nonce.IsZero() {
		return SampleQuery{}, fmt.Errorf("query selection, positive round, and nonce are required: %w", ErrInvalid)
	}
	return SampleQuery{selectionID, round, nonce}, nil
}

func (q SampleQuery) SelectionID() SelectionID { return q.selectionID }
func (q SampleQuery) Round() uint32            { return q.round }
func (q SampleQuery) Nonce() agency.Digest     { return q.nonce }

type SampleVote struct {
	selectionID SelectionID
	round       uint32
	nonce       agency.Digest
	preference  Preference
	claimedBy   ParticipantID
}

func NewSampleVote(selectionID SelectionID, round uint32, nonce agency.Digest,
	preference Preference, claimedBy ParticipantID,
) (SampleVote, error) {
	if selectionID.IsZero() || round == 0 || nonce.IsZero() ||
		!validPreference(preference) || claimedBy.IsZero() {
		return SampleVote{}, fmt.Errorf("vote fields are incomplete: %w", ErrInvalid)
	}
	return SampleVote{selectionID, round, nonce, preference, claimedBy}, nil
}

func (v SampleVote) SelectionID() SelectionID { return v.selectionID }
func (v SampleVote) Round() uint32            { return v.round }
func (v SampleVote) Nonce() agency.Digest     { return v.nonce }
func (v SampleVote) Preference() Preference   { return v.preference }
func (v SampleVote) ClaimedSource() ParticipantID {
	return v.claimedBy
}

// AuthenticatedVote is the only vote shape accepted by ApplyRound. Its source
// is bound to a peer identity authenticated outside this transport-neutral
// package; a wire claim alone never gains counting authority.
type AuthenticatedVote struct {
	wire   SampleVote
	source ParticipantID
}

// AuthenticateSampleVote binds a decoded wire vote to the independently
// authenticated transport peer that supplied it. The caller owns transport
// authentication; selector only verifies that the wire identity agrees with
// that authenticated observation. A mismatch fails closed before tallying.
func AuthenticateSampleVote(authenticatedSource ParticipantID,
	wire SampleVote,
) (AuthenticatedVote, error) {
	if authenticatedSource.IsZero() || wire.selectionID.IsZero() || wire.round == 0 ||
		wire.nonce.IsZero() || !validPreference(wire.preference) || wire.claimedBy.IsZero() {
		return AuthenticatedVote{}, fmt.Errorf("authenticated vote is incomplete: %w", ErrInvalid)
	}
	if authenticatedSource != wire.claimedBy {
		return AuthenticatedVote{}, fmt.Errorf("authenticated peer does not match vote source: %w", ErrInvalid)
	}
	return AuthenticatedVote{wire: wire, source: authenticatedSource}, nil
}

func (v AuthenticatedVote) SelectionID() SelectionID { return v.wire.selectionID }
func (v AuthenticatedVote) Round() uint32            { return v.wire.round }
func (v AuthenticatedVote) Nonce() agency.Digest     { return v.wire.nonce }
func (v AuthenticatedVote) Preference() Preference   { return v.wire.preference }
func (v AuthenticatedVote) Source() ParticipantID    { return v.source }

// VoteTally records accepted colors and bounded filtering diagnostics. Counts
// describe input frames except Equivocations, which counts equivocating peers.
type VoteTally struct {
	a              uint32
	b              uint32
	duplicates     uint32
	equivocations  uint32
	wrongSelection uint32
	wrongRound     uint32
	wrongNonce     uint32
	unselected     uint32
	invalid        uint32
}

func (t VoteTally) A() uint32              { return t.a }
func (t VoteTally) B() uint32              { return t.b }
func (t VoteTally) Duplicates() uint32     { return t.duplicates }
func (t VoteTally) Equivocations() uint32  { return t.equivocations }
func (t VoteTally) WrongSelection() uint32 { return t.wrongSelection }
func (t VoteTally) WrongRound() uint32     { return t.wrongRound }
func (t VoteTally) WrongNonce() uint32     { return t.wrongNonce }
func (t VoteTally) Unselected() uint32     { return t.unselected }
func (t VoteTally) Invalid() uint32        { return t.invalid }

type RoundResult struct {
	state     SelectionState
	tally     VoteTally
	quorum    Preference
	recolored bool
}

func (r RoundResult) State() SelectionState { return r.state }
func (r RoundResult) Tally() VoteTally      { return r.tally }
func (r RoundResult) Recolored() bool       { return r.recolored }
func (r RoundResult) Quorum() (Preference, bool) {
	return r.quorum, validPreference(r.quorum)
}

// ApplyRound validates one frozen sample, filters its votes, and returns the
// only permitted atomic state update. It has no side effects.
func ApplyRound(descriptor SelectionDescriptor, state SelectionState, self ParticipantID,
	query SampleQuery, sampled []ParticipantID, votes []AuthenticatedVote, now time.Time,
) (RoundResult, error) {
	if err := descriptor.validate(); err != nil {
		return RoundResult{}, err
	}
	if err := state.validate(descriptor); err != nil {
		return RoundResult{}, err
	}
	if !descriptor.contains(self) {
		return RoundResult{}, fmt.Errorf("local peer is outside participant roster: %w", ErrInvalid)
	}
	if abs64(state.margin) >= int64(descriptor.profile.threshold) || state.round >= descriptor.profile.maxRounds {
		return RoundResult{}, fmt.Errorf("selection is already terminal: %w", ErrState)
	}
	canonicalNow := now.Round(0).UTC()
	if now.IsZero() || canonicalNow.Before(descriptor.createdAt) || !canonicalNow.Before(descriptor.expiresAt) {
		return RoundResult{}, fmt.Errorf("selection is expired or clock is invalid: %w", ErrState)
	}
	if query.selectionID != descriptor.id || query.round != state.round+1 || query.nonce.IsZero() {
		return RoundResult{}, fmt.Errorf("query does not bind the next exact round: %w", ErrInvalid)
	}
	sampleSet, err := validateSample(descriptor, self, sampled)
	if err != nil {
		return RoundResult{}, err
	}
	if len(votes) > len(sampled)*2 {
		return RoundResult{}, fmt.Errorf("vote frames %d exceed two per sampled peer: %w", len(votes), ErrLimit)
	}
	tally := filterVotes(query, sampleSet, votes)
	next := state
	next.round++
	var quorum Preference
	if tally.a >= descriptor.profile.alpha {
		quorum = PreferenceA
		next.preference = PreferenceA
		next.margin++
	} else if tally.b >= descriptor.profile.alpha {
		quorum = PreferenceB
		next.preference = PreferenceB
		next.margin--
	}
	return RoundResult{next, tally, quorum, validPreference(quorum) && quorum != state.preference}, nil
}

func validateSample(descriptor SelectionDescriptor, self ParticipantID,
	sampled []ParticipantID,
) (map[ParticipantID]struct{}, error) {
	if len(sampled) != int(descriptor.profile.sampleSize) {
		return nil, fmt.Errorf("sample size %d does not equal profile size %d: %w",
			len(sampled), descriptor.profile.sampleSize, ErrInvalid)
	}
	set := make(map[ParticipantID]struct{}, len(sampled))
	for _, peer := range sampled {
		if peer.IsZero() || peer == self || !descriptor.contains(peer) {
			return nil, fmt.Errorf("sample contains an ineligible peer: %w", ErrInvalid)
		}
		if _, duplicate := set[peer]; duplicate {
			return nil, fmt.Errorf("sample contains duplicate peer %q: %w", peer.String(), ErrInvalid)
		}
		set[peer] = struct{}{}
	}
	return set, nil
}

func filterVotes(query SampleQuery, sampled map[ParticipantID]struct{}, votes []AuthenticatedVote) VoteTally {
	const (
		seenA uint8 = 1 << iota
		seenB
	)
	seen := make(map[ParticipantID]uint8, len(sampled))
	tally := VoteTally{}
	for _, vote := range votes {
		switch {
		case vote.source.IsZero() || vote.wire.claimedBy != vote.source ||
			!validPreference(vote.wire.preference):
			tally.invalid++
		case vote.wire.selectionID != query.selectionID:
			tally.wrongSelection++
		case vote.wire.round != query.round:
			tally.wrongRound++
		case vote.wire.nonce != query.nonce:
			tally.wrongNonce++
		case !sampleContains(sampled, vote.source):
			tally.unselected++
		default:
			bit := seenA
			if vote.wire.preference == PreferenceB {
				bit = seenB
			}
			if seen[vote.source]&bit != 0 {
				tally.duplicates++
			}
			seen[vote.source] |= bit
		}
	}
	for _, colors := range seen {
		switch colors {
		case seenA:
			tally.a++
		case seenB:
			tally.b++
		case seenA | seenB:
			tally.equivocations++
		}
	}
	return tally
}

func sampleContains(sampled map[ParticipantID]struct{}, peer ParticipantID) bool {
	_, present := sampled[peer]
	return present
}

func abs64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
