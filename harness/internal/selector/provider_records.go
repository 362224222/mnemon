package selector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const selectionProjectionSQL = `SELECT s.selection_id, s.descriptor_json,
	s.local_participant, s.phase, s.seed_principal_id, s.seed_event_id,
	s.seed_event_digest, s.initial_preference, s.current_preference,
	s.signed_margin, s.completed_rounds, s.revision,
	s.observation_digest, s.observation_json, s.created_at, s.updated_at,
	p.round, p.nonce_digest, p.sample_json, p.deadline, p.state_revision
	FROM selections s LEFT JOIN pending_rounds p ON p.selection_id = s.selection_id`

type rowScanner interface {
	Scan(dest ...any) error
}

type storedSelectionRow struct {
	selectionID, self, phase, created, updated       string
	descriptor, observation, pendingSample           []byte
	seedPrincipal, seedEventID, seedEventDigest      sql.NullString
	initialPreference, currentPreference             sql.NullString
	observationDigest, pendingNonce, pendingDeadline sql.NullString
	pendingRound, pendingRevision                    sql.NullInt64
	margin                                           int64
	completedRounds, revision                        uint64
}

func scanSelection(row rowScanner) (SelectionSnapshot, error) {
	stored, err := scanStoredSelection(row)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	return reconstructSelection(stored)
}

func scanStoredSelection(row rowScanner) (storedSelectionRow, error) {
	var stored storedSelectionRow
	err := row.Scan(&stored.selectionID, &stored.descriptor, &stored.self, &stored.phase,
		&stored.seedPrincipal, &stored.seedEventID, &stored.seedEventDigest,
		&stored.initialPreference, &stored.currentPreference, &stored.margin,
		&stored.completedRounds, &stored.revision, &stored.observationDigest,
		&stored.observation, &stored.created, &stored.updated, &stored.pendingRound,
		&stored.pendingNonce, &stored.pendingSample, &stored.pendingDeadline,
		&stored.pendingRevision)
	return stored, err
}

func reconstructSelection(stored storedSelectionRow) (SelectionSnapshot, error) {
	descriptor, err := parseDescriptorCanonical(stored.descriptor)
	if err != nil || stored.selectionID != descriptor.id.String() {
		return SelectionSnapshot{}, fmt.Errorf("stored selector descriptor is corrupt: %w", ErrState)
	}
	self, err := NewParticipantID(stored.self)
	if err != nil || validateProviderActivation(descriptor, self) != nil {
		return SelectionSnapshot{}, fmt.Errorf("stored selector participant is corrupt: %w", ErrState)
	}
	if _, err := parseProviderTime(stored.created); err != nil {
		return SelectionSnapshot{}, err
	}
	if _, err := parseProviderTime(stored.updated); err != nil {
		return SelectionSnapshot{}, err
	}
	snapshot := SelectionSnapshot{descriptor: descriptor, self: self,
		phase: SelectionPhase(stored.phase), revision: stored.revision}
	if snapshot.revision == 0 {
		return SelectionSnapshot{}, fmt.Errorf("stored selector revision is zero: %w", ErrState)
	}
	if snapshot.phase == PhaseAwaitingSeed {
		return validateAwaitingSeedSnapshot(snapshot, stored)
	}
	return reconstructSeededSnapshot(snapshot, stored)
}

func validateAwaitingSeedSnapshot(snapshot SelectionSnapshot,
	stored storedSelectionRow,
) (SelectionSnapshot, error) {
	if stored.seedPrincipal.Valid || stored.seedEventID.Valid || stored.seedEventDigest.Valid ||
		stored.initialPreference.Valid || stored.currentPreference.Valid || stored.pendingRound.Valid {
		return SelectionSnapshot{}, fmt.Errorf("unseeded selector has active fields: %w", ErrState)
	}
	return snapshot, nil
}

func reconstructSeededSnapshot(snapshot SelectionSnapshot,
	stored storedSelectionRow,
) (SelectionSnapshot, error) {
	seed, state, err := reconstructSeedState(snapshot.descriptor, stored.seedPrincipal,
		stored.seedEventID, stored.seedEventDigest, stored.initialPreference,
		stored.currentPreference, stored.margin, stored.completedRounds)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	snapshot.seed, snapshot.state = seed, state
	if stored.pendingRound.Valid {
		pending, err := reconstructPending(snapshot, stored.pendingRound, stored.pendingNonce,
			stored.pendingSample, stored.pendingDeadline, stored.pendingRevision)
		if err != nil {
			return SelectionSnapshot{}, err
		}
		snapshot.pending = pending
	}
	switch snapshot.phase {
	case PhaseActive:
		if stored.observationDigest.Valid || len(stored.observation) != 0 {
			return SelectionSnapshot{}, fmt.Errorf("active selector has observation: %w", ErrState)
		}
	case PhaseObserved:
		if snapshot.pending.valid() || !stored.observationDigest.Valid || len(stored.observation) == 0 {
			return SelectionSnapshot{}, fmt.Errorf("observed selector shape is corrupt: %w", ErrState)
		}
		digest, err := agency.ParseDigest(stored.observationDigest.String)
		if err != nil {
			return SelectionSnapshot{}, fmt.Errorf("stored observation digest: %w", ErrState)
		}
		snapshot.observation, err = parseObservationCanonical(stored.observation, digest,
			snapshot.descriptor, state)
		if err != nil {
			return SelectionSnapshot{}, err
		}
	default:
		return SelectionSnapshot{}, fmt.Errorf("stored selector phase %q: %w", stored.phase, ErrState)
	}
	return snapshot, nil
}

func reconstructSeedState(descriptor SelectionDescriptor, principalValue, eventIDValue,
	eventDigestValue, initialValue, currentValue sql.NullString, margin int64,
	completedRounds uint64,
) (AcceptedSeedOpinion, SelectionState, error) {
	if !principalValue.Valid || !eventIDValue.Valid || !eventDigestValue.Valid ||
		!initialValue.Valid || !currentValue.Valid || completedRounds > uint64(^uint32(0)) {
		return AcceptedSeedOpinion{}, SelectionState{}, fmt.Errorf("stored seed fields are incomplete: %w", ErrState)
	}
	principal, err := agency.NewAgentPrincipalID(principalValue.String)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, fmt.Errorf("stored seed principal: %w", ErrState)
	}
	eventID, err := agency.NewEventID(eventIDValue.String)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, fmt.Errorf("stored seed event ID: %w", ErrState)
	}
	eventDigest, err := agency.ParseDigest(eventDigestValue.String)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, fmt.Errorf("stored seed event digest: %w", ErrState)
	}
	event, err := agency.NewEventRef(eventID, eventDigest)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, fmt.Errorf("stored seed Event: %w", ErrState)
	}
	initial, err := ParsePreference(initialValue.String)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, err
	}
	current, err := ParsePreference(currentValue.String)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, err
	}
	seed, err := NewAcceptedSeedOpinion(principal, event, initial)
	if err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, err
	}
	state := SelectionState{selectionID: descriptor.id, preference: current,
		margin: margin, round: uint32(completedRounds)}
	if err := state.validate(descriptor); err != nil {
		return AcceptedSeedOpinion{}, SelectionState{}, err
	}
	return seed, state, nil
}

func reconstructPending(snapshot SelectionSnapshot, round sql.NullInt64,
	nonce sql.NullString, sampleJSON []byte, deadline sql.NullString, revision sql.NullInt64,
) (PendingRound, error) {
	if !round.Valid || round.Int64 <= 0 || !revision.Valid || revision.Int64 <= 0 ||
		round.Int64 > int64(^uint32(0)) || !nonce.Valid || !deadline.Valid || len(sampleJSON) == 0 {
		return PendingRound{}, fmt.Errorf("stored pending round is incomplete: %w", ErrState)
	}
	nonceDigest, err := agency.ParseDigest(nonce.String)
	if err != nil {
		return PendingRound{}, fmt.Errorf("stored pending nonce: %w", ErrState)
	}
	sample, err := parseSampleCanonical(sampleJSON)
	if err != nil {
		return PendingRound{}, err
	}
	deadlineValue, err := parseProviderTime(deadline.String)
	if err != nil {
		return PendingRound{}, err
	}
	if deadlineValue.After(snapshot.descriptor.expiresAt) {
		return PendingRound{}, fmt.Errorf("stored pending deadline exceeds selection expiry: %w", ErrState)
	}
	query, err := NewSampleQuery(snapshot.descriptor.id, uint32(round.Int64), nonceDigest)
	if err != nil || query.round != snapshot.state.round+1 || uint64(revision.Int64) != snapshot.revision {
		return PendingRound{}, fmt.Errorf("stored pending round authority mismatch: %w", ErrState)
	}
	if _, err := validateSample(snapshot.descriptor, snapshot.self, sample); err != nil {
		return PendingRound{}, fmt.Errorf("stored pending sample: %w", ErrState)
	}
	return PendingRound{query: query, sample: sample, deadline: deadlineValue,
		stateRevision: uint64(revision.Int64)}, nil
}

func loadSelectionTx(ctx context.Context, tx *sql.Tx, id SelectionID) (SelectionSnapshot, error) {
	if id.IsZero() {
		return SelectionSnapshot{}, fmt.Errorf("selection ID is required: %w", ErrInvalid)
	}
	snapshot, err := scanSelection(tx.QueryRowContext(ctx,
		selectionProjectionSQL+" WHERE s.selection_id = ?", id.String()))
	if errors.Is(err, sql.ErrNoRows) {
		return SelectionSnapshot{}, ErrNotFound
	}
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("load selector selection: %w", err)
	}
	return snapshot, nil
}
