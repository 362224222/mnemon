package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrHandlingConflict = errors.New("agent handling conflicts with durable state")

// GetAgentHandling is a read-only projection. Agent handling creation remains
// transaction-scoped so callers cannot enqueue work independently of Event
// acceptance.
func (s *Store) GetAgentHandling(ctx context.Context, id model.HandlingID) (model.Handling, error) {
	if s == nil || s.db == nil || ctx == nil || id.IsZero() {
		return model.Handling{}, errors.New("get agent handling: incomplete input")
	}
	handling, err := readAgentHandling(ctx, s.db, id)
	if err != nil {
		return model.Handling{}, fmt.Errorf("get agent handling: %w", err)
	}
	return handling, nil
}

// insertAgentHandling is deliberately package-private. local_accept and inbox
// apply use it inside the transaction that first accepts the source Event.
// The unique Profile/Event key makes a byte-equivalent retry a replay.
func insertAgentHandling(ctx context.Context, tx *sql.Tx, handling model.Handling) (bool, error) {
	if ctx == nil || tx == nil || handling.ID().IsZero() {
		return false, errors.New("insert agent handling: incomplete input")
	}
	if handling.ProfileID() != model.TeamworkProfileID() || !handling.Status().Valid() {
		return false, errors.New("insert agent handling: invalid Profile or closed status")
	}
	if err := requireEnabledProfile(ctx, tx, handling.ProfileID()); err != nil {
		return false, fmt.Errorf("insert agent handling: %w", err)
	}
	var eventID string
	if err := tx.QueryRowContext(ctx, "SELECT event_id FROM events WHERE event_id = ?", handling.EventID().String()).Scan(&eventID); err != nil {
		return false, fmt.Errorf("insert agent handling: source Event: %w", err)
	}

	existing, err := readAgentHandlingByProfileEvent(ctx, tx, handling.ProfileID(), handling.EventID())
	if err == nil {
		if !sameHandlingCreation(existing, handling) {
			return false, ErrHandlingConflict
		}
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("insert agent handling: inspect replay: %w", err)
	}
	if _, err := readAgentHandling(ctx, tx, handling.ID()); err == nil {
		return false, ErrHandlingConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("insert agent handling: inspect identity: %w", err)
	}

	claimToken, leaseUntil := any(nil), any(nil)
	if digest, ok := handling.ClaimTokenHash(); ok {
		claimToken = digest.Bytes()
	}
	if value, ok := handling.LeaseUntil(); ok {
		leaseUntil = storeTime(value)
	}
	outcomeEvent, deadAt := any(nil), any(nil)
	if id, ok := handling.OutcomeEventID(); ok {
		outcomeEvent = id.String()
	}
	if value, ok := handling.DeadAt(); ok {
		deadAt = storeTime(value)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_handlings(
		handling_id, profile_id, event_id, status, priority, available_at,
		claim_owner, claim_token_hash, lease_until, attempts, last_disposition,
		outcome_event_id, last_error, recovery_count, dead_at, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		handling.ID().String(), handling.ProfileID().String(), handling.EventID().String(),
		string(handling.Status()), handling.Priority(), storeTime(handling.AvailableAt()),
		nullText(handling.ClaimOwner()), claimToken, leaseUntil, handling.Attempts(),
		nullText(handling.LastDisposition()), outcomeEvent, nullText(handling.LastError()),
		handling.RecoveryCount(), deadAt, storeTime(handling.CreatedAt()), storeTime(handling.UpdatedAt()))
	if err != nil {
		return false, fmt.Errorf("insert agent handling: %w", err)
	}
	return false, nil
}

// sameHandlingCreation compares only the immutable identity captured when a
// source Event first creates its Handling. A durable Handling may already have
// been claimed, retried, completed, rejected or declared dead when Inbox apply
// or restart recovery replays that creation. Mutable lifecycle state must
// never turn the same creation into a conflict or be reset by the replay.
func sameHandlingCreation(left, right model.Handling) bool {
	return left.ID() == right.ID() && left.ProfileID() == right.ProfileID() &&
		left.EventID() == right.EventID() && left.Priority() == right.Priority() &&
		left.CreatedAt().Equal(right.CreatedAt())
}

func readAgentHandling(ctx context.Context, q rowQuerier, id model.HandlingID) (model.Handling, error) {
	return scanAgentHandling(q.QueryRowContext(ctx, handlingSelect+" WHERE handling_id = ?", id.String()))
}

func readAgentHandlingByProfileEvent(ctx context.Context, q rowQuerier, profile model.ProfileID,
	event model.EventID,
) (model.Handling, error) {
	return scanAgentHandling(q.QueryRowContext(ctx, handlingSelect+" WHERE profile_id = ? AND event_id = ?",
		profile.String(), event.String()))
}

const handlingSelect = `SELECT handling_id, profile_id, event_id, status, priority, available_at,
	claim_owner, claim_token_hash, lease_until, attempts, last_disposition, outcome_event_id,
	last_error, recovery_count, dead_at, created_at, updated_at FROM agent_handlings`

func scanAgentHandling(row *sql.Row) (model.Handling, error) {
	var idText, profileText, eventText, statusText, availableText, createdText, updatedText string
	var claimOwner, leaseText, disposition, outcomeText, lastError, deadText sql.NullString
	var claimToken []byte
	var priority int
	var attempts, recovery uint32
	if err := row.Scan(&idText, &profileText, &eventText, &statusText, &priority, &availableText,
		&claimOwner, &claimToken, &leaseText, &attempts, &disposition, &outcomeText,
		&lastError, &recovery, &deadText, &createdText, &updatedText); err != nil {
		return model.Handling{}, err
	}
	id, err := model.ParseHandlingID(idText)
	if err != nil {
		return model.Handling{}, err
	}
	profile, err := model.ParseProfileID(profileText)
	if err != nil {
		return model.Handling{}, err
	}
	event, err := model.ParseEventID(eventText)
	if err != nil {
		return model.Handling{}, err
	}
	available, err := parseCanonicalStoreTime(availableText)
	if err != nil {
		return model.Handling{}, err
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Handling{}, err
	}
	updated, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.Handling{}, err
	}
	spec := model.HandlingSpec{ID: id, ProfileID: profile, EventID: event,
		Status: model.HandlingStatus(statusText), Priority: priority, AvailableAt: available,
		ClaimOwner: claimOwner.String, Attempts: attempts, LastDisposition: disposition.String,
		LastError: lastError.String, RecoveryCount: recovery, CreatedAt: created, UpdatedAt: updated}
	if len(claimToken) != 0 {
		digest, err := model.DigestFromBytes(claimToken)
		if err != nil {
			return model.Handling{}, err
		}
		spec.ClaimTokenHash = &digest
	}
	if leaseText.Valid {
		value, err := parseCanonicalStoreTime(leaseText.String)
		if err != nil {
			return model.Handling{}, err
		}
		spec.LeaseUntil = &value
	}
	if outcomeText.Valid {
		value, err := model.ParseEventID(outcomeText.String)
		if err != nil {
			return model.Handling{}, err
		}
		spec.OutcomeEventID = &value
	}
	if deadText.Valid {
		value, err := parseCanonicalStoreTime(deadText.String)
		if err != nil {
			return model.Handling{}, err
		}
		spec.DeadAt = &value
	}
	return model.NewHandling(spec)
}

func sameHandling(left, right model.Handling) bool {
	leftToken, leftClaim := left.ClaimTokenHash()
	rightToken, rightClaim := right.ClaimTokenHash()
	leftLease, leftHasLease := left.LeaseUntil()
	rightLease, rightHasLease := right.LeaseUntil()
	leftOutcome, leftHasOutcome := left.OutcomeEventID()
	rightOutcome, rightHasOutcome := right.OutcomeEventID()
	leftDead, leftHasDead := left.DeadAt()
	rightDead, rightHasDead := right.DeadAt()
	return left.ID() == right.ID() && left.ProfileID() == right.ProfileID() && left.EventID() == right.EventID() &&
		left.Status() == right.Status() && left.Priority() == right.Priority() && left.AvailableAt().Equal(right.AvailableAt()) &&
		left.ClaimOwner() == right.ClaimOwner() && leftClaim == rightClaim && (!leftClaim || leftToken == rightToken) &&
		leftHasLease == rightHasLease && (!leftHasLease || leftLease.Equal(rightLease)) && left.Attempts() == right.Attempts() &&
		left.LastDisposition() == right.LastDisposition() && leftHasOutcome == rightHasOutcome && (!leftHasOutcome || leftOutcome == rightOutcome) &&
		left.LastError() == right.LastError() && left.RecoveryCount() == right.RecoveryCount() &&
		leftHasDead == rightHasDead && (!leftHasDead || leftDead.Equal(rightDead)) &&
		left.CreatedAt().Equal(right.CreatedAt()) && left.UpdatedAt().Equal(right.UpdatedAt())
}

func parseCanonicalStoreTime(value string) (time.Time, error) {
	parsed, err := time.Parse(storeTimeLayout, value)
	if err != nil || parsed.IsZero() || storeTime(parsed) != value ||
		!time.Unix(0, parsed.UnixNano()).UTC().Equal(parsed) {
		return time.Time{}, fmt.Errorf("non-canonical store time %q", value)
	}
	return parsed, nil
}

func nullText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
