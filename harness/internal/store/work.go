package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrWorkCASConflict = errors.New("ReviewWork version or state changed")

// WorkMutation carries the exact optimistic predecessor for one canonical
// home Work write. ExpectedVersion zero means creation.
type WorkMutation struct {
	Work            model.ReviewWork
	ExpectedVersion uint64
	ExpectedState   model.WorkState
}

func NewWorkCreation(work model.ReviewWork) (WorkMutation, error) {
	if work.Ref().IsZero() || work.State() != model.WorkOffered || work.Version() != 1 || work.Iteration() != 1 {
		return WorkMutation{}, errors.New("new Work must be OFFERED version 1 iteration 1")
	}
	return WorkMutation{Work: work}, nil
}

func NewWorkTransition(work model.ReviewWork, expectedVersion uint64,
	expectedState model.WorkState,
) (WorkMutation, error) {
	if work.Ref().IsZero() || expectedVersion == 0 || !expectedState.Valid() ||
		expectedVersion >= model.MaxSQLiteInteger || work.Version() != expectedVersion+1 {
		return WorkMutation{}, errors.New("Work transition requires a valid predecessor and next version")
	}
	return WorkMutation{Work: work, ExpectedVersion: expectedVersion, ExpectedState: expectedState}, nil
}

func (s *Store) GetReviewWork(ctx context.Context, ref model.WorkRef) (model.ReviewWork, error) {
	if s == nil || s.db == nil || ctx == nil || ref.IsZero() {
		return model.ReviewWork{}, errors.New("get ReviewWork: incomplete input")
	}
	work, err := readReviewWork(ctx, s.db, ref)
	if err != nil {
		return model.ReviewWork{}, fmt.Errorf("get ReviewWork: %w", err)
	}
	return work, nil
}

func readReviewWork(ctx context.Context, q rowQuerier, ref model.WorkRef) (model.ReviewWork, error) {
	var channelText, homeText, workText, stateText, eventText, updatedText string
	var rosterRevision, version uint64
	var iteration uint8
	var deadline int64
	var stateJSON []byte
	err := q.QueryRowContext(ctx, `SELECT channel_id,home_peer_id,work_id,participant_roster_revision,
		version,iteration,deadline_unix_nano,state,state_json,updated_by_event,updated_at
		FROM works WHERE home_peer_id=? AND work_id=?`, ref.HomePeerID().String(), ref.WorkID().String()).
		Scan(&channelText, &homeText, &workText, &rosterRevision, &version, &iteration, &deadline,
			&stateText, &stateJSON, &eventText, &updatedText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	channel, err := model.ParseChannelID(channelText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	home, err := model.ParsePeerID(homeText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	workID, err := model.ParseWorkID(workText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	storedRef, err := model.NewWorkRef(home, workID)
	if err != nil || storedRef != ref {
		return model.ReviewWork{}, errors.New("stored Work identity mismatch")
	}
	var initiator, reviewer model.PeerID
	rows, err := queryWorkMembers(ctx, q, ref)
	if err != nil {
		return model.ReviewWork{}, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var peerText, roleText string
		if err := rows.Scan(&peerText, &roleText); err != nil {
			return model.ReviewWork{}, err
		}
		peer, err := model.ParsePeerID(peerText)
		if err != nil {
			return model.ReviewWork{}, err
		}
		switch model.WorkRole(roleText) {
		case model.WorkRoleInitiator:
			initiator = peer
		case model.WorkRoleReviewer:
			reviewer = peer
		default:
			return model.ReviewWork{}, errors.New("stored Work role is invalid")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return model.ReviewWork{}, err
	}
	if count != 2 || initiator.IsZero() || reviewer.IsZero() {
		return model.ReviewWork{}, errors.New("ReviewWork must have exactly two role members")
	}
	participants, err := model.NewParticipantSnapshot(channel, rosterRevision, initiator, reviewer)
	if err != nil {
		return model.ReviewWork{}, err
	}
	state, err := model.NewJSON(stateJSON)
	if err != nil {
		return model.ReviewWork{}, err
	}
	if !bytes.Equal(state.Bytes(), stateJSON) {
		return model.ReviewWork{}, errors.New("stored Work state JSON is not canonical")
	}
	updatedBy, err := model.ParseEventID(eventText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.ReviewWork{}, err
	}
	return model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel, Participants: participants,
		Version: version, Iteration: iteration, DeadlineUnixNano: deadline, State: model.WorkState(stateText),
		StateData: state, UpdatedBy: updatedBy, UpdatedAt: updatedAt})
}

type rowsQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryWorkMembers(ctx context.Context, q rowQuerier, ref model.WorkRef) (*sql.Rows, error) {
	query, ok := q.(rowsQuerier)
	if !ok {
		return nil, errors.New("Work reader does not support row queries")
	}
	return query.QueryContext(ctx, "SELECT peer_id,role FROM work_members WHERE home_peer_id=? AND work_id=? ORDER BY role",
		ref.HomePeerID().String(), ref.WorkID().String())
}

func applyWorkMutation(ctx context.Context, tx *sql.Tx, mutation WorkMutation, event model.Event) error {
	work := mutation.Work
	if err := work.ValidateUpdateEvent(event); err != nil {
		return fmt.Errorf("apply Work mutation: update Event mismatch: %w", err)
	}
	if !work.UpdatedAt().Equal(event.AcceptedAt()) {
		return errors.New("apply Work mutation: Work/Event update times differ")
	}
	if mutation.ExpectedVersion == 0 {
		return insertOfferedWork(ctx, tx, work, event)
	}
	current, err := readReviewWork(ctx, tx, work.Ref())
	if err != nil {
		return fmt.Errorf("apply Work mutation: load predecessor: %w", err)
	}
	if current.Version() != mutation.ExpectedVersion || current.State() != mutation.ExpectedState ||
		current.ChannelID() != work.ChannelID() || current.Participants() != work.Participants() ||
		current.DeadlineUnixNano() != work.DeadlineUnixNano() {
		return ErrWorkCASConflict
	}
	if current.State().DeadlineEligible() && event.AcceptedAt().UnixNano() >= current.DeadlineUnixNano() &&
		event.Type() != model.EventReviewExpired {
		return errors.New("apply Work mutation: due Work requires expiry")
	}
	if event.Type() == model.EventReviewExpired &&
		(!current.State().DeadlineEligible() || event.AcceptedAt().UnixNano() < current.DeadlineUnixNano()) {
		return errors.New("apply Work mutation: expiry requires a due deadline-eligible Work")
	}
	nextState, nextIteration, ok := model.NextReviewWorkState(current.State(), current.Iteration(), event.Type())
	if !ok || work.Version() != current.Version()+1 || work.State() != nextState || work.Iteration() != nextIteration {
		return errors.New("apply Work mutation: invalid durable successor")
	}
	result, err := tx.ExecContext(ctx, `UPDATE works SET version=?,iteration=?,state=?,state_json=?,
		updated_by_event=?,updated_at=? WHERE home_peer_id=? AND work_id=? AND version=? AND state=?`,
		work.Version(), work.Iteration(), string(work.State()), work.StateData().Bytes(), event.ID().String(),
		storeTime(work.UpdatedAt()), work.Ref().HomePeerID().String(), work.Ref().WorkID().String(),
		mutation.ExpectedVersion, string(mutation.ExpectedState))
	if err != nil {
		return fmt.Errorf("apply Work mutation: update: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return ErrWorkCASConflict
	}
	return nil
}

func insertOfferedWork(ctx context.Context, tx *sql.Tx, work model.ReviewWork, event model.Event) error {
	if work.State() != model.WorkOffered || work.Version() != 1 || work.Iteration() != 1 ||
		work.Ref().HomePeerID() != event.Scope().OriginPeerID() {
		return errors.New("apply Work mutation: invalid offered Work")
	}
	duration := time.Duration(work.DeadlineUnixNano() - event.AcceptedAt().UnixNano())
	if duration < model.MinimumReviewDeadline || duration > model.MaximumReviewDeadline {
		return errors.New("apply Work mutation: offer deadline out of range")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO works(channel_id,home_peer_id,work_id,
		participant_roster_revision,version,iteration,deadline_unix_nano,state,state_json,updated_by_event,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, work.ChannelID().String(), work.Ref().HomePeerID().String(),
		work.Ref().WorkID().String(), work.Participants().RosterRevision(), work.Version(), work.Iteration(),
		work.DeadlineUnixNano(), string(work.State()), work.StateData().Bytes(), event.ID().String(), storeTime(work.UpdatedAt()))
	if err != nil {
		return fmt.Errorf("apply Work mutation: insert: %w", err)
	}
	for _, member := range []struct {
		peer model.PeerID
		role model.WorkRole
	}{{work.Participants().InitiatorPeerID(), model.WorkRoleInitiator},
		{work.Participants().ReviewerPeerID(), model.WorkRoleReviewer}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role)
			VALUES(?,?,?,?,?)`, work.ChannelID().String(), work.Ref().HomePeerID().String(),
			work.Ref().WorkID().String(), member.peer.String(), string(member.role)); err != nil {
			return fmt.Errorf("apply Work mutation: insert participant: %w", err)
		}
	}
	return nil
}
