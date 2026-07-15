package model

import "time"

const (
	DefaultReviewDeadline = 24 * time.Hour
	MinimumReviewDeadline = 5 * time.Minute
	MaximumReviewDeadline = 168 * time.Hour
)

type WorkState string

const (
	WorkOffered   WorkState = "OFFERED"
	WorkActive    WorkState = "ACTIVE"
	WorkDelivered WorkState = "DELIVERED"
	WorkRework    WorkState = "REWORK"
	WorkClosed    WorkState = "CLOSED"
	WorkDeclined  WorkState = "DECLINED"
	WorkExpired   WorkState = "EXPIRED"
	WorkCancelled WorkState = "CANCELLED"
)

func (s WorkState) Valid() bool {
	switch s {
	case WorkOffered, WorkActive, WorkDelivered, WorkRework,
		WorkClosed, WorkDeclined, WorkExpired, WorkCancelled:
		return true
	default:
		return false
	}
}

func (s WorkState) Terminal() bool {
	return s == WorkClosed || s == WorkDeclined || s == WorkExpired || s == WorkCancelled
}

func (s WorkState) DeadlineEligible() bool {
	return s == WorkOffered || s == WorkActive || s == WorkRework
}

// NextReviewWorkState is the single record-shape authority for the closed T0
// state machine. Teamwork policy adds actor and deadline decisions; the store
// uses this shape to reject impossible durable successors without importing a
// higher layer.
func NextReviewWorkState(state WorkState, iteration uint8, eventType EventType) (WorkState, uint8, bool) {
	switch state {
	case WorkOffered:
		switch eventType {
		case EventReviewAccepted:
			return WorkActive, iteration, true
		case EventReviewDeclined:
			return WorkDeclined, iteration, true
		case EventReviewCancelled:
			return WorkCancelled, iteration, true
		case EventReviewExpired:
			return WorkExpired, iteration, true
		}
	case WorkActive, WorkRework:
		switch eventType {
		case EventReviewDelivered:
			return WorkDelivered, iteration, true
		case EventReviewCancelled:
			return WorkCancelled, iteration, true
		case EventReviewExpired:
			return WorkExpired, iteration, true
		}
	case WorkDelivered:
		switch eventType {
		case EventReviewReworkRequested:
			if iteration == 1 {
				return WorkRework, 2, true
			}
		case EventReviewClosed:
			return WorkClosed, iteration, true
		case EventReviewCancelled:
			return WorkCancelled, iteration, true
		}
	}
	return "", 0, false
}

type WorkRole string

const (
	WorkRoleInitiator WorkRole = "initiator"
	WorkRoleReviewer  WorkRole = "reviewer"
)

func (r WorkRole) Valid() bool { return r == WorkRoleInitiator || r == WorkRoleReviewer }

type ParticipantSnapshot struct {
	channelID      ChannelID
	rosterRevision uint64
	initiator      PeerID
	reviewer       PeerID
}

func NewParticipantSnapshot(channel ChannelID, rosterRevision uint64,
	initiator, reviewer PeerID,
) (ParticipantSnapshot, error) {
	if channel.IsZero() || initiator.IsZero() || reviewer.IsZero() {
		return ParticipantSnapshot{}, invalid("participant snapshot", "channel, roster revision and peers are required")
	}
	if err := validateSQLitePositive("participant roster revision", rosterRevision); err != nil {
		return ParticipantSnapshot{}, err
	}
	if initiator == reviewer {
		return ParticipantSnapshot{}, invariant("ReviewWork cannot review itself")
	}
	return ParticipantSnapshot{channel, rosterRevision, initiator, reviewer}, nil
}

func (s ParticipantSnapshot) ChannelID() ChannelID    { return s.channelID }
func (s ParticipantSnapshot) RosterRevision() uint64  { return s.rosterRevision }
func (s ParticipantSnapshot) InitiatorPeerID() PeerID { return s.initiator }
func (s ParticipantSnapshot) ReviewerPeerID() PeerID  { return s.reviewer }
func (s ParticipantSnapshot) PeerForRole(role WorkRole) (PeerID, bool) {
	switch role {
	case WorkRoleInitiator:
		return s.initiator, true
	case WorkRoleReviewer:
		return s.reviewer, true
	default:
		return PeerID{}, false
	}
}

func (s ParticipantSnapshot) MarshalJSON() ([]byte, error) {
	if s.channelID.IsZero() || s.rosterRevision == 0 || s.initiator.IsZero() || s.reviewer.IsZero() {
		return nil, invalid("participant snapshot", "zero snapshot")
	}
	return CanonicalMarshal(struct {
		ChannelID      ChannelID `json:"channel_id"`
		RosterRevision uint64    `json:"channel_roster_revision"`
		Initiator      PeerID    `json:"initiator_peer_id"`
		Reviewer       PeerID    `json:"reviewer_peer_id"`
	}{s.channelID, s.rosterRevision, s.initiator, s.reviewer})
}

type ReviewWorkSpec struct {
	Ref              WorkRef
	ChannelID        ChannelID
	Participants     ParticipantSnapshot
	Version          uint64
	Iteration        uint8
	DeadlineUnixNano int64
	State            WorkState
	StateData        JSON
	UpdatedBy        EventID
	UpdatedAt        time.Time
}

type ReviewWork struct {
	spec ReviewWorkSpec
}

func NewReviewWork(spec ReviewWorkSpec) (ReviewWork, error) {
	if spec.Ref.IsZero() || spec.ChannelID.IsZero() || spec.UpdatedBy.IsZero() {
		return ReviewWork{}, invalid("review work", "WorkRef, Channel and update Event are required")
	}
	if !spec.State.Valid() || spec.Iteration < 1 || spec.Iteration > 2 {
		return ReviewWork{}, invalid("review work", "state, positive version and iteration 1..2 are required")
	}
	if err := validateSQLitePositive("Work version", spec.Version); err != nil {
		return ReviewWork{}, err
	}
	if spec.DeadlineUnixNano <= 0 {
		return ReviewWork{}, invalid("review work deadline", "must be positive Unix nanoseconds")
	}
	if spec.StateData.IsZero() || spec.StateData.raw[0] != '{' {
		return ReviewWork{}, invalid("review work state", "must be a canonical JSON object")
	}
	if spec.Participants.ChannelID() != spec.ChannelID {
		return ReviewWork{}, invariant("participant snapshot Channel does not match Work Channel")
	}
	if spec.Participants.InitiatorPeerID() != spec.Ref.HomePeerID() {
		return ReviewWork{}, invariant("Work home must be the frozen initiator")
	}
	if spec.Participants.ReviewerPeerID() == spec.Ref.HomePeerID() {
		return ReviewWork{}, invariant("Work reviewer must be remote from home")
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return ReviewWork{}, err
	}
	if spec.State == WorkOffered && (spec.Version != 1 || spec.Iteration != 1) {
		return ReviewWork{}, invariant("OFFERED Work must begin at version 1, iteration 1")
	}
	if spec.State == WorkRework && spec.Iteration != 2 {
		return ReviewWork{}, invariant("REWORK is the second and final iteration")
	}
	if (spec.State == WorkActive || spec.State == WorkDeclined) && spec.Iteration != 1 {
		return ReviewWork{}, invariant("ACTIVE and DECLINED are only reachable in iteration 1")
	}
	spec.UpdatedAt = updatedAt
	return ReviewWork{spec: spec}, nil
}

func eventTypeForState(state WorkState) EventType {
	switch state {
	case WorkOffered:
		return EventReviewOffered
	case WorkActive:
		return EventReviewAccepted
	case WorkDelivered:
		return EventReviewDelivered
	case WorkRework:
		return EventReviewReworkRequested
	case WorkClosed:
		return EventReviewClosed
	case WorkDeclined:
		return EventReviewDeclined
	case WorkExpired:
		return EventReviewExpired
	case WorkCancelled:
		return EventReviewCancelled
	default:
		return ""
	}
}

func (w ReviewWork) Ref() WorkRef                      { return w.spec.Ref }
func (w ReviewWork) ChannelID() ChannelID              { return w.spec.ChannelID }
func (w ReviewWork) Participants() ParticipantSnapshot { return w.spec.Participants }
func (w ReviewWork) Version() uint64                   { return w.spec.Version }
func (w ReviewWork) Iteration() uint8                  { return w.spec.Iteration }
func (w ReviewWork) DeadlineUnixNano() int64           { return w.spec.DeadlineUnixNano }
func (w ReviewWork) Deadline() time.Time               { return time.Unix(0, w.spec.DeadlineUnixNano).UTC() }
func (w ReviewWork) State() WorkState                  { return w.spec.State }
func (w ReviewWork) StateData() JSON                   { return w.spec.StateData }
func (w ReviewWork) UpdatedBy() EventID                { return w.spec.UpdatedBy }
func (w ReviewWork) UpdatedAt() time.Time              { return w.spec.UpdatedAt }
func (w ReviewWork) Spec() ReviewWorkSpec              { return w.spec }

// ValidateUpdateEvent binds the compact durable Work record to the immutable
// Event loaded by the store. Keeping EventID in ReviewWorkSpec avoids embedding
// a duplicate Event while retaining one model-level scope/authority check.
func (w ReviewWork) ValidateUpdateEvent(event Event) error {
	if event.ID() != w.spec.UpdatedBy {
		return invariant("update Event ID does not match Work record")
	}
	scope := event.Scope()
	if scope.ChannelID() != w.spec.ChannelID || scope.WorkRef() != w.spec.Ref {
		return invariant("update Event scope does not match Work")
	}
	if scope.OriginPeerID() != w.spec.Ref.HomePeerID() {
		return invariant("only the Work home Event can update canonical Work state")
	}
	if event.Type() != eventTypeForState(w.spec.State) {
		return invariant("update Event type does not match Work state")
	}
	if w.spec.UpdatedAt.Before(event.AcceptedAt()) {
		return invariant("Work update time precedes update Event acceptance")
	}
	return nil
}
