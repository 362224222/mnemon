package teamwork

import (
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrInvalidTransition    = errors.New("invalid teamwork transition")
	ErrNotWorkHome          = errors.New("transition actor is not Work home")
	ErrParticipantInput     = errors.New("participant input cannot transition home Work")
	ErrVersionConflict      = errors.New("Work version conflict")
	ErrTransitionNotAllowed = errors.New("teamwork transition not allowed")
	ErrTerminalWork         = errors.New("terminal Work is immutable")
	ErrWorkVersionExhausted = errors.New("Work version exhausted")
)

// HomeTransitionSpec binds an authoritative decision to the exact Work
// version observed by the controller and to its trusted integer clock.
type HomeTransitionSpec struct {
	Work            model.ReviewWork
	ActorPeerID     model.PeerID
	ExpectedVersion uint64
	EventType       model.EventType
	NowUnixNano     int64
}

// TransitionIntent is an immutable Work-version CAS intent. Event identity,
// accepted time, sequence, publication and signature remain admission duties.
type TransitionIntent struct {
	workRef            model.WorkRef
	channelID          model.ChannelID
	expectedVersion    uint64
	expectedState      model.WorkState
	expectedIteration  uint8
	deadlineUnixNano   int64
	requestedEventType model.EventType
	authoritativeEvent model.EventType
	nextVersion        uint64
	nextState          model.WorkState
	nextIteration      uint8
	observedAtUnixNano int64
}

func (p TransitionIntent) WorkRef() model.WorkRef                  { return p.workRef }
func (p TransitionIntent) ChannelID() model.ChannelID              { return p.channelID }
func (p TransitionIntent) ExpectedVersion() uint64                 { return p.expectedVersion }
func (p TransitionIntent) ExpectedState() model.WorkState          { return p.expectedState }
func (p TransitionIntent) ExpectedIteration() uint8                { return p.expectedIteration }
func (p TransitionIntent) DeadlineUnixNano() int64                 { return p.deadlineUnixNano }
func (p TransitionIntent) RequestedEventType() model.EventType     { return p.requestedEventType }
func (p TransitionIntent) AuthoritativeEventType() model.EventType { return p.authoritativeEvent }
func (p TransitionIntent) NextVersion() uint64                     { return p.nextVersion }
func (p TransitionIntent) NextState() model.WorkState              { return p.nextState }
func (p TransitionIntent) NextIteration() uint8                    { return p.nextIteration }
func (p TransitionIntent) ObservedAtUnixNano() int64               { return p.observedAtUnixNano }
func (p TransitionIntent) DeadlineWon() bool {
	return p.authoritativeEvent == model.EventReviewExpired && p.requestedEventType != model.EventReviewExpired
}

// PlanHomeTransition applies the closed ReviewWork state policy. If an
// expiry-eligible Work is due, expiry replaces every competing home decision
// before the ordinary transition table is evaluated.
func PlanHomeTransition(spec HomeTransitionSpec) (TransitionIntent, error) {
	if err := validateHomeCAS(spec.Work, spec.ActorPeerID, spec.ExpectedVersion, spec.NowUnixNano); err != nil {
		return TransitionIntent{}, err
	}
	if !spec.EventType.Valid() {
		return TransitionIntent{}, fmt.Errorf("%w: unknown Event type %q", ErrInvalidTransition, spec.EventType)
	}
	if spec.EventType.ParticipantInput() {
		return TransitionIntent{}, fmt.Errorf("%w: %s", ErrParticipantInput, spec.EventType)
	}
	if !spec.EventType.HomeAuthoritative() {
		return TransitionIntent{}, fmt.Errorf("%w: %s is not a home state Event", ErrTransitionNotAllowed, spec.EventType)
	}
	if !workMutationEvent(spec.EventType) {
		return TransitionIntent{}, fmt.Errorf("%w: %s does not mutate an existing Work", ErrTransitionNotAllowed, spec.EventType)
	}
	if spec.Work.State().Terminal() {
		return TransitionIntent{}, fmt.Errorf("%w: %s", ErrTerminalWork, spec.Work.State())
	}
	if spec.Work.Version() == model.MaxSQLiteInteger {
		return TransitionIntent{}, ErrWorkVersionExhausted
	}

	if spec.EventType == model.EventReviewExpired {
		return PlanExpiry(ExpirySpec{
			Work:            spec.Work,
			HomePeerID:      spec.ActorPeerID,
			ExpectedVersion: spec.ExpectedVersion,
			NowUnixNano:     spec.NowUnixNano,
		})
	}
	if spec.Work.State().DeadlineEligible() && spec.NowUnixNano >= spec.Work.DeadlineUnixNano() {
		return planExpiryIntent(spec.Work, spec.ExpectedVersion, spec.NowUnixNano, spec.EventType), nil
	}

	nextState, nextIteration, ok := model.NextReviewWorkState(spec.Work.State(), spec.Work.Iteration(), spec.EventType)
	if !ok {
		return TransitionIntent{}, fmt.Errorf(
			"%w: %s cannot apply to %s iteration %d",
			ErrTransitionNotAllowed,
			spec.EventType,
			spec.Work.State(),
			spec.Work.Iteration(),
		)
	}
	return newTransitionIntent(spec.Work, spec.ExpectedVersion, spec.NowUnixNano, spec.EventType, spec.EventType, nextState, nextIteration), nil
}

func workMutationEvent(eventType model.EventType) bool {
	switch eventType {
	case model.EventReviewAccepted,
		model.EventReviewDelivered,
		model.EventReviewReworkRequested,
		model.EventReviewClosed,
		model.EventReviewDeclined,
		model.EventReviewCancelled,
		model.EventReviewExpired:
		return true
	default:
		return false
	}
}

func validateHomeCAS(work model.ReviewWork, actor model.PeerID, expectedVersion uint64, nowUnixNano int64) error {
	if work.Ref().IsZero() || work.ChannelID().IsZero() || actor.IsZero() || expectedVersion == 0 || nowUnixNano <= 0 {
		return fmt.Errorf("%w: complete Work, actor, version and positive trusted time are required", ErrInvalidTransition)
	}
	if actor != work.Ref().HomePeerID() {
		return fmt.Errorf("%w: actor %q, home %q", ErrNotWorkHome, actor.String(), work.Ref().HomePeerID().String())
	}
	if expectedVersion != work.Version() {
		return fmt.Errorf("%w: expected %d, current %d", ErrVersionConflict, expectedVersion, work.Version())
	}
	return nil
}

func newTransitionIntent(
	work model.ReviewWork,
	expectedVersion uint64,
	nowUnixNano int64,
	requestedEventType model.EventType,
	authoritativeEventType model.EventType,
	nextState model.WorkState,
	nextIteration uint8,
) TransitionIntent {
	return TransitionIntent{
		workRef:            work.Ref(),
		channelID:          work.ChannelID(),
		expectedVersion:    expectedVersion,
		expectedState:      work.State(),
		expectedIteration:  work.Iteration(),
		deadlineUnixNano:   work.DeadlineUnixNano(),
		requestedEventType: requestedEventType,
		authoritativeEvent: authoritativeEventType,
		nextVersion:        expectedVersion + 1,
		nextState:          nextState,
		nextIteration:      nextIteration,
		observedAtUnixNano: nowUnixNano,
	}
}
