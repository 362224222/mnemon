package teamwork

import (
	"errors"
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrInvalidDerivation = errors.New("invalid Work derivation")
	ErrDerivationScope   = errors.New("Work derivation scope mismatch")
)

// DerivationChildScope is one child Work identity within a context-bound offer
// operation. Ordinals are frozen by canonical reviewer order at admission.
type DerivationChildScope struct {
	ordinal   uint8
	channelID model.ChannelID
	workRef   model.WorkRef
}

func NewDerivationChildScope(ordinal uint8, channelID model.ChannelID, workRef model.WorkRef) (DerivationChildScope, error) {
	if ordinal >= model.MaxChildWorks || channelID.IsZero() || workRef.IsZero() {
		return DerivationChildScope{}, fmt.Errorf("%w: child ordinal and scope are required", ErrInvalidDerivation)
	}
	return DerivationChildScope{ordinal: ordinal, channelID: channelID, workRef: workRef}, nil
}

func (c DerivationChildScope) Ordinal() uint8             { return c.ordinal }
func (c DerivationChildScope) ChannelID() model.ChannelID { return c.channelID }
func (c DerivationChildScope) WorkRef() model.WorkRef     { return c.workRef }

// DerivationGroupSpec freezes the precise parent state observed by the
// context-bound offer and the complete one-to-seven child set.
type DerivationGroupSpec struct {
	OperationID       model.OperationID
	Parent            model.ReviewWork
	ParentSourceEvent model.EventID
	Children          []DerivationChildScope
}

// DerivationGroup is immutable policy input. Durable uniqueness and evidence
// rows are committed by the store under OperationID.
type DerivationGroup struct {
	operationID        model.OperationID
	parentChannelID    model.ChannelID
	parentWorkRef      model.WorkRef
	parentParticipants model.ParticipantSnapshot
	parentVersion      uint64
	parentState        model.WorkState
	parentDeadline     int64
	parentSourceEvent  model.EventID
	children           []DerivationChildScope
}

func NewDerivationGroup(spec DerivationGroupSpec) (DerivationGroup, error) {
	if spec.OperationID.IsZero() || spec.Parent.Ref().IsZero() || spec.Parent.ChannelID().IsZero() || spec.ParentSourceEvent.IsZero() {
		return DerivationGroup{}, fmt.Errorf("%w: operation and frozen parent scope are required", ErrInvalidDerivation)
	}
	if spec.Parent.State() != model.WorkActive && spec.Parent.State() != model.WorkRework {
		return DerivationGroup{}, fmt.Errorf("%w: parent must be reviewer ACTIVE or REWORK, got %s", ErrInvalidDerivation, spec.Parent.State())
	}
	if spec.Parent.UpdatedBy() != spec.ParentSourceEvent {
		return DerivationGroup{}, fmt.Errorf("%w: source Event does not match frozen parent version", ErrDerivationScope)
	}
	if len(spec.Children) == 0 || len(spec.Children) > model.MaxChildWorks {
		return DerivationGroup{}, fmt.Errorf("%w: child count must be between 1 and %d", ErrInvalidDerivation, model.MaxChildWorks)
	}

	children := append([]DerivationChildScope(nil), spec.Children...)
	sort.Slice(children, func(i, j int) bool { return children[i].ordinal < children[j].ordinal })
	childChannel := children[0].channelID
	childHome := children[0].workRef.HomePeerID()
	wantChildHome := spec.Parent.Participants().ReviewerPeerID()
	seen := make(map[string]struct{}, len(children))
	for index, child := range children {
		if child.channelID.IsZero() || child.workRef.IsZero() || int(child.ordinal) != index {
			return DerivationGroup{}, fmt.Errorf("%w: child ordinals must be exactly 0..%d", ErrInvalidDerivation, len(children)-1)
		}
		if child.channelID != childChannel {
			return DerivationGroup{}, fmt.Errorf("%w: one offer operation cannot span child Channels", ErrDerivationScope)
		}
		if child.workRef.HomePeerID() != childHome || child.workRef.HomePeerID() != wantChildHome {
			return DerivationGroup{}, fmt.Errorf("%w: every child home must be the frozen parent reviewer", ErrDerivationScope)
		}
		if child.workRef == spec.Parent.Ref() {
			return DerivationGroup{}, fmt.Errorf("%w: child cannot equal parent Work", ErrDerivationScope)
		}
		key := derivationWorkKey(child.workRef)
		if _, exists := seen[key]; exists {
			return DerivationGroup{}, fmt.Errorf("%w: duplicate child WorkRef", ErrInvalidDerivation)
		}
		seen[key] = struct{}{}
	}

	return DerivationGroup{
		operationID:        spec.OperationID,
		parentChannelID:    spec.Parent.ChannelID(),
		parentWorkRef:      spec.Parent.Ref(),
		parentParticipants: spec.Parent.Participants(),
		parentVersion:      spec.Parent.Version(),
		parentState:        spec.Parent.State(),
		parentDeadline:     spec.Parent.DeadlineUnixNano(),
		parentSourceEvent:  spec.ParentSourceEvent,
		children:           children,
	}, nil
}

func (g DerivationGroup) OperationID() model.OperationID   { return g.operationID }
func (g DerivationGroup) ParentChannelID() model.ChannelID { return g.parentChannelID }
func (g DerivationGroup) ParentWorkRef() model.WorkRef     { return g.parentWorkRef }
func (g DerivationGroup) ParentVersion() uint64            { return g.parentVersion }
func (g DerivationGroup) ParentState() model.WorkState     { return g.parentState }
func (g DerivationGroup) ParentSourceEvent() model.EventID { return g.parentSourceEvent }
func (g DerivationGroup) Children() []DerivationChildScope {
	return append([]DerivationChildScope(nil), g.children...)
}

type DerivationDisposition string

const (
	DerivationPending     DerivationDisposition = "pending"
	DerivationResume      DerivationDisposition = "resume"
	DerivationParentStale DerivationDisposition = "parent_stale"
)

func (d DerivationDisposition) Valid() bool {
	return d == DerivationPending || d == DerivationResume || d == DerivationParentStale
}

type DerivationChildResult struct {
	ordinal uint8
	workRef model.WorkRef
	state   model.WorkState
	version uint64
}

func (r DerivationChildResult) Ordinal() uint8         { return r.ordinal }
func (r DerivationChildResult) WorkRef() model.WorkRef { return r.workRef }
func (r DerivationChildResult) State() model.WorkState { return r.state }
func (r DerivationChildResult) Version() uint64        { return r.version }

// DerivationDispositionPlan is exactly one of pending, resume or
// parent_stale. Only resume is wakeable; no disposition publishes an Event.
type DerivationDispositionPlan struct {
	operationID         model.OperationID
	disposition         DerivationDisposition
	frozenParentVersion uint64
	latestParentVersion uint64
	childResults        []DerivationChildResult
}

func (p DerivationDispositionPlan) OperationID() model.OperationID     { return p.operationID }
func (p DerivationDispositionPlan) Disposition() DerivationDisposition { return p.disposition }
func (p DerivationDispositionPlan) FrozenParentVersion() uint64        { return p.frozenParentVersion }
func (p DerivationDispositionPlan) LatestParentVersion() uint64        { return p.latestParentVersion }
func (p DerivationDispositionPlan) ShouldWake() bool                   { return p.disposition == DerivationResume }
func (p DerivationDispositionPlan) Complete() bool                     { return p.disposition != DerivationPending }
func (p DerivationDispositionPlan) ChildResults() []DerivationChildResult {
	return append([]DerivationChildResult(nil), p.childResults...)
}

// PlanDerivationDisposition waits for the complete child set, then chooses
// resume and parent_stale mutually exclusively from the frozen parent version.
func PlanDerivationDisposition(
	group DerivationGroup,
	latestParent model.ReviewWork,
	latestChildren []model.ReviewWork,
) (DerivationDispositionPlan, error) {
	if group.operationID.IsZero() || len(group.children) == 0 {
		return DerivationDispositionPlan{}, fmt.Errorf("%w: zero group", ErrInvalidDerivation)
	}
	if latestParent.Ref() != group.parentWorkRef || latestParent.ChannelID() != group.parentChannelID {
		return DerivationDispositionPlan{}, fmt.Errorf("%w: latest parent identity changed", ErrDerivationScope)
	}
	if latestParent.Participants() != group.parentParticipants || latestParent.DeadlineUnixNano() != group.parentDeadline {
		return DerivationDispositionPlan{}, fmt.Errorf("%w: immutable parent snapshot changed", ErrDerivationScope)
	}
	if len(latestChildren) != len(group.children) {
		return DerivationDispositionPlan{}, fmt.Errorf("%w: got %d child Works, want %d", ErrDerivationScope, len(latestChildren), len(group.children))
	}

	currentByRef := make(map[string]model.ReviewWork, len(latestChildren))
	for _, child := range latestChildren {
		key := derivationWorkKey(child.Ref())
		if _, exists := currentByRef[key]; exists {
			return DerivationDispositionPlan{}, fmt.Errorf("%w: duplicate current child Work", ErrDerivationScope)
		}
		currentByRef[key] = child
	}

	results := make([]DerivationChildResult, len(group.children))
	allTerminal := true
	for index, childScope := range group.children {
		child, exists := currentByRef[derivationWorkKey(childScope.workRef)]
		if !exists || child.Ref() != childScope.workRef || child.ChannelID() != childScope.channelID {
			return DerivationDispositionPlan{}, fmt.Errorf("%w: child ordinal %d identity changed", ErrDerivationScope, childScope.ordinal)
		}
		results[index] = DerivationChildResult{
			ordinal: childScope.ordinal,
			workRef: child.Ref(),
			state:   child.State(),
			version: child.Version(),
		}
		allTerminal = allTerminal && child.State().Terminal()
	}

	disposition := DerivationPending
	if allTerminal {
		if latestParent.Version() == group.parentVersion &&
			(latestParent.State() == model.WorkActive || latestParent.State() == model.WorkRework) {
			disposition = DerivationResume
		} else {
			disposition = DerivationParentStale
		}
	}
	return DerivationDispositionPlan{
		operationID:         group.operationID,
		disposition:         disposition,
		frozenParentVersion: group.parentVersion,
		latestParentVersion: latestParent.Version(),
		childResults:        results,
	}, nil
}

func derivationWorkKey(ref model.WorkRef) string {
	return ref.HomePeerID().String() + "\x00" + ref.WorkID().String()
}
