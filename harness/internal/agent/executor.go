package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

// ArtifactCoordinationSpec carries server-owned operation/current authority to
// the path resolver. The coordinator, not the Agent, assigns produced versus
// referenced roles and checkpoints any produced closure before returning.
type ArtifactCoordinationSpec struct {
	Reservation store.ManagedOperationReservation
	Profile     model.Profile
	Action      model.OperationKind
	Paths       []string
	Current     model.CurrentReadReceipt
	HasCurrent  bool
	At          time.Time
}

type ArtifactCoordinationResult struct {
	References []model.ArtifactRef
}

type ArtifactCoordinator interface {
	Coordinate(context.Context, ArtifactCoordinationSpec) (ArtifactCoordinationResult, *ControlError)
}

type TeamworkActionExecutorOptions struct {
	Profile   model.Profile
	Signer    event.PublicationSigner
	Artifacts ArtifactCoordinator
	Clock     ServiceClock
}

type offerSelectionResolver interface {
	Resolve(context.Context, AgentOfferSelectionSpec) (AgentOfferSelection, error)
}

type teamworkExecutionBackend interface {
	Prepare(context.Context, model.ChannelID, model.Audience, uint8) (executionScope, error)
	GetReviewWork(context.Context, model.WorkRef) (model.ReviewWork, error)
	Commit(context.Context, executionAcceptanceSpec, time.Time) (store.LocalAcceptanceResult, error)
	ResolveDeadline(context.Context, executionDeadlineSpec, time.Time) (store.DeadlineResolutionResult, error)
	Reject(context.Context, model.OperationID, string, time.Time, model.JSON) (model.Operation, error)
}

type executionScope struct {
	node                 model.Node
	profile              model.Profile
	channelID            model.ChannelID
	originMember         model.RecordHead
	publicationRoster    model.RecordHead
	firstOriginSequence  uint64
	firstChannelSequence uint64
	count                uint8
	durable              store.LocalAdmissionScope
	hasDurable           bool
}

func (s executionScope) eventScope(index uint8, work model.WorkRef) (model.EventScope, error) {
	if index >= s.count {
		return model.EventScope{}, fmt.Errorf("execution admission index %d outside %d", index, s.count)
	}
	return model.NewEventScope(s.channelID, s.node.PeerID(), s.node.OriginEpoch(),
		s.firstOriginSequence+uint64(index), s.firstChannelSequence+uint64(index),
		s.originMember, s.publicationRoster, work)
}

type executionAcceptanceSpec struct {
	scope     executionScope
	items     []store.LocalAcceptanceItem
	operation store.LocalOperationAuthority
	deadline  *executionDeadlineAuthority
}

type executionDeadlineAuthority struct {
	scope       executionScope
	work        model.ReviewWork
	current     model.CurrentReadReceipt
	operation   store.LocalOperationAuthority
	contextHash model.Digest
	due         bool
}

type executionDeadlineSpec struct {
	scope       executionScope
	expiry      store.LocalAcceptanceItem
	operation   store.LocalOperationAuthority
	contextHash model.Digest
}

type storeTeamworkExecutionBackend struct{ store *store.Store }

func (b storeTeamworkExecutionBackend) Prepare(ctx context.Context, channel model.ChannelID,
	audience model.Audience, count uint8,
) (executionScope, error) {
	scope, err := b.store.PrepareLocalAdmission(ctx, channel, audience, count)
	if err != nil {
		return executionScope{}, err
	}
	return executionScope{node: scope.Node(), profile: scope.Profile(), channelID: scope.ChannelID(),
		originMember: scope.OriginMember(), publicationRoster: scope.PublicationRoster(),
		firstOriginSequence: scope.FirstOriginSequence(), firstChannelSequence: scope.FirstChannelSequence(),
		count: scope.Count(), durable: scope, hasDurable: true}, nil
}

func (b storeTeamworkExecutionBackend) GetReviewWork(ctx context.Context,
	ref model.WorkRef,
) (model.ReviewWork, error) {
	return b.store.GetReviewWork(ctx, ref)
}

func (b storeTeamworkExecutionBackend) Commit(ctx context.Context, spec executionAcceptanceSpec,
	at time.Time,
) (store.LocalAcceptanceResult, error) {
	if !spec.scope.hasDurable {
		return store.LocalAcceptanceResult{}, errors.New("Teamwork execution lacks durable admission scope")
	}
	return b.store.CommitManagedAcceptance(ctx, store.ManagedAcceptanceSpec{
		Scope: spec.scope.durable, Items: spec.items, Operation: spec.operation,
	}, at)
}

func (b storeTeamworkExecutionBackend) ResolveDeadline(ctx context.Context, spec executionDeadlineSpec,
	at time.Time,
) (store.DeadlineResolutionResult, error) {
	if !spec.scope.hasDurable {
		return store.DeadlineResolutionResult{}, errors.New("Teamwork deadline lacks durable admission scope")
	}
	return b.store.ResolveDeadlineWinner(ctx, store.DeadlineResolutionSpec{Scope: spec.scope.durable,
		Expiry: spec.expiry, Action: spec.operation, ContextHash: spec.contextHash}, at)
}

func (b storeTeamworkExecutionBackend) Reject(ctx context.Context, id model.OperationID,
	owner string, at time.Time, result model.JSON,
) (model.Operation, error) {
	return b.store.RejectOperation(ctx, id, owner, at, result)
}

type TeamworkActionExecutor struct {
	backend   teamworkExecutionBackend
	selector  offerSelectionResolver
	profile   model.Profile
	signer    event.PublicationSigner
	artifacts ArtifactCoordinator
	clock     ServiceClock
}

func NewTeamworkActionExecutor(st *store.Store,
	options TeamworkActionExecutorOptions,
) (*TeamworkActionExecutor, error) {
	if st == nil {
		return nil, errors.New("Teamwork action executor requires Store")
	}
	selector, err := NewOfferSelector(st)
	if err != nil {
		return nil, err
	}
	return newTeamworkActionExecutor(storeTeamworkExecutionBackend{store: st}, selector, options)
}

func newTeamworkActionExecutor(backend teamworkExecutionBackend, selector offerSelectionResolver,
	options TeamworkActionExecutorOptions,
) (*TeamworkActionExecutor, error) {
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if backend == nil || selector == nil || options.Signer == nil || options.Artifacts == nil ||
		options.Clock == nil || options.Profile.ID() != model.TeamworkProfileID() || !options.Profile.Enabled() {
		return nil, errors.New("Teamwork action executor requires backend, selector, Profile, signer and Artifact coordinator")
	}
	return &TeamworkActionExecutor{backend: backend, selector: selector, profile: options.Profile,
		signer: options.Signer, artifacts: options.Artifacts, clock: options.Clock}, nil
}

func (e *TeamworkActionExecutor) ExecuteTeamwork(ctx context.Context,
	spec TeamworkExecutionSpec,
) (OperationResponse, *ControlError) {
	if e == nil || e.backend == nil || e.selector == nil || e.signer == nil || e.artifacts == nil ||
		e.clock == nil || ctx == nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork action executor is unavailable")
	}
	operation := spec.Reservation.Operation
	acceptedAt, err := canonicalExecutionTime(spec.At)
	if operation.ID().IsZero() || spec.Action.Name == "" || err != nil {
		return OperationResponse{}, NewControlError(CodeInvalidArgument,
			"Teamwork execution input is incomplete")
	}
	spec.At = acceptedAt
	_, contextBound := operation.ContextHash()
	if operation.ProfileID() != e.profile.ID() || operation.Kind() != operationKindForAction(spec.Action.Name) ||
		spec.Action.HasContext != contextBound {
		return e.reject(ctx, operation, spec.At, NewControlError(CodeOperationMismatch,
			"reserved operation differs from Teamwork action"))
	}

	switch operation.Status() {
	case model.OperationCommitted:
		response, err := decodeCommittedTeamworkOperation(operation, true)
		if err != nil {
			return OperationResponse{}, NewControlError(CodeInternal,
				"committed Teamwork receipt is invalid")
		}
		return response, nil
	case model.OperationRejected:
		return OperationResponse{}, decodeRejectedTeamworkOperation(operation, true)
	case model.OperationStarted:
		if !spec.Reservation.Acquired {
			return OperationResponse{}, operationAPIError(CodeOperationPending,
				"operation lease is not acquired", operation.ID(), false)
		}
	default:
		return OperationResponse{}, NewControlError(CodeInternal,
			"reserved operation has an invalid state")
	}

	current, hasCurrent, apiErr := executionCurrent(spec.Reservation, spec.At)
	if apiErr != nil {
		return e.reject(ctx, operation, spec.At, apiErr)
	}
	selection := AgentOfferSelection{}
	if operation.Kind() == model.OperationTeamworkOffer {
		var err error
		selection, err = e.selector.Resolve(ctx, AgentOfferSelectionSpec{Profile: e.profile,
			ChannelAlias: spec.Action.ChannelAlias, ParticipantSelector: spec.Action.Participant, At: spec.At})
		if err != nil {
			return e.reject(ctx, operation, spec.At, mapOfferSelectionError(err))
		}
	}
	coordination, apiErr := e.artifacts.Coordinate(ctx, ArtifactCoordinationSpec{
		Reservation: spec.Reservation, Profile: e.profile, Action: operation.Kind(),
		Paths: append([]string(nil), spec.Action.ArtifactPaths...), Current: current,
		HasCurrent: hasCurrent, At: spec.At,
	})
	if apiErr != nil {
		return e.reject(ctx, operation, spec.At, apiErr)
	}
	artifactRefs, err := validateExecutionArtifactRefs(operation.Kind(), coordination.References)
	if err != nil {
		return e.reject(ctx, operation, spec.At, NewControlError(CodeArtifactInvalid,
			"Artifact coordinator returned invalid authority"))
	}

	var acceptance executionAcceptanceSpec
	if operation.Kind() == model.OperationTeamworkOffer {
		acceptance, apiErr = e.buildOffer(ctx, spec, selection, current, hasCurrent, artifactRefs)
	} else {
		acceptance, apiErr = e.buildCurrentAction(ctx, spec, current, hasCurrent, artifactRefs)
	}
	if apiErr != nil {
		return e.reject(ctx, operation, spec.At, apiErr)
	}
	commitAt, apiErr := e.freshNow(spec.At)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if acceptance.deadline != nil && (acceptance.deadline.due ||
		(acceptance.deadline.work.State().DeadlineEligible() &&
			commitAt.UnixNano() >= acceptance.deadline.work.DeadlineUnixNano())) {
		return e.resolveDeadline(ctx, *acceptance.deadline, commitAt)
	}
	result, err := e.backend.Commit(ctx, acceptance, commitAt)
	if err != nil {
		if acceptance.deadline != nil && errors.Is(err, store.ErrDeadlineResolution) {
			resolvedAt, clockErr := e.freshNow(commitAt)
			if clockErr != nil {
				return OperationResponse{}, clockErr
			}
			return e.resolveDeadline(ctx, *acceptance.deadline, resolvedAt)
		}
		return e.reject(ctx, operation, spec.At, mapTeamworkExecutionError(err))
	}
	response, err := decodeCommittedTeamworkReceipt(result.Receipt, operation, result.Replayed)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"committed Teamwork receipt cannot be projected")
	}
	return response, nil
}

func (e *TeamworkActionExecutor) buildOffer(ctx context.Context, spec TeamworkExecutionSpec,
	selection AgentOfferSelection, current model.CurrentReadReceipt, hasCurrent bool,
	artifacts []model.ArtifactRef,
) (executionAcceptanceSpec, *ControlError) {
	reviewers := selection.Reviewers()
	reviewerIDs := make([]model.PeerID, len(reviewers))
	for index := range reviewers {
		reviewerIDs[index] = reviewers[index].PeerID()
	}
	allAudience, err := model.NewAudience(reviewerIDs)
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"selected Teamwork audience is invalid")
	}
	scope, err := e.backend.Prepare(ctx, selection.ChannelID(), allAudience, uint8(len(reviewers)))
	if err != nil {
		return executionAcceptanceSpec{}, mapTeamworkExecutionError(err)
	}
	if scope.channelID != selection.ChannelID() || scope.publicationRoster != selection.RosterHead() ||
		!sameExecutionProfile(scope.profile, e.profile) {
		return executionAcceptanceSpec{}, NewControlError(CodeWorkConflict,
			"selected Channel changed before admission")
	}
	plan, err := teamwork.PlanOffer(teamwork.OfferPlanSpec{ChannelID: scope.channelID,
		RosterRevision: scope.publicationRoster.Revision(), HomePeerID: scope.node.PeerID(),
		ReviewerPeerIDs: reviewerIDs, AcceptedAt: spec.At, Deadline: spec.Action.Deadline})
	if err != nil {
		return executionAcceptanceSpec{}, mapTeamworkExecutionError(err)
	}
	planned := plan.Offers()
	if len(planned) != len(reviewers) {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"Teamwork offer plan changed reviewer count")
	}

	causes := []model.EventKey(nil)
	if hasCurrent {
		causes = []model.EventKey{current.SourceEvent()}
	}
	factory, err := event.NewFactory(executionClock{spec.At}, e.signer)
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"Teamwork Event factory is unavailable")
	}
	items := make([]store.LocalAcceptanceItem, len(planned))
	for index, offer := range planned {
		if offer.Ordinal() != uint8(index) || offer.Participants().ReviewerPeerID() != reviewers[index].PeerID() {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"Teamwork offer plan changed canonical reviewer order")
		}
		workID, eventID, err := derivedOfferIDs(spec.Reservation.Operation.ID(), uint8(index))
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not derive Teamwork identities")
		}
		workRef, _ := model.NewWorkRef(scope.node.PeerID(), workID)
		eventScope, err := scope.eventScope(uint8(index), workRef)
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not derive Event scope")
		}
		audience, _ := model.NewAudience([]model.PeerID{offer.Participants().ReviewerPeerID()})
		stamp, err := event.NewAdmissionStamp(event.AdmissionStampSpec{Node: scope.node, Profile: scope.profile,
			EventID: eventID, ChannelID: scope.channelID, WorkRef: workRef,
			OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
			OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(),
			Audience: audience, WorkVersion: 1, Iteration: 1, Artifacts: artifacts, CausedBy: causes})
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not bind offer authority")
		}
		bundle, err := factory.AdmitAgent(ctx, stamp, spec.Action.Candidate)
		if err != nil || bundle.WorkDeadlineUnixNano() != plan.DeadlineUnixNano() {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not admit offer Event")
		}
		work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: workRef, ChannelID: scope.channelID,
			Participants: offer.Participants(), Version: 1, Iteration: 1,
			DeadlineUnixNano: bundle.WorkDeadlineUnixNano(), State: model.WorkOffered,
			StateData: bundle.Event().Payload(), UpdatedBy: bundle.Event().ID(), UpdatedAt: bundle.Event().AcceptedAt()})
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not create offered Work")
		}
		mutation, err := store.NewWorkCreation(work)
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not freeze offered Work")
		}
		items[index] = store.LocalAcceptanceItem{Publication: bundle.Publication(), Work: &mutation}
	}
	return executionAcceptanceSpec{scope: scope, items: items,
		operation: localExecutionAuthority(spec.Reservation.Operation)}, nil
}

func (e *TeamworkActionExecutor) buildCurrentAction(ctx context.Context, spec TeamworkExecutionSpec,
	current model.CurrentReadReceipt, hasCurrent bool, artifacts []model.ArtifactRef,
) (executionAcceptanceSpec, *ControlError) {
	if !hasCurrent {
		return executionAcceptanceSpec{}, NewControlError(CodeContextRequired,
			"Teamwork action requires current authority")
	}
	work, err := e.backend.GetReviewWork(ctx, current.ActionWork())
	if err != nil || work.Version() != current.ActionWorkVersion() {
		return executionAcceptanceSpec{}, NewControlError(CodeWorkConflict,
			"current Work changed before admission")
	}
	participant := spec.Reservation.Operation.Kind() == model.OperationTeamworkAccept ||
		spec.Reservation.Operation.Kind() == model.OperationTeamworkDecline ||
		spec.Reservation.Operation.Kind() == model.OperationTeamworkDeliver
	localActor := work.Ref().HomePeerID()
	target := work.Participants().ReviewerPeerID()
	if participant {
		localActor, target = work.Participants().ReviewerPeerID(), work.Ref().HomePeerID()
	}
	audience, err := model.NewAudience([]model.PeerID{target})
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"current Work audience is invalid")
	}
	scope, err := e.backend.Prepare(ctx, work.ChannelID(), audience, 1)
	if err != nil {
		return executionAcceptanceSpec{}, mapTeamworkExecutionError(err)
	}
	if scope.node.PeerID() != localActor || scope.channelID != work.ChannelID() ||
		!sameExecutionProfile(scope.profile, e.profile) {
		return executionAcceptanceSpec{}, NewControlError(CodeActionNotAllowed,
			"local Node is not the frozen Work participant")
	}

	requestedType := operationEventType(spec.Reservation.Operation.Kind())
	var transition teamwork.TransitionIntent
	var deadline *executionDeadlineAuthority
	if !participant {
		contextHash, hasContext := spec.Reservation.Operation.ContextHash()
		if !hasContext {
			return executionAcceptanceSpec{}, NewControlError(CodeContextInvalid,
				"home Teamwork action lacks operation context")
		}
		deadline = &executionDeadlineAuthority{scope: scope, work: work, current: current,
			operation: localExecutionAuthority(spec.Reservation.Operation), contextHash: contextHash}
		transition, err = teamwork.PlanHomeTransition(teamwork.HomeTransitionSpec{Work: work,
			ActorPeerID: scope.node.PeerID(), ExpectedVersion: current.ActionWorkVersion(),
			EventType: requestedType, NowUnixNano: spec.At.UnixNano()})
		if err != nil {
			return executionAcceptanceSpec{}, mapTeamworkExecutionError(err)
		}
		if transition.DeadlineWon() {
			deadline.due = true
			return executionAcceptanceSpec{scope: scope,
				operation: localExecutionAuthority(spec.Reservation.Operation), deadline: deadline}, nil
		}
	}
	eventID, err := derivedActionEventID(spec.Reservation.Operation.ID())
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not derive action Event identity")
	}
	eventScope, err := scope.eventScope(0, work.Ref())
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not derive action Event scope")
	}
	stamp, err := event.NewAdmissionStamp(event.AdmissionStampSpec{Node: scope.node, Profile: scope.profile,
		EventID: eventID, ChannelID: scope.channelID, WorkRef: work.Ref(),
		OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
		OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(),
		Audience: audience, WorkVersion: work.Version(), Iteration: work.Iteration(),
		WorkDeadlineUnixNano: work.DeadlineUnixNano(), Artifacts: artifacts,
		CausedBy: []model.EventKey{current.SourceEvent()}})
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not bind action authority")
	}
	factory, err := event.NewFactory(executionClock{spec.At}, e.signer)
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"Teamwork Event factory is unavailable")
	}
	bundle, err := factory.AdmitAgent(ctx, stamp, spec.Action.Candidate)
	if err != nil || bundle.Event().Type() != requestedType {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not admit action Event")
	}
	item := store.LocalAcceptanceItem{Publication: bundle.Publication()}
	if !participant {
		nextSpec := work.Spec()
		nextSpec.Version, nextSpec.Iteration = transition.NextVersion(), transition.NextIteration()
		nextSpec.State, nextSpec.StateData = transition.NextState(), bundle.Event().Payload()
		nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().ID(), bundle.Event().AcceptedAt()
		next, err := model.NewReviewWork(nextSpec)
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not project Work transition")
		}
		mutation, err := store.NewWorkTransition(next, work.Version(), work.State())
		if err != nil {
			return executionAcceptanceSpec{}, NewControlError(CodeInternal,
				"server could not freeze Work transition")
		}
		item.Work = &mutation
	}
	return executionAcceptanceSpec{scope: scope, items: []store.LocalAcceptanceItem{item},
		operation: localExecutionAuthority(spec.Reservation.Operation), deadline: deadline}, nil
}

func (e *TeamworkActionExecutor) resolveDeadline(ctx context.Context,
	authority executionDeadlineAuthority, at time.Time,
) (OperationResponse, *ControlError) {
	transition, err := teamwork.PlanHomeTransition(teamwork.HomeTransitionSpec{Work: authority.work,
		ActorPeerID: authority.scope.node.PeerID(), ExpectedVersion: authority.current.ActionWorkVersion(),
		EventType: model.EventReviewExpired, NowUnixNano: at.UnixNano()})
	if err != nil || transition.AuthoritativeEventType() != model.EventReviewExpired {
		return OperationResponse{}, NewControlError(CodeWorkConflict,
			"current Work changed before deadline resolution")
	}
	eventID, err := derivedDeadlineEventID(authority.operation.ID)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not derive deadline Event identity")
	}
	eventScope, err := authority.scope.eventScope(0, authority.work.Ref())
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not derive deadline Event scope")
	}
	audience, err := model.NewAudience([]model.PeerID{authority.work.Participants().ReviewerPeerID()})
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"deadline Work audience is invalid")
	}
	stamp, err := event.NewAdmissionStamp(event.AdmissionStampSpec{Node: authority.scope.node,
		Profile: authority.scope.profile, EventID: eventID, ChannelID: authority.scope.channelID,
		WorkRef: authority.work.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: authority.work.Version(), Iteration: authority.work.Iteration(),
		WorkDeadlineUnixNano: authority.work.DeadlineUnixNano(),
		CausedBy:             []model.EventKey{authority.current.SourceEvent()}})
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not bind deadline authority")
	}
	factory, err := event.NewFactory(executionClock{at}, e.signer)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork deadline Event factory is unavailable")
	}
	bundle, err := factory.AdmitController(ctx, stamp, event.ExpiredDecision{})
	if err != nil || bundle.Event().Type() != model.EventReviewExpired ||
		!bundle.Event().AcceptedAt().Equal(at) {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not admit deadline Event")
	}
	nextSpec := authority.work.Spec()
	nextSpec.Version, nextSpec.Iteration = transition.NextVersion(), transition.NextIteration()
	nextSpec.State, nextSpec.StateData = transition.NextState(), bundle.Event().Payload()
	nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().ID(), bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not project expired Work")
	}
	mutation, err := store.NewWorkTransition(next, authority.work.Version(), authority.work.State())
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not freeze expired Work")
	}
	result, err := e.backend.ResolveDeadline(ctx, executionDeadlineSpec{scope: authority.scope,
		expiry:    store.LocalAcceptanceItem{Publication: bundle.Publication(), Work: &mutation},
		operation: authority.operation, contextHash: authority.contextHash}, at)
	if err != nil {
		apiErr := mapTeamworkExecutionError(err)
		return OperationResponse{}, operationAPIError(apiErr.Code, apiErr.Message,
			authority.operation.ID, false)
	}
	apiErr := decodeTeamworkRejectionReceipt(result.Receipt, authority.operation.ID,
		authority.operation.Kind, result.Replayed)
	if apiErr.Code != CodeWorkExpired {
		return OperationResponse{}, operationAPIError(CodeInternal,
			"deadline rejection receipt is invalid", authority.operation.ID, result.Replayed)
	}
	return OperationResponse{}, apiErr
}

func validateExecutionArtifactRefs(kind model.OperationKind,
	refs []model.ArtifactRef,
) ([]model.ArtifactRef, error) {
	if len(refs) > model.MaxArtifactRefs {
		return nil, model.ErrLimit
	}
	result := append([]model.ArtifactRef(nil), refs...)
	sort.Slice(result, func(left, right int) bool {
		return result[left].RootDigest().String() < result[right].RootDigest().String()
	})
	for index, ref := range result {
		if ref.RootDigest().IsZero() || !ref.Role().Valid() ||
			(index > 0 && result[index-1].RootDigest() == ref.RootDigest()) {
			return nil, model.ErrInvalid
		}
	}
	allows := kind == model.OperationTeamworkOffer || kind == model.OperationTeamworkDeliver ||
		kind == model.OperationTeamworkRework
	if !allows && len(result) != 0 {
		return nil, model.ErrInvalid
	}
	return result, nil
}

func (e *TeamworkActionExecutor) reject(ctx context.Context, operation model.Operation, at time.Time,
	apiErr *ControlError,
) (OperationResponse, *ControlError) {
	if apiErr == nil {
		apiErr = NewControlError(CodeInternal, "Teamwork operation was rejected")
	}
	if apiErr.Code.Retryable() {
		return OperationResponse{}, operationAPIError(apiErr.Code, apiErr.Message,
			operation.ID(), false)
	}
	evidence, err := buildTeamworkRejection(operation, apiErr)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork rejection evidence cannot be encoded")
	}
	rejectedAt, clockErr := e.freshNow(at)
	if clockErr != nil {
		return OperationResponse{}, clockErr
	}
	terminal, err := e.backend.Reject(ctx, operation.ID(), operation.LeaseOwner(), rejectedAt, evidence)
	if err != nil {
		return OperationResponse{}, mapTeamworkExecutionError(err)
	}
	switch terminal.Status() {
	case model.OperationCommitted:
		response, err := decodeCommittedTeamworkOperation(terminal, true)
		if err != nil {
			return OperationResponse{}, NewControlError(CodeInternal,
				"terminal Teamwork receipt is invalid")
		}
		return response, nil
	case model.OperationRejected:
		return OperationResponse{}, decodeRejectedTeamworkOperation(terminal, false)
	default:
		return OperationResponse{}, operationAPIError(CodeOperationPending,
			"operation rejection is pending", operation.ID(), false)
	}
}

func (e *TeamworkActionExecutor) freshNow(notBefore time.Time) (time.Time, *ControlError) {
	if e == nil || e.clock == nil {
		return time.Time{}, NewControlError(CodeInternal,
			"trusted Teamwork clock is unavailable")
	}
	now, err := canonicalExecutionTime(e.clock.Now())
	if err != nil || now.Before(notBefore) {
		return time.Time{}, NewControlError(CodeInternal,
			"trusted Teamwork clock is invalid")
	}
	return now, nil
}

func canonicalExecutionTime(value time.Time) (time.Time, error) {
	value = value.Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 || value.UnixNano() <= 0 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("time is outside canonical Unix nanoseconds")
	}
	return value, nil
}

func requireExecutionJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing Teamwork receipt value")
	}
	return nil
}

type teamworkRejectionWire struct {
	SchemaVersion int              `json:"schema_version"`
	Status        string           `json:"status"`
	Code          ControlErrorCode `json:"code"`
	Retryable     bool             `json:"retryable"`
	Replayed      bool             `json:"replayed"`
	Message       string           `json:"message"`
	OperationID   string           `json:"operation_id"`
}

func buildTeamworkRejection(operation model.Operation, apiErr *ControlError) (model.JSON, error) {
	if operation.ID().IsZero() || apiErr == nil || !apiErr.Code.Valid() || apiErr.Message == "" {
		return model.JSON{}, errors.New("incomplete Teamwork rejection")
	}
	return model.JSONFrom(teamworkRejectionWire{SchemaVersion: model.SchemaVersion, Status: "error",
		Code: apiErr.Code, Retryable: apiErr.Code.Retryable(), Replayed: false,
		Message: apiErr.Message, OperationID: operation.ID().String()})
}

func decodeRejectedTeamworkOperation(operation model.Operation, replayed bool) *ControlError {
	result, ok := operation.Result()
	if operation.Status() != model.OperationRejected || !ok {
		return operationAPIError(CodeInternal, "rejected Teamwork receipt is invalid",
			operation.ID(), replayed)
	}
	return decodeTeamworkRejectionReceipt(result, operation.ID(), operation.Kind(), replayed)
}

func decodeTeamworkRejectionReceipt(result model.JSON, operationID model.OperationID,
	kind model.OperationKind, replayed bool,
) *ControlError {
	if result.IsZero() || operationID.IsZero() || !operationEventType(kind).Valid() {
		return operationAPIError(CodeInternal, "rejected Teamwork receipt is invalid",
			operationID, replayed)
	}
	var wire teamworkRejectionWire
	decoder := json.NewDecoder(strings.NewReader(result.String()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire.SchemaVersion != model.SchemaVersion ||
		requireExecutionJSONEOF(decoder) != nil || wire.Status != "error" || wire.Replayed ||
		wire.OperationID != operationID.String() || !wire.Code.Valid() || wire.Message == "" ||
		wire.Retryable != wire.Code.Retryable() {
		return operationAPIError(CodeInternal, "rejected Teamwork receipt is invalid",
			operationID, replayed)
	}
	return operationAPIError(wire.Code, wire.Message, operationID, replayed)
}

func decodeCommittedTeamworkOperation(operation model.Operation,
	replayed bool,
) (OperationResponse, error) {
	receipt, ok := operation.Result()
	if operation.Status() != model.OperationCommitted || !ok {
		return OperationResponse{}, errors.New("operation is not committed")
	}
	return decodeCommittedTeamworkReceipt(receipt, operation, replayed)
}

func decodeCommittedTeamworkReceipt(receipt model.JSON, operation model.Operation,
	replayed bool,
) (OperationResponse, error) {
	var wire struct {
		CaptureRoots []struct {
			ManifestDigest string `json:"manifest_digest"`
			RootDigest     string `json:"root_digest"`
		} `json:"capture_roots"`
		Events []struct {
			ArtifactRoots []struct {
				RootDigest string             `json:"root_digest"`
				Role       model.ArtifactRole `json:"role"`
			} `json:"artifact_roots"`
			EventDigest string `json:"event_digest"`
			EventID     string `json:"event_id"`
			EventType   string `json:"event_type"`
			Work        struct {
				Ref struct {
					HomePeerID string `json:"home_peer_id"`
					WorkID     string `json:"work_id"`
				} `json:"ref"`
				State   string `json:"state"`
				Version uint64 `json:"version"`
			} `json:"work"`
		} `json:"events"`
		OperationID string `json:"operation_id"`
		Status      string `json:"status"`
	}
	decoder := json.NewDecoder(strings.NewReader(receipt.String()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire.Status != "committed" ||
		wire.OperationID != operation.ID().String() || wire.Events == nil || len(wire.Events) == 0 ||
		len(wire.Events) > model.MaxChildWorks || wire.CaptureRoots == nil ||
		requireExecutionJSONEOF(decoder) != nil {
		return OperationResponse{}, errors.New("invalid committed Teamwork receipt")
	}
	captured := make(map[model.Digest]struct{}, len(wire.CaptureRoots))
	previousCapture := ""
	for _, row := range wire.CaptureRoots {
		root, rootErr := model.ParseDigest(row.RootDigest)
		_, manifestErr := model.ParseDigest(row.ManifestDigest)
		if rootErr != nil || manifestErr != nil ||
			(previousCapture != "" && previousCapture >= row.RootDigest) {
			return OperationResponse{}, errors.New("invalid committed Teamwork capture roots")
		}
		previousCapture = row.RootDigest
		captured[root] = struct{}{}
	}
	wantType := operationEventType(operation.Kind())
	if !wantType.Valid() || (operation.Kind() != model.OperationTeamworkOffer && len(wire.Events) != 1) {
		return OperationResponse{}, errors.New("invalid committed Teamwork action shape")
	}
	results := make([]OperationResult, len(wire.Events))
	for index, row := range wire.Events {
		eventID, eventErr := model.ParseEventID(row.EventID)
		eventDigest, digestErr := model.ParseDigest(row.EventDigest)
		home, homeErr := model.ParsePeerID(row.Work.Ref.HomePeerID)
		workID, workErr := model.ParseWorkID(row.Work.Ref.WorkID)
		state := model.WorkState(row.Work.State)
		artifacts, artifactErr := executionReceiptArtifactRefs(row.ArtifactRoots)
		if artifactErr == nil {
			_, artifactErr = validateExecutionArtifactRefs(operation.Kind(), artifacts)
		}
		if eventErr != nil || digestErr != nil || eventDigest.IsZero() || homeErr != nil || workErr != nil ||
			row.EventType != string(wantType) || row.Work.Version == 0 || !state.Valid() ||
			row.ArtifactRoots == nil || artifactErr != nil {
			return OperationResponse{}, errors.New("invalid committed Teamwork result")
		}
		produced := make(map[model.Digest]struct{}, len(captured))
		for _, ref := range artifacts {
			if ref.Role() == model.ArtifactProduced {
				if _, exists := captured[ref.RootDigest()]; !exists {
					return OperationResponse{}, errors.New("committed Teamwork result exceeds capture")
				}
				produced[ref.RootDigest()] = struct{}{}
			} else if _, exists := captured[ref.RootDigest()]; exists {
				return OperationResponse{}, errors.New("committed Teamwork capture role is invalid")
			}
		}
		if len(produced) != len(captured) {
			return OperationResponse{}, errors.New("committed Teamwork result omits capture")
		}
		if operation.Kind() == model.OperationTeamworkOffer {
			wantWorkID, wantEventID, idErr := derivedOfferIDs(operation.ID(), uint8(index))
			if idErr != nil || workID != wantWorkID || eventID != wantEventID ||
				state != model.WorkOffered || row.Work.Version != 1 {
				return OperationResponse{}, errors.New("committed Teamwork offer identities are invalid")
			}
		} else {
			wantEventID, idErr := derivedActionEventID(operation.ID())
			if idErr != nil || eventID != wantEventID || !validCommittedActionState(operation.Kind(), state) {
				return OperationResponse{}, errors.New("committed Teamwork action identity is invalid")
			}
		}
		results[index] = OperationResult{EventID: eventID.String(), EventType: row.EventType,
			Work: WorkReceipt{Ref: home.String() + "/" + workID.String(),
				Version: row.Work.Version, State: row.Work.State}}
	}
	action := teamworkActionName(operation.Kind())
	if action == "" {
		return OperationResponse{}, errors.New("unknown committed Teamwork kind")
	}
	var handling *HandlingReceipt
	if _, hasContext := operation.ContextHash(); hasContext {
		handling = &HandlingReceipt{Status: "completed"}
	}
	return OperationResponse{SchemaVersion: model.SchemaVersion, Status: "accepted",
		Action: "teamwork." + action, OperationID: operation.ID().String(), Replayed: replayed,
		Handling: handling, Results: results, Receipt: model.Sum(receipt.Bytes()).String()}, nil
}

func executionReceiptArtifactRefs(rows []struct {
	RootDigest string             `json:"root_digest"`
	Role       model.ArtifactRole `json:"role"`
}) ([]model.ArtifactRef, error) {
	refs := make([]model.ArtifactRef, len(rows))
	for index, row := range rows {
		root, err := model.ParseDigest(row.RootDigest)
		if err != nil {
			return nil, err
		}
		refs[index], err = model.NewArtifactRef(root, row.Role)
		if err != nil {
			return nil, err
		}
		if index > 0 && refs[index-1].RootDigest().String() >= root.String() {
			return nil, errors.New("Artifact roots are not uniquely ordered")
		}
	}
	return refs, nil
}

func validCommittedActionState(kind model.OperationKind, state model.WorkState) bool {
	switch kind {
	case model.OperationTeamworkAccept, model.OperationTeamworkDecline:
		return state == model.WorkOffered
	case model.OperationTeamworkDeliver:
		return state == model.WorkActive || state == model.WorkRework
	case model.OperationTeamworkRework:
		return state == model.WorkRework
	case model.OperationTeamworkClose:
		return state == model.WorkClosed
	case model.OperationTeamworkCancel:
		return state == model.WorkCancelled
	default:
		return false
	}
}

func mapOfferSelectionError(err error) *ControlError {
	var candidates *AgentSelectionCandidatesError
	switch {
	case errors.Is(err, ErrAgentSelectionChannelAmbiguous):
		message := "Channel selector is ambiguous"
		if errors.As(err, &candidates) {
			message = boundedOfferSelectionMessage(message, candidates.Candidates())
		}
		return NewControlError(CodeAmbiguousChannel, message)
	case errors.Is(err, ErrAgentSelectionParticipantAmbiguous):
		message := "participant selector is ambiguous"
		if errors.As(err, &candidates) {
			message = boundedOfferSelectionMessage(message, candidates.Candidates())
		}
		return NewControlError(CodeAmbiguousParticipant, message)
	case errors.Is(err, ErrAgentSelectionChannelUnavailable),
		errors.Is(err, ErrAgentSelectionParticipantUnavailable):
		return NewControlError(CodePeerUnavailable, "selected Channel participant is unavailable")
	case errors.Is(err, ErrAgentSelectionInput):
		return NewControlError(CodeInvalidArgument, "Teamwork selector is invalid")
	case errors.Is(err, store.ErrAgentOfferCandidatesAuthority):
		return NewControlError(CodeAssetRevisionMismatch,
			"managed Profile authority drifted during selection")
	default:
		return mapTeamworkExecutionError(err)
	}
}

func boundedOfferSelectionMessage(prefix string, candidates []string) string {
	result := prefix
	for index, candidate := range candidates {
		separator := ": "
		if index > 0 {
			separator = ","
		}
		if len(result)+len(separator)+len(candidate) > MaxControlDiagnosticBytes {
			if len(result)+4 <= MaxControlDiagnosticBytes {
				result += ",..."
			}
			break
		}
		result += separator + candidate
	}
	return result
}

func mapTeamworkExecutionError(err error) *ControlError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, store.ErrOperationPending), errors.Is(err, store.ErrOperationFence):
		return NewControlError(CodeOperationPending, "Teamwork operation is pending")
	case errors.Is(err, store.ErrChannelUnavailable), errors.Is(err, store.ErrAudienceUnavailable):
		return NewControlError(CodePeerUnavailable, "selected Channel participant is unavailable")
	case errors.Is(err, store.ErrOperationMismatch), errors.Is(err, store.ErrCaptureMismatch):
		return NewControlError(CodeOperationMismatch, "operation authority differs from request")
	case errors.Is(err, store.ErrArtifactReference), errors.Is(err, store.ErrArtifactUnverified):
		return NewControlError(CodeArtifactInvalid, "Artifact authority changed before admission")
	case errors.Is(err, store.ErrAdmissionConflict), errors.Is(err, store.ErrWorkCASConflict),
		errors.Is(err, teamwork.ErrVersionConflict), errors.Is(err, teamwork.ErrWorkVersionExhausted):
		return NewControlError(CodeWorkConflict, "current Work changed before admission")
	case errors.Is(err, store.ErrDeadlineResolution):
		return NewControlError(CodeWorkExpired, "Work deadline already won")
	case errors.Is(err, teamwork.ErrTransitionNotAllowed), errors.Is(err, teamwork.ErrTerminalWork),
		errors.Is(err, teamwork.ErrNotWorkHome), errors.Is(err, teamwork.ErrParticipantInput):
		return NewControlError(CodeActionNotAllowed, "Teamwork action is not allowed")
	case errors.Is(err, teamwork.ErrDeadlineOutOfRange), errors.Is(err, teamwork.ErrInvalidOffer):
		return NewControlError(CodeInvalidArgument, "Teamwork offer is invalid")
	default:
		return mapControlError(err)
	}
}

func operationAPIError(code ControlErrorCode, message string, operation model.OperationID,
	replayed bool,
) *ControlError {
	apiErr := NewControlError(code, message)
	apiErr.Replayed = replayed
	if !operation.IsZero() {
		value := operation.String()
		apiErr.OperationID = &value
	}
	return apiErr
}

func localExecutionAuthority(operation model.Operation) store.LocalOperationAuthority {
	return store.LocalOperationAuthority{ID: operation.ID(), Kind: operation.Kind(),
		RequestDigest: operation.RequestDigest(), LeaseOwner: operation.LeaseOwner()}
}

func operationEventType(kind model.OperationKind) model.EventType {
	return map[model.OperationKind]model.EventType{
		model.OperationTeamworkOffer:   model.EventReviewOffered,
		model.OperationTeamworkAccept:  model.EventReviewAcceptRequested,
		model.OperationTeamworkDecline: model.EventReviewDeclineRequested,
		model.OperationTeamworkDeliver: model.EventReviewDeliveryReady,
		model.OperationTeamworkRework:  model.EventReviewReworkRequested,
		model.OperationTeamworkClose:   model.EventReviewClosed,
		model.OperationTeamworkCancel:  model.EventReviewCancelled,
	}[kind]
}

func teamworkActionName(kind model.OperationKind) string {
	return strings.TrimPrefix(string(kind), "teamwork.")
}

func sameExecutionProfile(left, right model.Profile) bool {
	return left.ID() == right.ID() && left.Principal() == right.Principal() &&
		left.WorkspaceRoot() == right.WorkspaceRoot() && left.CredentialHash() == right.CredentialHash() &&
		left.Host() == right.Host() && left.Runtime() == right.Runtime() &&
		left.ActiveAssetRevision() == right.ActiveAssetRevision() && left.Enabled() && right.Enabled()
}

type executionClock struct{ at time.Time }

func (clock executionClock) Now() time.Time { return clock.at }

var _ TeamworkExecutor = (*TeamworkActionExecutor)(nil)
