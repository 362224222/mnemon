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
	Handler     ActionHandler
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
	PublishAccepted(context.Context, model.OperationID) *ControlError
}

type TeamworkActionExecutorOptions struct {
	Profile   model.Profile
	Actions   ActionHandlers
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
	ReviewWorkUpdateCause(context.Context, model.ReviewWork) (model.EventKey, error)
	Probe(context.Context, store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error)
	Commit(context.Context, executionAcceptanceSpec, time.Time) (store.LocalAcceptanceResult, error)
	ResolveDeadline(context.Context, executionDeadlineSpec, time.Time) (store.DeadlineResolutionResult, error)
	Reject(context.Context, model.OperationID, string, time.Time, model.JSON) (store.OperationRejectionResult, error)
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

type currentActionTransitionPlan struct {
	transition teamwork.TransitionIntent
	deadline   *executionDeadlineAuthority
	due        bool
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

func (b storeTeamworkExecutionBackend) ReviewWorkUpdateCause(ctx context.Context,
	work model.ReviewWork,
) (model.EventKey, error) {
	return b.store.ReviewWorkUpdateCause(ctx, work)
}

func (b storeTeamworkExecutionBackend) Probe(ctx context.Context,
	spec store.ManagedOperationProbeSpec,
) (store.ManagedOperationProbe, error) {
	return b.store.ProbeManagedOperation(ctx, spec)
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
) (store.OperationRejectionResult, error) {
	return b.store.RejectOperation(ctx, id, owner, at, result)
}

type TeamworkActionExecutor struct {
	backend   teamworkExecutionBackend
	selector  offerSelectionResolver
	profile   model.Profile
	actions   ActionHandlers
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
		options.Clock == nil || options.Profile.ID() != model.TeamworkProfileID() || !options.Profile.Enabled() ||
		options.Actions.AssetRevision().String() != options.Profile.ActiveAssetRevision() {
		return nil, errors.New("Teamwork action executor requires backend, selector, Profile, Action handlers, signer and Artifact coordinator")
	}
	return &TeamworkActionExecutor{backend: backend, selector: selector, profile: options.Profile,
		actions: options.Actions, signer: options.Signer, artifacts: options.Artifacts, clock: options.Clock}, nil
}

func (e *TeamworkActionExecutor) ExecuteTeamwork(ctx context.Context,
	spec TeamworkExecutionSpec,
) (OperationResponse, *ControlError) {
	if e == nil || ctx == nil || e.actions.AssetRevision().IsZero() {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork action executor is unavailable")
	}
	operation := spec.Reservation.Operation
	if operation.ID().IsZero() || spec.Action.Name == "" {
		return OperationResponse{}, NewControlError(CodeInvalidArgument,
			"Teamwork execution input is incomplete")
	}
	if response, apiErr, terminal := e.replayTerminalTeamworkOperation(
		ctx, spec.Action, operation); terminal {
		return response, apiErr
	}
	if operation.Status() != model.OperationStarted {
		return OperationResponse{}, NewControlError(CodeInternal,
			"reserved operation has an invalid state")
	}
	if !e.available() {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork action executor is unavailable")
	}
	acceptedAt, err := canonicalExecutionTime(spec.At)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInvalidArgument,
			"Teamwork execution input is incomplete")
	}
	spec.At = acceptedAt
	if !e.matchesTeamworkOperation(spec.Action, operation) {
		return e.reject(ctx, operation, spec.At, NewControlError(CodeOperationMismatch,
			"reserved operation differs from Teamwork action"))
	}
	if !spec.Reservation.Acquired {
		return OperationResponse{}, operationAPIError(CodeOperationPending,
			"operation lease is not acquired", operation.ID(), false)
	}

	current, hasCurrent, apiErr := executionCurrent(spec.Reservation, spec.At)
	if apiErr != nil {
		return e.reject(ctx, operation, spec.At, apiErr)
	}
	selection, artifactRefs, apiErr := e.prepareActionInputs(ctx, spec, current, hasCurrent)
	if apiErr != nil {
		return e.reject(ctx, operation, spec.At, apiErr)
	}

	var acceptance executionAcceptanceSpec
	if spec.Action.handler.mechanic.actor == actionActorOffer {
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
	return e.projectFreshCommit(ctx, operation, spec.Action.handler,
		artifactRefs, result)
}

func (e *TeamworkActionExecutor) available() bool {
	return e != nil && e.backend != nil && e.selector != nil && e.signer != nil &&
		e.artifacts != nil && e.clock != nil && !e.actions.AssetRevision().IsZero()
}

func (e *TeamworkActionExecutor) prepareActionInputs(ctx context.Context, spec TeamworkExecutionSpec,
	current model.CurrentReadReceipt, hasCurrent bool,
) (AgentOfferSelection, []model.ArtifactRef, *ControlError) {
	selection := AgentOfferSelection{}
	if spec.Action.handler.mechanic.actor == actionActorOffer {
		resolved, err := e.selector.Resolve(ctx, AgentOfferSelectionSpec{Profile: e.profile,
			ChannelAlias: spec.Action.ChannelAlias, ParticipantSelector: spec.Action.Participant, At: spec.At})
		if err != nil {
			return AgentOfferSelection{}, nil, mapOfferSelectionError(err)
		}
		selection = resolved
	}
	coordination, apiErr := e.artifacts.Coordinate(ctx, ArtifactCoordinationSpec{
		Reservation: spec.Reservation, Profile: e.profile, Handler: spec.Action.handler,
		Paths: append([]string(nil), spec.Action.ArtifactPaths...), Current: current,
		HasCurrent: hasCurrent, At: spec.At,
	})
	if apiErr != nil {
		return AgentOfferSelection{}, nil, apiErr
	}
	refs, err := validateExecutionArtifactRefs(spec.Action.handler.Descriptor().Artifacts(),
		coordination.References)
	if err != nil {
		return AgentOfferSelection{}, nil, NewControlError(CodeArtifactInvalid,
			"Artifact coordinator returned invalid authority")
	}
	return selection, refs, nil
}

func (e *TeamworkActionExecutor) buildOffer(ctx context.Context, spec TeamworkExecutionSpec,
	selection AgentOfferSelection, current model.CurrentReadReceipt, hasCurrent bool,
	artifacts []model.ArtifactRef,
) (executionAcceptanceSpec, *ControlError) {
	reviewer := selection.Reviewer()
	audience, err := model.NewAudience([]model.PeerID{reviewer.PeerID()})
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"selected Teamwork audience is invalid")
	}
	scope, err := e.backend.Prepare(ctx, selection.ChannelID(), audience, 1)
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
		ReviewerPeerID: reviewer.PeerID(), AcceptedAt: spec.At, Deadline: spec.Action.Deadline})
	if err != nil {
		return executionAcceptanceSpec{}, mapTeamworkExecutionError(err)
	}
	participants := plan.Participants()
	if participants.ReviewerPeerID() != reviewer.PeerID() {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"Teamwork offer plan changed reviewer")
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
	workID, eventID, err := derivedOfferIDs(spec.Reservation.Operation.ID(), 0)
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not derive Teamwork identities")
	}
	workRef, _ := model.NewWorkRef(scope.node.PeerID(), workID)
	eventScope, err := scope.eventScope(0, workRef)
	if err != nil {
		return executionAcceptanceSpec{}, NewControlError(CodeInternal,
			"server could not derive Event scope")
	}
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
		Participants: participants, Version: 1, Iteration: 1,
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
	return executionAcceptanceSpec{scope: scope,
		items:     []store.LocalAcceptanceItem{{Publication: bundle.Publication(), Work: &mutation}},
		operation: localExecutionAuthority(spec.Reservation.Operation)}, nil
}

func (e *TeamworkActionExecutor) buildCurrentAction(ctx context.Context, spec TeamworkExecutionSpec,
	current model.CurrentReadReceipt, hasCurrent bool, artifacts []model.ArtifactRef,
) (executionAcceptanceSpec, *ControlError) {
	if !hasCurrent {
		return executionAcceptanceSpec{}, NewControlError(CodeContextRequired,
			"Teamwork action requires current authority")
	}
	prepared, apiErr := e.prepareCurrentAction(ctx, spec, current)
	if apiErr != nil {
		return executionAcceptanceSpec{}, apiErr
	}
	work, audience, scope := prepared.work, prepared.audience, prepared.scope
	participant := prepared.participant

	requestedType := spec.Action.handler.EventType()
	plan, apiErr := e.planCurrentActionTransition(spec, current, work, scope,
		participant, requestedType)
	if apiErr != nil {
		return executionAcceptanceSpec{}, apiErr
	}
	if plan.due {
		return executionAcceptanceSpec{scope: scope,
			operation: localExecutionAuthority(spec.Reservation.Operation), deadline: plan.deadline}, nil
	}
	bundle, apiErr := e.admitCurrentActionEvent(ctx, spec, current, work, audience, scope,
		artifacts, requestedType)
	if apiErr != nil {
		return executionAcceptanceSpec{}, apiErr
	}
	item, apiErr := buildCurrentActionItem(bundle, work, plan.transition, participant)
	if apiErr != nil {
		return executionAcceptanceSpec{}, apiErr
	}
	return executionAcceptanceSpec{scope: scope, items: []store.LocalAcceptanceItem{item},
		operation: localExecutionAuthority(spec.Reservation.Operation), deadline: plan.deadline}, nil
}

func (e *TeamworkActionExecutor) planCurrentActionTransition(spec TeamworkExecutionSpec,
	current model.CurrentReadReceipt, work model.ReviewWork, scope executionScope,
	participant bool, requestedType model.EventType,
) (currentActionTransitionPlan, *ControlError) {
	if participant {
		return currentActionTransitionPlan{}, nil
	}
	contextHash, hasContext := spec.Reservation.Operation.ContextHash()
	if !hasContext {
		return currentActionTransitionPlan{}, NewControlError(CodeContextInvalid,
			"home Teamwork action lacks operation context")
	}
	deadline := &executionDeadlineAuthority{scope: scope, work: work, current: current,
		operation: localExecutionAuthority(spec.Reservation.Operation), contextHash: contextHash}
	transition, err := teamwork.PlanHomeTransition(teamwork.HomeTransitionSpec{Work: work,
		ActorPeerID: scope.node.PeerID(), ExpectedVersion: current.ActionWorkVersion(),
		EventType: requestedType, NowUnixNano: spec.At.UnixNano()})
	if err != nil {
		return currentActionTransitionPlan{}, mapTeamworkExecutionError(err)
	}
	if transition.DeadlineWon() {
		deadline.due = true
		return currentActionTransitionPlan{transition: transition, deadline: deadline, due: true}, nil
	}
	return currentActionTransitionPlan{transition: transition, deadline: deadline}, nil
}

func (e *TeamworkActionExecutor) admitCurrentActionEvent(ctx context.Context,
	spec TeamworkExecutionSpec, current model.CurrentReadReceipt, work model.ReviewWork,
	audience model.Audience, scope executionScope, artifacts []model.ArtifactRef,
	requestedType model.EventType,
) (event.Bundle, *ControlError) {
	eventID, err := derivedActionEventID(spec.Reservation.Operation.ID())
	if err != nil {
		return event.Bundle{}, NewControlError(CodeInternal,
			"server could not derive action Event identity")
	}
	eventScope, err := scope.eventScope(0, work.Ref())
	if err != nil {
		return event.Bundle{}, NewControlError(CodeInternal,
			"server could not derive action Event scope")
	}
	cause, apiErr := e.currentActionCause(ctx, spec.Action, current, work)
	if apiErr != nil {
		return event.Bundle{}, apiErr
	}
	stamp, err := event.NewAdmissionStamp(event.AdmissionStampSpec{Node: scope.node, Profile: scope.profile,
		EventID: eventID, ChannelID: scope.channelID, WorkRef: work.Ref(),
		OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
		OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(),
		Audience: audience, WorkVersion: work.Version(), Iteration: work.Iteration(),
		WorkDeadlineUnixNano: work.DeadlineUnixNano(), Artifacts: artifacts,
		CausedBy: []model.EventKey{cause}})
	if err != nil {
		return event.Bundle{}, NewControlError(CodeInternal,
			"server could not bind action authority")
	}
	factory, err := event.NewFactory(executionClock{spec.At}, e.signer)
	if err != nil {
		return event.Bundle{}, NewControlError(CodeInternal,
			"Teamwork Event factory is unavailable")
	}
	bundle, err := factory.AdmitAgent(ctx, stamp, spec.Action.Candidate)
	if err != nil || bundle.Event().Type() != requestedType {
		return event.Bundle{}, NewControlError(CodeInternal,
			"server could not admit action Event")
	}
	return bundle, nil
}

func buildCurrentActionItem(bundle event.Bundle, work model.ReviewWork,
	transition teamwork.TransitionIntent, participant bool,
) (store.LocalAcceptanceItem, *ControlError) {
	item := store.LocalAcceptanceItem{Publication: bundle.Publication()}
	if participant {
		return item, nil
	}
	nextSpec := work.Spec()
	nextSpec.Version, nextSpec.Iteration = transition.NextVersion(), transition.NextIteration()
	nextSpec.State, nextSpec.StateData = transition.NextState(), bundle.Event().Payload()
	nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().ID(), bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		return store.LocalAcceptanceItem{}, NewControlError(CodeInternal,
			"server could not project Work transition")
	}
	mutation, err := store.NewWorkTransition(next, work.Version(), work.State())
	if err != nil {
		return store.LocalAcceptanceItem{}, NewControlError(CodeInternal,
			"server could not freeze Work transition")
	}
	item.Work = &mutation
	return item, nil
}

func (e *TeamworkActionExecutor) currentActionCause(ctx context.Context,
	action ValidatedAction, current model.CurrentReadReceipt, work model.ReviewWork,
) (model.EventKey, *ControlError) {
	if action.handler.OperationKind() != model.OperationTeamworkDeliver ||
		len(current.Projection().ChildResults()) == 0 {
		return current.SourceEvent(), nil
	}
	cause, err := e.backend.ReviewWorkUpdateCause(ctx, work)
	if err != nil {
		return model.EventKey{}, mapTeamworkExecutionError(err)
	}
	return cause, nil
}

func validateExecutionArtifactRefs(policy teamwork.ActionArtifactPolicy,
	refs []model.ArtifactRef,
) ([]model.ArtifactRef, error) {
	if uint64(len(refs)) > uint64(policy.MaxRoots()) {
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
	if !policy.Allowed() && len(result) != 0 {
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
	if response, replayErr, handled := e.probeRejectionTerminal(ctx, operation); handled {
		return response, replayErr
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
		if response, replayErr, handled := e.probeRejectionTerminal(ctx, operation); handled {
			return response, replayErr
		}
		return OperationResponse{}, clockErr
	}
	result, err := e.backend.Reject(ctx, operation.ID(), operation.LeaseOwner(), rejectedAt, evidence)
	if err != nil {
		return OperationResponse{}, mapTeamworkExecutionError(err)
	}
	return e.projectRejectionTerminal(ctx, operation, result, evidence)
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

func decodeCommittedTeamworkOperation(handlers ActionHandlers, operation model.Operation,
	replayed bool,
) (OperationResponse, error) {
	receipt, ok := operation.Result()
	if operation.Status() != model.OperationCommitted || !ok {
		return OperationResponse{}, errors.New("operation is not committed")
	}
	handler, supported := handlers.Operation(operation.Kind())
	if !supported {
		return OperationResponse{}, errors.New("unsupported committed Teamwork action")
	}
	return decodeCommittedTeamworkReceipt(handler, receipt, operation, replayed)
}

func decodeCommittedTeamworkReceipt(handler ActionHandler, receipt model.JSON, operation model.Operation,
	replayed bool,
) (OperationResponse, error) {
	receiptPolicy := handler.Descriptor().Receipt()
	var wire struct {
		CaptureRoots []committedTeamworkCaptureRoot `json:"capture_roots"`
		Events       []struct {
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
		len(wire.Events) > int(receiptPolicy.MaxResults()) || wire.CaptureRoots == nil ||
		requireExecutionJSONEOF(decoder) != nil {
		return OperationResponse{}, errors.New("invalid committed Teamwork receipt")
	}
	captured, err := validateCommittedTeamworkCapture(operation, wire.CaptureRoots)
	if err != nil {
		return OperationResponse{}, err
	}
	wantType := handler.EventType()
	if !wantType.Valid() {
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
			_, artifactErr = validateExecutionArtifactRefs(handler.Descriptor().Artifacts(), artifacts)
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
		if handler.mechanic.selection {
			wantWorkID, wantEventID, idErr := derivedOfferIDs(operation.ID(), uint8(index))
			if idErr != nil || workID != wantWorkID || eventID != wantEventID ||
				state != model.WorkOffered || row.Work.Version != 1 {
				return OperationResponse{}, errors.New("committed Teamwork offer identities are invalid")
			}
		} else {
			wantEventID, idErr := derivedActionEventID(operation.ID())
			if idErr != nil || eventID != wantEventID || !handler.mechanic.committedState(state) {
				return OperationResponse{}, errors.New("committed Teamwork action identity is invalid")
			}
		}
		results[index] = OperationResult{EventID: eventID.String(), EventType: row.EventType,
			Work: WorkReceipt{Ref: home.String() + "/" + workID.String(),
				Version: row.Work.Version, State: row.Work.State}}
	}
	var handling *HandlingReceipt
	_, hasContext := operation.ContextHash()
	if receiptPolicy.Handling() == teamwork.ReceiptHandlingCompleted || hasContext {
		handling = &HandlingReceipt{Status: "completed"}
	}
	return OperationResponse{SchemaVersion: model.SchemaVersion, Status: "accepted",
		Action: string(handler.OperationKind()), OperationID: operation.ID().String(), Replayed: replayed,
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

func mapOfferSelectionError(err error) *ControlError {
	var candidates *AgentSelectionCandidatesError
	switch {
	case errors.Is(err, ErrAgentSelectionChannelAmbiguous):
		message := "Channel selector is ambiguous"
		if errors.As(err, &candidates) {
			message = boundedOfferSelectionMessage(message, candidates.Candidates())
		}
		return NewControlError(CodeAmbiguousChannel, message)
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

func sameExecutionProfile(left, right model.Profile) bool {
	return left.ID() == right.ID() && left.Principal() == right.Principal() &&
		left.WorkspaceRoot() == right.WorkspaceRoot() && left.CredentialHash() == right.CredentialHash() &&
		left.Host() == right.Host() && left.Runtime() == right.Runtime() &&
		left.ActiveAssetRevision() == right.ActiveAssetRevision() && left.Enabled() && right.Enabled()
}

type executionClock struct{ at time.Time }

func (clock executionClock) Now() time.Time { return clock.at }

var _ TeamworkExecutor = (*TeamworkActionExecutor)(nil)
