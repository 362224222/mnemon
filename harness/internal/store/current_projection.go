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

type freshCurrentAuthority struct {
	event        model.Event
	work         model.ReviewWork
	role         model.CurrentRole
	brief        model.CurrentBrief
	childResults []model.CurrentChildResult
}

func deriveFreshCurrentProjection(ctx context.Context, tx *sql.Tx, handling model.Handling,
	budget model.HandlingBudget, at time.Time,
) (model.CurrentProjection, []AgentCurrentArtifactMaterialization, error) {
	authority, err := readFreshCurrentAuthority(ctx, tx, handling, budget, at)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	parentResume := len(authority.childResults) != 0
	exactUpdate := false
	if !parentResume {
		facts, err := decodeClosedEventPayload(authority.event)
		if err != nil {
			return model.CurrentProjection{}, nil, fmt.Errorf("%w: %v", ErrCurrentReadInvariant, err)
		}
		exactUpdate, err = currentWorkIsExactSource(authority.event, authority.work, facts)
		if err != nil {
			return model.CurrentProjection{}, nil, err
		}
	}
	currentEvent, err := buildCurrentEventProjection(authority.event)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	currentWork, err := buildCurrentWorkProjection(authority)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	actions := deriveCurrentActions(authority.role, authority.event, authority.work, exactUpdate)
	if parentResume {
		actions = deriveParentResumeCurrentActions(authority.role, authority.work)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{
		SourceEvent: currentEvent, ActionWork: currentWork,
		AllowedActions: actions, ChildResults: authority.childResults,
	})
	if errors.Is(err, model.ErrLimit) {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: %v", ErrCurrentReadTooLarge, err)
	}
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: projection: %v", ErrCurrentReadInvariant, err)
	}
	if err := requireCurrentProjectionBudget(projection, budget); err != nil {
		return model.CurrentProjection{}, nil, err
	}
	artifacts, err := currentArtifactMaterializationPlan(ctx, tx, projection.ArtifactRefs())
	return projection, artifacts, err
}

func readFreshCurrentAuthority(ctx context.Context, tx *sql.Tx, handling model.Handling,
	budget model.HandlingBudget, at time.Time,
) (freshCurrentAuthority, error) {
	event, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return freshCurrentAuthority{}, fmt.Errorf("%w: source Event: %v", ErrCurrentReadInvariant, err)
	}
	parentResume, err := handlingIsParentResume(ctx, tx, handling, event)
	if err != nil {
		return freshCurrentAuthority{}, fmt.Errorf("%w: inspect derivation: %v", ErrCurrentReadInvariant, err)
	}
	if parentResume {
		return readParentResumeCurrentAuthority(ctx, tx, handling, event, budget, at)
	}
	work, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return freshCurrentAuthority{}, fmt.Errorf("%w: action Work: %v", ErrCurrentReadInvariant, err)
	}
	if work.ChannelID() != event.Scope().ChannelID() {
		return freshCurrentAuthority{}, fmt.Errorf("%w: source Event and action Work Channels differ", ErrCurrentReadInvariant)
	}
	if at.Before(event.AcceptedAt()) || at.Before(work.UpdatedAt()) {
		return freshCurrentAuthority{}, fmt.Errorf("%w: trusted time precedes projected Event or Work evidence", ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return freshCurrentAuthority{}, fmt.Errorf("%w: local Node: %v", ErrCurrentReadInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	brief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	if event.Source() == model.EventSourceImported && !event.Audience().Contains(node.PeerID()) {
		return freshCurrentAuthority{}, fmt.Errorf("%w: imported source Event does not address the local Node", ErrCurrentReadInvariant)
	}
	if len(event.Artifacts()) > budget.Spec().MaxCurrentArtifactRefs {
		return freshCurrentAuthority{}, fmt.Errorf("%w: source Event has %d Artifact refs, Profile budget is %d",
			ErrCurrentReadTooLarge, len(event.Artifacts()), budget.Spec().MaxCurrentArtifactRefs)
	}
	if err := requireCurrentArtifacts(ctx, tx, event); err != nil {
		return freshCurrentAuthority{}, err
	}
	return freshCurrentAuthority{event: event, work: work, role: role, brief: brief}, nil
}

func buildCurrentEventProjection(event model.Event) (model.CurrentEvent, error) {
	artifacts := make([]model.CurrentArtifactRef, len(event.Artifacts()))
	for index, ref := range event.Artifacts() {
		var err error
		artifacts[index], err = model.NewCurrentArtifactRef(ref.RootDigest())
		if err != nil {
			return model.CurrentEvent{}, fmt.Errorf("%w: Artifact projection: %v", ErrCurrentReadInvariant, err)
		}
	}
	current, err := model.NewCurrentEvent(model.CurrentEventSpec{
		Key: event.Key(), Digest: event.Digest(), Type: event.Type(), WorkRef: event.Scope().WorkRef(),
		Summary: event.Summary(), Payload: event.Payload(), ArtifactRefs: artifacts, AcceptedAt: event.AcceptedAt(),
	})
	if err != nil {
		return model.CurrentEvent{}, fmt.Errorf("%w: Event projection: %v", ErrCurrentReadInvariant, err)
	}
	return current, nil
}

func buildCurrentWorkProjection(authority freshCurrentAuthority) (model.CurrentWork, error) {
	work := authority.work
	current, err := model.NewCurrentWork(model.CurrentWorkSpec{
		Ref: work.Ref(), Version: work.Version(), Iteration: work.Iteration(),
		DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(), StateData: work.StateData(),
		LocalRole: authority.role, Brief: authority.brief,
	})
	if err != nil {
		return model.CurrentWork{}, fmt.Errorf("%w: Work projection: %v", ErrCurrentReadInvariant, err)
	}
	return current, nil
}

func validateStoredCurrentAuthority(ctx context.Context, tx *sql.Tx,
	receipt model.CurrentReadReceipt, run model.AgentRun, handling model.Handling,
	budget model.HandlingBudget,
) ([]AgentCurrentArtifactMaterialization, error) {
	event, err := validateStoredCurrentEvent(ctx, tx, receipt.Projection(), handling)
	if err != nil {
		return nil, err
	}
	if err := validateStoredCurrentWork(ctx, tx, receipt, event, budget); err != nil {
		return nil, err
	}
	projection := receipt.Projection()
	if err := requireCurrentProjectionBudget(projection, budget); err != nil {
		return nil, err
	}
	artifacts, err := currentArtifactMaterializationPlan(ctx, tx, projection.ArtifactRefs())
	if err != nil {
		return nil, err
	}
	storedViews, err := requireExactCurrentArtifactViews(run.ID(), artifacts, receipt.ArtifactRefs(),
		budget.Spec().MaxCurrentPathBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: stored receipt Artifact mapping: %v", ErrCurrentReadInvariant, err)
	}
	if !sameCurrentArtifactViews(storedViews, receipt.ArtifactRefs()) {
		return nil, fmt.Errorf("%w: stored receipt Artifact mapping is not exact", ErrCurrentReadInvariant)
	}
	return artifacts, nil
}

func validateStoredCurrentEvent(ctx context.Context, tx *sql.Tx, projection model.CurrentProjection,
	handling model.Handling,
) (model.Event, error) {
	event, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return model.Event{}, fmt.Errorf("%w: replay source Event: %v", ErrCurrentReadInvariant, err)
	}
	parentResume, err := handlingIsParentResume(ctx, tx, handling, event)
	if err != nil {
		return model.Event{}, fmt.Errorf("%w: inspect replay derivation: %v", ErrCurrentReadInvariant, err)
	}
	projected := projection.SourceEvent()
	if projected.Key() != event.Key() || projected.Digest() != event.Digest() ||
		projected.Type() != event.Type() || projected.WorkRef() != event.Scope().WorkRef() ||
		projected.Summary() != event.Summary() ||
		!bytes.Equal(projected.Payload().Bytes(), event.Payload().Bytes()) ||
		!projected.AcceptedAt().Equal(event.AcceptedAt()) ||
		!sameCurrentEventArtifactRoots(projected.ArtifactRefs(), event.Artifacts()) {
		return model.Event{}, fmt.Errorf("%w: stored projection source Event differs from durable Event",
			ErrCurrentReadInvariant)
	}
	if err := requireCurrentArtifacts(ctx, tx, event); err != nil {
		return model.Event{}, err
	}
	if parentResume && len(projection.ChildResults()) == 0 {
		return model.Event{}, fmt.Errorf("%w: parent-resume receipt omitted child results", ErrCurrentReadInvariant)
	}
	if !parentResume && len(projection.ChildResults()) != 0 {
		return model.Event{}, fmt.Errorf("%w: base current receipt carried child results", ErrCurrentReadInvariant)
	}
	return event, nil
}

func validateStoredCurrentWork(ctx context.Context, tx *sql.Tx, receipt model.CurrentReadReceipt,
	event model.Event, budget model.HandlingBudget,
) error {
	work, err := readReviewWork(ctx, tx, receipt.ActionWork())
	if err != nil {
		return fmt.Errorf("%w: replay action Work: %v", ErrCurrentReadInvariant, err)
	}
	if len(receipt.Projection().ChildResults()) != 0 {
		return validateStoredParentResumeCurrentWork(ctx, tx, receipt, event, work, budget)
	}
	if work.ChannelID() != event.Scope().ChannelID() || work.Ref() != event.Scope().WorkRef() {
		return fmt.Errorf("%w: replay source Event and Work authority differ", ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: replay local Node: %v", ErrCurrentReadInvariant, err)
	}
	if event.Source() == model.EventSourceImported && !event.Audience().Contains(node.PeerID()) {
		return fmt.Errorf("%w: replay imported Event does not address the local Node", ErrCurrentReadInvariant)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return err
	}
	brief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return err
	}
	projected := receipt.Projection().ActionWork()
	projectedBrief, ok := projected.Brief()
	if !ok || projected.Ref() != work.Ref() || projected.DeadlineUnixNano() != work.DeadlineUnixNano() ||
		projected.LocalRole() != role || projectedBrief.Content() != brief.Content() ||
		projectedBrief.DeadlineUnixNano() != brief.DeadlineUnixNano() ||
		!sameCurrentArtifactRoots(projectedBrief.ArtifactRefs(), brief.ArtifactRefs()) {
		return fmt.Errorf("%w: stored projection Work brief differs from durable authority", ErrCurrentReadInvariant)
	}
	return nil
}

func deriveCurrentActions(role model.CurrentRole, event model.Event, work model.ReviewWork,
	exactUpdate bool,
) []model.OperationKind {
	if !exactUpdate {
		return currentResolutionActions()
	}
	var domain []model.OperationKind
	switch role {
	case model.CurrentReviewer:
		domain = deriveReviewerCurrentActions(event, work)
	case model.CurrentInitiator:
		domain = deriveInitiatorCurrentActions(event, work)
	}
	if len(domain) == 0 {
		return currentResolutionActions()
	}
	return append(domain, model.OperationResolveRetry)
}

func deriveReviewerCurrentActions(event model.Event, work model.ReviewWork) []model.OperationKind {
	switch {
	case work.State() == model.WorkOffered && event.Type() == model.EventReviewOffered:
		return []model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkDecline}
	case work.State() == model.WorkActive && event.Type() == model.EventReviewAccepted:
		return []model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver}
	case work.State() == model.WorkRework && event.Type() == model.EventReviewReworkRequested:
		return []model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver}
	default:
		return nil
	}
}

func deriveInitiatorCurrentActions(event model.Event, work model.ReviewWork) []model.OperationKind {
	var actions []model.OperationKind
	if work.State() == model.WorkDelivered && event.Type() == model.EventReviewDelivered {
		if work.Iteration() == 1 {
			actions = append(actions, model.OperationTeamworkRework)
		}
		actions = append(actions, model.OperationTeamworkClose)
	}
	if !work.State().Terminal() {
		actions = append(actions, model.OperationTeamworkCancel)
	}
	return actions
}

func currentResolutionActions() []model.OperationKind {
	return []model.OperationKind{
		model.OperationResolveNoAction,
		model.OperationResolveRetry,
		model.OperationResolveReject,
	}
}
