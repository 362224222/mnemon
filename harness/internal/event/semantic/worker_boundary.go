package semantic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (backend durablePeerInboxSemanticWorkerBackend) claim(ctx context.Context, owner string,
	at time.Time,
) (peerInboxSemanticWorkerClaim, bool, error) {
	result, err := backend.store.ClaimPeerInboxSemantic(ctx,
		store.ClaimPeerInboxSemanticSpec{LeaseOwner: owner, At: at})
	if err != nil || !result.Found() {
		return peerInboxSemanticWorkerClaim{}, false, err
	}
	claim := result.Claim()
	return peerInboxSemanticWorkerClaim{value: claim, imported: claim.ImportedEvent(),
		decisionSeed: claim.DecisionSeed(), attempt: claim.Fence().Attempt()}, true, nil
}

func (backend durablePeerInboxSemanticWorkerBackend) retry(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, diagnostic store.PeerInboxSemanticRetryDiagnostic,
	after time.Duration, at time.Time,
) error {
	_, err := backend.store.RetryPeerInboxSemantic(ctx, store.RetryPeerInboxSemanticSpec{
		Fence: claim.value.Fence(), Diagnostic: diagnostic, RetryAfter: after, At: at})
	return err
}

func (backend durablePeerInboxSemanticWorkerBackend) probe(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, at time.Time,
) error {
	return backend.store.ProbePeerInboxSemanticAuthority(ctx,
		store.ProbePeerInboxSemanticAuthoritySpec{Fence: claim.value.Fence(), At: at})
}

func (backend durablePeerInboxSemanticWorkerBackend) prepare(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, count uint8,
) (peerInboxSemanticWorkerAdmission, error) {
	audience, err := model.NewAudience([]model.PeerID{claim.imported.Scope().OriginPeerID()})
	if err != nil {
		return peerInboxSemanticWorkerAdmission{}, err
	}
	scope, err := backend.store.PrepareLocalAdmission(ctx,
		claim.imported.Scope().ChannelID(), audience, count)
	if err != nil {
		return peerInboxSemanticWorkerAdmission{}, err
	}
	return peerInboxSemanticWorkerAdmission{value: scope,
		scope: storePeerInboxSemanticResponseScope{scope}}, nil
}

func (backend durablePeerInboxSemanticWorkerBackend) commit(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, decision peerInboxSemanticWorkerDecision,
	admission peerInboxSemanticWorkerAdmission, responses []model.SignedPublication, at time.Time,
) (peerInboxSemanticWorkerCommit, error) {
	result, err := backend.store.CommitPeerInboxSemantic(ctx, store.CommitPeerInboxSemanticSpec{
		Fence: claim.value.Fence(), Plan: decision.plan,
		Scope: admission.value, Responses: responses}, at)
	return peerInboxSemanticWorkerCommit{changed: result.Changed(), replayed: result.Replayed()}, err
}

func (teamworkPeerInboxSemanticWorkerPlanner) plan(claim peerInboxSemanticWorkerClaim,
	at time.Time,
) (peerInboxSemanticWorkerDecision, error) {
	result, err := PlanPeerInbox(claim.value, at)
	if err != nil {
		return peerInboxSemanticWorkerDecision{}, err
	}
	if diagnostic, retry := result.RetryDiagnostic(); retry {
		return peerInboxSemanticWorkerDecision{decisionAt: at, retry: true,
			diagnostic: diagnostic}, nil
	}
	plan, terminal := result.Plan()
	if !terminal {
		return peerInboxSemanticWorkerDecision{}, errors.New("planner returned neither terminal nor retry")
	}
	intents := plan.Responses()
	responses := make([]peerInboxSemanticWorkerIntent, len(intents))
	for index, intent := range intents {
		responses[index] = peerInboxSemanticWorkerIntent{eventType: intent.EventType(),
			payload: intent.Payload(), cause: intent.Cause()}
	}
	return peerInboxSemanticWorkerDecision{plan: plan, decisionAt: plan.DecisionAt(),
		responses: responses}, nil
}

func (scope storePeerInboxSemanticResponseScope) eventScope(index uint8,
	work model.WorkRef,
) (model.EventScope, error) {
	return scope.value.EventScope(index, work)
}

func (scope storePeerInboxSemanticResponseScope) principal() string {
	return scope.value.Profile().Principal()
}

func assemblePeerInboxSemanticResponses(ctx context.Context, signer PeerInboxSemanticSigner,
	claim peerInboxSemanticWorkerClaim, decision peerInboxSemanticWorkerDecision,
	scope peerInboxSemanticResponseScope,
) ([]model.SignedPublication, error) {
	if ctx == nil || signer == nil || scope == nil || claim.imported.ID().IsZero() ||
		claim.decisionSeed.IsZero() || decision.decisionAt.IsZero() || len(decision.responses) == 0 ||
		len(decision.responses) > 2 {
		return nil, errors.New("incomplete semantic response authority")
	}
	audience, err := model.NewAudience([]model.PeerID{claim.imported.Scope().OriginPeerID()})
	if err != nil {
		return nil, err
	}
	responses := make([]model.SignedPublication, len(decision.responses))
	for index, intent := range decision.responses {
		publication, err := assemblePeerInboxSemanticResponse(ctx, signer, claim, decision,
			scope, audience, intent, uint8(index))
		if err != nil {
			return nil, fmt.Errorf("response %d: %w", index, err)
		}
		responses[index] = publication
	}
	return responses, nil
}

func assemblePeerInboxSemanticResponse(ctx context.Context, signer PeerInboxSemanticSigner,
	claim peerInboxSemanticWorkerClaim, decision peerInboxSemanticWorkerDecision,
	scope peerInboxSemanticResponseScope, audience model.Audience,
	intent peerInboxSemanticWorkerIntent, ordinal uint8,
) (model.SignedPublication, error) {
	eventID, err := store.PeerInboxSemanticResponseEventID(claim.decisionSeed, ordinal)
	if err != nil {
		return model.SignedPublication{}, err
	}
	eventScope, err := scope.eventScope(ordinal, claim.imported.Scope().WorkRef())
	if err != nil {
		return model.SignedPublication{}, err
	}
	artifacts, err := peerInboxSemanticResponseArtifacts(claim.imported, intent)
	if err != nil {
		return model.SignedPublication{}, err
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: eventScope,
		Source: model.EventSourceLocal, ActorPrincipal: scope.principal(), Type: intent.eventType,
		Audience: audience, Summary: store.PeerInboxSemanticResponseSummary(intent.eventType),
		Payload: intent.payload, Artifacts: artifacts, CausedBy: []model.EventKey{intent.cause},
		CreatedAt: decision.decisionAt, AcceptedAt: decision.decisionAt})
	if err != nil {
		return model.SignedPublication{}, err
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		return model.SignedPublication{}, err
	}
	message, err := model.PublicationSigningMessage(eventScope.ChannelID(), body.Digest())
	if err != nil {
		return model.SignedPublication{}, err
	}
	signature, err := signer.Sign(ctx, message)
	if err != nil {
		return model.SignedPublication{}, err
	}
	return model.AttachSignature(body, signature)
}

func peerInboxSemanticResponseArtifacts(imported model.Event,
	intent peerInboxSemanticWorkerIntent,
) ([]model.ArtifactRef, error) {
	if intent.eventType != model.EventReviewDelivered {
		return nil, nil
	}
	if imported.Type() != model.EventReviewDeliveryReady || intent.cause != imported.Key() {
		return nil, errors.New("delivered response lacks delivery-ready authority")
	}
	source := imported.Artifacts()
	result := make([]model.ArtifactRef, len(source))
	for index, ref := range source {
		if ref.Role() != model.ArtifactProduced && ref.Role() != model.ArtifactReferenced {
			return nil, errors.New("delivery-ready Artifact has an unauthorized role")
		}
		converted, err := model.NewArtifactRef(ref.RootDigest(), model.ArtifactReferenced)
		if err != nil {
			return nil, err
		}
		result[index] = converted
	}
	return result, nil
}
