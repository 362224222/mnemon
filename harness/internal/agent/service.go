package agent

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const managedSecretBytes = 32

type ServiceClock interface {
	Now() time.Time
}

type wallServiceClock struct{}

func (wallServiceClock) Now() time.Time { return time.Now() }

type TeamworkExecutionSpec struct {
	Request     TeamworkActionRequest
	Action      ValidatedAction
	Reservation store.ManagedOperationReservation
	At          time.Time
}

type TeamworkExecutor interface {
	ExecuteTeamwork(context.Context, TeamworkExecutionSpec) (OperationResponse, *ControlError)
}

// ActivationGate verifies the mutable, disk-backed authority that cannot be
// frozen safely into a long-lived Service. A nil gate is intentionally
// supported for low-level composition and unit tests.
type ActivationGate interface {
	Check(context.Context, model.Profile) *ControlError
}

// ControlStore is the narrow durable authority surface used by the local Agent
// routes. Keeping this boundary explicit makes it impossible for route
// orchestration to reach around the Store's managed transactions.
type ControlStore interface {
	ProbeAgentClaim(context.Context, store.AgentClaimProbeSpec) (store.AgentClaimStatus, error)
	ClaimAgentCurrent(context.Context, store.AgentClaimSpec) (store.AgentClaimResult, error)
	PeekAgentRunAttachment(context.Context, store.AgentAttachmentSpec) error
	ConsumeAgentRunAttachment(context.Context, store.AgentAttachmentSpec) (store.AgentClaimResult, error)
	PlanAgentCurrentRead(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error)
	FinalizeAgentCurrentRead(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error)
	ReadAgentInitiationContext(context.Context, model.Profile, time.Time) (store.AgentInitiationContext, error)
	ProbeManagedOperation(context.Context, store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error)
	ReserveManagedOperation(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error)
	CommitManagedResolution(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error)
}

type ServiceOptions struct {
	Actions        ActionHandlers
	Clock          ServiceClock
	Random         io.Reader
	OperationLease time.Duration
	Executor       TeamworkExecutor
	CurrentViews   AgentCurrentViews
	ActivationGate ActivationGate
}

// Service implements the four closed Agent routes. It owns only orchestration;
// all durable authority is re-derived by Store transactions.
type Service struct {
	store          ControlStore
	assetRevision  string
	actions        ActionHandlers
	clock          ServiceClock
	random         io.Reader
	randomMu       sync.Mutex
	operationLease time.Duration
	executor       TeamworkExecutor
	currentViews   AgentCurrentViews
	activationGate ActivationGate
}

func NewService(st ControlStore, options ServiceOptions) (*Service, error) {
	if st == nil || options.Actions.AssetRevision().IsZero() || options.CurrentViews == nil {
		return nil, errors.New("Agent service requires Store, Action handlers and current view coordinator")
	}
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if options.Random == nil {
		options.Random = cryptorand.Reader
	}
	if options.OperationLease == 0 {
		options.OperationLease = 2 * time.Minute
	}
	if options.OperationLease < 5*time.Second || options.OperationLease > 10*time.Minute {
		return nil, errors.New("Agent service operation lease must be 5s..10m")
	}
	return &Service{store: st, assetRevision: options.Actions.AssetRevision().String(),
		actions: options.Actions, clock: options.Clock,
		random: options.Random, operationLease: options.OperationLease, executor: options.Executor,
		currentViews: options.CurrentViews, activationGate: options.ActivationGate}, nil
}

func (s *Service) HookCheck(ctx context.Context,
	metadata ControlMetadata,
) (HookCheckResponse, *ControlError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	if metadata.HasRunAttachment {
		err := s.store.PeekAgentRunAttachment(ctx, store.AgentAttachmentSpec{
			ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
			AttachmentTokenHash: metadata.RunAttachmentHash, At: at,
		})
		if err != nil {
			return HookCheckResponse{}, mapControlError(err)
		}
		return HookCheckResponse{Pending: true}, nil
	}
	status, err := s.store.ProbeAgentClaim(ctx, store.AgentClaimProbeSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision, At: at,
	})
	if err != nil {
		return HookCheckResponse{}, mapControlError(err)
	}
	return HookCheckResponse{Pending: status == store.AgentClaimActionable}, nil
}

func (s *Service) AgentCurrent(ctx context.Context,
	metadata ControlMetadata,
) (AgentCurrentResponse, *ControlError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	claim, apiErr := s.claimAgentCurrent(ctx, metadata, at)
	if apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	defer clear(claim.secret)
	claimed, claimHash, claimSecret := claim.result, claim.hash, claim.secret
	if claimed.Status != store.AgentClaimActionable {
		response := AgentCurrentResponse{Status: string(claimed.Status)}
		if claimed.Status == store.AgentClaimNone {
			initiation, err := s.store.ReadAgentInitiationContext(ctx, metadata.Profile, at)
			if err != nil {
				return AgentCurrentResponse{}, mapControlError(err)
			}
			projection, err := initiation.CanonicalJSON()
			if err != nil {
				return AgentCurrentResponse{}, NewControlError(CodeInternal,
					"initiation context cannot be projected")
			}
			response.Projection = projection.Bytes()
		}
		return response, nil
	}
	readSpec := store.AgentCurrentReadSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
		RunID: claimed.Run.ID(), ClaimTokenHash: claimHash, At: at,
	}
	plan, err := s.store.PlanAgentCurrentRead(ctx, readSpec)
	if err != nil {
		return AgentCurrentResponse{}, mapControlError(err)
	}
	if plan.RunID != claimed.Run.ID() {
		return AgentCurrentResponse{}, NewControlError(CodeInternal,
			"managed current view plan differs from its Run")
	}
	views, err := s.currentViews.Materialize(ctx, plan)
	if err != nil {
		return AgentCurrentResponse{}, NewControlError(CodeInternal,
			"managed current Artifact views cannot be materialized safely")
	}
	finalAt, apiErr := s.trustedNow()
	if apiErr != nil {
		s.cleanupCurrentViews(claimed.Run.ID())
		return AgentCurrentResponse{}, apiErr
	}
	readSpec.At, readSpec.ArtifactViews = finalAt, views
	current, err := s.store.FinalizeAgentCurrentRead(ctx, readSpec)
	if err != nil {
		s.cleanupCurrentViews(claimed.Run.ID())
		return AgentCurrentResponse{}, mapControlError(err)
	}
	response := AgentCurrentResponse{Status: string(store.AgentClaimActionable),
		RunID: claimed.Run.ID().String(), Projection: current.Projection.CanonicalJSON().Bytes()}
	if !metadata.HasRunAttachment {
		response.ClaimSecret = base64.RawURLEncoding.EncodeToString(claimSecret)
	}
	return response, nil
}

func (s *Service) TeamworkAction(ctx context.Context, metadata ControlMetadata,
	request TeamworkActionRequest,
) (OperationResponse, *ControlError) {
	if apiErr := s.requireAuthenticatedMetadata(metadata); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := requireOperationCapabilities(metadata, false); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	validated, apiErr := s.actions.Validate(ActionInput{Action: request.Action,
		HasContext: metadata.HasClaimContext, ChannelAlias: request.Channel,
		Participant: request.To, Deadline: request.Deadline, Content: request.Content,
		ArtifactPaths: request.Artifacts})
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	requestDigest, err := validated.requestDigest(metadata.ClaimContextHash,
		metadata.HasClaimContext)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork request authority cannot be canonicalized")
	}
	terminal, found, apiErr := s.probeManagedOperation(ctx, metadata,
		validated.handler.OperationKind(), requestDigest)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if found {
		return s.replayTeamworkOperation(ctx, validated, terminal)
	}
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if s.executor == nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork action executor is unavailable")
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	owner, apiErr := s.operationOwner()
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	reservation, err := s.store.ReserveManagedOperation(ctx, store.ManagedOperationSpec{
		Profile: metadata.Profile, ClientKeyHash: metadata.OperationKeyHash,
		RequestDigest: requestDigest, Kind: validated.handler.OperationKind(),
		LeaseOwner: owner, At: at, LeaseUntil: at.Add(s.operationLease),
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
	})
	if err != nil {
		return OperationResponse{}, mapControlError(err)
	}
	if reservation.Operation.Status().Terminal() {
		return s.replayTeamworkOperation(ctx, validated, reservation.Operation)
	}
	response, executionErr := s.executor.ExecuteTeamwork(ctx, TeamworkExecutionSpec{
		Request: request, Action: validated, Reservation: reservation, At: at,
	})
	if executionErr == nil {
		s.cleanupCurrentViews(reservation.Operation.AgentRunID())
	}
	return response, executionErr
}

func (s *Service) AgentResolve(ctx context.Context, metadata ControlMetadata,
	request AgentResolveRequest,
) (OperationResponse, *ControlError) {
	if apiErr := s.requireAuthenticatedMetadata(metadata); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := requireOperationCapabilities(metadata, true); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	validated, apiErr := ValidateResolve(ResolveInput{Decision: request.Decision,
		HasContext: metadata.HasClaimContext, Content: request.Content})
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	requestDigest, err := store.ManagedResolutionRequestDigest(s.actions.AssetRevision(),
		metadata.ClaimContextHash, validated.Kind, validated.Content)
	if err != nil {
		return OperationResponse{}, mapControlError(err)
	}
	terminal, found, apiErr := s.probeManagedOperation(ctx, metadata, validated.Kind, requestDigest)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if found {
		return s.replayResolutionOperation(terminal)
	}
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	owner, apiErr := s.operationOwner()
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	reservation, err := s.store.ReserveManagedOperation(ctx, store.ManagedOperationSpec{
		Profile: metadata.Profile, ClientKeyHash: metadata.OperationKeyHash,
		RequestDigest: requestDigest, Kind: validated.Kind, LeaseOwner: owner,
		At: at, LeaseUntil: at.Add(s.operationLease), ClaimContextHash: metadata.ClaimContextHash,
		HasClaimContext: true,
	})
	if err != nil {
		return OperationResponse{}, mapControlError(err)
	}
	if reservation.Operation.Status().Terminal() {
		return s.replayResolutionOperation(reservation.Operation)
	}
	resolved, err := s.store.CommitManagedResolution(ctx, store.ManagedResolutionSpec{
		Reservation: reservation, Content: validated.Content, At: at,
	})
	if err != nil {
		return OperationResponse{}, mapControlError(err)
	}
	s.cleanupCurrentViews(reservation.Operation.AgentRunID())
	response, err := decodeResolutionResponse(resolved.Receipt, resolved.Operation)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"durable resolution receipt is invalid")
	}
	response.Replayed = resolved.Replayed
	return response, nil
}

func (s *Service) cleanupCurrentViews(runID model.RunID) {
	if s == nil || s.currentViews == nil || runID.IsZero() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.currentViews.CleanupRun(ctx, runID)
}

func (s *Service) requireMetadata(metadata ControlMetadata) *ControlError {
	if apiErr := s.requireAuthenticatedMetadata(metadata); apiErr != nil {
		return apiErr
	}
	if !metadata.Profile.Enabled() || metadata.Profile.ActiveAssetRevision() != s.assetRevision {
		return NewControlError(CodeAssetRevisionMismatch,
			"managed Profile activation differs from the active asset revision")
	}
	return nil
}

func (s *Service) requireAuthenticatedMetadata(metadata ControlMetadata) *ControlError {
	if s == nil || s.store == nil || metadata.Profile.ID() != model.TeamworkProfileID() {
		return NewControlError(CodeAuthenticationFailed, "profile authentication failed")
	}
	return nil
}

func (s *Service) checkActivation(ctx context.Context, profile model.Profile) *ControlError {
	if s == nil || s.activationGate == nil {
		return nil
	}
	return s.activationGate.Check(ctx, profile)
}

func (s *Service) trustedNow() (time.Time, *ControlError) {
	if s == nil || s.clock == nil {
		return time.Time{}, NewControlError(CodeInternal, "managed clock is unavailable")
	}
	now := s.clock.Now().Round(0).UTC()
	if now.IsZero() || now.UnixNano() <= 0 || !time.Unix(0, now.UnixNano()).UTC().Equal(now) {
		return time.Time{}, NewControlError(CodeInternal, "managed clock is invalid")
	}
	return now, nil
}

func (s *Service) operationOwner() (string, *ControlError) {
	raw, err := s.drawSecret()
	if err != nil {
		return "", NewControlError(CodeInternal, "operation lease owner cannot be generated")
	}
	defer clear(raw)
	return "run-" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) drawSecret() ([]byte, error) {
	if s == nil {
		return nil, errors.New("managed entropy is unavailable")
	}
	s.randomMu.Lock()
	defer s.randomMu.Unlock()
	return drawManagedSecret(s.random)
}

func drawManagedSecret(random io.Reader) ([]byte, error) {
	if random == nil {
		return nil, errors.New("managed entropy is unavailable")
	}
	value := make([]byte, managedSecretBytes)
	if _, err := io.ReadFull(random, value); err != nil {
		clear(value)
		return nil, err
	}
	return value, nil
}

func mapControlError(err error) *ControlError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrAgentClaimAsset), errors.Is(err, store.ErrManagedProfileAuthority):
		return NewControlError(CodeAssetRevisionMismatch, "managed Profile authority drifted")
	case errors.Is(err, store.ErrAgentClaimProfile):
		return NewControlError(CodeProfileHostMismatch, "managed Profile is unavailable")
	case errors.Is(err, store.ErrCurrentReadTooLarge):
		return NewControlError(CodeCurrentTooLarge, "current projection exceeds its bound")
	case errors.Is(err, store.ErrCurrentReadStale), errors.Is(err, store.ErrManagedContextStale):
		return NewControlError(CodeContextStale, "managed context is stale")
	case errors.Is(err, store.ErrAgentAttachmentStale):
		return NewControlError(CodeContextStale, "managed Run attachment is stale")
	case errors.Is(err, store.ErrAgentAttachmentInput):
		return NewControlError(CodeContextInvalid, "managed Run attachment is invalid")
	case errors.Is(err, store.ErrManagedContextRequired):
		return NewControlError(CodeContextRequired, "managed context is required")
	case errors.Is(err, store.ErrManagedActionNotAllowed):
		return NewControlError(CodeActionNotAllowed, "action is not allowed by current")
	case errors.Is(err, store.ErrOperationMismatch):
		return NewControlError(CodeOperationMismatch, "operation identity differs from request")
	case errors.Is(err, store.ErrOperationPending), errors.Is(err, store.ErrOperationFence):
		return NewControlError(CodeOperationPending, "operation is still pending")
	case errors.Is(err, store.ErrManagedResolutionStale), errors.Is(err, store.ErrAdmissionConflict),
		errors.Is(err, store.ErrWorkCASConflict):
		return NewControlError(CodeWorkConflict, "current Work changed before admission")
	case errors.Is(err, store.ErrDeadlineResolution):
		return NewControlError(CodeWorkExpired, "Work deadline already won")
	case errors.Is(err, store.ErrManagedResolutionInput), errors.Is(err, store.ErrManagedOperationInput):
		return NewControlError(CodeInvalidArgument, "managed request is invalid")
	default:
		return NewControlError(CodeInternal, fmt.Sprintf("managed control failed: %T", err))
	}
}

var _ ControlService = (*Service)(nil)
