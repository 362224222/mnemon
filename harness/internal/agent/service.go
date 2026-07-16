package agent

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	Request     localapi.TeamworkActionRequest
	Action      ValidatedAction
	Reservation store.ManagedOperationReservation
	At          time.Time
}

type TeamworkExecutor interface {
	ExecuteTeamwork(context.Context, TeamworkExecutionSpec) (localapi.OperationResponse, *localapi.APIError)
}

// ActivationGate verifies the mutable, disk-backed authority that cannot be
// frozen safely into a long-lived Service. A nil gate is intentionally
// supported for low-level composition and unit tests.
type ActivationGate interface {
	Check(context.Context, model.Profile) *localapi.APIError
}

// ControlStore is the narrow durable authority surface used by the local Agent
// routes. Keeping this boundary explicit makes it impossible for route
// orchestration to reach around the Store's managed transactions.
type ControlStore interface {
	ProbeAgentClaim(context.Context, store.AgentClaimProbeSpec) (store.AgentClaimStatus, error)
	ClaimAgentCurrent(context.Context, store.AgentClaimSpec) (store.AgentClaimResult, error)
	PlanAgentCurrentRead(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error)
	FinalizeAgentCurrentRead(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error)
	ReadAgentInitiationContext(context.Context, model.Profile, time.Time) (store.AgentInitiationContext, error)
	ReserveManagedOperation(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error)
	CommitManagedResolution(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error)
}

type ServiceOptions struct {
	AssetRevision  string
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
	clock          ServiceClock
	random         io.Reader
	randomMu       sync.Mutex
	operationLease time.Duration
	executor       TeamworkExecutor
	currentViews   AgentCurrentViews
	activationGate ActivationGate
}

func NewService(st ControlStore, options ServiceOptions) (*Service, error) {
	if st == nil || options.AssetRevision == "" || options.CurrentViews == nil {
		return nil, errors.New("Agent service requires Store, asset revision and current view coordinator")
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
	return &Service{store: st, assetRevision: options.AssetRevision, clock: options.Clock,
		random: options.Random, operationLease: options.OperationLease, executor: options.Executor,
		currentViews: options.CurrentViews, activationGate: options.ActivationGate}, nil
}

func (s *Service) HookCheck(ctx context.Context, metadata localapi.RequestMetadata,
	_ localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return localapi.HookCheckResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return localapi.HookCheckResponse{}, apiErr
	}
	if metadata.HasRunAttachment {
		return localapi.HookCheckResponse{}, localapi.NewAPIError(localapi.CodeContextInvalid,
			"managed Run attachment is not available")
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return localapi.HookCheckResponse{}, apiErr
	}
	status, err := s.store.ProbeAgentClaim(ctx, store.AgentClaimProbeSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision, At: at,
	})
	if err != nil {
		return localapi.HookCheckResponse{}, mapControlError(err)
	}
	return localapi.HookCheckResponse{Pending: status == store.AgentClaimActionable}, nil
}

func (s *Service) AgentCurrent(ctx context.Context, metadata localapi.RequestMetadata,
	_ localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return localapi.AgentCurrentResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return localapi.AgentCurrentResponse{}, apiErr
	}
	if metadata.HasRunAttachment {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeContextInvalid,
			"managed Run attachment is not available")
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return localapi.AgentCurrentResponse{}, apiErr
	}
	budget, err := model.ParseHandlingBudget(metadata.Profile.HandlingBudget())
	if err != nil {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"Profile handling budget is invalid")
	}
	leaseUntil := at.Add(time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second)
	if !leaseUntil.After(at) {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed claim lease cannot be represented")
	}
	claimSecret, err := s.drawSecret()
	if err != nil {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed claim capability cannot be generated")
	}
	defer clear(claimSecret)
	claimOwnerBytes, err := s.drawSecret()
	if err != nil {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed claim owner cannot be generated")
	}
	claimOwner := "claim-" + base64.RawURLEncoding.EncodeToString(claimOwnerBytes)
	clear(claimOwnerBytes)
	claimed, err := s.store.ClaimAgentCurrent(ctx, store.AgentClaimSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
		ClaimOwner: claimOwner, ClaimTokenHash: model.Sum(claimSecret), At: at, LeaseUntil: leaseUntil,
	})
	if err != nil {
		return localapi.AgentCurrentResponse{}, mapControlError(err)
	}
	if claimed.Status != store.AgentClaimActionable {
		response := localapi.AgentCurrentResponse{Status: string(claimed.Status)}
		if claimed.Status == store.AgentClaimNone {
			initiation, err := s.store.ReadAgentInitiationContext(ctx, metadata.Profile, at)
			if err != nil {
				return localapi.AgentCurrentResponse{}, mapControlError(err)
			}
			projection, err := initiation.CanonicalJSON()
			if err != nil {
				return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
					"initiation context cannot be projected")
			}
			response.Projection = projection.Bytes()
		}
		return response, nil
	}
	readSpec := store.AgentCurrentReadSpec{
		ProfileID: metadata.Profile.ID(), ExpectedAssetRevision: s.assetRevision,
		RunID: claimed.Run.ID(), ClaimTokenHash: model.Sum(claimSecret), At: at,
	}
	plan, err := s.store.PlanAgentCurrentRead(ctx, readSpec)
	if err != nil {
		return localapi.AgentCurrentResponse{}, mapControlError(err)
	}
	if plan.RunID != claimed.Run.ID() {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed current view plan differs from its Run")
	}
	views, err := s.currentViews.Materialize(ctx, plan)
	if err != nil {
		return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed current Artifact views cannot be materialized safely")
	}
	finalAt, apiErr := s.trustedNow()
	if apiErr != nil {
		s.cleanupCurrentViews(claimed.Run.ID())
		return localapi.AgentCurrentResponse{}, apiErr
	}
	readSpec.At, readSpec.ArtifactViews = finalAt, views
	current, err := s.store.FinalizeAgentCurrentRead(ctx, readSpec)
	if err != nil {
		s.cleanupCurrentViews(claimed.Run.ID())
		return localapi.AgentCurrentResponse{}, mapControlError(err)
	}
	return localapi.AgentCurrentResponse{Status: string(store.AgentClaimActionable),
		RunID: claimed.Run.ID().String(), ClaimSecret: base64.RawURLEncoding.EncodeToString(claimSecret),
		Projection: current.Projection.CanonicalJSON().Bytes()}, nil
}

func (s *Service) TeamworkAction(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	if apiErr := requireOperationCapabilities(metadata, request.Action != "offer"); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	validated, apiErr := ValidateAction(ActionInput{Action: request.Action,
		HasContext: metadata.HasClaimContext, ChannelAlias: request.Channel,
		Participant: request.To, Deadline: request.Deadline, Content: request.Content,
		ArtifactPaths: request.Artifacts})
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	if s.executor == nil {
		return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInternal,
			"Teamwork action executor is unavailable")
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	raw, err := model.CanonicalMarshal(request)
	if err != nil {
		return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInvalidArgument,
			"Teamwork request cannot be canonicalized")
	}
	owner, apiErr := s.operationOwner()
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	reservation, err := s.store.ReserveManagedOperation(ctx, store.ManagedOperationSpec{
		Profile: metadata.Profile, ClientKeyHash: metadata.OperationKeyHash,
		RequestDigest: model.Sum(raw), Kind: operationKindForAction(request.Action),
		LeaseOwner: owner, At: at, LeaseUntil: at.Add(s.operationLease),
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
	})
	if err != nil {
		return localapi.OperationResponse{}, mapControlError(err)
	}
	response, executionErr := s.executor.ExecuteTeamwork(ctx, TeamworkExecutionSpec{
		Request: request, Action: validated, Reservation: reservation, At: at,
	})
	if executionErr == nil {
		s.cleanupCurrentViews(reservation.Operation.AgentRunID())
	}
	return response, executionErr
}

func (s *Service) AgentResolve(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	if apiErr := s.requireMetadata(metadata); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	if apiErr := s.checkActivation(ctx, metadata.Profile); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	if apiErr := requireOperationCapabilities(metadata, true); apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	validated, apiErr := ValidateResolve(ResolveInput{Decision: request.Decision,
		HasContext: metadata.HasClaimContext, Content: request.Content})
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	at, apiErr := s.trustedNow()
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	requestDigest, err := store.ManagedResolutionRequestDigest(metadata.ClaimContextHash,
		validated.Kind, validated.Content)
	if err != nil {
		return localapi.OperationResponse{}, mapControlError(err)
	}
	owner, apiErr := s.operationOwner()
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	reservation, err := s.store.ReserveManagedOperation(ctx, store.ManagedOperationSpec{
		Profile: metadata.Profile, ClientKeyHash: metadata.OperationKeyHash,
		RequestDigest: requestDigest, Kind: validated.Kind, LeaseOwner: owner,
		At: at, LeaseUntil: at.Add(s.operationLease), ClaimContextHash: metadata.ClaimContextHash,
		HasClaimContext: true,
	})
	if err != nil {
		return localapi.OperationResponse{}, mapControlError(err)
	}
	resolved, err := s.store.CommitManagedResolution(ctx, store.ManagedResolutionSpec{
		Reservation: reservation, Content: validated.Content, At: at,
	})
	if err != nil {
		return localapi.OperationResponse{}, mapControlError(err)
	}
	s.cleanupCurrentViews(reservation.Operation.AgentRunID())
	response, err := decodeResolutionResponse(resolved.Receipt, resolved.Operation)
	if err != nil {
		return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInternal,
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

func (s *Service) requireMetadata(metadata localapi.RequestMetadata) *localapi.APIError {
	if s == nil || s.store == nil || metadata.Profile.ID() != model.TeamworkProfileID() {
		return localapi.NewAPIError(localapi.CodeAuthenticationFailed, "profile authentication failed")
	}
	if !metadata.Profile.Enabled() || metadata.Profile.ActiveAssetRevision() != s.assetRevision {
		return localapi.NewAPIError(localapi.CodeAssetRevisionMismatch,
			"managed Profile activation differs from the active asset revision")
	}
	return nil
}

func (s *Service) checkActivation(ctx context.Context, profile model.Profile) *localapi.APIError {
	if s == nil || s.activationGate == nil {
		return nil
	}
	return s.activationGate.Check(ctx, profile)
}

func requireOperationCapabilities(metadata localapi.RequestMetadata,
	contextRequired bool,
) *localapi.APIError {
	if !metadata.HasOperationKey || metadata.OperationKeyHash.IsZero() {
		return localapi.NewAPIError(localapi.CodeInvalidArgument, "operation key is required")
	}
	if contextRequired && (!metadata.HasClaimContext || metadata.ClaimContextHash.IsZero()) {
		return localapi.NewAPIError(localapi.CodeContextRequired, "managed context is required")
	}
	return nil
}

func (s *Service) trustedNow() (time.Time, *localapi.APIError) {
	if s == nil || s.clock == nil {
		return time.Time{}, localapi.NewAPIError(localapi.CodeInternal, "managed clock is unavailable")
	}
	now := s.clock.Now().Round(0).UTC()
	if now.IsZero() || now.UnixNano() <= 0 || !time.Unix(0, now.UnixNano()).UTC().Equal(now) {
		return time.Time{}, localapi.NewAPIError(localapi.CodeInternal, "managed clock is invalid")
	}
	return now, nil
}

func (s *Service) operationOwner() (string, *localapi.APIError) {
	raw, err := s.drawSecret()
	if err != nil {
		return "", localapi.NewAPIError(localapi.CodeInternal, "operation lease owner cannot be generated")
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

func operationKindForAction(action string) model.OperationKind {
	return map[string]model.OperationKind{
		"offer": model.OperationTeamworkOffer, "accept": model.OperationTeamworkAccept,
		"decline": model.OperationTeamworkDecline, "deliver": model.OperationTeamworkDeliver,
		"rework": model.OperationTeamworkRework, "close": model.OperationTeamworkClose,
		"cancel": model.OperationTeamworkCancel,
	}[action]
}

func decodeResolutionResponse(receipt model.JSON, operation model.Operation) (localapi.OperationResponse, error) {
	var response localapi.OperationResponse
	if receipt.IsZero() || operation.ID().IsZero() || operation.Status() != model.OperationCommitted {
		return response, errors.New("zero resolution receipt")
	}
	if err := json.Unmarshal(receipt.Bytes(), &response); err != nil {
		return localapi.OperationResponse{}, err
	}
	if response.SchemaVersion != localapi.SchemaVersion || response.Status != "resolved" ||
		response.OperationID != operation.ID().String() || response.Action != string(operation.Kind()) ||
		response.Results == nil || len(response.Results) != 0 || response.Handling == nil ||
		response.Receipt == "" {
		return localapi.OperationResponse{}, errors.New("invalid resolution receipt shape")
	}
	wantHandling := map[model.OperationKind]string{
		model.OperationResolveNoAction: "completed",
		model.OperationResolveRetry:    "requeued",
		model.OperationResolveReject:   "rejected",
	}[operation.Kind()]
	if wantHandling == "" || response.Handling.Status != wantHandling {
		return localapi.OperationResponse{}, errors.New("invalid resolution receipt lifecycle")
	}
	return response, nil
}

func mapControlError(err error) *localapi.APIError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrAgentClaimAsset), errors.Is(err, store.ErrManagedProfileAuthority):
		return localapi.NewAPIError(localapi.CodeAssetRevisionMismatch, "managed Profile authority drifted")
	case errors.Is(err, store.ErrAgentClaimProfile):
		return localapi.NewAPIError(localapi.CodeProfileHostMismatch, "managed Profile is unavailable")
	case errors.Is(err, store.ErrCurrentReadTooLarge):
		return localapi.NewAPIError(localapi.CodeCurrentTooLarge, "current projection exceeds its bound")
	case errors.Is(err, store.ErrCurrentReadStale), errors.Is(err, store.ErrManagedContextStale):
		return localapi.NewAPIError(localapi.CodeContextStale, "managed context is stale")
	case errors.Is(err, store.ErrManagedContextRequired):
		return localapi.NewAPIError(localapi.CodeContextRequired, "managed context is required")
	case errors.Is(err, store.ErrManagedActionNotAllowed):
		return localapi.NewAPIError(localapi.CodeActionNotAllowed, "action is not allowed by current")
	case errors.Is(err, store.ErrOperationMismatch):
		return localapi.NewAPIError(localapi.CodeOperationMismatch, "operation identity differs from request")
	case errors.Is(err, store.ErrOperationPending), errors.Is(err, store.ErrOperationFence):
		return localapi.NewAPIError(localapi.CodeOperationPending, "operation is still pending")
	case errors.Is(err, store.ErrManagedResolutionStale), errors.Is(err, store.ErrAdmissionConflict),
		errors.Is(err, store.ErrWorkCASConflict):
		return localapi.NewAPIError(localapi.CodeWorkConflict, "current Work changed before admission")
	case errors.Is(err, store.ErrDeadlineResolution):
		return localapi.NewAPIError(localapi.CodeWorkExpired, "Work deadline already won")
	case errors.Is(err, store.ErrManagedResolutionInput), errors.Is(err, store.ErrManagedOperationInput):
		return localapi.NewAPIError(localapi.CodeInvalidArgument, "managed request is invalid")
	default:
		return localapi.NewAPIError(localapi.CodeInternal, fmt.Sprintf("managed control failed: %T", err))
	}
}

var _ localapi.Service = (*Service)(nil)
