package event

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type Clock interface {
	Now() time.Time
}

type WallClock struct{}

func (WallClock) Now() time.Time { return time.Now() }

type AdmissionStampSpec struct {
	Node                 model.Node
	Profile              model.Profile
	EventID              model.EventID
	ChannelID            model.ChannelID
	WorkRef              model.WorkRef
	OriginSequence       uint64
	ChannelSequence      uint64
	OriginMember         model.RecordHead
	PublicationRoster    model.RecordHead
	Audience             model.Audience
	WorkVersion          uint64
	Iteration            uint8
	WorkDeadlineUnixNano int64
	Artifacts            []model.ArtifactRef
	CausedBy             []model.EventKey
}

// AdmissionStamp is the opaque, trusted side of local admission. Candidate
// types contain no actor, source, Node identity, sequence, accepted time,
// Event ID, Work identity, audience, Artifact digest, or causality field.
type AdmissionStamp struct {
	node              model.Node
	profile           model.Profile
	eventID           model.EventID
	channelID         model.ChannelID
	workRef           model.WorkRef
	originSequence    uint64
	channelSequence   uint64
	originMember      model.RecordHead
	publicationRoster model.RecordHead
	audience          model.Audience
	workVersion       uint64
	iteration         uint8
	deadlineUnixNano  int64
	artifacts         []model.ArtifactRef
	causedBy          []model.EventKey
}

func NewAdmissionStamp(spec AdmissionStampSpec) (AdmissionStamp, error) {
	if spec.Node.PeerID().IsZero() || spec.Node.OriginEpoch().IsZero() {
		return AdmissionStamp{}, stampError("validated Node authority is required")
	}
	if spec.Profile.ID() != model.TeamworkProfileID() || !spec.Profile.Enabled() {
		return AdmissionStamp{}, stampError("enabled teamwork-default Profile authority is required")
	}
	if spec.Node.ActiveAssetRevision() != spec.Profile.ActiveAssetRevision() {
		return AdmissionStamp{}, stampError("Node and Profile asset revisions differ")
	}
	if spec.EventID.IsZero() || spec.ChannelID.IsZero() || spec.WorkRef.IsZero() {
		return AdmissionStamp{}, stampError("Event, Channel and Work identities are required")
	}
	if spec.OriginSequence == 0 || spec.ChannelSequence == 0 {
		return AdmissionStamp{}, stampError("origin and Channel sequences must be positive")
	}
	if spec.OriginSequence > model.MaxSQLiteInteger || spec.ChannelSequence > model.MaxSQLiteInteger ||
		spec.WorkVersion > model.MaxSQLiteInteger {
		return AdmissionStamp{}, fmt.Errorf("%w: counters exceed SQLite INTEGER range: %w",
			ErrInvalidStamp, model.ErrLimit)
	}
	if spec.OriginMember.IsZero() || spec.PublicationRoster.IsZero() ||
		spec.PublicationRoster.Revision() < spec.OriginMember.Revision() {
		return AdmissionStamp{}, stampError("valid member and publication roster heads are required")
	}
	if spec.Audience.Len() == 0 || spec.Audience.Contains(spec.Node.PeerID()) {
		return AdmissionStamp{}, stampError("audience must be nonempty and exclude the origin Node")
	}
	if spec.WorkVersion == 0 || spec.Iteration < 1 || spec.Iteration > 2 {
		return AdmissionStamp{}, stampError("positive Work version and iteration 1..2 are required")
	}
	if spec.WorkDeadlineUnixNano < 0 {
		return AdmissionStamp{}, stampError("Work deadline must be zero or positive Unix nanoseconds")
	}
	return AdmissionStamp{
		spec.Node, spec.Profile, spec.EventID, spec.ChannelID, spec.WorkRef,
		spec.OriginSequence, spec.ChannelSequence, spec.OriginMember, spec.PublicationRoster,
		spec.Audience, spec.WorkVersion, spec.Iteration, spec.WorkDeadlineUnixNano,
		append([]model.ArtifactRef(nil), spec.Artifacts...), append([]model.EventKey(nil), spec.CausedBy...),
	}, nil
}

type eventDraft struct {
	eventType        model.EventType
	summary          string
	payload          any
	deadlineUnixNano int64
}

type Factory struct {
	clock  Clock
	signer PublicationSigner
}

func NewFactory(clock Clock, signer PublicationSigner) (*Factory, error) {
	if clock == nil || signer == nil {
		return nil, fmt.Errorf("event Factory requires a clock and publication signer")
	}
	return &Factory{clock, signer}, nil
}

type Bundle struct {
	event            model.Event
	publication      model.SignedPublication
	deadlineUnixNano int64
}

func (bundle Bundle) Event() model.Event                   { return bundle.event }
func (bundle Bundle) Publication() model.SignedPublication { return bundle.publication }
func (bundle Bundle) WorkDeadlineUnixNano() int64          { return bundle.deadlineUnixNano }
func (factory *Factory) AdmitAgent(ctx context.Context, stamp AdmissionStamp,
	candidate AgentCandidate,
) (Bundle, error) {
	if factory == nil || factory.clock == nil || factory.signer == nil {
		return Bundle{}, fmt.Errorf("event Factory is not initialized")
	}
	if !knownAgentCandidate(candidate) {
		return Bundle{}, candidateError("Agent candidate", model.ErrInvalid, "must not be nil")
	}
	now, err := trustedClockTime(factory.clock)
	if err != nil {
		return Bundle{}, err
	}
	draft, err := candidate.draft(stamp, now)
	if err != nil {
		return Bundle{}, err
	}
	if !draft.eventType.AgentAdmitted() {
		return Bundle{}, candidateError("Agent candidate", model.ErrInvariant,
			"maps outside the descriptor-authorized Agent Event set")
	}
	return factory.admit(ctx, stamp, draft, now)
}

func (factory *Factory) AdmitController(ctx context.Context, stamp AdmissionStamp,
	candidate ControllerCandidate,
) (Bundle, error) {
	if factory == nil || factory.clock == nil || factory.signer == nil {
		return Bundle{}, fmt.Errorf("event Factory is not initialized")
	}
	if !knownControllerCandidate(candidate) {
		return Bundle{}, candidateError("controller candidate", model.ErrInvalid, "must not be nil")
	}
	now, err := trustedClockTime(factory.clock)
	if err != nil {
		return Bundle{}, err
	}
	draft, err := candidate.draft(stamp, now)
	if err != nil {
		return Bundle{}, err
	}
	if !draft.eventType.ControllerAdmitted() {
		return Bundle{}, candidateError("controller candidate", model.ErrInvariant,
			"maps outside the descriptor-authorized controller Event set")
	}
	return factory.admit(ctx, stamp, draft, now)
}

func knownAgentCandidate(candidate AgentCandidate) bool {
	switch candidate.(type) {
	case OfferCandidate, AcceptCandidate, DeclineCandidate, DeliverCandidate,
		ReworkCandidate, CloseCandidate, CancelCandidate:
		return true
	default:
		return false
	}
}

func knownControllerCandidate(candidate ControllerCandidate) bool {
	switch candidate.(type) {
	case AcceptedDecision, AcceptRejectedDecision, DeliveredDecision,
		DeclinedDecision, ExpiredDecision, OutcomeDecision:
		return true
	default:
		return false
	}
}

func (factory *Factory) admit(ctx context.Context, stamp AdmissionStamp, draft eventDraft,
	now time.Time,
) (Bundle, error) {
	if factory == nil || factory.clock == nil || factory.signer == nil {
		return Bundle{}, fmt.Errorf("event Factory is not initialized")
	}
	if now.IsZero() {
		return Bundle{}, stampError("trusted clock returned zero time")
	}
	descriptor, valid := draft.eventType.Descriptor()
	if !valid {
		return Bundle{}, stampError("draft uses an unknown Event type")
	}
	if draft.eventType == model.EventReviewOffered {
		if stamp.deadlineUnixNano != 0 || stamp.workVersion != 1 || stamp.iteration != 1 {
			return Bundle{}, stampError("new offer requires zero stored deadline and Work version/iteration 1")
		}
	} else if stamp.deadlineUnixNano <= 0 {
		return Bundle{}, stampError("existing Work Event requires a frozen deadline")
	}
	if descriptor.RequiresAdmissionCausality() && len(stamp.causedBy) == 0 {
		return Bundle{}, stampError("%s requires trusted source Event causality", draft.eventType)
	}
	if !descriptor.AllowsArtifacts() && len(stamp.artifacts) != 0 {
		return Bundle{}, stampError("%s forbids Artifact roots", draft.eventType)
	}
	scope, err := model.NewEventScope(stamp.channelID, stamp.node.PeerID(), stamp.node.OriginEpoch(),
		stamp.originSequence, stamp.channelSequence, stamp.originMember, stamp.publicationRoster, stamp.workRef)
	if err != nil {
		return Bundle{}, fmt.Errorf("create Event scope: %w", err)
	}
	payload, err := model.JSONFrom(draft.payload)
	if err != nil {
		return Bundle{}, fmt.Errorf("create closed Event payload: %w", err)
	}
	event, err := model.NewEvent(model.EventSpec{
		ID: stamp.eventID, Scope: scope, Source: model.EventSourceLocal,
		ActorPrincipal: stamp.profile.Principal(), Type: draft.eventType, Audience: stamp.audience,
		Summary: draft.summary, Payload: payload, Artifacts: stamp.artifacts, CausedBy: stamp.causedBy,
		CreatedAt: now, AcceptedAt: now,
	})
	if err != nil {
		return Bundle{}, fmt.Errorf("admit canonical Event: %w", err)
	}
	publication, err := factory.sign(ctx, event)
	if err != nil {
		return Bundle{}, err
	}
	return Bundle{event, publication, draft.deadlineUnixNano}, nil
}

func (factory *Factory) sign(ctx context.Context, event model.Event) (model.SignedPublication, error) {
	body, err := model.NewPublicationBody(event)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("build publication body: %w", err)
	}
	signature, err := factory.signer.Sign(ctx, publicationSigningMessage(body.Key().ChannelID(), body.Digest()))
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("sign publication: %w", err)
	}
	if len(signature) != ed25519.SignatureSize {
		return model.SignedPublication{}, fmt.Errorf("%w: signer returned %d bytes, want %d",
			ErrSignature, len(signature), ed25519.SignatureSize)
	}
	publication, err := model.AttachSignature(body, signature)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("build signed publication: %w", err)
	}
	return publication, nil
}

func formatTimestamp(unixNano int64) string {
	return time.Unix(0, unixNano).UTC().Format(time.RFC3339Nano)
}

func trustedClockTime(clock Clock) (time.Time, error) {
	now := clock.Now().Round(0).UTC()
	unixNano := now.UnixNano()
	if now.IsZero() || unixNano <= 0 || !time.Unix(0, unixNano).UTC().Equal(now) {
		return time.Time{}, stampError("trusted clock must return a positive, exact Unix-nanosecond time")
	}
	return now, nil
}
