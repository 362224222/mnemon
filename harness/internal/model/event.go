package model

import (
	"sort"
	"time"
)

type EventType string

const (
	EventReviewOffered          EventType = "review.offered"
	EventReviewAcceptRequested  EventType = "review.accept.requested"
	EventReviewDeclineRequested EventType = "review.decline.requested"
	EventReviewDeliveryReady    EventType = "review.delivery.ready"
	EventReviewAccepted         EventType = "review.accepted"
	EventReviewAcceptRejected   EventType = "review.accept_rejected"
	EventReviewDelivered        EventType = "review.delivered"
	EventReviewReworkRequested  EventType = "review.rework_requested"
	EventReviewClosed           EventType = "review.closed"
	EventReviewDeclined         EventType = "review.declined"
	EventReviewCancelled        EventType = "review.cancelled"
	EventReviewExpired          EventType = "review.expired"
	EventReviewOutcome          EventType = "review.outcome"
)

func (t EventType) Valid() bool {
	switch t {
	case EventReviewOffered, EventReviewAcceptRequested, EventReviewDeclineRequested,
		EventReviewDeliveryReady, EventReviewAccepted, EventReviewAcceptRejected,
		EventReviewDelivered, EventReviewReworkRequested, EventReviewClosed,
		EventReviewDeclined, EventReviewCancelled, EventReviewExpired, EventReviewOutcome:
		return true
	default:
		return false
	}
}

func (t EventType) HomeAuthoritative() bool {
	switch t {
	case EventReviewOffered, EventReviewAccepted, EventReviewAcceptRejected,
		EventReviewDelivered, EventReviewReworkRequested, EventReviewClosed,
		EventReviewDeclined, EventReviewCancelled, EventReviewExpired:
		return true
	default:
		return false
	}
}

func (t EventType) ParticipantInput() bool {
	return t == EventReviewAcceptRequested || t == EventReviewDeclineRequested || t == EventReviewDeliveryReady
}

type EventSource string

const (
	EventSourceLocal    EventSource = "local"
	EventSourceImported EventSource = "imported"
)

func (s EventSource) Valid() bool { return s == EventSourceLocal || s == EventSourceImported }

type Audience struct {
	peers []PeerID
}

func NewAudience(peers []PeerID) (Audience, error) {
	if len(peers) == 0 {
		return Audience{}, invalid("audience", "must contain at least one remote peer")
	}
	if len(peers) > MaxChildWorks {
		return Audience{}, limit("audience", len(peers), MaxChildWorks)
	}
	result := append([]PeerID(nil), peers...)
	for _, peer := range result {
		if peer.IsZero() {
			return Audience{}, invalid("audience", "contains a zero PeerID")
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return Audience{}, invalid("audience", "contains a duplicate PeerID")
		}
	}
	return Audience{peers: result}, nil
}

func (a Audience) Peers() []PeerID { return append([]PeerID(nil), a.peers...) }
func (a Audience) Len() int        { return len(a.peers) }
func (a Audience) Contains(peer PeerID) bool {
	index := sort.Search(len(a.peers), func(i int) bool { return a.peers[i].String() >= peer.String() })
	return index < len(a.peers) && a.peers[index] == peer
}

func (a Audience) MarshalJSON() ([]byte, error) {
	if len(a.peers) == 0 {
		return nil, invalid("audience", "zero audience")
	}
	return CanonicalMarshal(a.peers)
}

type EventScope struct {
	channelID         ChannelID
	originPeerID      PeerID
	originEpoch       OriginEpoch
	originSequence    uint64
	channelSequence   uint64
	originMember      RecordHead
	publicationRoster RecordHead
	work              WorkRef
}

func NewEventScope(channel ChannelID, origin PeerID, epoch OriginEpoch, originSequence,
	channelSequence uint64, originMember, publicationRoster RecordHead, work WorkRef,
) (EventScope, error) {
	if channel.IsZero() || origin.IsZero() || epoch.IsZero() || work.IsZero() {
		return EventScope{}, invalid("event scope", "channel, origin, epoch and WorkRef are required")
	}
	if err := validateSQLitePositive("origin sequence", originSequence); err != nil {
		return EventScope{}, err
	}
	if err := validateSQLitePositive("Channel sequence", channelSequence); err != nil {
		return EventScope{}, err
	}
	if originMember.IsZero() || publicationRoster.IsZero() {
		return EventScope{}, invalid("event scope", "member and publication roster heads are required")
	}
	if publicationRoster.Revision() < originMember.Revision() {
		return EventScope{}, invariant("publication roster revision precedes origin member revision")
	}
	return EventScope{channel, origin, epoch, originSequence, channelSequence, originMember, publicationRoster, work}, nil
}

func (s EventScope) ChannelID() ChannelID          { return s.channelID }
func (s EventScope) OriginPeerID() PeerID          { return s.originPeerID }
func (s EventScope) OriginEpoch() OriginEpoch      { return s.originEpoch }
func (s EventScope) OriginSequence() uint64        { return s.originSequence }
func (s EventScope) ChannelSequence() uint64       { return s.channelSequence }
func (s EventScope) OriginMember() RecordHead      { return s.originMember }
func (s EventScope) PublicationRoster() RecordHead { return s.publicationRoster }
func (s EventScope) WorkRef() WorkRef              { return s.work }
func (s EventScope) EventKey(id EventID) (EventKey, error) {
	return NewEventKey(s.originPeerID, s.originEpoch, id)
}
func (s EventScope) PublicationKey() (PublicationKey, error) {
	return NewPublicationKey(s.channelID, s.originPeerID, s.originEpoch, s.channelSequence)
}

type EventSpec struct {
	ID             EventID
	Scope          EventScope
	Source         EventSource
	ActorPrincipal string
	Type           EventType
	Audience       Audience
	Summary        string
	Payload        JSON
	Artifacts      []ArtifactRef
	CausedBy       []EventKey
	CreatedAt      time.Time
	AcceptedAt     time.Time
}

type Event struct {
	spec      EventSpec
	canonical JSON
	digest    Digest
}

func NewEvent(spec EventSpec) (Event, error) {
	if spec.ID.IsZero() || spec.Scope.channelID.IsZero() {
		return Event{}, invalid("event", "ID and complete scope are required")
	}
	if !spec.Source.Valid() || !spec.Type.Valid() {
		return Event{}, invalid("event", "source and type must be closed enum values")
	}
	if err := validateIdentifier("actor_principal", spec.ActorPrincipal); err != nil {
		return Event{}, err
	}
	if err := validateText("summary", spec.Summary, MaxSummaryBytes, true); err != nil {
		return Event{}, err
	}
	if spec.Payload.IsZero() || len(spec.Payload.raw) == 0 || spec.Payload.raw[0] != '{' {
		return Event{}, invalid("event payload", "must be a canonical JSON object")
	}
	if spec.Audience.Len() == 0 || spec.Audience.Contains(spec.Scope.originPeerID) {
		return Event{}, invariant("Event audience must be nonempty and exclude its origin")
	}
	home := spec.Scope.work.HomePeerID()
	if spec.Type.HomeAuthoritative() && spec.Scope.originPeerID != home {
		return Event{}, invariant("home-authoritative Event must originate at Work home")
	}
	if spec.Type.ParticipantInput() {
		if spec.Scope.originPeerID == home || !spec.Audience.Contains(home) {
			return Event{}, invariant("participant input must originate remotely and address Work home")
		}
	}
	artifacts, err := normalizeArtifactRefs(spec.Artifacts, MaxArtifactRefs)
	if err != nil {
		return Event{}, err
	}
	causedBy, err := normalizeEventKeys(spec.CausedBy)
	if err != nil {
		return Event{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Event{}, err
	}
	acceptedAt, err := canonicalTime(spec.AcceptedAt)
	if err != nil {
		return Event{}, err
	}
	if acceptedAt.Before(createdAt) {
		return Event{}, invariant("Event accepted time precedes creation time")
	}
	spec.Artifacts, spec.CausedBy = artifacts, causedBy
	spec.CreatedAt, spec.AcceptedAt = createdAt, acceptedAt
	key, _ := spec.Scope.EventKey(spec.ID)
	for _, cause := range causedBy {
		if cause == key {
			return Event{}, invariant("Event cannot cause itself")
		}
	}
	body, err := eventBodyJSON(spec)
	if err != nil {
		return Event{}, err
	}
	// Every R5 domain Event is Channel-bound and must be publishable. This is
	// only the necessary Event-body ceiling; SignedPublication applies the
	// exact 64 KiB limit again after adding its roster and digest fields.
	if len(body.raw) > MaxPublicationBytes {
		return Event{}, limit("canonical Event", len(body.raw), MaxPublicationBytes)
	}
	return Event{spec: spec, canonical: body, digest: Sum(body.Bytes())}, nil
}

func normalizeEventKeys(keys []EventKey) ([]EventKey, error) {
	if len(keys) > MaxCausalityRefs {
		return nil, limit("caused_by", len(keys), MaxCausalityRefs)
	}
	result := append([]EventKey{}, keys...)
	for _, key := range result {
		if key.IsZero() {
			return nil, invalid("caused_by", "contains a zero Event key")
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].key() < result[j].key() })
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, invalid("caused_by", "contains a duplicate Event key")
		}
	}
	return result, nil
}

func eventBodyJSON(spec EventSpec) (JSON, error) {
	body := struct {
		SchemaVersion int           `json:"schema_version"`
		EventID       EventID       `json:"event_id"`
		ChannelID     ChannelID     `json:"channel_id"`
		OriginPeerID  PeerID        `json:"origin_peer_id"`
		OriginEpoch   OriginEpoch   `json:"origin_epoch"`
		OriginSeq     uint64        `json:"origin_seq"`
		ChannelSeq    uint64        `json:"channel_seq"`
		OriginMember  RecordHead    `json:"origin_member"`
		RosterHead    RecordHead    `json:"publication_roster"`
		Actor         string        `json:"actor_principal"`
		Type          EventType     `json:"event_type"`
		Audience      Audience      `json:"audience"`
		Resource      WorkRef       `json:"resource"`
		Summary       string        `json:"summary"`
		Payload       JSON          `json:"payload"`
		Artifacts     []ArtifactRef `json:"artifact_roots"`
		CausedBy      []EventKey    `json:"caused_by"`
		CreatedAt     string        `json:"created_at"`
		AcceptedAt    string        `json:"accepted_at"`
	}{SchemaVersion, spec.ID, spec.Scope.channelID, spec.Scope.originPeerID, spec.Scope.originEpoch,
		spec.Scope.originSequence, spec.Scope.channelSequence, spec.Scope.originMember,
		spec.Scope.publicationRoster, spec.ActorPrincipal, spec.Type, spec.Audience, spec.Scope.work,
		spec.Summary, spec.Payload, spec.Artifacts, spec.CausedBy, formatTime(spec.CreatedAt), formatTime(spec.AcceptedAt)}
	return JSONFrom(body)
}

func (e Event) ID() EventID            { return e.spec.ID }
func (e Event) Key() EventKey          { key, _ := e.spec.Scope.EventKey(e.spec.ID); return key }
func (e Event) Scope() EventScope      { return e.spec.Scope }
func (e Event) Source() EventSource    { return e.spec.Source }
func (e Event) ActorPrincipal() string { return e.spec.ActorPrincipal }
func (e Event) Type() EventType        { return e.spec.Type }
func (e Event) Audience() Audience {
	audience, _ := NewAudience(e.spec.Audience.Peers())
	return audience
}
func (e Event) Summary() string          { return e.spec.Summary }
func (e Event) Payload() JSON            { return e.spec.Payload }
func (e Event) Artifacts() []ArtifactRef { return append([]ArtifactRef{}, e.spec.Artifacts...) }
func (e Event) CausedBy() []EventKey     { return append([]EventKey{}, e.spec.CausedBy...) }
func (e Event) CreatedAt() time.Time     { return e.spec.CreatedAt }
func (e Event) AcceptedAt() time.Time    { return e.spec.AcceptedAt }
func (e Event) CanonicalJSON() JSON      { return e.canonical }
func (e Event) Digest() Digest           { return e.digest }
func (e Event) Spec() EventSpec {
	spec := e.spec
	spec.Artifacts = e.Artifacts()
	spec.CausedBy = e.CausedBy()
	return spec
}
func (e Event) MarshalJSON() ([]byte, error) { return e.canonical.MarshalJSON() }
