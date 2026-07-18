package model

import (
	"sort"
	"time"
)

type ArrivalSource string

const (
	ArrivalGossip ArrivalSource = "gossip"
	ArrivalPull   ArrivalSource = "pull"
)

func (s ArrivalSource) Valid() bool { return s == ArrivalGossip || s == ArrivalPull }

type InboxStatus string

const (
	InboxStored          InboxStatus = "stored"
	InboxWaitingArtifact InboxStatus = "waiting_artifact"
	InboxReady           InboxStatus = "ready"
	InboxProcessing      InboxStatus = "processing"
	InboxAccepted        InboxStatus = "accepted"
	InboxRejected        InboxStatus = "rejected"
	InboxConflicted      InboxStatus = "conflicted"
	InboxRetry           InboxStatus = "retry"
	InboxQuarantined     InboxStatus = "quarantined"
	InboxIgnored         InboxStatus = "ignored"
)

func (s InboxStatus) Valid() bool {
	switch s {
	case InboxStored, InboxWaitingArtifact, InboxReady, InboxProcessing, InboxAccepted,
		InboxRejected, InboxConflicted, InboxRetry, InboxQuarantined, InboxIgnored:
		return true
	default:
		return false
	}
}

func (s InboxStatus) Terminal() bool {
	return s == InboxAccepted || s == InboxRejected || s == InboxConflicted || s == InboxQuarantined || s == InboxIgnored
}

type PeerInboxSpec struct {
	ID                    InboxID
	Publication           SignedPublication
	TransportPeerID       PeerID
	ArrivalSource         ArrivalSource
	IsAudience            bool
	RequiredArtifactRoots []Digest
	Status                InboxStatus
	Attempts              uint32
	NextAttemptAt         time.Time
	LeaseOwner            string
	LeaseUntil            *time.Time
	LocalEventID          *EventID
	Decision              *JSON
	ReceiptEventID        *EventID
	Diagnostic            string
	ReceivedAt            time.Time
	UpdatedAt             time.Time
}

type PeerInbox struct {
	spec           PeerInboxSpec
	requiredRoots  []Digest
	leaseUntil     time.Time
	hasLease       bool
	localEventID   EventID
	hasLocalEvent  bool
	decision       JSON
	hasDecision    bool
	receiptEventID EventID
	hasReceipt     bool
}

func NewPeerInbox(localPeer PeerID, spec PeerInboxSpec) (PeerInbox, error) {
	if localPeer.IsZero() || spec.ID.IsZero() || spec.Publication.Event().ID().IsZero() || spec.TransportPeerID.IsZero() {
		return PeerInbox{}, invalid("peer inbox", "local/transport peer, Inbox ID and publication are required")
	}
	if !spec.ArrivalSource.Valid() || !spec.Status.Valid() {
		return PeerInbox{}, invalid("peer inbox", "unknown arrival source or status")
	}
	event := spec.Publication.Event()
	origin := event.Scope().OriginPeerID()
	if event.Source() != EventSourceImported {
		return PeerInbox{}, invariant("Peer Inbox accepts only imported publication Events")
	}
	if spec.TransportPeerID == localPeer {
		return PeerInbox{}, invariant("Peer Inbox transport cannot be the local Peer")
	}
	if spec.ArrivalSource == ArrivalPull && spec.TransportPeerID != origin {
		return PeerInbox{}, invariant("Pull repair transport must be the publication origin")
	}
	isAudience := event.Audience().Contains(localPeer)
	if spec.IsAudience != isAudience {
		return PeerInbox{}, invariant("Inbox audience flag must match immutable Event audience")
	}
	if !isAudience {
		if spec.Status != InboxIgnored || spec.LocalEventID != nil || spec.ReceiptEventID != nil || spec.Decision != nil {
			return PeerInbox{}, invariant("non-audience publication must terminate ignored without domain effect")
		}
	} else if spec.Status == InboxIgnored {
		return PeerInbox{}, invariant("audience publication cannot be ignored")
	}
	requiredRoots, err := normalizeDigests(spec.RequiredArtifactRoots, MaxArtifactRefs)
	if err != nil {
		return PeerInbox{}, err
	}
	if !equalDigestSet(requiredRoots, artifactDigests(event.Artifacts())) {
		return PeerInbox{}, invariant("Inbox required Artifact roots must exactly match immutable Event refs")
	}
	if err := validateText("Inbox diagnostic", spec.Diagnostic, MaxContentBytes, true); err != nil {
		return PeerInbox{}, err
	}
	nextAttempt, err := canonicalTime(spec.NextAttemptAt)
	if err != nil {
		return PeerInbox{}, err
	}
	receivedAt, err := canonicalTime(spec.ReceivedAt)
	if err != nil {
		return PeerInbox{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return PeerInbox{}, err
	}
	if updatedAt.Before(receivedAt) {
		return PeerInbox{}, invariant("Inbox update precedes receipt")
	}
	result := PeerInbox{spec: spec, requiredRoots: requiredRoots}
	result.spec.RequiredArtifactRoots = nil
	result.spec.NextAttemptAt, result.spec.ReceivedAt, result.spec.UpdatedAt = nextAttempt, receivedAt, updatedAt
	result.spec.LeaseUntil, result.spec.LocalEventID = nil, nil
	result.spec.Decision, result.spec.ReceiptEventID = nil, nil
	if spec.Status == InboxWaitingArtifact || spec.Status == InboxProcessing {
		if spec.LeaseOwner == "" || spec.LeaseUntil == nil {
			return PeerInbox{}, invariant("leased Inbox phase requires owner and lease")
		}
		if err := validateIdentifier("Inbox lease owner", spec.LeaseOwner); err != nil {
			return PeerInbox{}, err
		}
		leaseUntil, err := canonicalTime(*spec.LeaseUntil)
		if err != nil {
			return PeerInbox{}, err
		}
		if !leaseUntil.After(updatedAt) {
			return PeerInbox{}, invariant("Inbox lease must end after update time")
		}
		result.leaseUntil, result.hasLease = leaseUntil, true
	} else if spec.LeaseOwner != "" || spec.LeaseUntil != nil {
		return PeerInbox{}, invariant("unleased Inbox phase cannot retain lease authority")
	}
	if spec.LocalEventID != nil {
		if spec.LocalEventID.IsZero() || *spec.LocalEventID != event.ID() {
			return PeerInbox{}, invariant("local imported Event identity must equal publication Event ID")
		}
		result.localEventID, result.hasLocalEvent = *spec.LocalEventID, true
	}
	if spec.Status == InboxAccepted && !result.hasLocalEvent {
		return PeerInbox{}, invariant("accepted Inbox row requires its imported Event")
	}
	if spec.Decision != nil {
		if spec.Decision.IsZero() || spec.Decision.raw[0] != '{' {
			return PeerInbox{}, invalid("Inbox decision", "must be a canonical JSON object")
		}
		result.decision, result.hasDecision = *spec.Decision, true
	}
	if spec.ReceiptEventID != nil {
		if spec.ReceiptEventID.IsZero() {
			return PeerInbox{}, invalid("receipt Event ID", "must not be zero")
		}
		result.receiptEventID, result.hasReceipt = *spec.ReceiptEventID, true
	}
	return result, nil
}

func normalizeDigests(values []Digest, max int) ([]Digest, error) {
	if len(values) > max {
		return nil, limit("digests", len(values), max)
	}
	result := append([]Digest{}, values...)
	for _, digest := range result {
		if digest.IsZero() {
			return nil, invalid("digests", "contains zero digest")
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, invalid("digests", "contains duplicate digest")
		}
	}
	return result, nil
}

func artifactDigests(refs []ArtifactRef) []Digest {
	result := make([]Digest, len(refs))
	for i, ref := range refs {
		result[i] = ref.RootDigest()
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}

func equalDigestSet(left, right []Digest) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (i PeerInbox) ID() InboxID                     { return i.spec.ID }
func (i PeerInbox) Publication() SignedPublication  { return i.spec.Publication }
func (i PeerInbox) TransportPeerID() PeerID         { return i.spec.TransportPeerID }
func (i PeerInbox) OriginPeerID() PeerID            { return i.spec.Publication.Event().Scope().OriginPeerID() }
func (i PeerInbox) ArrivalSource() ArrivalSource    { return i.spec.ArrivalSource }
func (i PeerInbox) IsAudience() bool                { return i.spec.IsAudience }
func (i PeerInbox) RequiredArtifactRoots() []Digest { return append([]Digest(nil), i.requiredRoots...) }
func (i PeerInbox) Status() InboxStatus             { return i.spec.Status }
func (i PeerInbox) Attempts() uint32                { return i.spec.Attempts }
func (i PeerInbox) NextAttemptAt() time.Time        { return i.spec.NextAttemptAt }
func (i PeerInbox) LeaseOwner() string              { return i.spec.LeaseOwner }
func (i PeerInbox) LeaseUntil() (time.Time, bool)   { return i.leaseUntil, i.hasLease }
func (i PeerInbox) LocalEventID() (EventID, bool)   { return i.localEventID, i.hasLocalEvent }
func (i PeerInbox) Decision() (JSON, bool)          { return i.decision, i.hasDecision }
func (i PeerInbox) ReceiptEventID() (EventID, bool) { return i.receiptEventID, i.hasReceipt }
func (i PeerInbox) Diagnostic() string              { return i.spec.Diagnostic }
func (i PeerInbox) ReceivedAt() time.Time           { return i.spec.ReceivedAt }
func (i PeerInbox) UpdatedAt() time.Time            { return i.spec.UpdatedAt }

func (i PeerInbox) ValidateReceiptEvent(event Event) error {
	if !i.hasReceipt || event.ID() != i.receiptEventID ||
		event.Scope().ChannelID() != i.spec.Publication.Event().Scope().ChannelID() {
		return invariant("Inbox receipt Event does not match stored Channel receipt identity")
	}
	return nil
}
