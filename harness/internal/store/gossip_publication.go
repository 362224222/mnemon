package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const maxGossipPublicationLease = 5 * time.Minute

var (
	ErrGossipPublicationInput     = errors.New("invalid Gossip publication worker input")
	ErrGossipPublicationAuthority = errors.New("Gossip publication Channel authority is unavailable")
	ErrGossipPublicationStale     = errors.New("Gossip publication lease is stale")
	ErrGossipPublicationInvariant = errors.New("Gossip publication durable invariant violated")
)

// GossipPublicationClaimSpec asks the Store for at most one due publication
// in one exact Channel. The hard one-row bound keeps a worker from retaining
// an unbounded set of durable capabilities while it waits on GossipSub.
type GossipPublicationClaimSpec struct {
	ChannelID  model.ChannelID
	LeaseOwner string
	At         time.Time
	LeaseUntil time.Time
}

// GossipPublicationFence is the complete durable capability needed to settle
// one publish attempt. Attempt advances at claim time and is therefore both
// the attempt counter and the lease generation. RosterHead binds the lease to
// the same authority generation whose topic gate admits Topic.Publish.
type GossipPublicationFence struct {
	EventID    model.EventID
	ChannelID  model.ChannelID
	LeaseOwner string
	Attempt    uint32
	LeaseUntil time.Time
	RosterHead model.RecordHead
}

type GossipPublicationLease struct {
	Record model.PublicationRecord
	Fence  GossipPublicationFence
}

type GossipPublicationClaimResult struct {
	Lease   GossipPublicationLease
	Claimed bool
}

type MarkGossipPublicationPublishedSpec struct {
	Fence GossipPublicationFence
	At    time.Time
}

type RetryGossipPublicationSpec struct {
	Fence         GossipPublicationFence
	At            time.Time
	NextAttemptAt time.Time
	Diagnostic    string
}

type GossipPublicationSettlement struct {
	Record   model.PublicationRecord
	Changed  bool
	Replayed bool
}

type storedGossipPublication struct {
	Record    model.PublicationRecord
	Fence     GossipPublicationFence
	FenceJSON model.JSON
	HasFence  bool
}

// MarkGossipPublicationPublished records only the local Gossip router accept
// linearization point. It deliberately does not mutate delivery, cursor,
// Inbox, Artifact or semantic state.
func (s *Store) MarkGossipPublicationPublished(ctx context.Context,
	spec MarkGossipPublicationPublishedSpec,
) (GossipPublicationSettlement, error) {
	at, expectedFence, err := validateGossipPublicationSettlementInput(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("mark Gossip publication published: begin: %w", err)
	}
	defer tx.Rollback()
	stored, err := readGossipPublication(ctx, tx, spec.Fence.EventID)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	record := stored.Record
	if publishedAt, ok := record.PublishedAt(); ok && record.Status() == model.PublicationPublished &&
		stored.HasFence && bytes.Equal(stored.FenceJSON.Bytes(), expectedFence.Bytes()) &&
		record.Attempts() == spec.Fence.Attempt && publishedAt.Equal(at) && record.UpdatedAt().Equal(at) {
		if err := tx.Commit(); err != nil {
			return GossipPublicationSettlement{}, fmt.Errorf("mark Gossip publication published: commit replay: %w", err)
		}
		return GossipPublicationSettlement{Record: record, Replayed: true}, nil
	}
	node, authority, err := requireCurrentGossipPublicationFence(ctx, tx, spec.Fence,
		expectedFence, stored, at)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	if err := validateGossipPublicationAuthority(node, authority, record.Publication()); err != nil {
		return GossipPublicationSettlement{}, err
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE gossip_publications SET status='published',
		lease_owner=NULL,lease_until=NULL,published_at=?,last_error=NULL,updated_at=?
		WHERE event_id=? AND channel_id=? AND status='leased' AND attempts=?
		AND lease_owner=? AND lease_until=? AND lease_fence_json=?`, storeTime(at), storeTime(at),
		spec.Fence.EventID.String(), spec.Fence.ChannelID.String(), spec.Fence.Attempt,
		spec.Fence.LeaseOwner, storeTime(spec.Fence.LeaseUntil), expectedFence.Bytes())
	if err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("mark Gossip publication published: update: %w", err)
	}
	if exactlyOne(mutation) != nil {
		return GossipPublicationSettlement{}, ErrGossipPublicationStale
	}
	settled, err := readGossipPublication(ctx, tx, spec.Fence.EventID)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	if err := tx.Commit(); err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("mark Gossip publication published: commit: %w", err)
	}
	return GossipPublicationSettlement{Record: settled.Record, Changed: true}, nil
}

// RetryGossipPublication retires one exact lease and applies the caller's
// canonical durable backoff. Attempt was already advanced by claim; retaining
// it makes a later claim the next generation and fences delayed callbacks.
func (s *Store) RetryGossipPublication(ctx context.Context,
	spec RetryGossipPublicationSpec,
) (GossipPublicationSettlement, error) {
	at, expectedFence, err := validateGossipPublicationSettlementInput(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	nextAttempt, err := canonicalStoreTime(spec.NextAttemptAt)
	if err != nil || nextAttempt.Before(at) || !validPublicationDiagnostic(spec.Diagnostic) ||
		spec.Diagnostic == "" {
		return GossipPublicationSettlement{}, ErrGossipPublicationInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("retry Gossip publication: begin: %w", err)
	}
	defer tx.Rollback()
	stored, err := readGossipPublication(ctx, tx, spec.Fence.EventID)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	record := stored.Record
	if record.Status() == model.PublicationQueued && record.Attempts() == spec.Fence.Attempt &&
		stored.HasFence && bytes.Equal(stored.FenceJSON.Bytes(), expectedFence.Bytes()) &&
		record.NextAttemptAt().Equal(nextAttempt) && record.UpdatedAt().Equal(at) &&
		record.LastError() == spec.Diagnostic {
		if err := tx.Commit(); err != nil {
			return GossipPublicationSettlement{}, fmt.Errorf("retry Gossip publication: commit replay: %w", err)
		}
		return GossipPublicationSettlement{Record: record, Replayed: true}, nil
	}
	node, authority, err := requireCurrentGossipPublicationFence(ctx, tx, spec.Fence,
		expectedFence, stored, at)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	if err := validateGossipPublicationAuthority(node, authority, record.Publication()); err != nil {
		return GossipPublicationSettlement{}, err
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE gossip_publications SET status='queued',
		lease_owner=NULL,lease_until=NULL,next_attempt_at=?,published_at=NULL,last_error=?,updated_at=?
		WHERE event_id=? AND channel_id=? AND status='leased' AND attempts=?
		AND lease_owner=? AND lease_until=? AND lease_fence_json=?`, storeTime(nextAttempt), spec.Diagnostic, storeTime(at),
		spec.Fence.EventID.String(), spec.Fence.ChannelID.String(), spec.Fence.Attempt,
		spec.Fence.LeaseOwner, storeTime(spec.Fence.LeaseUntil), expectedFence.Bytes())
	if err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("retry Gossip publication: update: %w", err)
	}
	if exactlyOne(mutation) != nil {
		return GossipPublicationSettlement{}, ErrGossipPublicationStale
	}
	settled, err := readGossipPublication(ctx, tx, spec.Fence.EventID)
	if err != nil {
		return GossipPublicationSettlement{}, err
	}
	if err := tx.Commit(); err != nil {
		return GossipPublicationSettlement{}, fmt.Errorf("retry Gossip publication: commit: %w", err)
	}
	return GossipPublicationSettlement{Record: settled.Record, Changed: true}, nil
}

func validateGossipPublicationSettlementInput(s *Store, ctx context.Context,
	fence GossipPublicationFence, atValue time.Time,
) (time.Time, model.JSON, error) {
	if s == nil || s.db == nil || ctx == nil || fence.EventID.IsZero() || fence.ChannelID.IsZero() ||
		!validPublicationIdentifier(fence.LeaseOwner) || fence.Attempt == 0 || fence.RosterHead.IsZero() {
		return time.Time{}, model.JSON{}, ErrGossipPublicationInput
	}
	leaseUntil, err := canonicalStoreTime(fence.LeaseUntil)
	if err != nil {
		return time.Time{}, model.JSON{}, ErrGossipPublicationInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || !at.Before(leaseUntil) {
		return time.Time{}, model.JSON{}, ErrGossipPublicationStale
	}
	fence.LeaseUntil = leaseUntil
	canonicalFence, err := canonicalGossipPublicationFence(fence)
	if err != nil {
		return time.Time{}, model.JSON{}, err
	}
	return at, canonicalFence, nil
}

func requireCurrentGossipPublicationFence(ctx context.Context, tx *sql.Tx,
	fence GossipPublicationFence, expectedFence model.JSON, stored storedGossipPublication, at time.Time,
) (model.Node, verifiedChannelAuthority, error) {
	record := stored.Record
	if record.Publication().Event().ID() != fence.EventID ||
		record.Publication().Key().ChannelID() != fence.ChannelID ||
		record.Status() != model.PublicationLeased || record.LeaseOwner() != fence.LeaseOwner ||
		record.Attempts() != fence.Attempt || !stored.HasFence ||
		!bytes.Equal(stored.FenceJSON.Bytes(), expectedFence.Bytes()) || at.Before(record.UpdatedAt()) {
		return model.Node{}, verifiedChannelAuthority{}, ErrGossipPublicationStale
	}
	leaseUntil, ok := record.LeaseUntil()
	if !ok || !leaseUntil.Equal(fence.LeaseUntil) || !at.Before(leaseUntil) {
		return model.Node{}, verifiedChannelAuthority{}, ErrGossipPublicationStale
	}
	node, authority, err := readGossipPublicationAuthority(ctx, tx, fence.ChannelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, err
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined ||
		authority.channel.RosterHead() != fence.RosterHead {
		return model.Node{}, verifiedChannelAuthority{}, ErrGossipPublicationStale
	}
	return node, authority, nil
}

func readGossipPublication(ctx context.Context, q rowQuerier,
	eventID model.EventID,
) (storedGossipPublication, error) {
	if ctx == nil || q == nil || eventID.IsZero() {
		return storedGossipPublication{}, ErrGossipPublicationInput
	}
	var eventText, channelText, peerText, epochText, statusText string
	var nextText, createdText, updatedText, eventCreatedText, acceptedText, sourceText string
	var attempts, channelSequence int64
	var leaseOwner, leaseText, publishedText, lastError sql.NullString
	var eventRaw, eventDigestRaw, bodyRaw, publicationDigestRaw, signature, fenceRaw []byte
	err := q.QueryRowContext(ctx, `SELECT p.event_id,p.channel_id,p.origin_peer_id,p.origin_epoch,
		p.channel_seq,p.status,p.attempts,p.next_attempt_at,p.lease_owner,p.lease_until,p.lease_fence_json,p.published_at,p.last_error,
		p.created_at,p.updated_at,e.canonical_event_json,e.event_digest,e.canonical_publication_json,
		e.publication_digest,e.origin_signature,e.created_at,e.accepted_at,e.source
		FROM gossip_publications p JOIN events e ON e.event_id=p.event_id WHERE p.event_id=?`,
		eventID.String()).Scan(&eventText, &channelText, &peerText, &epochText, &channelSequence, &statusText, &attempts,
		&nextText, &leaseOwner, &leaseText, &fenceRaw, &publishedText, &lastError, &createdText, &updatedText,
		&eventRaw, &eventDigestRaw, &bodyRaw, &publicationDigestRaw, &signature, &eventCreatedText,
		&acceptedText, &sourceText)
	if err != nil {
		return storedGossipPublication{}, fmt.Errorf("read Gossip publication %q: %w", eventID.String(), err)
	}
	if attempts < 0 || attempts > math.MaxUint32 {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	body, err := model.NewJSON(bodyRaw)
	if err != nil || !bytes.Equal(body.Bytes(), bodyRaw) {
		return storedGossipPublication{}, fmt.Errorf("%w: noncanonical publication body", ErrGossipPublicationInvariant)
	}
	digest, err := model.DigestFromBytes(publicationDigestRaw)
	if err != nil {
		return storedGossipPublication{}, fmt.Errorf("%w: publication digest: %v", ErrGossipPublicationInvariant, err)
	}
	wire, err := model.JSONFrom(struct {
		Publication       model.JSON   `json:"publication"`
		PublicationDigest model.Digest `json:"publication_digest"`
		OriginSignature   []byte       `json:"origin_signature"`
	}{body, digest, signature})
	if err != nil {
		return storedGossipPublication{}, fmt.Errorf("%w: signed publication wire: %v", ErrGossipPublicationInvariant, err)
	}
	publication, err := model.ParseSignedPublication(wire.Bytes())
	if err != nil || publication.Digest() != digest ||
		!bytes.Equal(publication.CanonicalJSON().Bytes(), bodyRaw) ||
		!bytes.Equal(publication.OriginSignature(), signature) ||
		!bytes.Equal(publication.Event().CanonicalJSON().Bytes(), eventRaw) ||
		!bytes.Equal(publication.Event().Digest().Bytes(), eventDigestRaw) {
		return storedGossipPublication{}, fmt.Errorf("%w: signed publication projection mismatch: %v",
			ErrGossipPublicationInvariant, err)
	}
	scope := publication.Event().Scope()
	if eventText != eventID.String() || eventText != publication.Event().ID().String() ||
		channelText != scope.ChannelID().String() || peerText != scope.OriginPeerID().String() ||
		epochText != scope.OriginEpoch().String() || channelSequence <= 0 ||
		uint64(channelSequence) != scope.ChannelSequence() || sourceText != string(model.EventSourceLocal) ||
		eventCreatedText != storeTime(publication.Event().CreatedAt()) ||
		acceptedText != storeTime(publication.Event().AcceptedAt()) {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	nextAttempt, err := parseCanonicalStoreTime(nextText)
	if err != nil {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	createdAt, err := parseCanonicalStoreTime(createdText)
	if err != nil || !createdAt.Equal(publication.Event().AcceptedAt()) {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	leaseUntil, err := parseOptionalStoreTime(leaseText)
	if err != nil {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	publishedAt, err := parseOptionalStoreTime(publishedText)
	if err != nil {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	record, err := model.NewPublicationRecord(model.PublicationRecordSpec{Publication: publication,
		Status: model.PublicationStatus(statusText), Attempts: uint32(attempts), NextAttemptAt: nextAttempt,
		LeaseOwner: leaseOwner.String, LeaseUntil: leaseUntil, PublishedAt: publishedAt,
		LastError: lastError.String, CreatedAt: createdAt, UpdatedAt: updatedAt})
	if err != nil {
		return storedGossipPublication{}, fmt.Errorf("%w: %v", ErrGossipPublicationInvariant, err)
	}
	result := storedGossipPublication{Record: record}
	if len(fenceRaw) == 0 {
		if record.Attempts() != 0 {
			return storedGossipPublication{}, ErrGossipPublicationInvariant
		}
		return result, nil
	}
	fence, fenceJSON, err := parseGossipPublicationFence(fenceRaw)
	if err != nil || record.Attempts() == 0 || fence.EventID != publication.Event().ID() ||
		fence.ChannelID != publication.Key().ChannelID() || fence.Attempt != record.Attempts() {
		return storedGossipPublication{}, fmt.Errorf("%w: durable lease fence mismatch: %v",
			ErrGossipPublicationInvariant, err)
	}
	if record.Status() == model.PublicationLeased {
		leaseUntil, hasLease := record.LeaseUntil()
		if !hasLease || fence.LeaseOwner != record.LeaseOwner() || !fence.LeaseUntil.Equal(leaseUntil) {
			return storedGossipPublication{}, ErrGossipPublicationInvariant
		}
	}
	result.Fence, result.FenceJSON, result.HasFence = fence, fenceJSON, true
	return result, nil
}

func validPublicationIdentifier(value string) bool {
	if value == "" || len(value) > model.MaxIdentifierBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validPublicationDiagnostic(value string) bool {
	if len(value) > model.MaxContentBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == 0 || (character < 0x20 && character != '\n' && character != '\t') {
			return false
		}
	}
	return true
}
